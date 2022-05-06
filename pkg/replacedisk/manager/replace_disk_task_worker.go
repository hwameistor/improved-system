package manager

import (
	"context"
	errs "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	apisv1alpha1 "github.com/hwameistor/improved-system/pkg/apis/hwameistor/v1alpha1"
	"github.com/hwameistor/improved-system/pkg/utils"
	ldm "github.com/hwameistor/local-disk-manager/pkg/apis/hwameistor/v1alpha1"
	"github.com/hwameistor/local-disk-manager/pkg/controller/localdisk"
	lsapisv1alpha1 "github.com/hwameistor/local-storage/pkg/apis/hwameistor/v1alpha1"
	log "github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (m *manager) startReplaceDiskTaskWorker(stopCh <-chan struct{}) {
	m.logger.Debug("ReplaceDisk Worker is working now")
	go func() {
		for {
			task, shutdown := m.ReplaceDiskNodeManager().ReplaceDiskTaskQueue().Get()
			m.logger.Debugf("startReplaceDiskTaskWorker task = %+v", task)
			if shutdown {
				m.logger.WithFields(log.Fields{"task": task}).Debug("Stop the ReplaceDisk worker")
				break
			}
			if err := m.processReplaceDisk(task); err != nil {
				m.logger.WithFields(log.Fields{"task": task, "error": err.Error()}).Error("Failed to process ReplaceDisk task, retry later")
				m.ReplaceDiskNodeManager().ReplaceDiskTaskQueue().AddRateLimited(task)
			} else {
				m.logger.WithFields(log.Fields{"task": task}).Debug("Completed a ReplaceDisk task.")
				m.ReplaceDiskNodeManager().ReplaceDiskTaskQueue().Forget(task)
			}
			m.ReplaceDiskNodeManager().ReplaceDiskTaskQueue().Done(task)
		}
	}()

	<-stopCh
	m.ReplaceDiskNodeManager().ReplaceDiskTaskQueue().Shutdown()
}

func (m *manager) processReplaceDisk(replaceDiskNameSpacedName string) error {
	m.logger.Debug("processReplaceDisk start ...")
	logCtx := m.logger.WithFields(log.Fields{"ReplaceDisk": replaceDiskNameSpacedName})

	logCtx.Debug("Working on a ReplaceDisk task")
	splitRes := strings.Split(replaceDiskNameSpacedName, "/")
	var nameSpace, replaceDiskName string
	if len(splitRes) >= 2 {
		nameSpace = splitRes[0]
		replaceDiskName = splitRes[1]
	}
	replaceDisk := &apisv1alpha1.ReplaceDisk{}
	if err := m.apiClient.Get(context.TODO(), types.NamespacedName{Namespace: nameSpace, Name: replaceDiskName}, replaceDisk); err != nil {
		if !errors.IsNotFound(err) {
			logCtx.WithError(err).Error("Failed to get ReplaceDisk from cache, retry it later ...")
			return err
		}
		//logCtx.Info("Not found the ReplaceDisk from cache, should be deleted already. err = %v", err)
		fmt.Printf("Not found the ReplaceDisk from cache, should be deleted already. err = %v", err)
		return nil
	}

	rdhandler := m.rdhandler.SetReplaceDisk(*replaceDisk)
	m.rdhandler = rdhandler

	err := m.rdhandler.Refresh()
	if err != nil {
		logCtx.WithError(err).Error("Reconciling Refresh err", err)
		return err
	}

	m.logger.Debugf("Required node name %s, current node name %s.", replaceDisk.Spec.NodeName, m.nodeName)
	if replaceDisk.Spec.NodeName != m.nodeName {
		return nil
	}

	m.logger.Debugf("processReplaceDisk replaceDisk.Status %v.", replaceDisk.Status)
	switch replaceDisk.Status.OldDiskReplaceStatus {
	case apisv1alpha1.ReplaceDisk_Init:
		// do init job
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_WaitDataRepair
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "processReplaceDisk UpdateReplaceDiskStatus ReplaceDisk_WaitDataRepair,ReplaceDisk_Init failed")
				return err
			}
		}
		// update init stage
		err := m.processOldReplaceDiskStatusInit(replaceDisk)
		if err != nil {
			m.logger.Errorf("processOldReplaceDiskStatusInit failed err = %v", err)
			m.rdhandler.SetErrMsg(err.Error())
			return err
		}
		return nil
	case apisv1alpha1.ReplaceDisk_WaitDataRepair:
		err := m.processOldReplaceDiskStatusWaitDataRepair(replaceDisk)
		if err != nil {
			log.Error(err, "processReplaceDisk processOldReplaceDiskStatusWaitDataRepair failed")
			return err
		}
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDataRepair && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_WaitDiskLVMRelease
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				m.rdhandler.SetErrMsg(err.Error())
				log.Error(err, "processReplaceDisk UpdateReplaceDiskStatus ReplaceDisk_WaitDiskLVMRelease,ReplaceDisk_Init failed")
				return err
			}
		}

		return nil
	case apisv1alpha1.ReplaceDisk_WaitDiskLVMRelease:
		err := m.processOldReplaceDiskStatusWaitDiskLVMRelease(replaceDisk)
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if err != nil {
			log.Error(err, "processReplaceDisk processOldReplaceDiskStatusWaitDiskLVMRelease failed")
			return err
		}
		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDiskLVMRelease && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "processReplaceDisk UpdateReplaceDiskStatus ReplaceDisk_DiskLVMReleased,ReplaceDisk_Init failed")
				return err
			}
		}
		return nil
	case apisv1alpha1.ReplaceDisk_DiskLVMReleased:
		if replaceDisk.Spec.ReplaceDiskStage == apisv1alpha1.ReplaceDiskStage_Init || replaceDisk.Spec.ReplaceDiskStage == apisv1alpha1.ReplaceDiskStage_WaitDiskReplaced {
			return m.processOldReplaceDiskStatusDiskLVMReleased(replaceDisk)
		}
	case apisv1alpha1.ReplaceDisk_Failed:
		return m.processOldReplaceDiskStatusFailed(replaceDisk)
	default:
		logCtx.Error("Invalid ReplaceDisk status")
		return errs.New("invalid ReplaceDisk status")
	}

	if replaceDisk.Status.OldDiskReplaceStatus != apisv1alpha1.ReplaceDisk_DiskLVMReleased {
		logCtx.Errorf("Invalid ReplaceDisk OldDiskReplaceStatus,replaceDisk.Status.OldDiskReplaceStatus = %v", replaceDisk.Status.OldDiskReplaceStatus)
		return errs.New("invalid ReplaceDisk OldDiskReplaceStatus")
	}

	switch replaceDisk.Status.NewDiskReplaceStatus {
	case apisv1alpha1.ReplaceDisk_Init:
		// todo create job formating disk
		// 触发ld更新
		err := m.updateLocalDisk(replaceDisk)
		if err != nil {
			log.Error(err, "processReplaceDisk NewDiskReplaceStatus updateLocalDisk failed")
			return err
		}
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DiskLVMReleased && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_WaitDiskLVMRejoin
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "ReplaceDiskStage_WaitSvcRestor UpdateReplaceDiskStatus ReplaceDisk_DiskLVMReleased,ReplaceDisk_WaitDiskLVMRejoin failed")
				return err
			}
		}
		return nil
	case apisv1alpha1.ReplaceDisk_WaitDiskLVMRejoin:
		err := m.processNewReplaceDiskStatusWaitDiskLVMRejoin(replaceDisk)
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if err != nil {
			log.Error(err, "processReplaceDisk processNewReplaceDiskStatusWaitDiskLVMRejoin failed")
			m.rdhandler.SetErrMsg(err.Error())
			return err
		}
		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DiskLVMReleased && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDiskLVMRejoin {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_WaitDataBackup
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "processReplaceDisk UpdateReplaceDiskStatus ReplaceDisk_DiskLVMReleased,ReplaceDisk_WaitDataBackup failed")
				return err
			}
		}

		return nil
	case apisv1alpha1.ReplaceDisk_WaitDataBackup:
		err := m.processNewReplaceDiskStatusWaitDataBackup(replaceDisk)
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if err != nil {
			log.Error(err, "processReplaceDisk processNewReplaceDiskStatusWaitDataBackup failed")
			m.rdhandler.SetErrMsg(err.Error())
			return err
		}
		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DiskLVMReleased && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDataBackup {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DataBackuped
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "processReplaceDisk UpdateReplaceDiskStatus ReplaceDisk_DiskLVMReleased,ReplaceDisk_DataBackuped failed")
				return err
			}
		}

		return nil
	case apisv1alpha1.ReplaceDisk_DataBackuped:
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DiskLVMReleased && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DataBackuped {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Succeed
			if err := m.rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "processReplaceDisk UpdateReplaceDiskStatus ReplaceDisk_DiskLVMReleased,ReplaceDisk_Succeed failed")
				return err
			}
		}
		err := m.processNewReplaceDiskStatusDataBackuped(replaceDisk)
		if err != nil {
			log.Error(err, "processReplaceDisk processNewReplaceDiskStatusDataBackuped failed")
			m.rdhandler.SetErrMsg(err.Error())
			return err
		}
		return nil
	case apisv1alpha1.ReplaceDisk_Succeed:
		return m.processNewReplaceDiskStatusSucceed(replaceDisk)
	case apisv1alpha1.ReplaceDisk_Failed:
		return m.processNewReplaceDiskStatusFailed(replaceDisk)
	default:
		logCtx.Error("Invalid ReplaceDisk status")
	}

	return fmt.Errorf("invalid ReplaceDisk status")
}

func (m *manager) processOldReplaceDiskStatusInit(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	replaceDisk.Spec.ReplaceDiskStage = apisv1alpha1.ReplaceDiskStage_WaitDiskReplaced
	m.rdhandler.SetReplaceDiskStage(replaceDisk.Spec.ReplaceDiskStage)
	return m.rdhandler.UpdateReplaceDiskCR()
}

func (m *manager) processOldReplaceDiskStatusWaitDataRepair(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	m.logger.Debug("processOldReplaceDiskStatusWaitDataRepair start ... ")

	logCtx := m.logger.WithFields(log.Fields{"ReplaceDisk": replaceDisk.Name})

	oldDiskName, err := m.getDiskNameByDiskUUID(replaceDisk.Spec.OldUUID, replaceDisk.Spec.NodeName)
	if err != nil {
		logCtx.Errorf("processOldReplaceDiskStatusWaitDataRepair getDiskNameByDiskUUID failed err = %v", err)
		return err
	}

	localVolumeReplicasMap, err := m.getAllLocalVolumeAndReplicasMapOnDisk(oldDiskName, replaceDisk.Spec.NodeName)
	if err != nil {
		logCtx.Errorf("processOldReplaceDiskStatusWaitDataRepair getAllLocalVolumeAndReplicasMapOnDisk failed err = %v", err)
		return err
	}

	var migrateVolumeNames []string
	var localVolumeList []lsapisv1alpha1.LocalVolume
	for localVolumeName := range localVolumeReplicasMap {
		vol := &lsapisv1alpha1.LocalVolume{}
		if err = m.apiClient.Get(context.TODO(), types.NamespacedName{Name: localVolumeName}, vol); err != nil {
			if !errors.IsNotFound(err) {
				log.Errorf("Failed to get Volume from cache, retry it later ...")
				return err
			}
			log.Printf("Not found the Volume from cache, should be deleted already.")
			return nil
		}
		err = m.createMigrateTaskByLocalVolume(*vol)
		if err != nil {
			log.Error(err, "createMigrateTaskByLocalVolume failed")
			return err
		}
		migrateVolumeNames = append(migrateVolumeNames, vol.Name)
		m.rdhandler.SetMigrateVolumeNames(migrateVolumeNames)
		localVolumeList = append(localVolumeList, *vol)
	}

	err = m.waitMigrateTaskByLocalVolumeDone(localVolumeList)
	m.logger.Debug("processOldReplaceDiskStatusWaitDataRepair end ... ")
	return err
}

func (m *manager) createMigrateTaskByLocalVolume(vol lsapisv1alpha1.LocalVolume) error {
	m.logger.Debug("createMigrateTaskByLocalVolume start ... ")
	localVolumeMigrate, err := m.migrateCtr.ConstructLocalVolumeMigrate(vol)
	if err != nil {
		log.Error(err, "createMigrateTaskByLocalVolume ConstructLocalVolumeMigrate failed")
		return err
	}
	err = m.migrateCtr.CreateLocalVolumeMigrate(*localVolumeMigrate)
	if err != nil {
		log.Error(err, "createMigrateTaskByLocalVolume CreateLocalVolumeMigrate failed")
		return err
	}
	m.logger.Debug("createMigrateTaskByLocalVolume end ... ")
	return nil
}

// 统计数据迁移情况
func (m *manager) waitMigrateTaskByLocalVolumeDone(volList []lsapisv1alpha1.LocalVolume) error {
	m.logger.Debug("waitMigrateTaskByLocalVolumeDone start ... ")
	var wg sync.WaitGroup
	var migrateSucceedVols []string
	var migrateFailedVols []string
	for _, vol := range volList {
		vol := vol
		wg.Add(1)
		go func() {
			for {
				migrateStatus, err := m.migrateCtr.GetLocalVolumeMigrateStatusByLocalVolume(vol)
				if err != nil {
					log.Error(err, "waitMigrateTaskByLocalVolumeDone GetLocalVolumeMigrateStatusByLocalVolume failed")
					return
				}
				if migrateStatus.State == lsapisv1alpha1.OperationStateSubmitted || migrateStatus.State == lsapisv1alpha1.OperationStateInProgress {
					time.Sleep(2 * time.Second)
					continue
				}
				if migrateStatus.State == lsapisv1alpha1.OperationStateAborted || migrateStatus.State == lsapisv1alpha1.OperationStateAborting ||
					migrateStatus.State == lsapisv1alpha1.OperationStateToBeAborted {
					migrateFailedVols = append(migrateFailedVols, vol.Name)
					continue
				}
				migrateSucceedVols = append(migrateSucceedVols, vol.Name)
				break
			}
			wg.Done()
		}()
	}
	wg.Wait()
	err := m.rdhandler.SetMigrateSucceededVolumeNames(migrateSucceedVols).SetMigrateFailededVolumeNames(migrateFailedVols).UpdateReplaceDiskStatus(m.rdhandler.ReplaceDiskStatus())
	if err != nil {
		m.logger.Errorf("waitMigrateTaskByLocalVolumeDone UpdateReplaceDiskStatus failed ... ")
		return err
	}

	m.logger.Debug("waitMigrateTaskByLocalVolumeDone end ... ")
	return nil
}

func (m *manager) getLocalDiskByDiskName(diskName, nodeName string) (ldm.LocalDisk, error) {
	m.logger.Debug("getLocalDiskByDiskName start ... ")
	// replacedDiskName e.g.(/dev/sdb -> sdb)
	var replacedDiskName string
	if strings.HasPrefix(diskName, "/dev") {
		replacedDiskName = strings.Replace(diskName, "/dev/", "", 1)
	}

	// ConvertNodeName e.g.(10.23.10.12 => 10-23-10-12)
	localDiskName := utils.ConvertNodeName(nodeName) + "-" + replacedDiskName
	key := client.ObjectKey{Name: localDiskName, Namespace: ""}
	localDisk, err := m.localDiskController.GetLocalDisk(key)
	if err != nil {
		m.logger.WithError(err).Error("getLocalDiskByDiskName: Failed to GetLocalDisk")
		return localDisk, err
	}
	m.logger.Debug("getLocalDiskByDiskName end ... ")
	return localDisk, nil
}

func (m *manager) processOldReplaceDiskStatusWaitDiskLVMRelease(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	m.logger.Debug("processOldReplaceDiskStatusWaitDiskLVMRelease start ... ")
	diskUuid := replaceDisk.Spec.OldUUID
	nodeName := replaceDisk.Spec.NodeName
	diskName, err := m.getDiskNameByDiskUUID(diskUuid, nodeName)
	if err != nil {
		m.logger.WithError(err).Error("processOldReplaceDiskStatusWaitDiskLVMRelease: Failed to getDiskNameByDiskUUID")
		return err
	}

	localDisk, err := m.getLocalDiskByDiskName(diskName, nodeName)
	if err != nil {
		m.logger.WithError(err).Error("processOldReplaceDiskStatusWaitDiskLVMRelease: Failed to getLocalDiskByDiskName")
		return err
	}

	diskType := localDisk.Spec.DiskAttributes.Type
	volGroupName, err := utils.GetPoolNameAccordingDiskType(diskType)
	if err != nil {
		m.logger.WithError(err).Error("processOldReplaceDiskStatusWaitDiskLVMRelease: Failed to GetPoolNameAccordingDiskType")
		return err
	}

	var options []string
	err = m.cmdExec.vgreduce(volGroupName, diskName, options)
	if err != nil {
		m.logger.WithError(err).Error("processOldReplaceDiskStatusWaitDiskLVMRelease: Failed to vgreduce")
		err1 := m.cmdExec.pvremove(volGroupName, diskName, options)
		if err1 != nil {
			m.logger.WithError(err1).Error("processOldReplaceDiskStatusWaitDiskLVMRelease: Failed to pvremove")
			return err1
		}
		return err
	}
	// todo update ld ldmv1alpha1.LocalDiskReleased status
	key := client.ObjectKey{Name: localDisk.Name, Namespace: ""}
	ldisk, err := m.ldhandler.GetLocalDisk(key)
	if err != nil {
		return err
	}
	m.logger.Debugf("processOldReplaceDiskStatusWaitDiskLVMRelease GetLocalDisk ldisk before = %v", ldisk)

	ldhandler := m.ldhandler.For(*ldisk)
	ldhandler.SetupStatus(ldm.LocalDiskReleased)
	if err := m.ldhandler.UpdateStatus(); err != nil {
		log.WithError(err).Errorf("Update LocalDisk %v status fail", localDisk.Name)
		return err
	}
	ldisk, _ = m.ldhandler.GetLocalDisk(key)
	m.logger.Debugf("processOldReplaceDiskStatusWaitDiskLVMRelease GetLocalDisk ldisk after = %v", ldisk)

	m.logger.Debugf("processOldReplaceDiskStatusWaitDiskLVMRelease end ... ")
	return nil
}

func (m manager) processOldReplaceDiskStatusDiskLVMReleased(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	replaceDisk.Spec.ReplaceDiskStage = apisv1alpha1.ReplaceDiskStage_WaitSvcRestor
	m.rdhandler.SetReplaceDiskStage(replaceDisk.Spec.ReplaceDiskStage)
	return m.rdhandler.UpdateReplaceDiskCR()
}

func (m *manager) processOldReplaceDiskStatusFailed(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

//func (m *manager) processNewReplaceDiskStatusInit(replaceDisk *apisv1alpha1.ReplaceDisk) error {
//	return nil
//}

func (m *manager) processNewReplaceDiskStatusWaitDiskLVMRejoin(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	m.logger.Debug("processNewReplaceDiskStatusWaitDiskLVMRejoin start ... ")
	err := m.waitDiskLVMRejoinDone(replaceDisk)
	if err != nil {
		m.logger.WithError(err).Error("processNewReplaceDiskStatusWaitDiskLVMRejoin: Failed to waitDiskLVMRejoinDone")
		return err
	}
	m.logger.Debug("processNewReplaceDiskStatusWaitDiskLVMRejoin end ... ")

	return nil
}

func (m *manager) waitDiskLVMRejoinDone(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	m.logger.Debug("waitDiskLVMRejoinDone start ... ")

	diskUuid := replaceDisk.Spec.NewUUID
	nodeName := replaceDisk.Spec.NodeName
	diskName, err := m.getDiskNameByDiskUUID(diskUuid, nodeName)
	if err != nil {
		m.logger.WithError(err).Error("waitDiskLVMRejoinDone: Failed to getDiskNameByDiskUUID")
		return err
	}

	for {
		localDisk, err := m.getLocalDiskByDiskName(diskName, nodeName)
		if err != nil {
			m.logger.WithError(err).Error("waitDiskLVMRejoinDone: Failed to getLocalDiskByDiskName")
			return err
		}

		m.logger.WithError(err).Errorf("waitDiskLVMRejoinDone getLocalDiskByDiskName localDisk.Status.State = %v", localDisk.Status.State)

		if localDisk.Status.State == ldm.LocalDiskClaimed {
			break
		}
	}
	m.logger.Debug("waitDiskLVMRejoinDone end ... ")

	return nil
}

func (m *manager) processNewReplaceDiskStatusWaitDataBackup(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	m.logger.Debug("processNewReplaceDiskStatusWaitDataBackup start ... ")

	if replaceDisk == nil {
		return errs.New("processNewReplaceDiskStatusWaitDataBackup replaceDisk is nil")
	}
	migrateLocalVolumes := replaceDisk.Status.MigrateVolumeNames
	var localVolumeList []lsapisv1alpha1.LocalVolume
	for _, localVolumeName := range migrateLocalVolumes {
		vol := &lsapisv1alpha1.LocalVolume{}
		if err := m.apiClient.Get(context.TODO(), types.NamespacedName{Name: localVolumeName}, vol); err != nil {
			if !errors.IsNotFound(err) {
				log.Errorf("Failed to get Volume from cache, retry it later ...")
				return err
			}
			log.Printf("Not found the Volume from cache, should be deleted already.")
			return nil
		}
		err := m.createMigrateTaskByLocalVolume(*vol)
		if err != nil {
			log.Error(err, "processNewReplaceDiskStatusWaitDataBackup: createMigrateTaskByLocalVolume failed")
			return err
		}
		localVolumeList = append(localVolumeList, *vol)
	}

	err := m.waitMigrateTaskByLocalVolumeDone(localVolumeList)
	if err != nil {
		m.logger.WithError(err).Error("processNewReplaceDiskStatusWaitDataBackup: Failed to waitMigrateTaskByLocalVolumeDone")
		return err
	}
	m.logger.Debug("processNewReplaceDiskStatusWaitDataBackup end ... ")

	return nil
}

func (m manager) processNewReplaceDiskStatusDataBackuped(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	replaceDisk.Spec.ReplaceDiskStage = apisv1alpha1.ReplaceDiskStage_Succeed
	m.rdhandler.SetReplaceDiskStage(replaceDisk.Spec.ReplaceDiskStage)
	return m.rdhandler.UpdateReplaceDiskCR()
}

// 统计结果并上报
func (m *manager) processNewReplaceDiskStatusSucceed(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

// 告警
func (m *manager) processNewReplaceDiskStatusFailed(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) getAllLocalVolumeAndReplicasMapOnDisk(diskName, nodeName string) (map[string][]*lsapisv1alpha1.LocalVolumeReplica, error) {
	m.logger.Debug("getAllLocalVolumeAndReplicasMapOnDisk start ... ")

	replicaList := &lsapisv1alpha1.LocalVolumeReplicaList{}
	if err := m.apiClient.List(context.TODO(), replicaList); err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	localVolumeReplicasMap := make(map[string][]*lsapisv1alpha1.LocalVolumeReplica)
	for _, replica := range replicaList.Items {
		if replica.Spec.NodeName != nodeName {
			continue
		}
		for _, dName := range replica.Status.Disks {
			// start with /dev/sdx
			if dName == diskName {
				replicas, ok := localVolumeReplicasMap[replica.Spec.VolumeName]
				if !ok {
					var replicaList []*lsapisv1alpha1.LocalVolumeReplica
					replicaList = append(replicaList, &replica)
					localVolumeReplicasMap[replica.Spec.VolumeName] = replicaList
				} else {
					replicas = append(replicas, &replica)
					localVolumeReplicasMap[replica.Spec.VolumeName] = replicas
				}
			}
		}
	}
	m.logger.Debug("getAllLocalVolumeAndReplicasMapOnDisk end ... ")

	return localVolumeReplicasMap, nil
}

//func (m *manager) getReplicasForVolume(volName string) ([]*lsapisv1alpha1.LocalVolumeReplica, error) {
//	m.logger.Debug("getReplicasForVolume start ... ")
//
//	replicaList := &lsapisv1alpha1.LocalVolumeReplicaList{}
//	if err := m.apiClient.List(context.TODO(), replicaList); err != nil {
//		return nil, err
//	}
//
//	var replicas []*lsapisv1alpha1.LocalVolumeReplica
//	for i := range replicaList.Items {
//		if replicaList.Items[i].Spec.VolumeName == volName {
//			replicas = append(replicas, &replicaList.Items[i])
//		}
//	}
//	m.logger.Debug("getReplicasForVolume end ... ")
//
//	return replicas, nil
//}

//func (m *manager) isLocalVolumeReplicasAllNotReady(volName string) (bool, error) {
//	m.logger.Debug("isLocalVolumeReplicasAllNotReady start ... ")
//
//	volumeRelicas, err := m.getReplicasForVolume(volName)
//	if err != nil {
//		return true, err
//	}
//	notReadyFlag, err := m.isReplicasForVolumeAllNotReady(volumeRelicas)
//	if err != nil {
//		return true, err
//	}
//
//	m.logger.Debug("isLocalVolumeReplicasAllNotReady end ... ")
//	return notReadyFlag, nil
//}

//func (m *manager) isReplicasForVolumeAllNotReady(replicas []*lsapisv1alpha1.LocalVolumeReplica) (bool, error) {
//	m.logger.Debug("isReplicasForVolumeAllNotReady start ... ")
//
//	for _, replica := range replicas {
//		if replica.Status.State == lsapisv1alpha1.VolumeReplicaStateReady {
//			fmt.Print("Not all VolumeReplicas are not ready")
//			return false, nil
//		}
//	}
//	m.logger.Debug("isReplicasForVolumeAllNotReady end ... ")
//
//	return true, nil
//}

//func (m *manager) isReplicasForVolumeAllReady(replicas []*lsapisv1alpha1.LocalVolumeReplica) (bool, error) {
//	m.logger.Debug("isReplicasForVolumeAllReady start ... ")
//
//	for _, replica := range replicas {
//		if replica.Status.State == lsapisv1alpha1.VolumeReplicaStateNotReady {
//			fmt.Print("Not all VolumeReplicas are ready")
//			return false, nil
//		}
//	}
//
//	m.logger.Debug("isReplicasForVolumeAllReady end ... ")
//
//	return true, nil
//}

//func (m *manager) isReplicaOnDiskMaster(replica *lsapisv1alpha1.LocalVolumeReplica) (bool, error) {
//	m.logger.Debug("isReplicaOnDiskMaster start ... ")
//
//	var volName = replica.Spec.VolumeName
//	vol := &lsapisv1alpha1.LocalVolume{}
//	if err := m.apiClient.Get(context.TODO(), types.NamespacedName{Name: volName}, vol); err != nil {
//		if !errors.IsNotFound(err) {
//			m.logger.Errorf("Failed to get Volume from cache, retry it later ...")
//			return false, err
//		}
//		m.logger.Debugf("Not found the Volume from cache, should be deleted already.")
//		return false, nil
//	}
//	if vol.Spec.Config != nil {
//		for _, replicasVal := range vol.Spec.Config.Replicas {
//			if replicasVal.Hostname == replica.Spec.NodeName {
//				isPrimary := replicasVal.Primary
//				return isPrimary, nil
//			}
//		}
//	}
//	m.logger.Debug("isReplicaOnDiskMaster end ... ")
//
//	return false, nil
//}

func (m *manager) CheckVolumeInReplaceDiskTask(nodeName, volName string) (bool, error) {
	m.logger.Debug("CheckVolumeInReplaceDiskTask start ... ")

	allLocalVolumeReplicasMap, err := m.getAllLocalVolumesAndReplicasInRepalceDiskTask(nodeName)
	if err != nil {
		m.logger.WithError(err).Error("Failed to getAllLocalVolumesAndReplicasInRepalceDiskTask")
		return false, err
	}
	if _, ok := allLocalVolumeReplicasMap[volName]; ok {
		return true, nil
	}
	m.logger.Debug("CheckVolumeInReplaceDiskTask end ... ")

	return false, nil
}

//func (m *manager) migrateLocalVolumeReplica(localVolumeReplicaName, nodeName string) error {
//
//	return nil
//}

func (m *manager) getAllLocalVolumesAndReplicasInRepalceDiskTask(nodeName string) (map[string][]*lsapisv1alpha1.LocalVolumeReplica, error) {
	m.logger.Debug("getAllLocalVolumesAndReplicasInRepalceDiskTask start ... ")

	diskNames, err := m.getDiskNamesInRepalceDiskTask(nodeName)
	if err != nil {
		m.logger.WithError(err).Error("Failed to get ReplaceDiskList")
		return nil, err
	}

	var allLocalVolumeReplicasMap = make(map[string][]*lsapisv1alpha1.LocalVolumeReplica)
	for _, diskName := range diskNames {
		localVolumeReplicasMap, err := m.getAllLocalVolumeAndReplicasMapOnDisk(diskName, nodeName)
		if err != nil {
			m.logger.WithError(err).Error("Failed to getAllLocalVolumeAndReplicasMapOnDisk")
			return nil, err
		}
		allLocalVolumeReplicasMap = utils.MergeLocalVolumeReplicaMap(allLocalVolumeReplicasMap, localVolumeReplicasMap)
	}

	m.logger.Debug("getAllLocalVolumesAndReplicasInRepalceDiskTask end ... ")
	return allLocalVolumeReplicasMap, nil
}

func (m *manager) getDiskNamesInRepalceDiskTask(nodeName string) ([]string, error) {
	m.logger.Debug("getDiskNamesInRepalceDiskTask start ... ")

	replaceDiskList, err := m.rdhandler.ListReplaceDisk()
	if err != nil {
		m.logger.WithError(err).Error("Failed to get ReplaceDiskList")
		return nil, err
	}

	var diskNames []string
	for _, replacedisk := range replaceDiskList.Items {
		if replacedisk.Spec.NodeName == nodeName {
			diskUuid := replacedisk.Spec.OldUUID
			diskName, err := m.getDiskNameByDiskUUID(diskUuid, nodeName)
			if err != nil {
				m.logger.WithError(err).Error("Failed to getDiskNameByDiskUUID")
				return nil, err
			}
			diskNames = append(diskNames, diskName)
		}
	}
	m.logger.Debug("getDiskNamesInRepalceDiskTask end ... ")

	return diskNames, nil
}

func (m *manager) CheckLocalDiskInReplaceDiskTaskByUUID(nodeName, diskUuid string) (bool, error) {
	m.logger.Debug("CheckLocalDiskInReplaceDiskTaskByUUID start ... ")

	replaceDiskList, err := m.rdhandler.ListReplaceDisk()
	if err != nil {
		m.logger.WithError(err).Error("Failed to get ReplaceDiskList")
		return false, err
	}

	for _, replacedisk := range replaceDiskList.Items {
		if replacedisk.Spec.NodeName == nodeName {
			oldDiskUuid := replacedisk.Spec.OldUUID
			newDiskUuid := replacedisk.Spec.NewUUID
			if diskUuid == oldDiskUuid || diskUuid == newDiskUuid {
				return true, nil
			}
		}
	}

	m.logger.Debug("CheckLocalDiskInReplaceDiskTaskByUUID end ... ")
	return false, nil
}

// start with /dev/sdx
func (m *manager) getDiskNameByDiskUUID(diskUUID, nodeName string) (string, error) {
	var recorder record.EventRecorder
	ldHandler := localdisk.NewLocalDiskHandler(m.apiClient, recorder)
	ldList, err := ldHandler.ListLocalDisk()
	if err != nil {
		return "", err
	}
	var diskName string
	for _, ld := range ldList.Items {
		if ld.Spec.NodeName == nodeName {
			if ld.Spec.UUID == diskUUID {
				diskName = ld.Spec.DevicePath
				break
			}
		}
	}

	return diskName, nil
}

func (m *manager) updateLocalDisk(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	m.logger.Debug("updateLocalDisk start ... ")
	// replacedDiskName e.g.(/dev/sdb -> sdb)
	diskUuid := replaceDisk.Spec.NewUUID
	nodeName := replaceDisk.Spec.NodeName
	diskName, err := m.getDiskNameByDiskUUID(diskUuid, nodeName)
	if err != nil {
		m.logger.WithError(err).Error("updateLocalDisk: Failed to getDiskNameByDiskUUID")
		return err
	}

	localDisk, err := m.getLocalDiskByDiskName(diskName, nodeName)
	if err != nil {
		m.logger.WithError(err).Error("updateLocalDisk: Failed to getLocalDiskByDiskName")
		return err
	}

	localDisk.Spec.DiskAttributes.Product = "ReplaceDisk"
	err = m.localDiskController.UpdateLocalDisk(localDisk)
	if err != nil {
		m.logger.WithError(err).Error("updateLocalDisk: Failed to UpdateLocalDisk")
		return err
	}

	return nil
}
