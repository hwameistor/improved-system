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
	m.logger.Debug("startReplaceDiskTaskWorker m.replaceDiskTaskQueue = %v", m.ReplaceDiskNodeManager().ReplaceDiskTaskQueue())
	go func() {
		for {
			//m.replaceDiskTaskQueue.Add("hwameistor/replacedisk-sample")
			task, shutdown := m.ReplaceDiskNodeManager().ReplaceDiskTaskQueue().Get()
			m.logger.Debug("startReplaceDiskTaskWorker task = %v", task)
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

	m.logger.Debugf("Required node name %s, current node name %s.", replaceDisk.Spec.NodeName, m.nodeName)
	if replaceDisk.Spec.NodeName != m.nodeName {
		return nil
	}

	m.logger.Debugf("processReplaceDisk replaceDisk.Status %v.", replaceDisk.Status)
	switch replaceDisk.Status.OldDiskReplaceStatus {
	case apisv1alpha1.ReplaceDisk_Init:
		return m.processOldReplaceDiskStatusInit(replaceDisk)
	case apisv1alpha1.ReplaceDisk_WaitDataRepair:
		err := m.processOldReplaceDiskStatusWaitDataRepair(replaceDisk)
		if err != nil {
			replaceDiskStatus := rdhandler.ReplaceDiskStatus()
			if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDataRepair && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init {
				replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
				replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
				if err := m.rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
					log.Error(err, "UpdateReplaceDiskStatus failed")
					return err
				}
			}
		}
		return nil
	case apisv1alpha1.ReplaceDisk_WaitDiskLVMRelease:
		err := m.processOldReplaceDiskStatusWaitDiskLVMRelease(replaceDisk)
		if err != nil {
			replaceDiskStatus := rdhandler.ReplaceDiskStatus()
			if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDiskLVMRelease && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init {
				replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_WaitDataRepair
				replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
				if err := m.rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
					log.Error(err, "UpdateReplaceDiskStatus failed")
					return err
				}
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
		return errs.New("Invalid ReplaceDisk status")
	}

	if replaceDisk.Status.OldDiskReplaceStatus != apisv1alpha1.ReplaceDisk_DiskLVMReleased {
		logCtx.Error("Invalid ReplaceDisk OldDiskReplaceStatus,replaceDisk.Status.OldDiskReplaceStatus = %v", replaceDisk.Status.OldDiskReplaceStatus)
		return errs.New("Invalid ReplaceDisk OldDiskReplaceStatus")
	}

	switch replaceDisk.Status.NewDiskReplaceStatus {
	case apisv1alpha1.ReplaceDisk_Init:
		return m.processNewReplaceDiskStatusInit(replaceDisk)
	case apisv1alpha1.ReplaceDisk_WaitDiskLVMRejoin:
		err := m.processNewReplaceDiskStatusWaitDiskLVMRejoin(replaceDisk)
		if err != nil {
			replaceDiskStatus := rdhandler.ReplaceDiskStatus()
			if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DiskLVMReleased && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDiskLVMRejoin {
				replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
				replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
				if err := m.rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
					log.Error(err, "UpdateReplaceDiskStatus failed")
					return err
				}
			}
		}
		return nil
	case apisv1alpha1.ReplaceDisk_WaitDataBackup:
		err := m.processNewReplaceDiskStatusWaitDataBackup(replaceDisk)
		if err != nil {
			replaceDiskStatus := rdhandler.ReplaceDiskStatus()
			if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DiskLVMReleased && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDataBackup {
				replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
				replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_WaitDiskLVMRejoin
				if err := m.rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
					log.Error(err, "UpdateReplaceDiskStatus failed")
					return err
				}
			}
		}
		return nil
	case apisv1alpha1.ReplaceDisk_DataBackuped:
		return m.processNewReplaceDiskStatusDataBackuped(replaceDisk)
	case apisv1alpha1.ReplaceDisk_Succeed:
		return m.processNewReplaceDiskStatusSucceed(replaceDisk)
	case apisv1alpha1.ReplaceDisk_Failed:
		return m.processNewReplaceDiskStatusFailed(replaceDisk)
	default:
		logCtx.Error("Invalid ReplaceDisk status")
	}

	return fmt.Errorf("Invalid ReplaceDisk status")
}

func (m *manager) processOldReplaceDiskStatusInit(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	replaceDisk.Spec.ReplaceDiskStage = apisv1alpha1.ReplaceDiskStage_WaitDiskReplaced
	m.rdhandler.SetReplaceDiskStage(replaceDisk.Spec.ReplaceDiskStage)
	return m.rdhandler.UpdateReplaceDiskCR()
}

func (m *manager) processOldReplaceDiskStatusWaitDataRepair(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	logCtx := m.logger.WithFields(log.Fields{"ReplaceDisk": replaceDisk.Name})

	oldDiskName, err := m.getDiskNameByDiskUUID(replaceDisk.Spec.OldUUID, replaceDisk.Spec.NodeName)
	if err != nil {
		logCtx.Error("processOldReplaceDiskStatusWaitDataRepair getDiskNameByDiskUUID failed err = %v", err)
		return err
	}

	localVolumeReplicasMap, err := m.getAllLocalVolumeAndReplicasMapOnDisk(oldDiskName, replaceDisk.Spec.NodeName)
	if err != nil {
		logCtx.Error("processOldReplaceDiskStatusWaitDataRepair getAllLocalVolumeAndReplicasMapOnDisk failed err = %v", err)
		return err
	}

	var migrateVolumeNames []string
	var localVolumeList = []lsapisv1alpha1.LocalVolume{}
	for localVolumeName, _ := range localVolumeReplicasMap {
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
	return err
}

func (m *manager) createMigrateTaskByLocalVolume(vol lsapisv1alpha1.LocalVolume) error {
	localVolumeMigrate, err := m.migrateCtr.ConstructLocalVolumeMigrate(vol)
	err = m.migrateCtr.CreateLocalVolumeMigrate(*localVolumeMigrate)
	if err != nil {
		log.Error(err, "CreateLocalVolumeMigrate failed")
		return err
	}
	return nil
}

// 统计数据迁移情况
func (m *manager) waitMigrateTaskByLocalVolumeDone(volList []lsapisv1alpha1.LocalVolume) error {
	var wg sync.WaitGroup
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
				break
			}
			wg.Done()
			return
		}()
	}
	wg.Wait()

	return nil
}

func (m *manager) getLocalDiskByDiskName(diskName, nodeName string) (ldm.LocalDisk, error) {
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
	return localDisk, nil
}

func (m *manager) processOldReplaceDiskStatusWaitDiskLVMRelease(replaceDisk *apisv1alpha1.ReplaceDisk) error {
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

	options := []string{}
	err = m.cmdExec.vgreduce(volGroupName, diskName, options)
	if err != nil {
		m.logger.WithError(err).Error("processOldReplaceDiskStatusWaitDiskLVMRelease: Failed to vgreduce")
		return err
	}
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

func (m *manager) processNewReplaceDiskStatusInit(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processNewReplaceDiskStatusWaitDiskLVMRejoin(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	err := m.waitDiskLVMRejoinDone(replaceDisk)
	if err != nil {
		m.logger.WithError(err).Error("processNewReplaceDiskStatusWaitDiskLVMRejoin: Failed to waitDiskLVMRejoinDone")
		return err
	}
	return nil
}

func (m *manager) waitDiskLVMRejoinDone(replaceDisk *apisv1alpha1.ReplaceDisk) error {
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
		if localDisk.Spec.ClaimRef != nil {
			break
		}
	}
	return nil
}

func (m *manager) processNewReplaceDiskStatusWaitDataBackup(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	if replaceDisk == nil {
		return errs.New("processNewReplaceDiskStatusWaitDataBackup replaceDisk is nil")
	}
	migrateLocalVolumes := replaceDisk.Status.MigrateVolumeNames
	var localVolumeList = []lsapisv1alpha1.LocalVolume{}
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
	return nil
}

func (m manager) processNewReplaceDiskStatusDataBackuped(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	replaceDisk.Spec.ReplaceDiskStage = apisv1alpha1.ReplaceDiskStage_Succeed
	m.rdhandler.SetReplaceDiskStage(replaceDisk.Spec.ReplaceDiskStage)
	return m.rdhandler.UpdateReplaceDiskCR()
}

func (m *manager) processNewReplaceDiskStatusSucceed(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processNewReplaceDiskStatusFailed(replaceDisk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) getAllLocalVolumeAndReplicasMapOnDisk(diskName, nodeName string) (map[string][]*lsapisv1alpha1.LocalVolumeReplica, error) {
	m.logger.WithField("node", nodeName).Debug("Start to getAllLocalVolumeReplicasOnDisk")
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
					replicaList := []*lsapisv1alpha1.LocalVolumeReplica{}
					replicaList = append(replicaList, &replica)
					localVolumeReplicasMap[replica.Spec.VolumeName] = replicaList
				} else {
					replicas = append(replicas, &replica)
					localVolumeReplicasMap[replica.Spec.VolumeName] = replicas
				}
			}
		}
	}

	return localVolumeReplicasMap, nil
}

func (m *manager) getReplicasForVolume(volName string) ([]*lsapisv1alpha1.LocalVolumeReplica, error) {
	replicaList := &lsapisv1alpha1.LocalVolumeReplicaList{}
	if err := m.apiClient.List(context.TODO(), replicaList); err != nil {
		return nil, err
	}

	var replicas []*lsapisv1alpha1.LocalVolumeReplica
	for i := range replicaList.Items {
		if replicaList.Items[i].Spec.VolumeName == volName {
			replicas = append(replicas, &replicaList.Items[i])
		}
	}
	return replicas, nil
}

func (m *manager) isLocalVolumeReplicasAllNotReady(volName string) (bool, error) {
	volumeRelicas, err := m.getReplicasForVolume(volName)
	if err != nil {
		return true, err
	}
	notReadyFlag, err := m.isReplicasForVolumeAllNotReady(volumeRelicas)
	if err != nil {
		return true, err
	}

	return notReadyFlag, nil
}

func (m *manager) isReplicasForVolumeAllNotReady(replicas []*lsapisv1alpha1.LocalVolumeReplica) (bool, error) {
	for _, replica := range replicas {
		if replica.Status.State == lsapisv1alpha1.VolumeReplicaStateReady {
			fmt.Print("Not all VolumeReplicas are not ready")
			return false, nil
		}
	}

	return true, nil
}

func (m *manager) isReplicasForVolumeAllReady(replicas []*lsapisv1alpha1.LocalVolumeReplica) (bool, error) {
	for _, replica := range replicas {
		if replica.Status.State == lsapisv1alpha1.VolumeReplicaStateNotReady {
			fmt.Print("Not all VolumeReplicas are ready")
			return false, nil
		}
	}

	return true, nil
}

func (m *manager) isReplicaOnDiskMaster(replica *lsapisv1alpha1.LocalVolumeReplica) (bool, error) {

	var volName = replica.Spec.VolumeName
	vol := &lsapisv1alpha1.LocalVolume{}
	if err := m.apiClient.Get(context.TODO(), types.NamespacedName{Name: volName}, vol); err != nil {
		if !errors.IsNotFound(err) {
			fmt.Errorf("Failed to get Volume from cache, retry it later ...")
			return false, err
		}
		fmt.Printf("Not found the Volume from cache, should be deleted already.")
		return false, nil
	}
	if vol.Spec.Config != nil {
		for _, replicasVal := range vol.Spec.Config.Replicas {
			if replicasVal.Hostname == replica.Spec.NodeName {
				isPrimary := replicasVal.Primary
				return isPrimary, nil
			}
		}
	}
	return false, nil
}

func (m *manager) CheckVolumeInReplaceDiskTask(nodeName, volName string) (bool, error) {
	allLocalVolumeReplicasMap, err := m.getAllLocalVolumesAndReplicasInRepalceDiskTask(nodeName)
	if err != nil {
		m.logger.WithError(err).Error("Failed to getAllLocalVolumesAndReplicasInRepalceDiskTask")
		return false, err
	}
	if _, ok := allLocalVolumeReplicasMap[volName]; ok {
		return true, nil
	}
	return false, nil
}

func (m *manager) migrateLocalVolumeReplica(localVolumeReplicaName, nodeName string) error {

	return nil
}

func (m *manager) getAllLocalVolumesAndReplicasInRepalceDiskTask(nodeName string) (map[string][]*lsapisv1alpha1.LocalVolumeReplica, error) {

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

	return allLocalVolumeReplicasMap, nil
}

func (m *manager) getDiskNamesInRepalceDiskTask(nodeName string) ([]string, error) {
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

	return diskNames, nil
}

func (m *manager) CheckLocalDiskInReplaceDiskTaskByUUID(nodeName, diskUuid string) (bool, error) {
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
