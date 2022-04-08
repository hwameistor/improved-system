package manager

import (
	"context"
	"fmt"
	apisv1alpha1 "github.com/hwameistor/improved-system/pkg/apis/hwameistor/v1alpha1"
	lsapisv1alpha1 "github.com/hwameistor/local-storage/pkg/apis/hwameistor/v1alpha1"

	log "github.com/sirupsen/logrus"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

func (m *manager) startReplaceDiskTaskWorker(stopCh <-chan struct{}) {

	m.logger.Debug("ReplaceDisk Worker is working now")
	go func() {
		for {
			task, shutdown := m.replaceDiskTaskQueue.Get()
			if shutdown {
				m.logger.WithFields(log.Fields{"task": task}).Debug("Stop the ReplaceDisk worker")
				break
			}
			if err := m.processReplaceDisk(task); err != nil {
				m.logger.WithFields(log.Fields{"task": task, "error": err.Error()}).Error("Failed to process ReplaceDisk task, retry later")
				m.replaceDiskTaskQueue.AddRateLimited(task)
			} else {
				m.logger.WithFields(log.Fields{"task": task}).Debug("Completed a ReplaceDisk task.")
				m.replaceDiskTaskQueue.Forget(task)
			}
			m.replaceDiskTaskQueue.Done(task)
		}
	}()

	<-stopCh
	m.replaceDiskTaskQueue.Shutdown()
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

	m.logger.Debugf("Required node name %s, current node name %s.", replaceDisk.Spec.NodeName, m.nodeName)
	if replaceDisk.Spec.NodeName != m.nodeName {
		return nil
	}

	switch replaceDisk.Spec.ReplaceDiskStage {
	case "":
		return m.processReplaceDiskSubmit(replaceDisk)
	case apisv1alpha1.ReplaceDiskStage_Init:
		return m.processReplaceDiskInit(replaceDisk)
	case apisv1alpha1.ReplaceDiskStage_WaitDiskReplaced:
		return m.processReplaceDiskWaitDiskReplaced(replaceDisk)
	case apisv1alpha1.ReplaceDiskStage_WaitSvcRestor:
		return m.processReplaceDiskWaitSvcRestor(replaceDisk)
	case apisv1alpha1.ReplaceDiskStage_Succeed:
		return m.processReplaceDiskSucceed(replaceDisk)
	case apisv1alpha1.ReplaceDiskStage_Failed:
		return m.processReplaceDiskFailed(replaceDisk)
	default:
		logCtx.Error("Invalid ReplaceDisk stage")
	}

	switch replaceDisk.Status.OldDiskReplaceStatus {
	case apisv1alpha1.ReplaceDisk_Init:
		return m.processOldReplaceDiskInit(replaceDisk)
	case apisv1alpha1.ReplaceDisk_WaitDataRepair:
		return m.processOldReplaceDiskWaitDataRepair(replaceDisk)
	case apisv1alpha1.ReplaceDisk_WaitDiskLVMRelease:
		return m.processOldReplaceDiskWaitDiskLVMRelease(replaceDisk)
	case apisv1alpha1.ReplaceDisk_Succeed:
		return m.processOldReplaceDiskSucceed(replaceDisk)
	case apisv1alpha1.ReplaceDisk_Failed:
		return m.processOldReplaceDiskFailed(replaceDisk)
	default:
		logCtx.Error("Invalid ReplaceDisk status")
	}

	switch replaceDisk.Status.NewDiskReplaceStatus {
	case apisv1alpha1.ReplaceDisk_Init:
		return m.processNewReplaceDiskInit(replaceDisk)
	case apisv1alpha1.ReplaceDisk_WaitDiskLVMRejoin:
		return m.processNewReplaceDiskWaitDiskLVMRejoin(replaceDisk)
	case apisv1alpha1.ReplaceDisk_WaitDataBackup:
		return m.processNewReplaceDiskWaitDataBackup(replaceDisk)
	case apisv1alpha1.ReplaceDisk_Succeed:
		return m.processNewReplaceDiskSucceed(replaceDisk)
	case apisv1alpha1.ReplaceDisk_Failed:
		return m.processNewReplaceDiskFailed(replaceDisk)
	default:
		logCtx.Error("Invalid ReplaceDisk status")
	}

	return fmt.Errorf("Invalid ReplaceDisk status")
}

func (m *manager) processReplaceDiskSubmit(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processReplaceDiskInit(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processReplaceDiskWaitDiskReplaced(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processReplaceDiskWaitSvcRestor(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processReplaceDiskSucceed(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processReplaceDiskFailed(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processOldReplaceDiskInit(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processOldReplaceDiskWaitDataRepair(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processOldReplaceDiskWaitDiskLVMRelease(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processOldReplaceDiskSucceed(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processOldReplaceDiskFailed(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processNewReplaceDiskInit(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processNewReplaceDiskWaitDiskLVMRejoin(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processNewReplaceDiskWaitDataBackup(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processNewReplaceDiskSucceed(disk *apisv1alpha1.ReplaceDisk) error {
	return nil
}

func (m *manager) processNewReplaceDiskFailed(disk *apisv1alpha1.ReplaceDisk) error {
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

	existingLocalVolumeReplicasMap := make(map[string][]*lsapisv1alpha1.LocalVolumeReplica)
	for _, replica := range replicaList.Items {
		if replica.Spec.NodeName != nodeName {
			continue
		}
		for _, dName := range replica.Status.Disks {
			// start with /dev/sdx
			if dName == diskName {
				replicas, ok := existingLocalVolumeReplicasMap[replica.Spec.VolumeName]
				if !ok {
					replicaList := []*lsapisv1alpha1.LocalVolumeReplica{}
					replicaList = append(replicaList, &replica)
					existingLocalVolumeReplicasMap[replica.Spec.VolumeName] = replicaList
				} else {
					replicas = append(replicas, &replica)
					existingLocalVolumeReplicasMap[replica.Spec.VolumeName] = replicas
				}
			}
		}
	}

	return existingLocalVolumeReplicasMap, nil
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

func (m *manager) migrateLocalVolumeReplica(localVolumeReplicaName, nodeName string) error {

	return nil
}
