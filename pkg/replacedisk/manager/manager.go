package manager

import (
	"github.com/hwameistor/improved-system/pkg/apis"
	"github.com/hwameistor/improved-system/pkg/apis/hwameistor/v1alpha1"
	"github.com/hwameistor/improved-system/pkg/utils"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/hwameistor/improved-system/pkg/common"
	log "github.com/sirupsen/logrus"
)

// Infinitely retry
const maxRetries = 0

type manager struct {
	nodeName string

	namespace string

	apiClient client.Client

	replaceDiskTaskQueue *common.TaskQueue

	logger *log.Entry
}

func (m manager) Run(stopCh <-chan struct{}) {
	go m.startReplaceDiskTaskWorker(stopCh)
}

func (m manager) ReconcileReplaceDisk(replaceDisk *v1alpha1.ReplaceDisk) {
	if replaceDisk.Spec.NodeName == m.nodeName {
		m.replaceDiskTaskQueue.Add(replaceDisk.Namespace + "/" + replaceDisk.Name)
	}
}

// New replacedisk manager
func New(cli client.Client) (apis.ReplaceDiskManager, error) {
	return &manager{
		nodeName:             utils.GetNodeName(),
		namespace:            utils.GetNamespace(),
		apiClient:            cli,
		replaceDiskTaskQueue: common.NewTaskQueue("ReplaceDisk", maxRetries),
		logger:               log.WithField("Module", "ReplaceDisk"),
	}, nil
}
