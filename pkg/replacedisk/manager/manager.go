package manager

import (
	"os"

	"github.com/hwameistor/improved-system/pkg/apis"
	"github.com/hwameistor/improved-system/pkg/apis/hwameistor/v1alpha1"
	"github.com/hwameistor/improved-system/pkg/common"
	"github.com/hwameistor/improved-system/pkg/controller/replacedisk"
	"github.com/hwameistor/improved-system/pkg/exechelper"
	"github.com/hwameistor/improved-system/pkg/exechelper/nsexecutor"
	migratepkg "github.com/hwameistor/improved-system/pkg/migrate"
	"github.com/hwameistor/improved-system/pkg/utils"
	"github.com/hwameistor/local-disk-manager/pkg/localdisk"
	log "github.com/sirupsen/logrus"

	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	mgrpkg "sigs.k8s.io/controller-runtime/pkg/manager"
)

// Infinitely retry
const maxRetries = 0

type lvmExecutor struct {
	cmdExec exechelper.Executor
}

var lvmExecutorInstance *lvmExecutor

func newLVMExecutor() *lvmExecutor {
	if lvmExecutorInstance == nil {
		lvmExecutorInstance = &lvmExecutor{
			cmdExec: nsexecutor.New(),
		}
	}
	return lvmExecutorInstance
}

type manager struct {
	nodeName string

	namespace string

	apiClient client.Client

	replaceDiskTaskQueue *common.TaskQueue

	rdhandler *replacedisk.ReplaceDiskHandler

	migrateCtr migratepkg.Controller

	localDiskController localdisk.Controller

	mgr mgrpkg.Manager

	cmdExec *lvmExecutor

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
	var recorder record.EventRecorder
	// Get a config to talk to the apiserver
	cfg, err := config.GetConfig()

	// Set default manager options
	options := mgrpkg.Options{}

	// Create a new manager to provide shared dependencies and start components
	mgr, err := mgrpkg.New(cfg, options)
	if err != nil {
		log.Error(err, "")
		os.Exit(1)
	}

	return &manager{
		nodeName:             utils.GetNodeName(),
		namespace:            utils.GetNamespace(),
		apiClient:            cli,
		replaceDiskTaskQueue: common.NewTaskQueue("ReplaceDisk", maxRetries),
		rdhandler:            replacedisk.NewReplaceDiskHandler(cli, recorder),
		migrateCtr:           migratepkg.NewController(mgr),
		mgr:                  mgr,
		localDiskController:  localdisk.NewController(mgr),
		cmdExec:              newLVMExecutor(),
		logger:               log.WithField("Module", "ReplaceDisk"),
	}, nil
}
