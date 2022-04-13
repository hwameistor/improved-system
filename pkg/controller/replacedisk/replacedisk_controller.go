package replacedisk

import (
	"context"
	"github.com/hwameistor/improved-system/pkg/apis"
	apisv1alpha1 "github.com/hwameistor/improved-system/pkg/apis/hwameistor/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_replacedisk")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new ReplaceDisk Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileReplaceDisk{client: mgr.GetClient(), scheme: mgr.GetScheme(), Recorder: mgr.GetEventRecorderFor("replacedisk-controller")}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("replacedisk-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource ReplaceDisk
	err = c.Watch(&source.Kind{Type: &apisv1alpha1.ReplaceDisk{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner ReplaceDisk
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &apisv1alpha1.ReplaceDisk{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileReplaceDisk implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileReplaceDisk{}

// ReconcileReplaceDisk reconciles a ReplaceDisk object
type ReconcileReplaceDisk struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client             client.Client
	scheme             *runtime.Scheme
	Recorder           record.EventRecorder
	replaceDiskManager apis.ReplaceDiskManager
}

// Reconcile reads that state of the cluster for a ReplaceDisk object and makes changes based on the state read
// and what is in the ReplaceDisk.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileReplaceDisk) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling ReplaceDisk")

	// Fetch the ReplaceDisk instance
	replaceDisk := &apisv1alpha1.ReplaceDisk{}
	err := r.client.Get(context.TODO(), request.NamespacedName, replaceDisk)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	rdhandler := NewReplaceDiskHandler(r.client, r.Recorder)

	switch replaceDisk.Spec.ReplaceDiskStage {
	case "":
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if replaceDiskStatus.OldDiskReplaceStatus == "" && replaceDiskStatus.NewDiskReplaceStatus == "" {
			rdhandler.UpdateReplaceDiskStage(apisv1alpha1.ReplaceDiskStage_Init)
		}
		return reconcile.Result{Requeue: true}, nil
	case apisv1alpha1.ReplaceDiskStage_Init:
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if replaceDiskStatus.OldDiskReplaceStatus == "" && replaceDiskStatus.NewDiskReplaceStatus == "" {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "UpdateReplaceDiskStatus failed")
				return reconcile.Result{Requeue: true}, nil
			}
		}
	case apisv1alpha1.ReplaceDiskStage_WaitDiskReplaced:
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_WaitDataRepair
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "UpdateReplaceDiskStatus failed")
				return reconcile.Result{Requeue: true}, nil
			}
		}

		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDataRepair && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_WaitDiskLVMRelease
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "UpdateReplaceDiskStatus failed")
				return reconcile.Result{Requeue: true}, nil
			}
		}

		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDiskLVMRelease && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "UpdateReplaceDiskStatus failed")
				return reconcile.Result{Requeue: true}, nil
			}
		}

	case apisv1alpha1.ReplaceDiskStage_WaitSvcRestor:
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DiskLVMReleased && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_Init {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_WaitDiskLVMRejoin
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "UpdateReplaceDiskStatus failed")
				return reconcile.Result{Requeue: true}, nil
			}
		}

		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DiskLVMReleased && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDiskLVMRejoin {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_WaitDataBackup
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "UpdateReplaceDiskStatus failed")
				return reconcile.Result{Requeue: true}, nil
			}
		}

		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DiskLVMReleased && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_WaitDataBackup {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DataBackuped
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "UpdateReplaceDiskStatus failed")
				return reconcile.Result{Requeue: true}, nil
			}
		}

	case apisv1alpha1.ReplaceDiskStage_Succeed:
		replaceDiskStatus := rdhandler.ReplaceDiskStatus()
		if replaceDiskStatus.OldDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DiskLVMReleased && replaceDiskStatus.NewDiskReplaceStatus == apisv1alpha1.ReplaceDisk_DataBackuped {
			replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_DiskLVMReleased
			replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Succeed
			if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
				log.Error(err, "UpdateReplaceDiskStatus failed")
				return reconcile.Result{Requeue: true}, nil
			}
		}

	case apisv1alpha1.ReplaceDiskStage_Failed:
		return reconcile.Result{Requeue: true}, nil

	default:
		reqLogger.Error(err, "Invalid ReplaceDisk stage")
	}

	r.replaceDiskManager.ReconcileReplaceDisk(&rdhandler.rd)

	return reconcile.Result{}, nil
}

// ReplaceDiskHandler
type ReplaceDiskHandler struct {
	client.Client
	record.EventRecorder
	rd apisv1alpha1.ReplaceDisk
}

// NewReplaceDiskHandler
func NewReplaceDiskHandler(client client.Client, recorder record.EventRecorder) *ReplaceDiskHandler {
	return &ReplaceDiskHandler{
		Client:        client,
		EventRecorder: recorder,
	}
}

// ListReplaceDisk
func (rdHandler *ReplaceDiskHandler) ListReplaceDisk() (*apisv1alpha1.ReplaceDiskList, error) {
	list := &apisv1alpha1.ReplaceDiskList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplaceDisk",
			APIVersion: "v1alpha1",
		},
	}

	err := rdHandler.List(context.TODO(), list)
	return list, err
}

// GetReplaceDisk
func (rdHandler *ReplaceDiskHandler) GetReplaceDisk(key client.ObjectKey) (*apisv1alpha1.ReplaceDisk, error) {
	ldc := &apisv1alpha1.ReplaceDisk{}
	if err := rdHandler.Get(context.Background(), key, ldc); err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	return ldc, nil
}

// UpdateReplaceDiskStatus
func (rdHandler *ReplaceDiskHandler) UpdateReplaceDiskStatus(status apisv1alpha1.ReplaceDiskStatus) error {
	rdHandler.rd.Status.OldDiskReplaceStatus = status.OldDiskReplaceStatus
	rdHandler.rd.Status.NewDiskReplaceStatus = status.NewDiskReplaceStatus
	return rdHandler.Status().Update(context.Background(), &rdHandler.rd)
}

// Refresh
func (rdHandler *ReplaceDiskHandler) Refresh() error {
	rd, err := rdHandler.GetReplaceDisk(client.ObjectKey{Name: rdHandler.rd.GetName(), Namespace: rdHandler.rd.GetNamespace()})
	if err != nil {
		return err
	}
	rdHandler.SetReplaceDisk(*rd.DeepCopy())
	return nil
}

// SetReplaceDisk
func (rdHandler *ReplaceDiskHandler) SetReplaceDisk(rd apisv1alpha1.ReplaceDisk) *ReplaceDiskHandler {
	rdHandler.rd = rd
	return rdHandler
}

// SetReplaceDisk
func (rdHandler *ReplaceDiskHandler) SetMigrateVolumeNames(volumeNames []string) *ReplaceDiskHandler {
	rdHandler.rd.Status.MigrateVolumeNames = volumeNames
	return rdHandler
}

// ReplaceDiskStage
func (rdHandler *ReplaceDiskHandler) ReplaceDiskStage() apisv1alpha1.ReplaceDiskStage {
	return rdHandler.rd.Spec.ReplaceDiskStage
}

// ReplaceDiskStage
func (rdHandler *ReplaceDiskHandler) ReplaceDiskStatus() apisv1alpha1.ReplaceDiskStatus {
	return rdHandler.rd.Status
}

// UpdateReplaceDiskStage
func (rdHandler *ReplaceDiskHandler) UpdateReplaceDiskStage(stage apisv1alpha1.ReplaceDiskStage) error {
	rd, err := rdHandler.GetReplaceDisk(client.ObjectKey{Name: rdHandler.rd.GetName(), Namespace: rdHandler.rd.GetNamespace()})
	if err != nil {
		return err
	}
	rd.Spec.ReplaceDiskStage = stage
	rdHandler.SetReplaceDisk(*rd)

	return nil
}
