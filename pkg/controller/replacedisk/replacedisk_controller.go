package replacedisk

import (
	"context"
	"github.com/hwameistor/reliable-helper-system/pkg/apis"
	apisv1alpha1 "github.com/hwameistor/reliable-helper-system/pkg/apis/hwameistor/v1alpha1"
	replacediskmanager "github.com/hwameistor/reliable-helper-system/pkg/replacedisk/manager"
	logr "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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
	replaceDiskManager, _ := replacediskmanager.New(mgr)
	return &ReconcileReplaceDisk{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		Recorder:           mgr.GetEventRecorderFor("replacedisk-controller"),
		replaceDiskManager: replaceDiskManager}
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
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name, "Request.NamespacedName", request.NamespacedName)
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
	logr.Debug("Debug Reconciling ReplaceDisk", replaceDisk)

	rdhandler := replacediskmanager.NewReplaceDiskHandler(r.client, r.Recorder)
	rdhandler = rdhandler.SetReplaceDisk(*replaceDisk)
	replaceDiskStatus := rdhandler.ReplaceDiskStatus()
	//rdhandler, err = rdhandler.Refresh()
	//if err != nil {
	//	logr.Error("Reconciling Refresh err", err)
	//	return reconcile.Result{}, err
	//}
	logr.Debug("replaceDisk.Spec.ReplaceDiskStage = %v, replaceDiskStatus = %v", replaceDisk.Spec.ReplaceDiskStage.String(), replaceDiskStatus)

	switch replaceDisk.Spec.ReplaceDiskStage {
	case "":
		logr.Debug("replaceDisk.Spec.ReplaceDiskStage = %v, replaceDiskStatus = %v", rdhandler.ReplaceDiskStage().String(), rdhandler.ReplaceDiskStatus())
		rdhandler = rdhandler.SetReplaceDiskStage(apisv1alpha1.ReplaceDiskStage_Init)
		err := rdhandler.UpdateReplaceDiskCR()
		if err != nil {
			logr.Error(err, "UpdateReplaceDiskCR SetReplaceDiskStage ReplaceDiskStage_Init failed")
			return reconcile.Result{Requeue: true}, nil
		}
		replaceDiskStatus.OldDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
		replaceDiskStatus.NewDiskReplaceStatus = apisv1alpha1.ReplaceDisk_Init
		if err := rdhandler.UpdateReplaceDiskStatus(replaceDiskStatus); err != nil {
			logr.Error(err, "ReplaceDiskStage_Init UpdateReplaceDiskStatus ReplaceDisk_Init,ReplaceDisk_Init failed")
			return reconcile.Result{Requeue: true}, nil
		}

		logr.Debug("replaceDisk.Spec.ReplaceDiskStage = %v, replaceDiskStatus = %v", rdhandler.ReplaceDiskStage().String(), rdhandler.ReplaceDiskStatus())
		return reconcile.Result{Requeue: true}, nil
	case apisv1alpha1.ReplaceDiskStage_Init:
		logr.Debug("ReplaceDiskStage_Init replaceDisk.Spec.ReplaceDiskStage = %v, replaceDiskStatus = %v", rdhandler.ReplaceDiskStage().String(), rdhandler.ReplaceDiskStatus())

	case apisv1alpha1.ReplaceDiskStage_WaitDiskReplaced:
		logr.Debug("ReplaceDiskStage_WaitDiskReplaced replaceDisk.Spec.ReplaceDiskStage 1= %v, replaceDiskStatus = %v", rdhandler.ReplaceDiskStage().String(), rdhandler.ReplaceDiskStatus())

	case apisv1alpha1.ReplaceDiskStage_WaitSvcRestor:
		logr.Debug("ReplaceDiskStage_WaitSvcRestor replaceDisk.Spec.ReplaceDiskStage = %v, replaceDiskStatus = %v", rdhandler.ReplaceDiskStage().String(), rdhandler.ReplaceDiskStatus())

	case apisv1alpha1.ReplaceDiskStage_Succeed:
		logr.Debug("ReplaceDiskStage_Succeed replaceDisk.Spec.ReplaceDiskStage = %v, replaceDiskStatus = %v", rdhandler.ReplaceDiskStage().String(), rdhandler.ReplaceDiskStatus())

	case apisv1alpha1.ReplaceDiskStage_Failed:
		return reconcile.Result{Requeue: true}, nil

	default:
		reqLogger.Error(err, "Invalid ReplaceDisk stage")
	}

	r.replaceDiskManager.ReplaceDiskNodeManager().ReconcileReplaceDisk(&rdhandler.ReplaceDisk)

	return reconcile.Result{}, nil
}
