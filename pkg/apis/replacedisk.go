package apis

import apisv1alpha1 "github.com/hwameistor/improved-system/pkg/apis/hwameistor/v1alpha1"

// ReplaceDiskManager interface
type ReplaceDiskManager interface {
	Run(stopCh <-chan struct{})

	ReconcileReplaceDisk(replaceDisk *apisv1alpha1.ReplaceDisk)
}
