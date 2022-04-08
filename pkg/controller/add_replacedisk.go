package controller

import (
	"github.com/hwameistor/improved-system/pkg/controller/replacedisk"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, replacedisk.Add)
}
