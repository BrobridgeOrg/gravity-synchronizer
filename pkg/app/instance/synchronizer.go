package instance

import (
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer"
)

func (a *AppInstance) initSynchronizer() error {
	return a.synchronizer.Init()
}

func (a *AppInstance) GetSynchronizer() synchronizer.Synchronizer {
	return synchronizer.Synchronizer(a.synchronizer)
}
