package instance

import (
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	log "github.com/sirupsen/logrus"
)

func (a *AppInstance) initStoreManager() error {
	log.Info("Initialized store manager")
	return a.storeManager.Init()
}

func (a *AppInstance) GetStoreManager() datastore.StoreManager {
	return datastore.StoreManager(a.storeManager)
}
