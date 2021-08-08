package synchronizer

import (
	"sync"

	gravity_collection_manager "github.com/BrobridgeOrg/gravity-sdk/collection_manager"
	"github.com/BrobridgeOrg/gravity-sdk/collection_manager/types"
	log "github.com/sirupsen/logrus"
)

type CollectionManager struct {
	synchronizer *Synchronizer
	manager      *gravity_collection_manager.CollectionManager
	collections  map[string]*types.Collection
	mutex        sync.RWMutex
}

func NewCollectionManager(synchronizer *Synchronizer) *CollectionManager {
	return &CollectionManager{
		synchronizer: synchronizer,
		collections:  make(map[string]*types.Collection),
	}
}

func (cm *CollectionManager) Initialize() error {

	log.Info("Initializing CollectionManager")

	// Connecting to collection manager
	options := gravity_collection_manager.NewOptions()
	options.Verbose = false
	options.Key = cm.synchronizer.keyring.Get("gravity")
	collectionManager := gravity_collection_manager.NewCollectionManagerWithClient(cm.synchronizer.gravityClient, options)
	cm.manager = collectionManager

	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	for _, rule := range cm.synchronizer.ruleConfig.Rules {

		// Collection exists already
		if _, ok := cm.collections[rule.Collection]; ok {
			continue
		}

		collection := types.NewCollection(rule.Collection)
		collection.ID = rule.Collection
		collection.Name = rule.Collection

		cm.collections[rule.Collection] = collection

		// Register to controller
		err := cm.manager.Register(collection)
		if err != nil {
			log.Errorf("Failed to register collection: %v", err)
			continue
		}

		log.Infof("    Registered collection: %s", rule.Collection)
	}

	return nil
}
