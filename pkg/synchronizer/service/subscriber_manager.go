package synchronizer

import (
	"sync"

	//	gravity_subscriber_manager "github.com/BrobridgeOrg/gravity-sdk/subscriber_manager"
	gravity_subscriber_manager "github.com/BrobridgeOrg/gravity-sdk/subscriber_manager"
	log "github.com/sirupsen/logrus"
)

type SubscriberManager struct {
	synchronizer      *Synchronizer
	subscriberManager *gravity_subscriber_manager.SubscriberManager
	subscribers       sync.Map
}

func NewSubscriberManager(synchronizer *Synchronizer) *SubscriberManager {

	return &SubscriberManager{
		synchronizer: synchronizer,
	}
}

func (sm *SubscriberManager) Initialize() error {

	// Connecting to subscriber manager
	options := gravity_subscriber_manager.NewOptions()
	options.Verbose = false
	options.Key = sm.synchronizer.keyring.Get("gravity")
	subscriberManager := gravity_subscriber_manager.NewSubscriberManagerWithClient(sm.synchronizer.gravityClient, options)
	sm.subscriberManager = subscriberManager

	// Getting all subscribers to restore
	subscribers, err := subscriberManager.GetSubscribers()
	if err != nil {
		return err
	}

	// Restore subscribers
	for _, sub := range subscribers {
		log.WithFields(log.Fields{
			"id":        sub.ID,
			"name":      sub.Name,
			"component": sub.Component,
		}).Info("Restored subscriber")

		for _, col := range sub.Collections {
			log.Infof("  %s", col)
		}

		subscriber := NewSubscriber(sub.ID, sub.Name, sub.AppID)
		subscriber.SubscribeToCollections(sub.Collections)

		// Resume pipelines
		for _, pid := range sub.Pipelines {
			if p, ok := sm.synchronizer.pipelines[pid]; ok {
				subscriber.suspendPipelines.Store(pid, p)
			}
		}

		err := sm.Register(sub.ID, subscriber)
		if err != nil {
			log.Error(err)
			continue
		}
		/*
			// Add access key to keyring
			if len(sub.AppID) > 0 && len(sub.AccessKey) > 0 {
				keyInfo := sm.synchronizer.keyring.Put(sub.AppID, sub.AccessKey)
				keyInfo.Permission().AddPermissions(sub.Permissions)
			}
		*/
	}

	return nil
}

func (sm *SubscriberManager) Register(id string, subscriber *Subscriber) error {

	subscriber.Initialize(sm.synchronizer)
	sm.subscribers.Store(id, subscriber)

	return nil
}

func (sm *SubscriberManager) Unregister(id string) error {

	sm.subscribers.Delete(id)

	return nil
}

func (sm *SubscriberManager) Get(id string) *Subscriber {

	element, ok := sm.subscribers.Load(id)
	if !ok {
		return nil
	}

	return element.(*Subscriber)
}

func (sm *SubscriberManager) AwakeAllSubscribers(pipeline *Pipeline) error {

	sm.subscribers.Range(func(key interface{}, value interface{}) bool {
		subscriber := value.(*Subscriber)
		subscriber.Awake(pipeline)
		return true
	})

	return nil
}
