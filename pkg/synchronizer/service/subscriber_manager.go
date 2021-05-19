package synchronizer

import (
	"fmt"
	"sync"

	"github.com/BrobridgeOrg/gravity-sdk/core"
	gravity_subscriber_manager "github.com/BrobridgeOrg/gravity-sdk/subscriber_manager"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
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

	options := gravity_subscriber_manager.NewOptions()
	options.Verbose = false

	host := viper.GetString("eventbus.host")
	port := viper.GetInt("eventbus.port")
	address := fmt.Sprintf("%s:%d", host, port)

	log.WithFields(log.Fields{
		"address": address,
	}).Info("Connecting to gravity")

	// Connecting to subscriber manager
	subscriberManager := gravity_subscriber_manager.NewSubscriberManager(options)
	opts := core.NewOptions()
	err := subscriberManager.Connect(address, opts)
	if err != nil {
		return err
	}

	sm.subscriberManager = subscriberManager

	// Getting all subscribers to restore
	subscribers, err := subscriberManager.GetSubscribers()
	if err != nil {
		return err
	}

	// Restore
	for _, sub := range subscribers {
		log.WithFields(log.Fields{
			"id":        sub.ID,
			"name":      sub.Name,
			"component": sub.Component,
		}).Info("Restored subscriber")
		subscriber := NewSubscriber(sub.ID, sub.Name)
		sm.Register(sub.ID, subscriber)
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
