package synchronizer

import "sync"

type SubscriberManager struct {
	synchronizer *Synchronizer
	subscribers  sync.Map
}

func NewSubscriberManager(synchronizer *Synchronizer) *SubscriberManager {

	return &SubscriberManager{
		synchronizer: synchronizer,
	}
}

func (sm *SubscriberManager) Initialize() error {
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
