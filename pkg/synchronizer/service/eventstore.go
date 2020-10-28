package synchronizer

import (
	"fmt"
	"sync"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	log "github.com/sirupsen/logrus"
)

type EventStore struct {
	synchronizer  *Synchronizer
	id            uint64
	store         datastore.Store
	subscriptions sync.Map
}

func NewEventStore(synchronizer *Synchronizer, id uint64) *EventStore {
	return &EventStore{
		synchronizer: synchronizer,
		id:           id,
	}
}

func (es *EventStore) Init() error {

	name := fmt.Sprintf("%d", es.id)

	// Initializing a data store
	store, err := es.synchronizer.app.GetStoreManager().GetStore(name)
	if err != nil {
		return err
	}

	es.store = store

	// Adding event souce to all stores
	es.synchronizer.storeMgr.AddEventSource(es)

	return nil
}

func (es *EventStore) Close() {
	//	es.close <- struct{}{}
	es.store.Close()

	// Remove event source from stores
	es.synchronizer.storeMgr.RemoveEventSource(es.id)
}

func (es *EventStore) GetLastSequenceOfSnapshot() uint64 {
	return es.store.GetLastSequenceOfSnapshot()
}

func (es *EventStore) GetLastSequence() uint64 {
	return es.store.GetLastSequence()
}

func (es *EventStore) GetDurableState(durableName string) (uint64, error) {
	lastSeq, err := es.store.GetDurableState(durableName)
	if err != nil {
		return 0, err
	}

	return lastSeq, nil
}

func (es *EventStore) UpdateDurableState(durableName string, lastSeq uint64) error {
	return es.store.UpdateDurableState(durableName, lastSeq)
}

func (es *EventStore) Subscribe(seq uint64, fn datastore.StoreHandler) (datastore.Subscription, error) {
	return es.store.Subscribe(seq, fn)
}

func (es *EventStore) Write(data []byte) error {

	// Event sourcing
	seq, err := es.store.Write(data)
	if err != nil {

		log.WithFields(log.Fields{
			"pipeline": es.id,
			"seq":      seq,
		}).Error(err)

		return err
	}
	/*
		log.WithFields(log.Fields{
			"pipeline": es.id,
			"seq":      seq,
		}).Info("Stored event")
	*/
	return nil
}
