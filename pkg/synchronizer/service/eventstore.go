package synchronizer

import (
	"fmt"
	"sync"

	eventstore "github.com/BrobridgeOrg/EventStore"
	log "github.com/sirupsen/logrus"
)

type EventStore struct {
	synchronizer *Synchronizer
	id           uint64
	name         string
	//	store         datastore.Store
	store         *eventstore.Store
	subscriptions sync.Map
}

func NewEventStore(synchronizer *Synchronizer, id uint64) *EventStore {
	return &EventStore{
		synchronizer: synchronizer,
		id:           id,
		name:         fmt.Sprintf("%d", id),
	}
}

func (es *EventStore) Init() error {

	store, err := es.synchronizer.eventStore.GetStore(es.name)

	// Initializing a data store
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

/*
func (es *EventStore) GetLastSequenceOfSnapshot() uint64 {
	return es.store.GetLastSequenceOfSnapshot()
}
*/

func (es *EventStore) CreateSnapshot() (*eventstore.SnapshotView, error) {

	view := es.store.CreateSnapshotView()
	err := view.Initialize()
	if err != nil {
		return nil, err
	}

	return view, nil
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

func (es *EventStore) Subscribe(durableName string, seq uint64, fn func(event *eventstore.Event)) (*eventstore.Subscription, error) {
	return es.store.Subscribe(durableName, seq, fn)
}

func (es *EventStore) Fetch(startAt uint64, offset uint64, count int) ([]*eventstore.Event, error) {
	return es.store.Fetch(startAt, offset, count)
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
