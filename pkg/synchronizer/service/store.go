package synchronizer

import (
	"sync"

	eventstore "github.com/BrobridgeOrg/EventStore"
	gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/transmitter"
	log "github.com/sirupsen/logrus"
)

type Store struct {
	Name           string
	Collection     string `json:"collection"`
	Database       string `json:"database"`
	Table          string `json:"table"`
	Transmitter    *transmitter.Transmitter
	TriggerManager *TriggerManager

	SourceSubs sync.Map
}

func NewStore() *Store {
	return &Store{}
}

func (store *Store) Init() error {
	return nil
}

func (store *Store) AddEventSource(eventStore *EventStore) error {

	// Getting durable state of store
	durableSeq, err := eventStore.GetDurableState(store.Name)
	if err != nil {
		log.WithFields(log.Fields{
			"component": "store",
			"store":     store.Name,
		}).Error(err)
		return err
	}

	log.WithFields(log.Fields{
		"lastSeq": durableSeq,
		"store":   store.Name,
	}).Info("  connected to store")

	// Subscribe to event source
	sub, err := eventStore.Subscribe(store.Name, durableSeq, func(event *eventstore.Event) {
		/*
			if event.Sequence <= durableSeq {
				event.Ack()
				return
			}
		*/
		store.ProcessData(event)
	})
	if err != nil {
		return err
	}

	store.SourceSubs.Store(eventStore.id, sub)

	return nil
}

func (store *Store) RemoveEventSource(sourceID uint64) error {

	obj, ok := store.SourceSubs.Load(sourceID)
	if !ok {
		return nil
	}

	sub := obj.(*eventstore.Subscription)
	sub.Close()

	store.SourceSubs.Delete(sourceID)

	return nil
}

func (store *Store) IsMatch(pj *gravity_sdk_types_projection.Projection) bool {

	if pj.Collection != store.Collection {
		return false
	}

	return true
}

func (store *Store) ProcessData(event *eventstore.Event) {

	// Parsing data
	pj := projectionPool.Get().(*gravity_sdk_types_projection.Projection)
	err := gravity_sdk_types_projection.Unmarshal(event.Data, pj)
	if err != nil {
		log.Error(err)
		projectionPool.Put(pj)
		event.Ack()
		return
	}
	/*
		log.WithFields(log.Fields{
			"source":     eventStore.id,
			"seq":        seq,
			"store":      store.Name,
			"collection": pj.Collection,
			"sc":         store.Collection,
		}).Warn("Got")
	*/
	// Ignore store which is not matched
	if !store.IsMatch(pj) {
		projectionPool.Put(pj)
		event.Ack()
		return
	}
	/*
		log.WithFields(log.Fields{
			"seq":        event.Sequence,
			"store":      store.Name,
			"collection": pj.Collection,
		}).Info("Processing record")
	*/
	if store.Transmitter != nil {
		err = store.Transmitter.ProcessData(store.Table, event.Sequence, pj)
		if err != nil {
			log.WithFields(log.Fields{
				"component": "transmitter",
			}).Error(err)
			projectionPool.Put(pj)
			return
		}
	}

	// Trigger
	err = store.TriggerManager.Handle(store.Name, pj, event.Data)
	if err != nil {
		log.WithFields(log.Fields{
			"component": "trigger",
			//			"source":    eventStore.id,
			"seq":   event.Sequence,
			"store": store.Name,
		}).Error(err)
	}

	projectionPool.Put(pj)
	event.Ack()

	return
}
