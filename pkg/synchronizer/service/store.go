package synchronizer

import (
	"sync"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
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

	// TODO: get last sequence then compare with it, to determine whether it does sync from snapshot first
	//	lastSeq := eventStore.GetLastSequence()

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
		"seq":   durableSeq,
		"store": store.Name,
	}).Info("  Initializing store")

	// Subscribe to event source
	sub, err := eventStore.Subscribe(durableSeq, func(seq uint64, data []byte) bool {

		if seq <= durableSeq {
			return true
		}

		success := store.ProcessData(eventStore, seq, data)

		if success {
			err = eventStore.UpdateDurableState(store.Name, seq)
			if err != nil {
				log.WithFields(log.Fields{
					"component": "store",
					"store":     store.Name,
					"seq":       seq,
				}).Error(err)
			}
		}

		return success
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

	sub := obj.(datastore.Subscription)
	sub.Close()

	store.SourceSubs.Delete(sourceID)

	return nil
}

func (store *Store) IsMatch(pj *projection.Projection) bool {

	if pj.Collection != store.Collection {
		return false
	}

	return true
}

//func (store *Store) ProcessData(eventStore *EventStore, seq uint64, pj *projection.Projection) bool {
func (store *Store) ProcessData(eventStore *EventStore, seq uint64, data []byte) bool {

	// Parsing data
	pj := projectionPool.Get().(*projection.Projection)
	err := projection.Unmarshal(data, pj)
	if err != nil {
		log.Error(err)
		return true
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
		return true
	}
	/*
		log.WithFields(log.Fields{
			"source":     eventStore.id,
			"seq":        seq,
			"store":      store.Name,
			"collection": pj.Collection,
		}).Info("Processing record")
	*/
	if store.Transmitter != nil {
		err = store.Transmitter.ProcessData(store.Table, seq, pj)
		if err != nil {
			log.WithFields(log.Fields{
				"component": "transmitter",
			}).Error(err)
			return false
		}
	}

	// Trigger
	err = store.TriggerManager.Handle(store.Name, pj)
	if err != nil {
		log.WithFields(log.Fields{
			"component": "trigger",
			"source":    eventStore.id,
			"seq":       seq,
			"store":     store.Name,
		}).Error(err)
	}

	projectionPool.Put(pj)

	return true
}
