package synchronizer

import (
	"encoding/json"
	"sync"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/projection"
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

	SourceSubs   sync.Map
	SourceStates sync.Map
	IsReady      bool
}

func NewStore() *Store {
	return &Store{}
}

func (store *Store) Init() error {
	store.IsReady = true
	return nil
}

func (store *Store) LoadSourceState(sourceID uint64, lastSeq uint64) error {
	store.SourceStates.Store(sourceID, lastSeq)
	return nil
}

func (store *Store) DeleteSourceState(id uint64) error {
	store.SourceStates.Delete(id)
	return nil
}

func (store *Store) AddEventSource(eventStore *EventStore) error {

	// Getting durable state of store
	durableSeq, err := eventStore.GetDurableState(store.Name)
	if err != nil {
		log.Error(err)
		return err
	}

	log.WithFields(log.Fields{
		"seq":   durableSeq,
		"store": store.Name,
	}).Info("  Initializing store")

	store.LoadSourceState(eventStore.id, durableSeq)

	// Subscribe to event source
	sub, err := eventStore.Subscribe(durableSeq, func(seq uint64, data []byte) bool {

		if seq == durableSeq {
			return true
		}

		// Parse event
		var pj projection.Projection
		err := json.Unmarshal(data, &pj)
		if err != nil {
			log.WithFields(log.Fields{
				"source": eventStore.id,
				"seq":    seq,
				"store":  store.Name,
			}).Error(err)
			return true
		}

		// Ignore store which is not matched
		if !store.IsMatch(&pj) {
			err = eventStore.UpdateDurableState(store.Name, seq)
			return true
		}

		log.WithFields(log.Fields{
			"source": eventStore.id,
			"seq":    seq,
			"store":  store.Name,
		}).Info(" -> Process record")

		err = store.Handle(eventStore.id, seq, &pj)
		if err != nil {
			log.Error(err)
			return false
		}

		err = eventStore.UpdateDurableState(store.Name, seq)

		// Trigger
		err = store.TriggerManager.Handle(store.Name, &pj)
		if err != nil {
			log.WithFields(log.Fields{
				"source": eventStore.id,
				"seq":    seq,
				"store":  store.Name,
			}).Error(err)
		}

		return true
	})

	store.SourceSubs.Store(eventStore.id, sub)

	return nil
}

func (store *Store) RemoveEventSource(sourceID uint64) error {

	store.DeleteSourceState(sourceID)

	obj, ok := store.SourceSubs.Load(sourceID)
	if !ok {
		return nil
	}

	sub := obj.(datastore.Subscription)
	sub.Close()

	return nil
}

func (store *Store) IsMatch(pj *projection.Projection) bool {

	if pj.Collection != store.Collection {
		return false
	}

	return true
}

func (store *Store) Handle(pipelineID uint64, seq uint64, pj *projection.Projection) error {

	// Ignore everythin if store is not ready yet
	if !store.IsReady {
		return nil
	}

	state, ok := store.SourceStates.Load(pipelineID)
	if !ok {
		return nil
	}

	if state.(uint64) >= seq {
		return nil
	}

	err := store.Transmitter.ProcessData(store.Table, seq, pj)
	if err != nil {
		return err
	}

	store.SourceStates.Store(pipelineID, seq)

	return nil
}
