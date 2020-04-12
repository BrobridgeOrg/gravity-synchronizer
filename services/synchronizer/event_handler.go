package synchronizer

import (
	"encoding/json"
	"errors"
	app "gravity-synchronizer/app/interface"
	"gravity-synchronizer/internal/projection"

	"github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
)

type EventHandler struct {
	app        app.AppImpl
	sequence   uint64
	cacheStore *CacheStore
	dbMgr      *DatabaseManager
	exMgr      *ExporterManager
	storeMgr   *StoreManager
	triggerMgr *TriggerManager
}

func CreateEventHandler(a app.AppImpl) *EventHandler {

	eventHandler := &EventHandler{
		app:      a,
		sequence: 0,
	}

	// Cache
	cacheStore := CreateCacheStore(a)
	if cacheStore == nil {
		log.Error("Failed to create cache store")
		return nil
	}

	eventHandler.cacheStore = cacheStore

	// Database manager
	dm := CreateDatabaseManager()
	if dm == nil {
		log.Error("Failed to create database manager")
		return nil
	}

	eventHandler.dbMgr = dm

	// Exporter manager
	em := CreateExporterManager()
	if dm == nil {
		log.Error("Failed to create exporter manager")
		return nil
	}

	eventHandler.exMgr = em

	// Store manager
	storeMgr := CreateStoreManager(eventHandler)
	if storeMgr == nil {
		log.Error("Failed to create store manager")
		return nil
	}

	eventHandler.storeMgr = storeMgr

	return eventHandler
}

func (eh *EventHandler) Initialize() error {

	// Load Database configs and establish connection
	err := eh.dbMgr.Initialize()
	if err != nil {
		log.Error(err)
		return err
	}

	// Load exporter configs and establish connection
	err = eh.exMgr.Initialize()
	if err != nil {
		log.Error(err)
		return err
	}

	// Initialize store manager
	err = eh.storeMgr.Initialize()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Try to receovery
	err = eh.Recovery()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Load triggers
	triggerMgr := CreateTriggerManager(eh)
	eh.triggerMgr = triggerMgr
	err = triggerMgr.Initialize()
	if err != nil {
		return err
	}

	// Listen to event store
	log.WithFields(log.Fields{
		"channel": "gravity.store.eventStored",
		"startAt": eh.sequence,
	}).Info("Subscribe to event bus")
	eb := eh.app.GetEventBus()
	err = eb.On("gravity.store.eventStored", func(msg *stan.Msg) {

		// Parse event
		var pj projection.Projection
		err := json.Unmarshal(msg.Data, &pj)
		if err != nil {
			msg.Ack()
			return
		}

		log.WithFields(log.Fields{
			"seq":        msg.Sequence,
			"event":      pj.EventName,
			"collection": pj.Collection,
			"method":     pj.Method,
		}).Info("Received event")

		// Process
		err = eh.ProcessEvent(msg.Sequence, &pj)
		if err != nil {
			log.Error(err)
			return
		}

		msg.Ack()
	}, eh.sequence)

	if err != nil {
		return err
	}

	return nil
}

func (eh *EventHandler) GetApp() app.AppImpl {
	return eh.app
}

func (eh *EventHandler) ProcessEvent(sequence uint64, pj *projection.Projection) error {

	// Data store
	for _, store := range eh.storeMgr.Stores {
		if store.IsMatch(pj) == false {
			continue
		}

		// Apply action for this store
		err := eh.ApplyStore(store, sequence, pj)
		if err != nil {
			log.Error(err)
			continue
		}
	}

	// Trigger
	for _, trigger := range eh.triggerMgr.Triggers {
		if trigger.IsMatch(pj) == false {
			continue
		}

		err := eh.ApplyTrigger(trigger, sequence, pj)
		if err != nil {
			log.Error(err)
			continue
		}
	}
	return nil
}

func (eh *EventHandler) Recovery() error {

	log.Warn("Checking stores for recovery...")

	for _, store := range eh.storeMgr.Stores {

		// Update global sequence
		if store.State.Sequence > eh.sequence {
			eh.sequence = store.State.Sequence
		}

		err := eh.RecoveryStore(store)
		if err != nil {
			log.Error(err)
			continue
		}
	}

	return nil
}

func (eh *EventHandler) RecoveryStore(store *Store) error {

	log.WithFields(log.Fields{
		"collection": store.Collection,
		"database":   store.Database,
		"table":      store.Table,
	}).Info("Checking store...")

	// Getting state of store
	seq, err := eh.cacheStore.GetSnapshotState(store.Collection)
	if err != nil {
		// cache store doesn't work
		log.Info("No cache found")
		return err
	}

	log.WithFields(log.Fields{
		"collection": store.Collection,
		"seq":        seq,
	}).Info("Fetched cache state")

	if seq == 0 {
		// No need to recovery because no cache out there
		return nil
	}

	if store.State.Sequence+100000 > seq {
		// Ignore if not being too far behind
		log.WithFields(log.Fields{
			"cache.seq": seq,
			"store.seq": store.State.Sequence,
		}).Info("No need to recovery from cache, because not being too far behind")
		return nil
	}

	// Get dataase handle
	db := eh.dbMgr.GetDatabase(store.Database)
	if db == nil {
		return errors.New("Not found database \"" + store.Database + "\"")
	}

	// truncate table
	log.WithFields(log.Fields{
		"table": store.Table,
	}).Warn("Truncate table...")

	err = db.Truncate(store.Table)
	if err != nil {
		return err
	}

	// Start recovering
	log.WithFields(log.Fields{
		"collection": store.Collection,
		"database":   store.Database,
		"table":      store.Table,
	}).Warn("Recovering store...")

	newSeq, err := eh.cacheStore.FetchSnapshot(store.Collection, func(data map[string]interface{}) error {
		err := db.Import(store.Table, data)
		if err != nil {
			log.Error(err)
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Update state
	store.State.Sequence = newSeq
	err = store.State.Sync()
	if err != nil {
		log.Error(err)
	}

	// Update global sequence
	if store.State.Sequence > eh.sequence {
		eh.sequence = store.State.Sequence
	}

	return nil
}

func (eh *EventHandler) ApplyStore(store *Store, sequence uint64, pj *projection.Projection) error {

	// Get dataase handle
	db := eh.dbMgr.GetDatabase(store.Database)
	if db == nil {
		return errors.New("Not found database \"" + store.Database + "\"")
	}

	// store data
	err := db.ProcessData(store.Table, sequence, pj)
	if err != nil {
		return err
	}

	// Update state
	store.State.Sequence = sequence
	err = store.State.Sync()
	if err != nil {
		log.Error(err)
	}

	return nil
}

func (eh *EventHandler) ApplyTrigger(trigger *Trigger, sequence uint64, pj *projection.Projection) error {

	switch trigger.Action.Type {
	case "exporter":
		exName := trigger.Action.Exporter
		ex := eh.exMgr.GetExporter(exName)
		if ex == nil {
			log.Warning("Not support such exporter type: " + exName)
			break
		}

		err := ex.Send(sequence, pj)
		if err != nil {
			log.Warning("Failed to send by exporter: " + exName)
			break
		}

	}

	return nil
}
