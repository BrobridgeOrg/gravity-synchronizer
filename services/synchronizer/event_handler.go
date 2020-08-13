package synchronizer

import (
	"encoding/json"
	app "gravity-synchronizer/app/interface"
	"gravity-synchronizer/internal/projection"

	"github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
)

type Event struct {
	Sequence   uint64
	Projection projection.Projection
}

type EventHandler struct {
	app            app.AppImpl
	sequence       uint64
	cacheStore     *CacheStore
	exMgr          *ExporterManager
	storeMgr       *StoreManager
	triggerMgr     *TriggerManager
	transmitterMgr *TransmitterManager

	incoming chan *Event
}

func NewEventHandler(a app.AppImpl) *EventHandler {

	eventHandler := &EventHandler{
		app:      a,
		sequence: 0,
		incoming: make(chan *Event, 1024),
	}

	// Cache
	cacheStore := CreateCacheStore(a)
	if cacheStore == nil {
		log.Error("Failed to create cache store")
		return nil
	}

	eventHandler.cacheStore = cacheStore

	// Transmitter manager
	tm := NewTransmitterManager()
	if tm == nil {
		log.Error("Failed to create transmitter manager")
		return nil
	}

	eventHandler.transmitterMgr = tm

	// Exporter manager
	em := CreateExporterManager()
	if em == nil {
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

	// Load transmitter configs and initialize
	err := eh.transmitterMgr.Initialize()
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
		return err
	}

	// Initializing cache store
	err = eh.cacheStore.Init()
	if err != nil {
		log.Error(err)
		return err
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

	// Register recovery handler for event bus
	eb := eh.app.GetEventBus()
	eb.RegisterRecoveryHandler(func() {
		log.Info("Recovering event bus...")
		eh.initEventBus()
	})

	// Initialize event bus
	return eh.initEventBus()
}

func (eh *EventHandler) initEventEngine() {
	for {
		select {
		case event := <-eh.incoming:

			log.WithFields(log.Fields{
				"seq":        event.Sequence,
				"event":      event.Projection.EventName,
				"collection": event.Projection.Collection,
				"method":     event.Projection.Method,
			}).Info("Received event")

			// Process
			err := eh.ProcessEvent(event.Sequence, &event.Projection)
			if err != nil {
				log.Error(err)
			}

		}
	}
}

func (eh *EventHandler) initEventBus() error {

	go eh.initEventEngine()

	// Listen to event store
	log.WithFields(log.Fields{
		"channel": "gravity.store.eventStored",
		"startAt": eh.sequence,
	}).Info("Subscribe to event bus")

	startAt := eh.sequence
	eb := eh.app.GetEventBus()
	err := eb.On("gravity.store.eventStored", func(msg *stan.Msg) {

		// Ignore the first message we have already
		if startAt == msg.Sequence {
			return
		}

		event := Event{
			Sequence: msg.Sequence,
		}

		// Parse event
		err := json.Unmarshal(msg.Data, &event.Projection)
		if err != nil {
			return
		}

		eh.incoming <- &event
	}, startAt)

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

	// truncate table
	log.WithFields(log.Fields{
		"table": store.Table,
	}).Warn("Truncate table...")

	err = store.Transmitter.Truncate(store.Table)
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
		err := store.Transmitter.Insert(store.Table, data)
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

	// store data
	err := store.Transmitter.ProcessData(store.Table, sequence, pj)
	if err != nil {
		return err
	}

	// Update state
	store.State.Sequence = sequence
	err = store.State.Sync()
	if err != nil {
		log.Error(err)
		return err
	}

	eh.sequence = sequence

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
