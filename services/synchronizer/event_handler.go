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
	dbMgr      *DatabaseManager
	exMgr      *ExporterManager
	ruleEngine *RuleEngine
}

func CreateEventHandler(a app.AppImpl) *EventHandler {

	return &EventHandler{
		app: a,
	}
}

func (eh *EventHandler) Initialize() error {

	dm := CreateDatabaseManager()
	eh.dbMgr = dm
	if dm == nil {
		return errors.New("Failed to create database manager")
	}

	// Load Database configs and establish connection
	err := dm.Initialize()
	if err != nil {
		return err
	}

	em := CreateExporterManager()
	eh.exMgr = em
	if dm == nil {
		return errors.New("Failed to create exporter manager")
	}

	// Load exporter configs and establish connection
	err = em.Initialize()
	if err != nil {
		return err
	}

	// Load rules
	ruleEngine := CreateRuleEngine(eh)
	eh.ruleEngine = ruleEngine
	err = ruleEngine.Initialize()
	if err != nil {
		return err
	}

	// Listen to event store
	eb := eh.app.GetEventBus()
	err = eb.On("gravity.store.eventStored", func(msg *stan.Msg) {

		log.Info(string(msg.Data))

		// Parse event
		var pj projection.Projection
		err := json.Unmarshal(msg.Data, &pj)
		if err != nil {
			return
		}

		// Process
		err = eh.ProcessEvent(msg.Sequence, &pj)
		if err != nil {
			log.Error(err)
			return
		}
	})

	if err != nil {
		return err
	}

	return nil
}

func (eh *EventHandler) ProcessEvent(sequence uint64, pj *projection.Projection) error {

	for _, rule := range eh.ruleEngine.Rules {
		if rule.Match(pj) == false {
			continue
		}

		// Apply rule and do action for it
		err := eh.ApplyRule(rule, sequence, pj)
		if err != nil {
			log.Error(err)
			continue
		}
	}

	return nil
}

func (eh *EventHandler) ApplyRule(rule *Rule, sequence uint64, pj *projection.Projection) error {

	// Get dataase handle
	db := eh.dbMgr.GetDatabase(rule.Database)
	if db == nil {
		return errors.New("Not found database \"" + rule.Database + "\"")
	}

	// store data
	err := db.ProcessData(sequence, pj)
	if err != nil {
		return err
	}

	// export event
	for _, exName := range rule.Exporter {
		ex := eh.exMgr.GetExporter(exName)
		if ex == nil {
			log.Warning("Not support such exporter type: " + exName)
			continue
		}

		err := ex.Send(sequence, pj)
		if err != nil {
			log.Warning("Failed to send by exporter: " + exName)
			continue
		}
	}

	return nil
}
