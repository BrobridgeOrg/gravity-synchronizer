package synchronizer

import (
	"fmt"
	"gravity-synchronizer/pkg/synchronizer/service/collection"
	data_handler "gravity-synchronizer/pkg/synchronizer/service/data_handler"
	"gravity-synchronizer/pkg/synchronizer/service/dsa"
	"os"
	"strings"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/rule"

	eventstore "github.com/BrobridgeOrg/EventStore"
	"github.com/BrobridgeOrg/broc"
	core "github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/app"
	"github.com/BrobridgeOrg/schemer"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Synchronizer struct {
	app              app.App
	gravityClient    *core.Client
	clientID         string
	domain           string
	pipelines        map[uint64]*Pipeline
	ruleConfig       *rule.RuleConfig
	collectionConfig *collection.Config

	// components
	keyring         *keyring.Keyring
	dsa             *dsa.DataSourceAdapter
	controller      *Controller
	dataHandler     *data_handler.DataHandler
	snapshotHandler *SnapshotHandler
	eventStore      *eventstore.EventStore
	//	storeMgr        *StoreManager
	subscriberMgr *SubscriberManager
	collectionMgr *CollectionManager

	// RPC
	eventStoreRPC *broc.Broc
	dsaRPC        *broc.Broc
}

func NewSynchronizer(a app.App) *Synchronizer {
	synchronizer := &Synchronizer{
		app:             a,
		pipelines:       make(map[uint64]*Pipeline, 0),
		snapshotHandler: NewSnapshotHandler(),
		gravityClient:   core.NewClient(),
		keyring:         keyring.NewKeyring(),
	}

	synchronizer.controller = NewController(synchronizer)
	//	synchronizer.storeMgr = NewStoreManager(synchronizer)
	synchronizer.subscriberMgr = NewSubscriberManager(synchronizer)
	synchronizer.collectionMgr = NewCollectionManager(synchronizer)

	return synchronizer
}

func (synchronizer *Synchronizer) Init() error {

	// Using hostname (pod name) by default
	host, err := os.Hostname()
	if err != nil {
		log.Error(err)
		return nil
	}

	host = strings.ReplaceAll(host, ".", "_")

	synchronizer.clientID = fmt.Sprintf("gravity_synchronizer-%s", host)

	log.WithFields(log.Fields{
		"clientID": synchronizer.clientID,
	}).Info("Initializing synchronizer")

	// Load collection config
	collectionConfig, err := collection.LoadConfigFile("./rules/collection.json")
	if err != nil {
		log.Error(err)
		synchronizer.collectionConfig = &collection.Config{}
	} else {
		synchronizer.collectionConfig = collectionConfig
	}

	// Load rule config
	ruleConfig, err := rule.LoadRuleFile("./rules/rules.json")
	if err != nil {
		return err
	}

	synchronizer.ruleConfig = ruleConfig

	// Initializing collection schema
	for _, rule := range ruleConfig.Rules {
		// Trying to get collection schema
		collectionConfig := synchronizer.collectionConfig.GetCollectionConfig(rule.Collection)
		if collectionConfig == nil {
			continue
		}

		schema := schemer.NewSchema()
		err := schemer.Unmarshal(collectionConfig.Schema, schema)
		if err != nil {
			log.Error(err)
			continue
		}

		rule.Handler.Transformer.SetDestinationSchema(schema)
	}

	// Initializing event store
	err = synchronizer.initializeEventStore()
	if err != nil {
		return err
	}

	// Connect to gravity
	err = synchronizer.initializeClient()
	if err != nil {
		return err
	}

	// Recovery subscriptions
	err = synchronizer.recoveryPipelines()
	if err != nil {
		return err
	}
	// Initializing subscriber
	err = synchronizer.subscriberMgr.Initialize()
	if err != nil {
		return err
	}
	/*
		// Initializing stores
		err = synchronizer.storeMgr.Init()
		if err != nil {
			return err
		}
	*/

	// Initializing Event Store RPC handlers
	err = synchronizer.initEventStoreRPC()
	if err != nil {
		return err
	}

	// register synchronizer
	err = synchronizer.controller.GetSynchronizerManager().Register(synchronizer.clientID)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"id": synchronizer.clientID,
	}).Info("Registered synchronizer to controller")

	// Initializing collection manager
	err = synchronizer.collectionMgr.Initialize()
	if err != nil {
		return err
	}

	// Initializing data handler
	err = synchronizer.initializeDataHandler()
	if err != nil {
		return err
	}

	// Initializing data source adapter
	err = synchronizer.initializeDataSourceAdapter()
	if err != nil {
		return err
	}

	// TODO: health checks

	return nil
}

func (synchronizer *Synchronizer) initializeEventStore() error {

	options := eventstore.NewOptions()
	options.DatabasePath = viper.GetString("datastore.path")
	options.EnabledSnapshot = true

	// Snapshot options
	viper.SetDefault("snapshot.workerCount", 8)
	viper.SetDefault("snapshot.workerBufferSize", 102400)
	options.SnapshotOptions.WorkerCount = viper.GetInt32("snapshot.workerCount")
	options.SnapshotOptions.BufferSize = viper.GetInt("snapshot.workerBufferSize")

	log.WithFields(log.Fields{
		"databasePath":        options.DatabasePath,
		"snapshotWorkerCount": options.SnapshotOptions.WorkerCount,
		"snapshotBufferSize":  options.SnapshotOptions.BufferSize,
	}).Info("Initialize event store engine")

	// Initialize event store
	es, err := eventstore.CreateEventStore(options)
	if err != nil {
		return err
	}

	synchronizer.eventStore = es

	// Setup snapshot
	es.SetSnapshotHandler(func(request *eventstore.SnapshotRequest) error {
		return synchronizer.snapshotHandler.handle(request)
	})

	return nil
}

func (synchronizer *Synchronizer) recoveryPipelines() error {

	log.Info("Attempt to recovery pipeline subscriptions")

	pipelines, err := synchronizer.controller.GetSynchronizerManager().GetPipelines(synchronizer.clientID)
	if err != nil {
		if err.Error() == "NotFound" {
			log.Errorf("Failed to get pipeline subsciprtion information: %v", err)
			return nil
		}

		return err
	}

	for _, pipelineID := range pipelines {
		err := synchronizer.AssignPipeline(pipelineID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (synchronizer *Synchronizer) revokePipeline(pipelineID uint64) {
	delete(synchronizer.pipelines, pipelineID)
}

func (synchronizer *Synchronizer) AssignPipeline(pipelineID uint64) error {

	_, ok := synchronizer.pipelines[pipelineID]
	if ok {
		// pipeline exists already
		return nil
	}

	pipeline := NewPipeline(synchronizer, pipelineID)
	err := pipeline.Init()
	if err != nil {
		return err
	}

	synchronizer.pipelines[pipelineID] = pipeline

	return nil
}

func (synchronizer *Synchronizer) RevokePipeline(pipelineID uint64) error {

	pipeline, ok := synchronizer.pipelines[pipelineID]
	if !ok {
		return nil
	}

	go func() {

		// Uninitializing and waiting for draining
		err := pipeline.Uninit()
		if err != nil {
			log.Error(err)
			return
		}

		// Take off from registry
		synchronizer.revokePipeline(pipelineID)
	}()

	return nil
}
