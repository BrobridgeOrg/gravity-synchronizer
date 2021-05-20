package synchronizer

import (
	"fmt"
	"gravity-synchronizer/pkg/synchronizer/service/data_handler"
	"os"
	"strings"

	eventstore "github.com/BrobridgeOrg/EventStore"
	core "github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/app"
	gosharding "github.com/cfsghost/gosharding"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Synchronizer struct {
	app           app.App
	gravityClient *core.Client
	shard         *gosharding.Shard
	clientID      string
	pipelines     map[uint64]*Pipeline

	// components
	dataHandler     *data_handler.DataHandler
	snapshotHandler *SnapshotHandler
	eventStore      *eventstore.EventStore
	storeMgr        *StoreManager
	subscriberMgr   *SubscriberManager
	triggerMgr      *TriggerManager
	exporterMgr     *ExporterManager
}

func NewSynchronizer(a app.App) *Synchronizer {
	synchronizer := &Synchronizer{
		app:             a,
		pipelines:       make(map[uint64]*Pipeline, 0),
		exporterMgr:     NewExporterManager(),
		snapshotHandler: NewSnapshotHandler(),
		gravityClient:   core.NewClient(),
	}

	synchronizer.storeMgr = NewStoreManager(synchronizer)
	synchronizer.subscriberMgr = NewSubscriberManager(synchronizer)
	synchronizer.triggerMgr = NewTriggerManager(synchronizer)

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

	// Initializing event store
	err = synchronizer.initializeEventStore()
	if err != nil {
		return err
	}

	// Initializing shard
	err = synchronizer.initializeShard()
	if err != nil {
		return err
	}

	// Connect to gravity
	err = synchronizer.initializeClient()
	if err != nil {
		return err
	}

	// Initializing RPC handlers
	err = synchronizer.initRPC()
	if err != nil {
		return err
	}

	// Initializing subscriber
	err = synchronizer.subscriberMgr.Initialize()
	if err != nil {
		return err
	}

	// Initializing stores
	err = synchronizer.storeMgr.Init()
	if err != nil {
		return err
	}

	// Initializing exporter
	err = synchronizer.exporterMgr.Initialize()
	if err != nil {
		return err
	}

	// Initializing trigger
	err = synchronizer.triggerMgr.Initialize()
	if err != nil {
		return err
	}

	// Recovery subscriptions
	err = synchronizer.recoveryPipelines()
	if err != nil {
		return err
	}

	// register synchronizer
	err = synchronizer.RegisterClient()
	if err != nil {
		return err
	}

	// Initializing data handler
	err = synchronizer.initializeDataHandler()
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

	pipelines, err := synchronizer.GetPipelines()
	if err != nil {
		if err.Error() == "NotFound" {
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
