package synchronizer

import (
	"fmt"
	"os"
	"strings"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/app"
	grpc_connection_pool "github.com/cfsghost/grpc-connection-pool"
	log "github.com/sirupsen/logrus"
)

type Synchronizer struct {
	app             app.App
	eventBus        *EventBus
	controllerConns *grpc_connection_pool.GRPCPool
	clientID        string
	pipelines       map[uint64]*Pipeline
}

func NewSynchronizer(a app.App) *Synchronizer {
	synchronizer := &Synchronizer{
		app:       a,
		pipelines: make(map[uint64]*Pipeline, 0),
	}

	synchronizer.eventBus = NewEventBus(synchronizer)

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

	// Initializing eventbus
	err = synchronizer.eventBus.Initialize()
	if err != nil {
		return err
	}

	// Initializing RPC handlers
	err = synchronizer.initRPC()
	if err != nil {
		return err
	}

	// Initializing controller connection pool
	err = synchronizer.initControllerConnection()
	if err != nil {
		return err
	}

	// Recovery subscriptions
	err = synchronizer.recoverySubscriptions()
	if err != nil {
		return err
	}

	// register synchronizer
	err = synchronizer.RegisterClient()
	if err != nil {
		return err
	}

	// TODO: health checks

	return nil
}

func (synchronizer *Synchronizer) recoverySubscriptions() error {

	log.Info("Attempt to recovery pipeline subscriptions")

	pipelines, err := synchronizer.GetPipelines()
	if err != nil {
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
