package synchronizer

import (
	"fmt"
	"gravity-synchronizer/pkg/synchronizer/service/request"

	synchronizer_manager "github.com/BrobridgeOrg/gravity-sdk/synchronizer_manager"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

var counter uint64

type Pipeline struct {
	synchronizer    *Synchronizer
	id              uint64
	subscription    *nats.Subscription
	eventStore      *EventStore
	snapshotManager *SnapshotManager
}

func NewPipeline(synchronizer *Synchronizer, id uint64) *Pipeline {

	pipeline := &Pipeline{
		synchronizer: synchronizer,
		id:           id,
		eventStore:   NewEventStore(synchronizer, id),
	}

	pipeline.snapshotManager = NewSnapshotManager(pipeline)

	return pipeline
}

func (pipeline *Pipeline) Init() error {

	// Initializing event store
	err := pipeline.eventStore.Init()
	if err != nil {
		return err
	}

	// TODO: uninitialize event store if failed to initialize pipeline

	// Subscribe to pipeline
	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.pipeline.%d", pipeline.synchronizer.domain, pipeline.id)
	log.WithFields(log.Fields{
		"channel": channel,
	}).Info("Subscribing to pipeline channel")
	sub, err := connection.Subscribe(channel, func(m *nats.Msg) {

		req := request.NewNATSRequest(m)

		pipeline.synchronizer.processEvent(pipeline, req)
	})
	if err != nil {
		return err
	}

	sub.SetPendingLimits(-1, -1)
	connection.Flush()

	pipeline.subscription = sub

	// RPC
	err = pipeline.initialize_rpc()
	if err != nil {
		return err
	}

	// Awake all existing subscribers
	pipeline.synchronizer.subscriberMgr.AwakeAllSubscribers(pipeline)

	return nil
}

func (pipeline *Pipeline) Uninit() error {

	// Stop receiving data and process events left from pipeline
	err := pipeline.subscription.Drain()
	if err != nil {
		return err
	}

	// Close data store
	pipeline.eventStore.Close()

	// Release pipeline
	err = pipeline.release()
	if err != nil {
		return err
	}

	return nil
}

func (pipeline *Pipeline) release() error {

	sm := synchronizer_manager.NewSynchronizerManagerWithClient(pipeline.synchronizer.gravityClient, synchronizer_manager.NewOptions())
	return sm.ReleasePipelines(
		pipeline.synchronizer.clientID,
		[]uint64{
			pipeline.id,
		},
	)
}

func (pipeline *Pipeline) push(event *PipelineEvent) {
	event.Pipeline.handleRequest(event.Request)
	pipelineEventPool.Put(event)
}

func (pipeline *Pipeline) awakeSubscriber() {

	pipeline.synchronizer.subscriberMgr.subscribers.Range(func(key interface{}, value interface{}) bool {

		subscriber := value.(*Subscriber)

		// If this pipeline was suspended by this subscriber
		if _, ok := subscriber.suspendPipelines.Load(pipeline.id); ok {
			err := subscriber.Awake(pipeline)
			if err != nil {
				log.WithFields(log.Fields{
					"pipeline":   pipeline.id,
					"subscriber": subscriber.id,
				}).Error(err)
				return true
			}

			subscriber.suspendPipelines.Delete(pipeline.id)
		}

		return true
	})
}

func (pipeline *Pipeline) handleRequest(req request.Request) {
	/*
		id := atomic.AddUint64((*uint64)(&counter), 1)
		if id%1000 == 0 {
			log.Info(id)
		}
	*/
	// Event sourcing
	err := pipeline.eventStore.Write(req.Data())
	if err != nil {
		req.Error(err)
		//		req.Respond(FailureReply)
		return
	}

	//	req.Respond(SuccessReply)
	req.Respond()

	// Awake susbscriber to receive data from this pipeline
	pipeline.awakeSubscriber()
}

func (pipeline *Pipeline) GetLastSequence() uint64 {
	return pipeline.eventStore.GetLastSequence()
}

func (pipeline *Pipeline) store(data []byte) error {
	/*
		id := atomic.AddUint64((*uint64)(&counter), 1)
		log.WithFields(log.Fields{
			"pipeline": pipeline.id,
		}).Info(id)
	*/
	/*
		id := atomic.AddUint64((*uint64)(&counter), 1)
		if id%1000 == 0 {
			log.Info(id)
		}
	*/
	// Event sourcing
	err := pipeline.eventStore.Write(data)
	if err != nil {
		return err
	}

	// Awake susbscriber to receive data from this pipeline
	pipeline.awakeSubscriber()

	return nil
}
