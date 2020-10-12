package synchronizer

import (
	"context"
	"errors"
	"fmt"

	controller "github.com/BrobridgeOrg/gravity-api/service/controller"
	pipeline_pb "github.com/BrobridgeOrg/gravity-api/service/pipeline"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

var SuccessReply, _ = proto.Marshal(&pipeline_pb.PipelineReply{
	Success: true,
})

var FailureReply, _ = proto.Marshal(&pipeline_pb.PipelineReply{
	Success: false,
	Reason:  "Failed to write to database",
})

type Pipeline struct {
	synchronizer *Synchronizer
	id           uint64
	subscription *nats.Subscription
	eventStore   *EventStore
}

func NewPipeline(synchronizer *Synchronizer, id uint64) *Pipeline {
	return &Pipeline{
		synchronizer: synchronizer,
		id:           id,
		eventStore:   NewEventStore(synchronizer, id),
	}
}

func (pipeline *Pipeline) Init() error {

	// Initializing event store
	err := pipeline.eventStore.Init()
	if err != nil {
		return err
	}

	// TODO: uninitialize event store if failed to initialize pipeline

	// Subscribe to pipeline
	connection := pipeline.synchronizer.eventBus.bus.GetConnection()
	channel := fmt.Sprintf("gravity.pipeline.%d", pipeline.id)
	log.WithFields(log.Fields{
		"channel": channel,
	}).Info("Subscribing to pipeline channel")
	sub, err := connection.Subscribe(channel, pipeline.handleMessage)
	if err != nil {
		return err
	}

	sub.SetPendingLimits(-1, -1)
	connection.Flush()

	pipeline.subscription = sub

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

	// call controller to release pipeline
	grpcConn, err := pipeline.synchronizer.controllerConns.Get()
	if err != nil {
		return err
	}

	// Create gRPC client
	client := controller.NewControllerClient(grpcConn)

	request := &controller.ReleasePipelinesRequest{
		ClientID: pipeline.synchronizer.clientID,
		Pipelines: []uint64{
			pipeline.id,
		},
	}

	reply, err := client.ReleasePipelines(context.Background(), request)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}

func (pipeline *Pipeline) handleMessage(m *nats.Msg) {

	// Event sourcing
	err := pipeline.eventStore.Write(m.Data)
	if err != nil {
		m.Respond(FailureReply)
		return
	}

	m.Respond(SuccessReply)
}
