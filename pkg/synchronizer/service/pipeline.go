package synchronizer

import (
	"context"
	"errors"
	"fmt"
	"gravity-synchronizer/pkg/synchronizer/service/request"

	controller "github.com/BrobridgeOrg/gravity-api/service/controller"
	pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

var counter uint64

/*
var SuccessReply, _ = proto.Marshal(&pipeline_pb.PipelineReply{
	Success: true,
})

var FailureReply, _ = proto.Marshal(&pipeline_pb.PipelineReply{
	Success: false,
	Reason:  "Failed to write to database",
})
*/
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

	// Initializing message receiver
	// TODO: close when pipeline was releaseed
	//go pipeline.messageReceiver()

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
	//sub, err := connection.Subscribe(channel, pipeline.handleMessage)
	sub, err := connection.Subscribe(channel, func(m *nats.Msg) {

		req := request.NewNATSRequest(m)

		pipeline.synchronizer.processEvent(pipeline, req)
		/*
			event := pipelineEventPool.Get().(*PipelineEvent)
			event.Pipeline = pipeline
			event.Payload = m
			pipeline.synchronizer.shard.Push(pipeline.id, event)
		*/
	})
	if err != nil {
		return err
	}

	sub.SetPendingLimits(-1, -1)
	connection.Flush()

	pipeline.subscription = sub

	// RPC
	err = pipeline.initRPC()
	if err != nil {
		return err
	}

	err = pipeline.initRPC_GetState()
	if err != nil {
		return err
	}

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

func (pipeline *Pipeline) initRPC() error {

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
	}).Info("Initializing pipeline RPC")

	connection := pipeline.synchronizer.eventBus.bus.GetConnection()
	channel := fmt.Sprintf("gravity.pipeline.%d.fetch", pipeline.id)
	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		var request pb.PipelineFetchRequest
		err := proto.Unmarshal(m.Data, &request)
		if err != nil {
			log.Error(err)
			return
		}

		// Find the subscriber
		subscriber := pipeline.synchronizer.subscriberMgr.Get(request.SubscriberID)
		if subscriber == nil {
			reply := &pb.PipelineFetchReply{
				Success: false,
				Reason:  "No such subscriber",
			}

			data, _ := proto.Marshal(reply)
			m.Respond(data)
			return
		}

		// Fetch data and push to subscriber
		count, lastSeq, err := subscriber.Fetch(pipeline, request.StartAt, request.Offset, int(request.Count))
		if err != nil {
			log.Error(err)
			reply := &pb.PipelineFetchReply{
				Success: false,
				Reason:  err.Error(),
			}

			data, _ := proto.Marshal(reply)
			m.Respond(data)
			return
		}

		// Success
		reply := &pb.PipelineFetchReply{
			LastSeq: lastSeq,
			Count:   uint64(count),
			Success: true,
		}

		data, _ := proto.Marshal(reply)
		m.Respond(data)
	})
	if err != nil {
		return err
	}

	return nil
}

func (pipeline *Pipeline) initRPC_GetState() error {

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
	}).Info("Initializing RPC pipeline.GetState")

	connection := pipeline.synchronizer.eventBus.bus.GetConnection()
	channel := fmt.Sprintf("gravity.pipeline.%d.getState", pipeline.id)
	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		var request pb.GetPipelineStateRequest
		err := proto.Unmarshal(m.Data, &request)
		if err != nil {
			log.Error(err)
			return
		}

		// TODO: Read state from storage

		// Success
		reply := &pb.GetPipelineStateReply{
			LastSeq: 0,
			Success: true,
		}

		data, _ := proto.Marshal(reply)
		m.Respond(data)
	})
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

func (pipeline *Pipeline) push(event *PipelineEvent) {
	event.Pipeline.handleRequest(event.Request)
	pipelineEventPool.Put(event)
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
}
