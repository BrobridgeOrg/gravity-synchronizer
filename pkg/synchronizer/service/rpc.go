package synchronizer

import (
	"fmt"

	pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

func (synchronizer *Synchronizer) initRPC() error {

	connection := synchronizer.eventBus.bus.GetConnection()

	assignPipelineCh := fmt.Sprintf("gravity.eventstore.%s.AssignPipeline", synchronizer.clientID)

	log.WithFields(log.Fields{
		"channel": assignPipelineCh,
	}).Info("Subscribing to RPC channel: AssignPipeline")

	_, err := connection.Subscribe(assignPipelineCh, func(m *nats.Msg) {

		var request pb.AssignPipelineRequest
		err := proto.Unmarshal(m.Data, &request)
		if err != nil {
			log.Error(err)
			return
		}

		if request.ClientID != synchronizer.clientID {
			log.Warn("Ignore request because client ID is incorrect")
			return
		}

		log.WithFields(log.Fields{
			"pipeline": request.PipelineID,
		}).Info("Assigned pipeline")

		reply := &pb.AssignPipelineReply{
			Success: true,
		}
		err = synchronizer.AssignPipeline(request.PipelineID)
		if err != nil {
			log.Error(err)
			reply.Success = false
			reply.Reason = err.Error()
		}

		data, _ := proto.Marshal(reply)
		m.Respond(data)
	})
	if err != nil {
		return err
	}

	revokePipelineCh := fmt.Sprintf("gravity.eventstore.%s.RevokePipeline", synchronizer.clientID)

	log.WithFields(log.Fields{
		"channel": revokePipelineCh,
	}).Info("Subscribing to RPC channel: RevokePipeline")

	_, err = connection.Subscribe(revokePipelineCh, func(m *nats.Msg) {

		var request pb.RevokePipelineRequest
		err := proto.Unmarshal(m.Data, &request)
		if err != nil {
			log.Error(err)
			return
		}

		if request.ClientID != synchronizer.clientID {
			log.Warn("Ignore request because client ID is incorrect")
			return
		}

		reply := &pb.RevokePipelineReply{
			Success: true,
		}
		err = synchronizer.RevokePipeline(request.PipelineID)
		if err != nil {
			log.Error(err)
			reply.Success = false
			reply.Reason = err.Error()
		}

		data, _ := proto.Marshal(reply)
		m.Respond(data)
	})
	if err != nil {
		return err
	}

	err = synchronizer.init_rpc_subscriber_register()
	if err != nil {
		return err
	}

	err = synchronizer.init_rpc_subscriber_subscribe_to_collections()
	if err != nil {
		return err
	}

	return nil
}

func (synchronizer *Synchronizer) init_rpc_subscriber_register() error {

	channel := fmt.Sprintf("gravity.eventstore.%s.registerSubscriber", synchronizer.clientID)

	log.WithFields(log.Fields{
		"channel": channel,
	}).Info("Subscribing to RPC channel: registerSubscriber")

	connection := synchronizer.eventBus.bus.GetConnection()
	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		var request pb.RegisterSubscriberRequest
		err := proto.Unmarshal(m.Data, &request)
		if err != nil {
			log.Error(err)
			return
		}

		reply := &pb.RegisterSubscriberReply{
			Success: true,
		}

		// Register subscriber
		subscriber := NewSubscriber(request.SubscriberID, request.Name)
		err = synchronizer.subscriberMgr.Register(request.SubscriberID, subscriber)
		if err != nil {
			log.Error(err)
			reply.Success = false
			reply.Reason = err.Error()
		}

		data, _ := proto.Marshal(reply)
		m.Respond(data)
	})
	if err != nil {
		return err
	}
	return nil
}

func (synchronizer *Synchronizer) init_rpc_subscriber_subscribe_to_collections() error {

	channel := fmt.Sprintf("gravity.eventstore.%s.subscribeToCollections", synchronizer.clientID)

	log.WithFields(log.Fields{
		"channel": channel,
	}).Info("Subscribing to RPC channel: subscribeToCollections")

	connection := synchronizer.eventBus.bus.GetConnection()
	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		reply := &pb.SubscribeToCollectionsReply{}
		defer func() {
			data, _ := proto.Marshal(reply)
			m.Respond(data)
		}()

		var request pb.SubscribeToCollectionsRequest
		err := proto.Unmarshal(m.Data, &request)
		if err != nil {
			log.Error(err)
			reply.Success = false
			reply.Reason = "Unknown parameters"
			return
		}

		// Getting subscriber
		subscriber := synchronizer.subscriberMgr.Get(request.SubscriberID)
		if subscriber == nil {
			reply.Success = false
			reply.Reason = "No such subscriber"
			return
		}

		// Subscribe to collections
		collections, err := subscriber.SubscribeToCollections(request.Collections)
		if err != nil {
			log.Error(err)
			reply.Success = false
			reply.Reason = err.Error()
			return
		}

		reply.Success = true
		reply.Collections = collections
	})
	if err != nil {
		return err
	}
	return nil
}
