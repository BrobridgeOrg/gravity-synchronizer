package synchronizer

import (
	"fmt"

	"github.com/BrobridgeOrg/broc"
	pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

func (synchronizer *Synchronizer) initRPC() error {

	log.WithFields(log.Fields{
		"domain":   synchronizer.domain,
		"clientID": synchronizer.clientID,
	}).Info("Initializing RPC Handlers for EventStore")

	// Initializing RPC handlers
	connection := synchronizer.gravityClient.GetConnection()
	synchronizer.eventStoreRPC = broc.NewBroc(connection)
	synchronizer.eventStoreRPC.SetPrefix(fmt.Sprintf("%s.eventstore.%s.", synchronizer.domain, synchronizer.clientID))

	// Registering methods
	synchronizer.eventStoreRPC.Register("AssignPipeline", synchronizer.rpc_eventstore_assignPipeline)
	synchronizer.eventStoreRPC.Register("RevokePipeline", synchronizer.rpc_eventstore_revokePipeline)
	synchronizer.eventStoreRPC.Register("registerSubscriber", synchronizer.rpc_eventstore_registerSubscriber)
	synchronizer.eventStoreRPC.Register("subscribeToCollections", synchronizer.rpc_eventstore_subscribeToCollections)

	return synchronizer.eventStoreRPC.Apply()
}

func (synchronizer *Synchronizer) rpc_eventstore_assignPipeline(ctx *broc.Context) (returnedValue interface{}, err error) {

	data := ctx.Get("request").([]byte)

	var request pb.AssignPipelineRequest
	err = proto.Unmarshal(data, &request)
	if err != nil {
		log.Error(err)
		return nil, nil
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

	return proto.Marshal(reply)
}

func (synchronizer *Synchronizer) rpc_eventstore_revokePipeline(ctx *broc.Context) (returnedValue interface{}, err error) {

	data := ctx.Get("request").([]byte)

	var request pb.RevokePipelineRequest
	err = proto.Unmarshal(data, &request)
	if err != nil {
		log.Error(err)
		return nil, nil
	}

	if request.ClientID != synchronizer.clientID {
		log.Warn("Ignore request because client ID is incorrect")
		return nil, nil
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

	return proto.Marshal(reply)
}

func (synchronizer *Synchronizer) rpc_eventstore_registerSubscriber(ctx *broc.Context) (returnedValue interface{}, err error) {

	data := ctx.Get("request").([]byte)

	var request pb.RegisterSubscriberRequest
	err = proto.Unmarshal(data, &request)
	if err != nil {
		log.Error(err)
		return nil, nil
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

	return proto.Marshal(reply)
}

func (synchronizer *Synchronizer) rpc_eventstore_subscribeToCollections(ctx *broc.Context) (returnedValue interface{}, err error) {

	reply := &pb.SubscribeToCollectionsReply{}
	defer func() {
		data, e := proto.Marshal(reply)
		returnedValue = data
		err = e
	}()

	var request pb.SubscribeToCollectionsRequest
	data := ctx.Get("request").([]byte)
	err = proto.Unmarshal(data, &request)
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

	return
}
