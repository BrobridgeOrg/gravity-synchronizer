package synchronizer

import (
	"fmt"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
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
	synchronizer.eventStoreRPC.Use(synchronizer.rpc_packetHandler)

	// Registering methods
	synchronizer.eventStoreRPC.Register("assignPipeline", synchronizer.rpc_requiredAuth, synchronizer.rpc_eventstore_assignPipeline)
	synchronizer.eventStoreRPC.Register("revokePipeline", synchronizer.rpc_requiredAuth, synchronizer.rpc_eventstore_revokePipeline)
	synchronizer.eventStoreRPC.Register("registerSubscriber", synchronizer.rpc_requiredAuth, synchronizer.rpc_eventstore_registerSubscriber)
	synchronizer.eventStoreRPC.Register("subscribeToCollections", synchronizer.rpc_requiredAuth, synchronizer.rpc_eventstore_subscribeToCollections)

	return synchronizer.eventStoreRPC.Apply()
}

func (synchronizer *Synchronizer) rpc_packetHandler(ctx *broc.Context) (interface{}, error) {

	var packet packet_pb.Packet
	err := proto.Unmarshal(ctx.Get("request").([]byte), &packet)
	if err != nil {
		// invalid request
		return nil, nil
	}

	ctx.Set("request", &packet)

	return ctx.Next()
}

func (synchronizer *Synchronizer) rpc_requiredAuth(ctx *broc.Context) (interface{}, error) {

	packet := ctx.Get("request").(*packet_pb.Packet)

	// find the key for specific app ID
	keyInfo := synchronizer.keyring.Get(packet.AppID)
	if keyInfo == nil {
		return ctx.Next()
	}

	// Decrypt
	data, err := keyInfo.Encryption().Decrypt(packet.Payload)
	if err != nil {
		return nil, nil
	}

	// pass decrypted payload to next handler
	packet.Payload = data
	returnedData, err := ctx.Next()
	if err != nil {
		return nil, err
	}

	// Encrypt
	encrypted, err := keyInfo.Encryption().Encrypt(returnedData.([]byte))
	if err != nil {
		return nil, nil
	}

	return encrypted, nil
}

func (synchronizer *Synchronizer) rpc_eventstore_assignPipeline(ctx *broc.Context) (returnedValue interface{}, err error) {

	var request pb.AssignPipelineRequest
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &request)
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

	var request pb.RevokePipelineRequest
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &request)
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

	var request pb.RegisterSubscriberRequest
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &request)
	if err != nil {
		log.Error(err)
		return nil, nil
	}

	reply := &pb.RegisterSubscriberReply{
		Success: true,
	}

	// Register subscriber
	subscriber := NewSubscriber(request.SubscriberID, request.Name, request.AppID)
	err = synchronizer.subscriberMgr.Register(request.SubscriberID, subscriber)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = err.Error()
	}

	// Add access key to keyring
	if len(request.AppID) > 0 && len(request.AccessKey) > 0 {
		synchronizer.keyring.Put(request.AppID, request.AccessKey)
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
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &request)
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
