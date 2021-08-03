package synchronizer

import (
	"fmt"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/middleware"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

func (synchronizer *Synchronizer) initEventStoreRPC() error {

	log.WithFields(log.Fields{
		"domain":   synchronizer.domain,
		"clientID": synchronizer.clientID,
	}).Info("Initializing RPC Handlers for EventStore")

	// Initializing authentication middleware
	m := middleware.NewMiddleware(map[string]interface{}{
		"Authentication": &middleware.Authentication{
			Enabled: true,
			Keyring: synchronizer.keyring,
		},
	})

	// Initializing RPC handlers
	connection := synchronizer.gravityClient.GetConnection()
	synchronizer.eventStoreRPC = broc.NewBroc(connection)
	synchronizer.eventStoreRPC.SetPrefix(fmt.Sprintf("%s.eventstore.%s.", synchronizer.domain, synchronizer.clientID))
	synchronizer.eventStoreRPC.Use(m.PacketHandler)

	// Registering methods
	synchronizer.eventStoreRPC.Register("updateKeyring", m.RequiredAuth("SYSTEM"), synchronizer.rpc_eventstore_updateKeyring)
	synchronizer.eventStoreRPC.Register("assignPipeline", m.RequiredAuth("SYSTEM"), synchronizer.rpc_eventstore_assignPipeline)
	synchronizer.eventStoreRPC.Register("revokePipeline", m.RequiredAuth("SYSTEM"), synchronizer.rpc_eventstore_revokePipeline)
	synchronizer.eventStoreRPC.Register("registerSubscriber", m.RequiredAuth("SYSTEM"), synchronizer.rpc_eventstore_registerSubscriber)
	synchronizer.eventStoreRPC.Register("subscribeToCollections", m.RequiredAuth("SYSTEM"), synchronizer.rpc_eventstore_subscribeToCollections)

	return synchronizer.eventStoreRPC.Apply()
}

func (synchronizer *Synchronizer) rpc_eventstore_updateKeyring(ctx *broc.Context) (returnedValue interface{}, err error) {

	reply := &pb.UpdateKeyringReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(reply)
		returnedValue = data
		err = e
	}()

	var request pb.UpdateKeyringRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
	if err != nil {
		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Update keyring
	for _, key := range request.Keys {

		log.WithFields(log.Fields{
			"appID": key.AppID,
		}).Info("Updating keyring")

		keyInfo := synchronizer.keyring.Get(key.AppID)
		if keyInfo == nil {
			// Create a new key info
			keyInfo = synchronizer.keyring.Put(key.AppID, string(key.Key))
		} else {
			keyInfo.Encryption().SetKey(key.Key)
		}

		// Update permissions
		keyInfo.Permission().Reset()
		keyInfo.Permission().AddPermissions(key.Permissions)
	}

	return
}

func (synchronizer *Synchronizer) rpc_eventstore_assignPipeline(ctx *broc.Context) (returnedValue interface{}, err error) {

	reply := &pb.AssignPipelineReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(reply)
		returnedValue = data
		err = e
	}()

	var request pb.AssignPipelineRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
	if err != nil {
		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	if request.ClientID != synchronizer.clientID {
		log.WithFields(log.Fields{
			"target": request.ClientID,
			"self":   synchronizer.clientID,
		}).Warn("Ignore request because client ID is incorrect")
		reply.Success = false
		reply.Reason = "InvalidClientID"
		return
	}

	log.WithFields(log.Fields{
		"pipeline": request.PipelineID,
	}).Info("Assigned pipeline")

	err = synchronizer.AssignPipeline(request.PipelineID)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (synchronizer *Synchronizer) rpc_eventstore_revokePipeline(ctx *broc.Context) (returnedValue interface{}, err error) {

	var request pb.RevokePipelineRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
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
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
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
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "UnknownParameters"
		return
	}

	if len(request.Collections) == 0 {
		reply.Success = false
		reply.Reason = "InvalidParameters"
		return
	}

	// Check collection permission
	key := ctx.Get("key").(*keyring.KeyInfo)
	targetCollections := make([]string, 0)
	for _, collection := range request.Collections {

		// Ignore collection that no permission to access
		if !key.Collection().Check(collection) {
			continue
		}

		targetCollections = append(targetCollections, collection)
	}

	if len(targetCollections) == 0 {
		reply.Success = false
		reply.Reason = "NoPermission"
		return
	}

	// Getting subscriber
	subscriber := synchronizer.subscriberMgr.Get(request.SubscriberID)
	if subscriber == nil {
		reply.Success = false
		reply.Reason = "NotFoundSubscriber"
		return
	}

	// Subscribe to collections
	collections, err := subscriber.SubscribeToCollections(targetCollections)
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
