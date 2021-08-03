package synchronizer

import (
	"fmt"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	pipeline_pb "github.com/BrobridgeOrg/gravity-api/service/pipeline"
	pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/middleware"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

func (pipeline *Pipeline) initialize_rpc() error {

	log.WithFields(log.Fields{
		"domain":   pipeline.synchronizer.domain,
		"pipeline": pipeline.id,
	}).Info("Initializing RPC Handlers for Pipeline")

	// Initializing authentication middleware
	m := middleware.NewMiddleware(map[string]interface{}{
		"Authentication": &middleware.Authentication{
			Enabled: true,
			Keyring: pipeline.synchronizer.keyring,
		},
	})

	// Initializing RPC handlers
	connection := pipeline.synchronizer.gravityClient.GetConnection()
	pipeline.rpcEngine = broc.NewBroc(connection)
	pipeline.rpcEngine.SetPrefix(fmt.Sprintf("%s.pipeline.%d.", pipeline.synchronizer.domain, pipeline.id))
	pipeline.rpcEngine.Use(m.PacketHandler)

	// Registering methods
	pipeline.rpcEngine.Register("fetch", m.RequiredAuth("SYSTEM", "SUBSCRIBER"), pipeline.rpc_fetch)
	pipeline.rpcEngine.Register("getState", m.RequiredAuth("SYSTEM", "SUBSCRIBER"), pipeline.rpc_getState)
	pipeline.rpcEngine.Register("suspend", m.RequiredAuth("SYSTEM", "SUBSCRIBER"), pipeline.rpc_suspend)
	pipeline.rpcEngine.Register("createSnapshot", m.RequiredAuth("SYSTEM", "SUBSCRIBER"), pipeline.rpc_createSnapshot)
	pipeline.rpcEngine.Register("releaseSnapshot", m.RequiredAuth("SYSTEM", "SUBSCRIBER"), pipeline.rpc_releaseSnapshot)
	pipeline.rpcEngine.Register("fetchSnapshot", m.RequiredAuth("SYSTEM", "SUBSCRIBER"), pipeline.rpc_fetchSnapshot)

	return pipeline.rpcEngine.Apply()
}

func (pipeline *Pipeline) rpc_fetch(ctx *broc.Context) (returnedValue interface{}, err error) {

	reply := &pb.PipelineFetchReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(reply)
		returnedValue = data
		err = e
	}()

	var request pb.PipelineFetchRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "UnknownParameters"
	}

	// Find the subscriber
	subscriber := pipeline.synchronizer.subscriberMgr.Get(request.SubscriberID)
	if subscriber == nil {
		reply.Success = false
		reply.Reason = "NotFoundSubscriber"
		return
	}

	// Fetch data and push to subscriber
	count, lastSeq, err := subscriber.Fetch(pipeline, request.StartAt, request.Offset, int(request.Count))
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "InternalError"
		return
	}

	// Success
	reply.LastSeq = lastSeq
	reply.Count = uint64(count)

	return
}

func (pipeline *Pipeline) rpc_getState(ctx *broc.Context) (returnedValue interface{}, err error) {

	reply := &pipeline_pb.GetStateReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(reply)
		returnedValue = data
		err = e
	}()

	var request pipeline_pb.GetStateRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "UnknownParameters"
		return
	}

	// Getting last sequence
	reply.LastSeq = pipeline.eventStore.GetLastSequence()

	return
}

func (pipeline *Pipeline) rpc_suspend(ctx *broc.Context) (returnedValue interface{}, err error) {

	reply := &pipeline_pb.SuspendReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(reply)
		returnedValue = data
		err = e
	}()

	var request pipeline_pb.SuspendRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "UnknownParameters"
		return
	}

	// This subscriber shouldn't suspend
	if pipeline.GetLastSequence() > request.Sequence {
		reply.Success = false
		return
	}

	subscriber := pipeline.synchronizer.subscriberMgr.Get(request.SubscriberID)
	if subscriber == nil {
		reply.Success = false
		reply.Reason = "NotFoundSubscriber"
		return
	}

	log.WithFields(log.Fields{
		"subscriber": request.SubscriberID,
		"pipeline":   pipeline.id,
	}).Info("Subscriber is suspended")

	subscriber.suspendPipelines.Store(pipeline.id, pipeline)

	// This subscriber shouldn't suspend
	if pipeline.GetLastSequence() > request.Sequence {
		reply.Success = false
		return
	}

	return
}

func (pipeline *Pipeline) rpc_createSnapshot(ctx *broc.Context) (returnedValue interface{}, err error) {

	reply := &pipeline_pb.CreateSnapshotReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(reply)
		returnedValue = data
		err = e
	}()

	var request pipeline_pb.CreateSnapshotRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "UnknownParameters"
		return
	}

	// Create a new snapshot
	_, err = pipeline.snapshotManager.CreateSnapshot(request.SnapshotID)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "InternalError"
		return
	}

	return
}

func (pipeline *Pipeline) rpc_releaseSnapshot(ctx *broc.Context) (returnedValue interface{}, err error) {

	reply := &pipeline_pb.ReleaseSnapshotReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(reply)
		returnedValue = data
		err = e
	}()

	var request pipeline_pb.ReleaseSnapshotRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "UnknownParameters"
		return
	}

	// Release snapshot
	err = pipeline.snapshotManager.ReleaseSnapshot(request.SnapshotID)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "InternalError"
		return
	}

	return
}

func (pipeline *Pipeline) rpc_fetchSnapshot(ctx *broc.Context) (returnedValue interface{}, err error) {

	reply := &pipeline_pb.FetchSnapshotReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(reply)
		returnedValue = data
		err = e
	}()

	var request pipeline_pb.FetchSnapshotRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "UnknownParameters"
		return
	}

	// Check collection permission
	key := ctx.Get("key").(*keyring.KeyInfo)
	if !key.Collection().Check(request.Collection) {
		reply.Success = false
		reply.Reason = "NoPermission"
		return
	}

	// Getting existing snapshot
	snapshot := pipeline.snapshotManager.GetSnapshot(request.SnapshotID)
	if snapshot == nil {
		reply.Success = false
		reply.Reason = "NotFoundSnapshot"
		return
	}

	// Fetch data from this snapshot
	count, lastKey, err := snapshot.Fetch(request.SubscriberID, request.Collection, request.Key, request.Offset, request.Count)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "InternalError"
		return
	}

	reply.Count = int64(count)
	reply.LastKey = lastKey

	return
}
