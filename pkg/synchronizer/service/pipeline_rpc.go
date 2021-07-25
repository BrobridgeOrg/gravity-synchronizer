package synchronizer

import (
	"fmt"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	pipeline_pb "github.com/BrobridgeOrg/gravity-api/service/pipeline"
	pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

func (pipeline *Pipeline) initialize_rpc() error {

	log.WithFields(log.Fields{
		"domain":   pipeline.synchronizer.domain,
		"pipeline": pipeline.id,
	}).Info("Initializing RPC Handlers for Pipeline")

	// Initializing RPC handlers
	connection := pipeline.synchronizer.gravityClient.GetConnection()
	pipeline.rpcEngine = broc.NewBroc(connection)
	pipeline.rpcEngine.Use(pipeline.rpc_packetHandler)
	pipeline.rpcEngine.SetPrefix(fmt.Sprintf("%s.pipeline.%d.", pipeline.synchronizer.domain, pipeline.id))

	// Registering methods
	pipeline.rpcEngine.Register("fetch", pipeline.rpc_fetch)
	pipeline.rpcEngine.Register("getState", pipeline.rpc_getState)
	pipeline.rpcEngine.Register("suspend", pipeline.rpc_suspend)
	pipeline.rpcEngine.Register("createSnapshot", pipeline.rpc_createSnapshot)
	pipeline.rpcEngine.Register("releaseSnapshot", pipeline.rpc_releaseSnapshot)
	pipeline.rpcEngine.Register("fetchSnapshot", pipeline.rpc_fetchSnapshot)

	return pipeline.rpcEngine.Apply()
}

func (pipeline *Pipeline) rpc_packetHandler(ctx *broc.Context) (interface{}, error) {

	var packet packet_pb.Packet
	err := proto.Unmarshal(ctx.Get("request").([]byte), &packet)
	if err != nil {
		// invalid request
		return nil, nil
	}

	ctx.Set("request", &packet)

	return ctx.Next()
}

func (pipeline *Pipeline) rpc_fetch(ctx *broc.Context) (interface{}, error) {

	var request pb.PipelineFetchRequest
	packet := ctx.Get("request").(*packet_pb.Packet)
	err := proto.Unmarshal(packet.Payload, &request)
	if err != nil {
		log.Error(err)
		return nil, nil
	}

	// Find the subscriber
	subscriber := pipeline.synchronizer.subscriberMgr.Get(request.SubscriberID)
	if subscriber == nil {
		reply := &pb.PipelineFetchReply{
			Success: false,
			Reason:  "No such subscriber",
		}

		return proto.Marshal(reply)
	}

	// Fetch data and push to subscriber
	count, lastSeq, err := subscriber.Fetch(pipeline, request.StartAt, request.Offset, int(request.Count))
	if err != nil {
		log.Error(err)
		reply := &pb.PipelineFetchReply{
			Success: false,
			Reason:  err.Error(),
		}

		return proto.Marshal(reply)
	}

	// Success
	reply := &pb.PipelineFetchReply{
		LastSeq: lastSeq,
		Count:   uint64(count),
		Success: true,
	}

	return proto.Marshal(reply)
}

func (pipeline *Pipeline) rpc_getState(ctx *broc.Context) (interface{}, error) {

	var request pipeline_pb.GetStateRequest
	packet := ctx.Get("request").(*packet_pb.Packet)
	err := proto.Unmarshal(packet.Payload, &request)
	if err != nil {
		log.Error(err)
		return nil, nil
	}

	// Getting last sequence
	lastSeq := pipeline.eventStore.GetLastSequence()

	// Success
	reply := &pipeline_pb.GetStateReply{
		LastSeq: lastSeq,
		Success: true,
	}

	return proto.Marshal(reply)
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
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "Unknown parameters"
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
		reply.Reason = "No such subscriber"
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
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "Unknown parameters"
		return
	}

	// Create a new snapshot
	_, err = pipeline.snapshotManager.CreateSnapshot(request.SnapshotID)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = err.Error()
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
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "Unknown parameters"
		return
	}

	// Release snapshot
	err = pipeline.snapshotManager.ReleaseSnapshot(request.SnapshotID)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = err.Error()
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
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &request)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = "Unknown parameters"
		return
	}

	// Getting existing snapshot
	snapshot := pipeline.snapshotManager.GetSnapshot(request.SnapshotID)
	if snapshot == nil {
		reply.Success = false
		reply.Reason = "No such snapshot"
		return
	}

	// Fetch data from this snapshot
	count, lastKey, err := snapshot.Fetch(request.SubscriberID, request.Collection, request.Key, request.Offset, request.Count)
	if err != nil {
		log.Error(err)
		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	reply.Count = int64(count)
	reply.LastKey = lastKey

	return
}
