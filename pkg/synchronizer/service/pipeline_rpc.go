package synchronizer

import (
	"fmt"

	pipeline_pb "github.com/BrobridgeOrg/gravity-api/service/pipeline"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

func (pipeline *Pipeline) initialize_rpc() error {

	err := pipeline.initRPC()
	if err != nil {
		return err
	}

	err = pipeline.initRPC_GetState()
	if err != nil {
		return err
	}

	err = pipeline.initialize_rpc_suspend()
	if err != nil {
		return err
	}

	err = pipeline.initialize_rpc_create_snapshot()
	if err != nil {
		return err
	}

	err = pipeline.initialize_rpc_release_snapshot()
	if err != nil {
		return err
	}

	err = pipeline.initialize_rpc_fetch_snapshot()
	if err != nil {
		return err
	}

	return nil
}

func (pipeline *Pipeline) initialize_rpc_suspend() error {

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
	}).Info("Initializing RPC gravity.pipeline.[id].suspend")

	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("gravity.pipeline.%d.suspend", pipeline.id)
	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		reply := &pipeline_pb.SuspendReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(reply)
			m.Respond(data)
		}()

		var request pipeline_pb.SuspendRequest
		err := proto.Unmarshal(m.Data, &request)
		if err != nil {
			log.Error(err)
			reply.Success = false
			reply.Reason = "Unknown parameters"
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
	})
	if err != nil {
		return err
	}

	return nil
}

func (pipeline *Pipeline) initialize_rpc_create_snapshot() error {

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
	}).Info("Initializing RPC gravity.pipeline.[id].createSnapshot")

	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("gravity.pipeline.%d.createSnapshot", pipeline.id)
	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		reply := &pipeline_pb.CreateSnapshotReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(reply)
			m.Respond(data)
		}()

		var request pipeline_pb.CreateSnapshotRequest
		err := proto.Unmarshal(m.Data, &request)
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

	})
	if err != nil {
		return err
	}

	return nil
}

func (pipeline *Pipeline) initialize_rpc_release_snapshot() error {

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
	}).Info("Initializing RPC gravity.pipeline.[id].releaseSnapshot")

	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("gravity.pipeline.%d.releaseSnapshot", pipeline.id)
	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		reply := &pipeline_pb.ReleaseSnapshotReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(reply)
			m.Respond(data)
		}()

		var request pipeline_pb.ReleaseSnapshotRequest
		err := proto.Unmarshal(m.Data, &request)
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
	})
	if err != nil {
		return err
	}

	return nil
}

func (pipeline *Pipeline) initialize_rpc_fetch_snapshot() error {

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
	}).Info("Initializing RPC gravity.pipeline.[id].fetchSnapshot")

	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("gravity.pipeline.%d.fetchSnapshot", pipeline.id)
	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		reply := &pipeline_pb.FetchSnapshotReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(reply)
			m.Respond(data)
		}()

		var request pipeline_pb.FetchSnapshotRequest
		err := proto.Unmarshal(m.Data, &request)
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

	})
	if err != nil {
		return err
	}

	return nil
}
