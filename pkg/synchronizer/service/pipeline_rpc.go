package synchronizer

import (
	"fmt"

	pipeline_pb "github.com/BrobridgeOrg/gravity-api/service/pipeline"
	pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

func (pipeline *Pipeline) initialize_rpc() error {

	err := pipeline.initialize_rpc_pipeline_fetch()
	if err != nil {
		return err
	}

	err = pipeline.initialize_rpc_pipeline_get_state()
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

func (pipeline *Pipeline) initialize_rpc_pipeline_fetch() error {

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
	}).Info("Initializing pipeline RPC")

	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.pipeline.%d.fetch", pipeline.synchronizer.domain, pipeline.id)
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

func (pipeline *Pipeline) initialize_rpc_pipeline_get_state() error {

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
	}).Info("Initializing RPC pipeline.[id].getState")

	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.pipeline.%d.getState", pipeline.synchronizer.domain, pipeline.id)
	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		var request pipeline_pb.GetStateRequest
		err := proto.Unmarshal(m.Data, &request)
		if err != nil {
			log.Error(err)
			return
		}

		// Getting last sequence
		lastSeq := pipeline.eventStore.GetLastSequence()

		// Success
		reply := &pipeline_pb.GetStateReply{
			LastSeq: lastSeq,
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

func (pipeline *Pipeline) initialize_rpc_suspend() error {

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
	}).Info("Initializing RPC pipeline.[id].suspend")

	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.pipeline.%d.suspend", pipeline.synchronizer.domain, pipeline.id)
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
	}).Info("Initializing RPC pipeline.[id].createSnapshot")

	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.pipeline.%d.createSnapshot", pipeline.synchronizer.domain, pipeline.id)
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
		/*
			// Getting subscriber
			subscriber := pipeline.synchronizer.subscriberMgr.Get(request)
			if subscriber == nil {
				reply.Success = false
				reply.Reason = "No such subscriber"
				return
			}
		*/
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
	}).Info("Initializing RPC pipeline.[id].releaseSnapshot")

	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.pipeline.%d.releaseSnapshot", pipeline.synchronizer.domain, pipeline.id)
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
	}).Info("Initializing RPC pipeline.[id].fetchSnapshot")

	connection := pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.pipeline.%d.fetchSnapshot", pipeline.synchronizer.domain, pipeline.id)
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
