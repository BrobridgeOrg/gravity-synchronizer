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

	err = pipeline.initialize_rpc_create_snapshot()
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

		var request pipeline_pb.CreateSnapshotRequest
		err := proto.Unmarshal(m.Data, &request)
		if err != nil {
			log.Error(err)
			return
		}

		// Getting last sequence
		//lastSeq := pipeline.eventStore.GetLastSequence()

		// Success
		reply := &pipeline_pb.CreateSnapshotReply{
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
