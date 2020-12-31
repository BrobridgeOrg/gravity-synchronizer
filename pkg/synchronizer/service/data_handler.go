package synchronizer

import (
	"errors"
	"fmt"
	"gravity-synchronizer/pkg/synchronizer/service/data_handler"
	"sync"
	"time"

	dsa "github.com/BrobridgeOrg/gravity-api/service/dsa"
	pb "github.com/BrobridgeOrg/gravity-api/service/pipeline"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

var SuccessDSAReply, _ = proto.Marshal(&dsa.PublishReply{
	Success: true,
})

var dsaReplyPool = sync.Pool{
	New: func() interface{} {
		return &dsa.PublishReply{}
	},
}

var replyPool = sync.Pool{
	New: func() interface{} {
		return &pb.PipelineReply{}
	},
}

func FailedReply(reason string) []byte {
	reply := dsaReplyPool.Get().(*dsa.PublishReply)
	reply.Success = false
	reply.Reason = reason
	resp, _ := proto.Marshal(reply)
	dsaReplyPool.Put(reply)
	return resp
}

func (synchronizer *Synchronizer) initializeDataHandler() error {

	synchronizer.dataHandler = data_handler.NewDataHandler()
	synchronizer.dataHandler.SetPipelineHandler(func(data *data_handler.PipelineData) {

		// Store data
		request := data.Request
		err := synchronizer.storeData(data)
		if err != nil {
			request.GetMsg().Respond(FailedReply(err.Error()))
			request.Free()
			return
		}

		// Complete
		request.GetMsg().Respond(SuccessDSAReply)
		request.Free()
	})

	err := synchronizer.dataHandler.Init()
	if err != nil {
		return err
	}

	// Subscribe to quque to receive events
	connection := synchronizer.eventBus.bus.GetConnection()
	sub, err := connection.QueueSubscribe("gravity.dsa.incoming", "synchronizer", func(m *nats.Msg) {

		// Parse incoming data
		var req dsa.PublishRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)
			m.Respond(FailedReply(err.Error()))
			return
		}

		// Do preprocess and mapping job
		synchronizer.dataHandler.ProcessData(m, &req)
	})
	if err != nil {
		return err
	}

	sub.SetPendingLimits(-1, -1)
	connection.Flush()

	return nil
}

func (synchronizer *Synchronizer) storeData(data *data_handler.PipelineData) error {

	// TODO: check whether pipeline is located on here

	// Getting channel name to dispatch
	channel := fmt.Sprintf("gravity.pipeline.%d", data.PipelineID)

	// Send request
	connection := synchronizer.eventBus.bus.GetConnection()
	resp, err := connection.Request(channel, data.Payload, time.Second*5)
	data.Free()
	if err != nil {
		return err
	}

	// Parsing response
	reply := replyPool.Get().(*pb.PipelineReply)
	err = proto.Unmarshal(resp.Data, reply)
	if err != nil {
		// Release
		replyPool.Put(reply)
		return err
	}

	if !reply.Success {
		err = errors.New(reply.Reason)

		// Release
		replyPool.Put(reply)

		return err
	}

	// Release
	replyPool.Put(reply)

	return nil
}
