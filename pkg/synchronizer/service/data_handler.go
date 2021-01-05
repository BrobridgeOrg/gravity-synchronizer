package synchronizer

import (
	"errors"
	"fmt"
	"gravity-synchronizer/pkg/synchronizer/service/data_handler"
	"gravity-synchronizer/pkg/synchronizer/service/request"
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
	synchronizer.dataHandler.SetPipelineHandler(func(packet *data_handler.PipelinePacket) {

		// Store data
		req := packet.Request
		err := synchronizer.storeData(packet)
		if err != nil {
			log.Error(err)
			req.GetMsg().Respond(FailedReply(err.Error()))
			req.Free()
			return
		}

		// Complete
		req.GetMsg().Respond(SuccessDSAReply)
		req.Free()
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
		err = synchronizer.dataHandler.ProcessData(m, &req)
		if err != nil {
			log.Error(err)
			m.Respond(FailedReply(err.Error()))
			return
		}
	})
	if err != nil {
		return err
	}

	sub.SetPendingLimits(-1, -1)
	connection.Flush()

	return nil
}

func (synchronizer *Synchronizer) storeData(packet *data_handler.PipelinePacket) error {

	// Trying to find pipeline from this node
	pipeline, ok := synchronizer.pipelines[uint64(packet.Data.PipelineID)]
	if ok {

		var err error
		var wg sync.WaitGroup
		wg.Add(1)

		// Found so create a customized request for internal pipeline
		customReq := request.NewCustomRequest(
			func() []byte {
				return packet.Data.Payload
			},
			func(e error) error {
				err = e
				wg.Done()
				return nil
			},
			func() error {
				wg.Done()
				return nil
			},
		)

		err = synchronizer.processEvent(pipeline, customReq)

		wg.Wait()

		return err
	}

	// Getting channel name to dispatch
	channel := fmt.Sprintf("gravity.pipeline.%d", packet.Data.PipelineID)

	// Send request
	connection := synchronizer.eventBus.bus.GetConnection()
	resp, err := connection.Request(channel, packet.Data.Payload, time.Second*5)
	packet.Free()
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
