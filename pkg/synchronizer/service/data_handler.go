package synchronizer

import (
	"errors"
	"fmt"
	"gravity-synchronizer/pkg/synchronizer/service/data_handler"
	"gravity-synchronizer/pkg/synchronizer/service/request"
	"sync"
	"time"

	pb "github.com/BrobridgeOrg/gravity-api/service/pipeline"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

var replyPool = sync.Pool{
	New: func() interface{} {
		return &pb.PipelineReply{}
	},
}

func (synchronizer *Synchronizer) initializeDataHandler() error {

	synchronizer.dataHandler = data_handler.NewDataHandler()
	synchronizer.dataHandler.SetPipelineHandler(func(packet *data_handler.PipelinePacket) {

		// Store data
		task := packet.Task
		err := synchronizer.storeData(packet)
		if err != nil {
			log.Error(err)
			task.Fail(err)
			return
		}

		task.Done()
	})

	err := synchronizer.dataHandler.Init()
	if err != nil {
		return err
	}

	// Subscribe to quque to receive events
	connection := synchronizer.eventBus.bus.GetConnection()
	/*
		sub, err := connection.QueueSubscribe("gravity.dsa.incoming", "synchronizer", func(m *nats.Msg) {
			id := atomic.AddUint64((*uint64)(&counter), 1)
			if id%1000 == 0 {
				log.Info(id)
			}
			synchronizer.dataHandler.PushData(m)
		})
	*/
	sub, err := connection.QueueSubscribe("gravity.dsa.batch", "synchronizer", func(m *nats.Msg) {
		synchronizer.dataHandler.BatchPushData(m)
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
	if !ok {
		return synchronizer.pushToExternalWorker(packet)
	}

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

func (synchronizer *Synchronizer) pushToExternalWorker(packet *data_handler.PipelinePacket) error {

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
