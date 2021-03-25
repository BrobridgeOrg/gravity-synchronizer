package data_handler

import (
	"sync"

	dsa "github.com/BrobridgeOrg/gravity-api/service/dsa"
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

var batchPublishReplyPool = sync.Pool{
	New: func() interface{} {
		return &dsa.BatchPublishReply{}
	},
}

func BatchPublishFailureReply(successCount int32, reason string) []byte {
	reply := batchPublishReplyPool.Get().(*dsa.BatchPublishReply)
	reply.Success = false
	reply.SuccessCount = successCount
	reply.Reason = reason
	resp, _ := proto.Marshal(reply)
	batchPublishReplyPool.Put(reply)
	return resp
}

func (dh *DataHandler) initBatchInput() error {

	// Initialize parapllel chunked flow
	pcfOpts := &parallel_chunked_flow.Options{
		BufferSize: 1024000,
		ChunkSize:  512,
		ChunkCount: 512,
		Handler:    dh.batchInputHandler,
	}

	dh.batchInput = parallel_chunked_flow.NewParallelChunkedFlow(pcfOpts)

	go dh.batchInputReceiver()

	return nil
}

func (dh *DataHandler) batchInputHandler(data interface{}, publish func(interface{})) {

	request := batchRequestPool.Get().(*BatchRequest)
	request.msg = data.(*nats.Msg)

	// Parsing
	input := &dsa.BatchPublishRequest{}
	err := proto.Unmarshal(request.msg.Data, input)
	if err != nil {
		log.Error(err)
		request.msg.Respond(BatchPublishFailureReply(0, err.Error()))
		batchRequestPool.Put(request)
		return
	}

	request.SetInput(input)

	publish(request)
}

func (dh *DataHandler) batchInputReceiver() {
	for {
		select {
		case batchInputData := <-dh.batchInput.Output():

			request := batchInputData.(*BatchRequest)
			dh.handleBatchInput(request)
		}
	}
}

func (dh *DataHandler) handleBatchInput(batchRequest *BatchRequest) error {

	for _, req := range batchRequest.input.Requests {

		// Prepare task
		task := batchTaskPool.Get().(*BatchTask)
		task.Request = batchRequest
		task.EventName = req.EventName
		task.Meta = req.Meta
		task.RawPayload = req.Payload

		dh.processor.ProcessTask(task)
	}

	return nil
}
