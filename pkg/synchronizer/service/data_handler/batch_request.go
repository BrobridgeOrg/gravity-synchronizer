package data_handler

import (
	"sync"
	"sync/atomic"

	dsa "github.com/BrobridgeOrg/gravity-api/service/dsa"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
)

type BatchRequest struct {
	msg         *nats.Msg
	input       *dsa.BatchPublishRequest
	isCompleted bool
	total       uint32
	done        uint32
}

var batchRequestPool = sync.Pool{
	New: func() interface{} {
		return &BatchRequest{}
	},
}

var dsaReplyPool = sync.Pool{
	New: func() interface{} {
		return &dsa.PublishReply{}
	},
}

var SuccessReply, _ = proto.Marshal(&dsa.BatchPublishReply{
	Success: true,
})

func FailureReply(reason string) []byte {
	reply := dsaReplyPool.Get().(*dsa.BatchPublishReply)
	reply.Success = false
	reply.Reason = reason
	resp, _ := proto.Marshal(reply)
	dsaReplyPool.Put(reply)
	return resp
}

func (request *BatchRequest) Free() {
	atomic.StoreUint32(&request.done, 0)
	request.isCompleted = false
	batchRequestPool.Put(request)
}

func (request *BatchRequest) Done() {

	// already done
	if request.isCompleted {
		return
	}

	count := atomic.AddUint32(&request.done, 1)

	// All requests were done
	if count == request.total {
		request.isCompleted = true
		request.msg.Respond(SuccessReply)
		request.Free()
	}
}

func (request *BatchRequest) Fail(err error) {

	// already done
	if request.isCompleted {
		return
	}

	request.isCompleted = true
	request.msg.Respond(BatchPublishFailureReply(0, err.Error()))
	request.Free()
}

func (request *BatchRequest) GetMsg() *nats.Msg {
	return request.msg
}

func (request *BatchRequest) SetInput(input *dsa.BatchPublishRequest) {
	request.input = input
	request.total = uint32(len(input.Requests))
}

func (request *BatchRequest) GetInput() *dsa.BatchPublishRequest {
	return request.input
}
