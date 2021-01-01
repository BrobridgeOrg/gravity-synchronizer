package request

import (
	"sync"

	pipeline_pb "github.com/BrobridgeOrg/gravity-api/service/pipeline"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
)

var SuccessReply, _ = proto.Marshal(&pipeline_pb.PipelineReply{
	Success: true,
})

var natsRequestPool = sync.Pool{
	New: func() interface{} {
		return &NATSRequest{}
	},
}

func NewNATSRequest(m *nats.Msg) Request {
	request := natsRequestPool.Get().(*NATSRequest)
	request.msg = m

	return Request(request)
}

type NATSRequest struct {
	msg *nats.Msg
}

func (request *NATSRequest) Free() {
	natsRequestPool.Put(request)
}

func (request *NATSRequest) SetMsg(msg *nats.Msg) {
	request.msg = msg
}

func (request *NATSRequest) Data() []byte {
	return request.msg.Data
}

func (request *NATSRequest) Error(err error) error {

	var FailureReply, _ = proto.Marshal(&pipeline_pb.PipelineReply{
		Success: false,
		Reason:  err.Error(),
	})

	return request.msg.Respond(FailureReply)
}

func (request *NATSRequest) Respond() error {
	return request.msg.Respond(SuccessReply)
}
