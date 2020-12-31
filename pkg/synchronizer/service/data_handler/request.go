package data_handler

import (
	"sync"

	dsa "github.com/BrobridgeOrg/gravity-api/service/dsa"
	"github.com/nats-io/nats.go"
)

type Request struct {
	msg   *nats.Msg
	input *dsa.PublishRequest
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &Request{}
	},
}

func (request *Request) Free() {
	requestPool.Put(request)
}

func (request *Request) GetMsg() *nats.Msg {
	return request.msg
}

func (request *Request) GetInput() *dsa.PublishRequest {
	return request.input
}
