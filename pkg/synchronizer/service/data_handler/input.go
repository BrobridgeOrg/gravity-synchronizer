package data_handler

import (
	"sync"

	dsa "github.com/BrobridgeOrg/gravity-api/service/dsa"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

var dsaReplyPool = sync.Pool{
	New: func() interface{} {
		return &dsa.PublishReply{}
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

func (dh *DataHandler) inputHandler(data interface{}, output chan interface{}) {

	request := requestPool.Get().(*Request)
	request.msg = data.(*nats.Msg)

	// Parsing
	request.input = &dsa.PublishRequest{}
	err := proto.Unmarshal(request.msg.Data, request.input)
	if err != nil {
		log.Error(err)
		request.msg.Respond(FailedReply(err.Error()))
		requestPool.Put(request)
		return
	}

	output <- request
}

func (dh *DataHandler) inputReceiver() {
	for {
		select {
		case inputData := <-dh.input.Output():

			request := inputData.(*Request)
			err := dh.processor.ProcessData(request)
			if err != nil {
				log.Error(err)
				request.msg.Respond(FailedReply(err.Error()))
			}
		}
	}
}
