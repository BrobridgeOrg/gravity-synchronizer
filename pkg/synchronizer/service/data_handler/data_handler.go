package data_handler

import (
	dsa "github.com/BrobridgeOrg/gravity-api/service/dsa"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type DataHandler struct {
	processor *Processor
}

func NewDataHandler() *DataHandler {
	return &DataHandler{
		processor: NewProcessor(),
	}
}

func (dh *DataHandler) Init() error {
	return dh.processor.LoadRuleFile("./rules/rules.json")
}

func (dh *DataHandler) ProcessData(msg *nats.Msg, req *dsa.PublishRequest) error {

	request := requestPool.Get().(*Request)
	request.msg = msg
	request.input = req

	return dh.processor.ProcessData(request)
}

/*
func (dh *DataHandler) ProcessData(eventName string, data []byte, meta map[string][]byte) error {
	return dh.processor.ProcessData(eventName, data, meta)
}
*/
func (dh *DataHandler) SetPipelineHandler(fn func(*PipelineData)) {
	dh.processor.SetPipelineHandler(fn)
}
