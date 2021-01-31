package data_handler

import (
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type DataHandler struct {
	processor *Processor
	//	input      *parallel_chunked_flow.ParallelChunkedFlow
	batchInput *parallel_chunked_flow.ParallelChunkedFlow
}

func NewDataHandler() *DataHandler {
	return &DataHandler{
		processor: NewProcessor(),
	}
}

func (dh *DataHandler) Init() error {

	err := dh.processor.LoadRuleFile("./rules/rules.json")
	if err != nil {
		return err
	}

	//	dh.initInput()
	dh.initBatchInput()

	return nil
}

func (dh *DataHandler) BatchPushData(msg *nats.Msg) error {
	return dh.batchInput.Push(msg)
}

/*
func (dh *DataHandler) PushData(msg *nats.Msg) error {
	return dh.input.Push(msg)
}
*/
/*
func (dh *DataHandler) ProcessData(msg *nats.Msg, req *dsa.PublishRequest) error {

	request := requestPool.Get().(*Request)
	request.msg = msg
	request.input = req

	return dh.processor.ProcessData(request)
}
*/
func (dh *DataHandler) SetPipelineHandler(fn func(*PipelinePacket)) {
	dh.processor.SetPipelineHandler(fn)
}
