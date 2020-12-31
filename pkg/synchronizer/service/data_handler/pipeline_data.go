package data_handler

import "sync"

type PipelineData struct {
	Request    *Request
	PipelineID int32
	Payload    []byte
}

var pipelineDataPool = sync.Pool{
	New: func() interface{} {
		return &PipelineData{}
	},
}

func (pipelineData *PipelineData) Free() {
	pipelineDataPool.Put(pipelineData)
}
