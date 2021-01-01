package data_handler

import "sync"

type PipelinePacket struct {
	Request *Request
	Data    *PipelineData
}

type PipelineData struct {
	PipelineID int32
	Payload    []byte
}

var pipelinePacketPool = sync.Pool{
	New: func() interface{} {
		return &PipelinePacket{
			Data: &PipelineData{},
		}
	},
}

func (pipelinePacket *PipelinePacket) Free() {
	pipelinePacketPool.Put(pipelinePacket)
}
