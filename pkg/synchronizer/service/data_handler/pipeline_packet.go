package data_handler

import "sync"

type PipelinePacket struct {
	Task *BatchTask
	Data *PipelineData
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
