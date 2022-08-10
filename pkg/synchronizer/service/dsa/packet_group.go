package dsa

import (
	"sync/atomic"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"
)

type PacketGroup struct {
	packets           map[int32]*PipelinePacket
	completed         int32
	completionHandler func()
}

func NewPacketGroup() *PacketGroup {
	return &PacketGroup{
		packets: make(map[int32]*PipelinePacket),
	}
}

func (pg *PacketGroup) AddTask(t *task.Task) {

	// Found existing pipeline
	if _, ok := pg.packets[t.PipelineID]; ok {
		pg.packets[t.PipelineID].AddTask(t)
		return
	}

	// New task group for pipeline
	packet := NewPipelinePacket(pg, t.PipelineID)
	packet.AddTask(t)
	pg.packets[t.PipelineID] = packet
}

func (pg *PacketGroup) Done() {
	completed := atomic.AddInt32(&pg.completed, 1)
	if int(completed) == len(pg.packets) {
		if pg.completionHandler != nil {
			pg.completionHandler()
		}
	}
}
