package dsa

import "github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"

type PipelinePacket struct {
	Group      *PacketGroup
	PipelineID int32
	TaskGroup  *task.TaskGroup
}

func NewPipelinePacket(group *PacketGroup, pipelineID int32) *PipelinePacket {
	return &PipelinePacket{
		Group:      group,
		PipelineID: pipelineID,
		TaskGroup:  task.NewTaskGroup(),
	}
}

func (pp *PipelinePacket) AddTask(t *task.Task) {
	pp.TaskGroup.AddTask(t)
}

func (pp *PipelinePacket) Done(err error) {

	if pp.Group == nil {
		return
	}

	pp.Group.Done()
}
