package data_handler

import "github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"

type TaskRequest struct {
	PrivData    interface{}
	Task        *task.Task
	OnCompleted func(error)
}

func NewTaskRequest() *TaskRequest {
	return &TaskRequest{}
}

func (tr *TaskRequest) Done(err error) {
	tr.OnCompleted(err)
}
