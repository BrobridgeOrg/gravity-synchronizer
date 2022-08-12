package data_handler

import (
	"sync"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"
)

var trPool = sync.Pool{
	New: func() interface{} {
		return &TaskRequest{}
	},
}

type TaskRequest struct {
	PrivData    interface{}
	Task        *task.Task
	OnCompleted func(error)
}

func NewTaskRequest() *TaskRequest {
	return trPool.Get().(*TaskRequest)
}

func (tr *TaskRequest) Done(err error) {
	tr.OnCompleted(err)
	tr.PrivData = nil
	tr.Task = nil
	trPool.Put(tr)
}
