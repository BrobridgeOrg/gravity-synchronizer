package dsa

import (
	"sync"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"
)

var bundlePool = sync.Pool{
	New: func() interface{} {
		return &Bundle{
			taskGroups: make([]*task.TaskGroup, 0),
		}
	},
}

type Bundle struct {
	taskGroups        []*task.TaskGroup
	completionHandler func()
}

func NewBundle() *Bundle {
	return bundlePool.Get().(*Bundle)
}

func (bundle *Bundle) AddTaskGroup(group *task.TaskGroup) {
	bundle.taskGroups = append(bundle.taskGroups, group)
}

func (bundle *Bundle) GetTaskGroups() []*task.TaskGroup {
	return bundle.taskGroups
}

func (bundle *Bundle) Release() {
	bundle.taskGroups = make([]*task.TaskGroup, 0)
	bundlePool.Put(bundle)
}

func (bundle *Bundle) OnCompleted(fn func()) {
	bundle.completionHandler = fn
}
