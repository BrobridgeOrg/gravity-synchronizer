package task

import "sync"

var tgPool = sync.Pool{
	New: func() interface{} {
		return &TaskGroup{}
	},
}

type TaskGroup struct {
	tasks []*Task
}

func NewTaskGroup() *TaskGroup {
	tg := tgPool.Get().(*TaskGroup)
	tg.tasks = make([]*Task, 0)
	return tg
}

func (group *TaskGroup) GetTaskCount() int32 {
	return int32(len(group.tasks))
}

func (group *TaskGroup) AddTask(task *Task) {
	group.tasks = append(group.tasks, task)
}

func (group *TaskGroup) GetTasks() []*Task {
	return group.tasks
}

func (group *TaskGroup) Release() {
	group.tasks = make([]*Task, 0)
	tgPool.Put(group)
}
