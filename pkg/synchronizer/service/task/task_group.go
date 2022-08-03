package task

import "sync/atomic"

type TaskGroup struct {
	completed int32
	tasks     []*Task
}

func NewTaskGroup() *TaskGroup {
	return &TaskGroup{
		tasks: make([]*Task, 0),
	}
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

func (group *TaskGroup) Done(task *Task) bool {

	completed := atomic.AddInt32(&group.completed, 1)
	if completed >= int32(len(group.tasks)) {
		return true
	}

	return false
}
