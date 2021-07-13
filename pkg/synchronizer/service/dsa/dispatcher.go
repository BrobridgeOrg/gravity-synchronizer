package dsa

import (
	"github.com/cfsghost/taskflow"
)

type Dispatcher struct {
	dsa  *DataSourceAdapter
	task *taskflow.Task
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		task: taskflow.NewTask(1, 1),
	}
}

func (dispatcher *Dispatcher) Init(dsa *DataSourceAdapter) error {

	// Initializing task
	dispatcher.dsa = dsa
	dispatcher.task = taskflow.NewTask(1, 1)

	// Initializing task handler
	dispatcher.task.SetHandler(dispatcher.handle)

	return nil
}

func (dispatcher *Dispatcher) handle(message *taskflow.Message) {

	packetGroup := NewPacketGroup()
	packetGroup.completionHandler = func() {
		dispatcher.dsa.completionHandler(message.Context.GetPrivData(), packetGroup, nil)
	}

	// Push each task to shard handler
	bundle := message.Data.(*Bundle)

	groups := bundle.GetTaskGroups()
	for _, group := range groups {

		// there are many tasks of one event
		for _, task := range group.GetTasks() {

			packetGroup.AddTask(task)
		}
	}

	// send packets by pipeline
	for _, packet := range packetGroup.packets {
		message.Send(0, packet)
	}
}