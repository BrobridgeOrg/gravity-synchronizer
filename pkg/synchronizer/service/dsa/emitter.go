package dsa

import (
	"github.com/cfsghost/taskflow"
)

type Emitter struct {
	dsa            *DataSourceAdapter
	task           *taskflow.Task
	emittedHandler func(*PipelinePacket)
}

func NewEmitter() *Emitter {
	return &Emitter{}
}

func (emitter *Emitter) Init(dsa *DataSourceAdapter) error {

	// Initializing task
	emitter.dsa = dsa
	emitter.task = taskflow.NewTask(1, 0)

	// Initializing task handler
	emitter.task.SetHandler(emitter.handle)

	return nil
}

func (emitter *Emitter) handle(message *taskflow.Message) {

	packet := message.Data.(*PipelinePacket)
	emitter.emittedHandler(packet)
}

func (emitter *Emitter) onEmitted(fn func(*PipelinePacket)) {
	emitter.emittedHandler = fn
}
