package data_handler

import "sync"

var batchTaskPool = sync.Pool{
	New: func() interface{} {
		return &BatchTask{}
	},
}

type BatchTask struct {
	Request    *BatchRequest
	EventName  string
	PrimaryKey string
	PipelineID int32
	RawPayload []byte
	Payload    Payload
	Rule       *Rule
	Meta       map[string][]byte
}

func (task *BatchTask) Done() {
	task.Request.Done()
}

func (task *BatchTask) Fail(err error) {
	task.Request.Fail(err)
}
