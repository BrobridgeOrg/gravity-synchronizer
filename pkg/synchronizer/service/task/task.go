package task

type Task struct {
	PipelineID int32
	Rev        uint64
	EventName  string
	PrimaryKey string
	Payload    []byte
	Rule       string
}

func NewTask() *Task {
	return &Task{}
}
