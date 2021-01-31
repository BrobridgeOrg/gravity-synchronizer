package data_handler

import (
	"sync"

	"github.com/cfsghost/gosharding"
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	"github.com/spf13/viper"
)

type Processor struct {
	ruleConfig *RuleConfig
	//	pipelineNames   map[int32]string
	pipelineCount   int32
	workerCount     int32
	shard           *gosharding.Shard
	preprocess      *parallel_chunked_flow.ParallelChunkedFlow
	pipelineHandler func(*PipelinePacket)
}

type Field struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type Payload map[string]interface{}

type RawData struct {
	EventName string
	Payload   []byte
	Meta      map[string][]byte
}

type Event struct {
	Request    *BatchRequest
	PrimaryKey string
	PipelineID int32
	Payload    Payload
	Rule       *Rule
	Meta       map[string][]byte
}

var rawDataPool = sync.Pool{
	New: func() interface{} {
		return &RawData{}
	},
}

var eventPool = sync.Pool{
	New: func() interface{} {
		return &Event{}
	},
}

func NewProcessor() *Processor {

	// Read configurations
	viper.SetDefault("pipeline.pipelineCount", 256)
	pipelineCount := viper.GetInt32("pipeline.pipelineCount")

	// Create a new object
	processor := &Processor{
		pipelineCount: pipelineCount,
	}

	// Initializing
	processor.initializePipelineWorkers()
	processor.initializePreprocessWorker()

	return processor
}

func (processor *Processor) processPipelineData(workerID int32, data *PipelinePacket) error {

	processor.pipelineHandler(data)

	pipelinePacketPool.Put(data)

	return nil
}

func (processor *Processor) SetPipelineHandler(fn func(*PipelinePacket)) {
	processor.pipelineHandler = fn
}

func (processor *Processor) ProcessTask(task *BatchTask) error {
	return processor.preprocessTask(task)
}
