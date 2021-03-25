package data_handler

import (
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	"github.com/lithammer/go-jump-consistent-hash"
)

func (processor *Processor) initializePreprocessWorker() error {

	// Initialize parapllel chunked flow
	pcfOpts := &parallel_chunked_flow.Options{
		BufferSize: 1024000,
		ChunkSize:  512,
		ChunkCount: 512,
		Handler:    processor.preprocessHandler,
	}

	processor.preprocess = parallel_chunked_flow.NewParallelChunkedFlow(pcfOpts)

	go processor.onPreprocessed()

	return nil
}

func (processor *Processor) preprocessHandler(data interface{}, publish func(interface{})) {

	task := data.(*BatchTask)

	// Parse payload
	err := json.Unmarshal(task.RawPayload, &task.Payload)
	if err != nil {
		return
	}

	for _, rule := range processor.ruleConfig.Rules {

		// Ignore events
		if rule.Event != task.EventName {
			continue
		}

		// Getting primary key
		primaryKey := processor.findPrimaryKey(rule, task.Payload)

		// Prepare event
		task.PrimaryKey = primaryKey
		task.PipelineID = jump.HashString(primaryKey, processor.pipelineCount, jump.NewCRC64())
		task.Rule = rule

		publish(task)
	}
}

func (processor *Processor) onPreprocessed() {
	for {
		select {
		case task := <-processor.preprocess.Output():
			// Push to pipelines
			processor.pushToPipeline(task.(*BatchTask))
		}
	}
}

func (processor *Processor) preprocessTask(task *BatchTask) error {
	return processor.preprocess.Push(task)
}
