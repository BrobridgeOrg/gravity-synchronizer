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

	go processor.eventReceiver()

	return nil
}

func (processor *Processor) preprocessHandler(data interface{}, output chan interface{}) {

	request := data.(*Request)
	input := request.GetInput()

	// Parse payload
	var payload Payload
	err := json.Unmarshal(input.Payload, &payload)
	if err != nil {
		return
	}

	eventName := input.EventName
	meta := input.Meta

	for _, rule := range processor.ruleConfig.Rules {

		// Ignore events
		if rule.Event != eventName {
			continue
		}

		// Getting primary key
		primaryKey := processor.findPrimaryKey(rule, payload)

		// Prepare event
		event := eventPool.Get().(*Event)
		event.Request = request
		event.PrimaryKey = primaryKey
		event.PipelineID = jump.HashString(primaryKey, processor.pipelineCount, jump.NewCRC64())
		event.Payload = payload
		event.Rule = rule
		event.Meta = meta

		output <- event
	}
}

func (processor *Processor) eventReceiver() {
	for {
		select {
		case event := <-processor.preprocess.Output():
			// Push event to pipeline
			processor.shard.PushKV(event.(*Event).PrimaryKey, event)
		}
	}
}

func (processor *Processor) preprocessData(request *Request) error {
	return processor.preprocess.Push(request)
}

/*
func (processor *Processor) preprocessData(rawData *RawData) error {
	processor.preprocess.Push(rawData)
	return nil
}
*/
