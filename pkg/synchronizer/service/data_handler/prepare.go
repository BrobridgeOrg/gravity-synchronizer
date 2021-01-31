package data_handler

import (
	"fmt"
	"reflect"
)

func (processor *Processor) preparePacket(task *BatchTask) []byte {

	// Preparing projection
	projection := projectionPool.Get().(*Projection)
	projection.EventName = task.Rule.Event
	projection.Method = task.Rule.Method
	projection.Collection = task.Rule.Collection
	projection.PrimaryKey = task.Rule.PrimaryKey
	projection.Fields = make([]Field, 0, len(task.Rule.Mapping))
	projection.Meta = task.Meta

	if len(task.Rule.Mapping) == 0 {

		// pass throuh
		for key, value := range task.Payload {

			field := Field{
				Name:  key,
				Value: value,
			}

			projection.Fields = append(projection.Fields, field)

		}
	} else {

		// Mapping to new fields
		for _, mapping := range task.Rule.Mapping {

			// Getting value from payload
			val, ok := task.Payload[mapping.Source]
			if !ok {
				continue
			}

			field := Field{
				Name:  mapping.Target,
				Value: val,
			}

			projection.Fields = append(projection.Fields, field)
		}
	}

	// Convert to packet
	data, _ := json.Marshal(&projection)

	// Release projection object
	projectionPool.Put(projection)

	return data
}

func (processor *Processor) preparePipelineData(workerID int32, task *BatchTask) (interface{}, error) {

	data := pipelinePacketPool.Get().(*PipelinePacket)
	data.Task = task
	data.Data.PipelineID = task.PipelineID
	data.Data.Payload = processor.preparePacket(task)

	return data, nil
}

func (processor *Processor) getPrimaryValueAsString(data interface{}) string {

	v := reflect.ValueOf(data)

	switch v.Kind() {
	case reflect.String:
		return data.(string)
	default:
		return fmt.Sprintf("%v", data)
	}
}

func (processor *Processor) findPrimaryKey(rule *Rule, payload Payload) string {

	if rule.PrimaryKey != "" {

		val, ok := payload[rule.PrimaryKey]
		if !ok {
			return ""
		}

		return processor.getPrimaryValueAsString(val)
	}

	return ""
}
