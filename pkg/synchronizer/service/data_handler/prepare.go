package data_handler

import (
	"fmt"
	"reflect"
)

func (processor *Processor) preparePacket(event *Event) []byte {

	// Preparing projection
	projection := projectionPool.Get().(*Projection)
	projection.EventName = event.Rule.Event
	projection.Method = event.Rule.Method
	projection.Collection = event.Rule.Collection
	projection.PrimaryKey = event.Rule.PrimaryKey
	projection.Fields = make([]Field, 0, len(event.Rule.Mapping))
	projection.Meta = event.Meta

	if len(event.Rule.Mapping) == 0 {

		// pass throuh
		for key, value := range event.Payload {

			field := Field{
				Name:  key,
				Value: value,
			}

			projection.Fields = append(projection.Fields, field)

		}
	} else {

		// Mapping to new fields
		for _, mapping := range event.Rule.Mapping {

			// Getting value from payload
			val, ok := event.Payload[mapping.Source]
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

func (processor *Processor) preparePipelineData(workerID int32, event *Event) (interface{}, error) {

	data := pipelinePacketPool.Get().(*PipelinePacket)
	data.Request = event.Request
	data.Data.PipelineID = event.PipelineID
	data.Data.Payload = processor.preparePacket(event)
	eventPool.Put(event)

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
