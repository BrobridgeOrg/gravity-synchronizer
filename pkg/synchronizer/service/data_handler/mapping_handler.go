package data_handler

import (
	"fmt"
	"sync"

	"github.com/cfsghost/taskflow"

	gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/rule"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"
	"github.com/cfsghost/gosharding"
	"github.com/spf13/viper"
)

var projectionPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_projection.Projection{}
	},
}

type MappingHandler struct {
	dataHandler *DataHandler
	task        *taskflow.Task
	shard       *gosharding.Shard
}

func (mh *MappingHandler) Init(dataHandler *DataHandler) error {

	// Initializin task
	mh.dataHandler = dataHandler
	mh.task = taskflow.NewTask(1, 1)

	viper.SetDefault("pipeline.workerCount", 32)
	workerCount := viper.GetInt32("pipeline.workerCount")

	// Initializing shard
	options := gosharding.NewOptions()
	options.PipelineCount = workerCount
	options.BufferSize = 10240
	options.PrepareHandler = func(id int32, data interface{}, c chan interface{}) {
		c <- data
	}
	options.Handler = func(id int32, data interface{}) {
		mh.processMessage(data.(*taskflow.Message))
	}

	// Create shard with options
	mh.shard = gosharding.NewShard(options)

	// Initializing task handler
	mh.task.SetHandler(mh.handle)

	return nil
}

func (mh *MappingHandler) handle(message *taskflow.Message) {

	t := message.Data.(*task.Task)

	mh.shard.Push(uint64(t.PipelineID), message)
}

func (mh *MappingHandler) processMessage(message *taskflow.Message) {

	t := message.Data.(*task.Task)

	// Getting specific rule settings
	r := mh.dataHandler.ruleConfig.Get(t.Rule)
	if r == nil {
		// Not found
		return
	}

	// Mapping and convert raw data to projection object
	pj, err := mh.convert(r, t)
	if err != nil {
		// Failed to parse payload

		fmt.Println(err)
		return
	}

	message.Context.SetMeta("task", t)
	message.Send(0, pj)
}

func (mh *MappingHandler) convert(rule *rule.Rule, t *task.Task) (*gravity_sdk_types_projection.Projection, error) {

	// Parse payload
	var payload map[string]interface{}
	err := json.Unmarshal(t.Payload, &payload)
	if err != nil {
		return nil, err
	}

	// Preparing projection
	projection := projectionPool.Get().(*gravity_sdk_types_projection.Projection)
	projection.EventName = t.EventName
	projection.Method = rule.Method
	projection.Collection = rule.Collection
	projection.PrimaryKey = rule.PrimaryKey
	projection.Fields = make([]gravity_sdk_types_projection.Field, 0, len(rule.Mapping))
	//	projection.Meta = task.Meta

	if len(rule.Mapping) == 0 {

		// pass throuh
		for key, value := range payload {

			field := gravity_sdk_types_projection.Field{
				Name:  key,
				Value: value,
			}

			projection.Fields = append(projection.Fields, field)
		}

	} else {

		// Mapping to new fields
		for _, mapping := range rule.Mapping {

			// Getting value from payload
			val, ok := payload[mapping.Source]
			if !ok {
				continue
			}

			field := gravity_sdk_types_projection.Field{
				Name:  mapping.Target,
				Value: val,
			}

			projection.Fields = append(projection.Fields, field)
		}
	}

	return projection, nil
}
