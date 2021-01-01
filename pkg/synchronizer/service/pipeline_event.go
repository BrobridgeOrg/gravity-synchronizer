package synchronizer

import (
	"gravity-synchronizer/pkg/synchronizer/service/request"
	"sync"

	gosharding "github.com/cfsghost/gosharding"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var pipelineEventPool = sync.Pool{
	New: func() interface{} {
		return &PipelineEvent{}
	},
}

type PipelineEvent struct {
	Pipeline *Pipeline
	Request  request.Request
}

func (synchronizer *Synchronizer) initializeShard() error {

	viper.SetDefault("pipeline.workerCount", 16)
	viper.SetDefault("pipeline.workerBufferSize", 102400)

	// Initializing shard
	options := gosharding.NewOptions()
	options.PipelineCount = viper.GetInt32("pipeline.workerCount")
	options.BufferSize = viper.GetInt("pipeline.workerBufferSize")
	options.Handler = func(id int32, data interface{}) {
		event := data.(*PipelineEvent)
		event.Pipeline.push(event)
	}

	// Create shard with options
	synchronizer.shard = gosharding.NewShard(options)

	log.WithFields(log.Fields{
		"count":      viper.GetInt32("pipeline.workerCount"),
		"bufferSize": viper.GetInt("pipeline.workerBufferSize"),
	}).Info("Initialized pipeline workers")

	return nil
}

func (synchronizer *Synchronizer) processEvent(pipeline *Pipeline, req request.Request) error {

	event := pipelineEventPool.Get().(*PipelineEvent)
	event.Pipeline = pipeline
	event.Request = req
	pipeline.synchronizer.shard.Push(pipeline.id, event)

	return nil
}
