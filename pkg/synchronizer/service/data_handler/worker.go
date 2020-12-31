package data_handler

import (
	"time"

	"github.com/cfsghost/gosharding"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func (processor *Processor) initializePipelineWorkers() error {

	viper.SetDefault("pipeline.workerCount", 32)
	workerCount := viper.GetInt32("pipeline.workerCount")

	// Initializing shard
	options := gosharding.NewOptions()
	options.PipelineCount = workerCount
	options.BufferSize = 10240
	options.PrepareHandler = func(id int32, data interface{}, c chan interface{}) {

		for {
			// Prepare request data for pipeline
			data, err := processor.preparePipelineData(id, data.(*Event))
			if err == nil {
				c <- data
				return
			}

			log.Error(err)
			time.Sleep(time.Second)
		}
	}
	options.Handler = func(id int32, data interface{}) {

		for {
			// Process data
			err := processor.processPipelineData(id, data.(*PipelineData))
			if err == nil {
				return
			}

			log.Error(err)
			time.Sleep(time.Second)
		}
	}

	// Create shard with options
	processor.workerCount = workerCount
	processor.shard = gosharding.NewShard(options)

	return nil
}
