package synchronizer

import (
	data_handler "gravity-synchronizer/pkg/synchronizer/service/data_handler"

	"github.com/spf13/viper"
)

func (synchronizer *Synchronizer) initializeDataHandler() error {

	synchronizer.dataHandler = data_handler.NewDataHandler()
	synchronizer.dataHandler.SetRuleConfig(synchronizer.ruleConfig)
	synchronizer.dataHandler.OnStore(func(privData interface{}, data []byte) error {
		pipeline := privData.(*Pipeline)
		return pipeline.store(data)
	})

	// Setup worker count
	viper.SetDefault("pipeline.workerCount", 8)
	workerCount := viper.GetInt("pipeline.workerCount")
	synchronizer.dataHandler.SetWorkerCount(workerCount)

	err := synchronizer.dataHandler.Init()
	if err != nil {
		return err
	}

	err = synchronizer.dataHandler.InitTasks()
	if err != nil {
		return err
	}

	/*
		// Subscribe to quque to receive events
		connection := synchronizer.gravityClient.GetConnection()
		channel := fmt.Sprintf("%s.pipeline.%d", synchronizer.domain, packet.Data.PipelineID)
		sub, err := connection.QueueSubscribe(channel, "synchronizer", func(m *nats.Msg) {
			synchronizer.dataHandler.PushData(m, m.Data)
		})
		if err != nil {
			return err
		}

		sub.SetPendingLimits(-1, -1)
		connection.Flush()
	*/
	return nil
}
