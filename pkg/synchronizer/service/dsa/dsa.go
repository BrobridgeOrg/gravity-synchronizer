package dsa

import (
	"github.com/cfsghost/taskflow"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/rule"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type DataSourceAdapter struct {
	pipelineCount  int32
	ruleConfig     *rule.RuleConfig
	taskflow       *taskflow.TaskFlow
	requestHandler RequestHandler
	dispatcher     Dispatcher
	emitter        Emitter

	// Handlers
	completionHandler func(interface{}, interface{}, error)
}

func NewDataSourceAdapter() *DataSourceAdapter {

	taskflowOpts := taskflow.NewOptions()

	return &DataSourceAdapter{
		taskflow: taskflow.NewTaskFlow(taskflowOpts),
	}
}

func (dsa *DataSourceAdapter) InitTasks() error {

	// Request handler
	err := dsa.requestHandler.Init(dsa)
	if err != nil {
		return err
	}

	dsa.taskflow.AddTask(dsa.requestHandler.task)

	// Dispatcher
	err = dsa.dispatcher.Init(dsa)
	if err != nil {
		return err
	}

	dsa.taskflow.AddTask(dsa.dispatcher.task)
	dsa.taskflow.Link(dsa.requestHandler.task, 0, dsa.dispatcher.task, 0)

	// Emitter
	err = dsa.emitter.Init(dsa)
	if err != nil {
		return err
	}

	dsa.taskflow.AddTask(dsa.emitter.task)
	dsa.taskflow.Link(dsa.dispatcher.task, 0, dsa.emitter.task, 0)

	return nil
}

func (dsa *DataSourceAdapter) Init() error {

	// Read configurations
	viper.SetDefault("pipeline.pipelineCount", 256)
	dsa.pipelineCount = viper.GetInt32("pipeline.pipelineCount")

	// Starting taskflow to execute task
	err := dsa.taskflow.Start()
	if err != nil {
		return err
	}

	return nil
}

func (dsa *DataSourceAdapter) SetRuleConfig(ruleConfig *rule.RuleConfig) {
	dsa.ruleConfig = ruleConfig
}

func (dsa *DataSourceAdapter) PushData(privData interface{}, data interface{}) error {
	ctx := taskflow.NewContext()
	ctx.SetPrivData(privData)
	return dsa.taskflow.PushWithContext(1, 0, ctx, data)
}

func (dsa *DataSourceAdapter) OnEmitted(fn func(*PipelinePacket)) {
	dsa.emitter.onEmitted(fn)

}

func (dsa *DataSourceAdapter) OnCompleted(fn func(interface{}, interface{}, error)) {
	dsa.completionHandler = fn

}
