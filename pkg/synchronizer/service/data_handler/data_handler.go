package data_handler

import (
	"sync"

	"github.com/cfsghost/taskflow"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/rule"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type DataHandler struct {
	workerCount    int
	ruleConfig     *rule.RuleConfig
	taskflow       *taskflow.TaskFlow
	mappingHandler MappingHandler
	store          Store

	// Handlers
	storeHandler func(interface{}, []byte) error
}

func NewDataHandler() *DataHandler {

	return &DataHandler{
		workerCount: 8,
	}
}

func (dh *DataHandler) InitTasks() error {

	// Request handler
	err := dh.mappingHandler.Init(dh)
	if err != nil {
		return err
	}

	dh.taskflow.AddTask(dh.mappingHandler.task)

	// Store
	err = dh.store.Init(dh)
	if err != nil {
		return err
	}

	dh.taskflow.AddTask(dh.store.task)
	dh.taskflow.Link(dh.mappingHandler.task, 0, dh.store.task, 0)

	return nil
}

func (dh *DataHandler) Init() error {

	// Initializing taskflow
	taskflowOpts := taskflow.NewOptions()
	taskflowOpts.WorkerCount = dh.workerCount
	dh.taskflow = taskflow.NewTaskFlow(taskflowOpts)

	// Starting taskflow to execute task
	err := dh.taskflow.Start()
	if err != nil {
		return err
	}

	return nil
}

func (dh *DataHandler) SetWorkerCount(count int) {
	dh.workerCount = count
}

func (dh *DataHandler) SetRuleConfig(ruleConfig *rule.RuleConfig) {
	dh.ruleConfig = ruleConfig
}

func (dh *DataHandler) PushTaskGroup(privData interface{}, taskGroup *task.TaskGroup) error {

	tasks := taskGroup.GetTasks()

	var wg sync.WaitGroup
	wg.Add(len(tasks))

	var e error
	for _, t := range tasks {
		tr := NewTaskRequest()
		tr.PrivData = privData
		tr.Task = t
		tr.OnCompleted = func(err error) {
			e = err
			wg.Done()
		}

		dh.PushData(tr, t)
	}

	wg.Wait()

	return e
}

func (dh *DataHandler) PushData(privData interface{}, data interface{}) error {

	// Create context for task
	ctx := taskflow.NewContext()
	ctx.SetPrivData(privData)

	err := dh.taskflow.PushWithContext(dh.mappingHandler.task.GetID(), 0, ctx, data)
	if err != nil {
		return err
	}

	return nil
}

func (dh *DataHandler) OnStore(fn func(interface{}, []byte) error) {
	dh.storeHandler = fn
}
