package data_handler

import (
	"testing"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/rule"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"
	"github.com/cfsghost/taskflow"
)

var testDH *DataHandler

func createTestTask(eventName string, payload []byte) *task.Task {

	dt := task.NewTask()
	dt.PipelineID = 0
	dt.PrimaryKey = "1"
	dt.EventName = eventName
	dt.Payload = payload

	// Find rule
	for _, r := range testDH.ruleConfig.Rules {

		if r.Event == dt.EventName {
			dt.Rule = r.ID
			break
		}
	}

	return dt
}

func TestDataHandlerInitialization(t *testing.T) {

	testDH = NewDataHandler()

	// Load rules
	ruleConfig, err := rule.LoadRuleFile("../../../../rules/rules.json")
	if err != nil {
		t.Error(err)
	}

	testDH.SetRuleConfig(ruleConfig)

	// Initializing
	err = testDH.Init()
	if err != nil {
		t.Error(err)
	}
}

func TestMappingHandler(t *testing.T) {

	// Initializing mapping handler
	err := testDH.mappingHandler.Init(testDH)
	if err != nil {
		t.Error()
	}

	testDH.taskflow.AddTask(testDH.mappingHandler.task)

	// Preparing task to receive results
	done := make(chan *gravity_sdk_types_record.Record, 10)
	defer close(done)
	checkTask := taskflow.NewTask(1, 0)
	checkTask.SetHandler(func(message *taskflow.Message) {
		record := message.Data.(*gravity_sdk_types_record.Record)
		done <- record
	})

	testDH.taskflow.AddTask(checkTask)
	testDH.taskflow.Link(testDH.mappingHandler.task, 0, checkTask, 0)

	// Preparing data task from DSA
	for i := 0; i < 10; i++ {
		dt := task.NewTask()
		dt.PipelineID = 0
		dt.PrimaryKey = "1"
		dt.EventName = "accountCreated"
		dt.Payload = []byte(`{"id":1,"name":"fred"}`)

		// Find rule
		for _, r := range testDH.ruleConfig.Rules {

			if r.Event == dt.EventName {
				dt.Rule = r.ID
				break
			}
		}

		//		group.AddTask(dt)
		ctx := taskflow.NewContext()
		err := testDH.taskflow.PushWithContext(testDH.mappingHandler.task.GetID(), 0, ctx, dt)
		if err != nil {
			t.Error(err)
		}
	}

	// Push task group to data handler
	//	testDH.PushTaskGroup(nil, group)

	totalResults := 0
	for pj := range done {
		totalResults++
		if pj.EventName != "accountCreated" {
			t.Fail()
		}

		if totalResults == 10 {
			break
		}
	}

	testDH.taskflow.RemoveTask(checkTask.GetID())
}

func TestEmptyPayload(t *testing.T) {

	// Preparing task to receive results
	done := make(chan *gravity_sdk_types_record.Record, 10)
	defer close(done)
	checkTask := taskflow.NewTask(1, 0)
	checkTask.SetHandler(func(message *taskflow.Message) {
		record := message.Data.(*gravity_sdk_types_record.Record)
		done <- record
	})

	testDH.taskflow.AddTask(checkTask)
	testDH.taskflow.Link(testDH.mappingHandler.task, 0, checkTask, 0)

	// Normal task data
	normalTaskData := createTestTask("accountCreated", []byte(`{"id":1,"name":"fred"}`))
	err := testDH.taskflow.PushWithContext(testDH.mappingHandler.task.GetID(), 0, taskflow.NewContext(), normalTaskData)
	if err != nil {
		t.Error(err)
	}

	// empty task data
	emptyTaskData := createTestTask("accountCreated", []byte(""))
	err = testDH.taskflow.PushWithContext(testDH.mappingHandler.task.GetID(), 0, taskflow.NewContext(), emptyTaskData)
	if err != nil {
		t.Error(err)
	}

	// send normal data again
	normalTaskData = createTestTask("accountCreated", []byte(`{"id":1,"name":"fred"}`))
	err = testDH.taskflow.PushWithContext(testDH.mappingHandler.task.GetID(), 0, taskflow.NewContext(), normalTaskData)
	if err != nil {
		t.Error(err)
	}

	totalResults := 0
	for pj := range done {
		totalResults++
		if pj.EventName != "accountCreated" {
			t.Fail()
		}

		if totalResults == 2 {
			break
		}
	}

	testDH.taskflow.RemoveTask(checkTask.GetID())
}

func TestStore(t *testing.T) {

	// Initializing store
	err := testDH.store.Init(testDH)
	if err != nil {
		t.Error()
	}

	testDH.taskflow.AddTask(testDH.store.task)
	testDH.taskflow.Link(testDH.mappingHandler.task, 0, testDH.store.task, 0)

	// Initializing store handler
	testDH.OnStore(func(privData interface{}, data []byte) error {
		return nil
	})

	// Preparing data task from DSA
	group := task.NewTaskGroup()
	for i := 0; i < 10; i++ {
		dt := task.NewTask()
		dt.PipelineID = 0
		dt.PrimaryKey = "1"
		dt.EventName = "accountCreated"
		dt.Payload = []byte(`{"id":1,"name":"fred"}`)

		// Find rule
		for _, r := range testDH.ruleConfig.Rules {

			if r.Event == dt.EventName {
				dt.Rule = r.ID
				break
			}
		}

		group.AddTask(dt)
	}

	// Push task group to data handler
	err = testDH.PushTaskGroup(nil, group)
	if err != nil {
		t.Error(err)
	}
}
