package data_handler

import (
	"testing"

	gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/rule"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"
	"github.com/cfsghost/taskflow"
)

var testDH *DataHandler

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
	done := make(chan *gravity_sdk_types_projection.Projection, 10)
	defer close(done)
	checkTask := taskflow.NewTask(1, 0)
	checkTask.SetHandler(func(message *taskflow.Message) {
		pj := message.Data.(*gravity_sdk_types_projection.Projection)
		done <- pj
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
