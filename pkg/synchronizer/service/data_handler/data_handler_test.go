package data_handler

import (
	"fmt"
	"testing"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/rule"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"
	"github.com/cfsghost/taskflow"
	"github.com/stretchr/testify/assert"
)

var testDH *DataHandler
var testEvent string
var testRule string

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

	testEvent = eventName
	testRule = dt.Rule

	return dt
}

func TestDataHandlerInitialization(t *testing.T) {

	testDH = NewDataHandler()
	testEvent = "accountCreated"

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

	// Find rule
	for _, r := range testDH.ruleConfig.Rules {

		if r.Event == testEvent {
			testRule = r.ID
			break
		}
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
	taskCount := 1000
	for i := 0; i < taskCount; i++ {
		dt := task.NewTask()
		dt.PipelineID = 0
		dt.PrimaryKey = fmt.Sprintf("%d", i+1)
		dt.EventName = testEvent
		dt.Payload = []byte(fmt.Sprintf(`{"id":%d}`, i+1))
		dt.Rule = testRule

		//		group.AddTask(dt)
		ctx := taskflow.NewContext()
		err := testDH.taskflow.PushWithContext(testDH.mappingHandler.task.GetID(), 0, ctx, dt)
		if err != nil {
			t.Error(err)
		}
	}

	totalResults := 0
	for pj := range done {
		totalResults++

		assert.Equal(t, pj.EventName, testEvent)
		assert.Equal(t, uint64(totalResults), BytesToUint64(pj.Fields[0].Value.Value))

		if totalResults == taskCount {
			break
		}
	}

	testDH.taskflow.RemoveTask(checkTask.GetID())
}

func TestSkipEmptyPayload(t *testing.T) {

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

	// empty task data that will be ignored
	emptyTaskData := createTestTask("accountCreated", []byte(""))
	err = testDH.taskflow.PushWithContext(testDH.mappingHandler.task.GetID(), 0, taskflow.NewContext(), emptyTaskData)
	if err != nil {
		t.Error(err)
	}

	// empty task data that will be ignored
	invalidTaskData := createTestTask("accountCreated", []byte("XYZ"))
	err = testDH.taskflow.PushWithContext(testDH.mappingHandler.task.GetID(), 0, taskflow.NewContext(), invalidTaskData)
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
		assert.Equal(t, pj.EventName, testEvent)

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
	totalResults := 0
	testDH.OnStore(func(privData interface{}, data []byte) error {
		totalResults++

		record := recordPool.Get().(*gravity_sdk_types_record.Record)
		defer recordPool.Put(record)

		gravity_sdk_types_record.Unmarshal(data, record)

		assert.Equal(t, uint64(totalResults), BytesToUint64(record.Fields[0].Value.Value))

		return nil
	})

	// Preparing data task from DSA
	taskCount := 10000
	group := task.NewTaskGroup()
	for i := 0; i < taskCount; i++ {
		dt := task.NewTask()
		dt.PipelineID = 0
		dt.PrimaryKey = fmt.Sprintf("%d", i+1)
		dt.EventName = testEvent
		dt.Payload = []byte(fmt.Sprintf(`{"id":%d}`, i+1))
		dt.Rule = testRule

		group.AddTask(dt)
	}

	// Push task group to data handler
	err = testDH.PushTaskGroup(nil, group)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, taskCount, totalResults)
}
