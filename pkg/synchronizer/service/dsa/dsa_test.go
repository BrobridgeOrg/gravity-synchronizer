package dsa

import (
	"testing"

	dsa_pb "github.com/BrobridgeOrg/gravity-api/service/dsa"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/rule"
	"github.com/cfsghost/taskflow"
	"github.com/golang/protobuf/proto"
)

var testDSA *DataSourceAdapter

func TestDataSourceAdapterInitialization(t *testing.T) {

	testDSA = NewDataSourceAdapter()

	// Load rules
	ruleConfig, err := rule.LoadRuleFile("../../../../rules/rules.json")
	if err != nil {
		t.Error(err)
	}

	testDSA.SetRuleConfig(ruleConfig)

	// Initializing
	err = testDSA.Init()
	if err != nil {
		t.Error(err)
	}

	testDSA.pipelineCount = 4
}

func TestRequestHandler(t *testing.T) {

	// Initializing request handler
	err := testDSA.requestHandler.Init(testDSA)
	if err != nil {
		t.Error()
	}

	testDSA.taskflow.AddTask(testDSA.requestHandler.task)

	// Preparing task to receive results
	done := make(chan *taskflow.Message, 1)
	defer close(done)
	checkTask := taskflow.NewTask(1, 0)
	checkTask.SetHandler(func(message *taskflow.Message) {
		done <- message
	})

	testDSA.taskflow.AddTask(checkTask)
	testDSA.taskflow.Link(testDSA.requestHandler.task, 0, checkTask, 0)

	// Preparing request
	req := &dsa_pb.BatchPublishRequest{
		Requests: []*dsa_pb.PublishRequest{
			&dsa_pb.PublishRequest{
				EventName: "accountCreated",
				Payload:   []byte(`{"id":1,"name":"fred"}`),
			},
		},
	}

	data, err := proto.Marshal(req)
	if err != nil {
		t.Error(err)
	}

	ctx := taskflow.NewContext()
	testDSA.taskflow.PushWithContext(1, 0, ctx, data)

	// Check results
	message := <-done
	bundle := message.Data.(*Bundle)
	if len(bundle.GetTaskGroups()) == 0 {
		t.Fail()
	}

	event := bundle.GetTaskGroups()[0].GetTasks()[0]
	if event.EventName != "accountCreated" {
		t.Error("event name is incorrect")
	}

	testDSA.taskflow.RemoveTask(checkTask.GetID())
}

func TestDispatcher(t *testing.T) {

	// Initializing dispatcher
	err := testDSA.dispatcher.Init(testDSA)
	if err != nil {
		t.Error()
	}

	testDSA.taskflow.AddTask(testDSA.dispatcher.task)
	testDSA.taskflow.Link(testDSA.requestHandler.task, 0, testDSA.dispatcher.task, 0)

	// Preparing task to receive results
	done := make(chan *taskflow.Message, 1)
	defer close(done)
	checkTask := taskflow.NewTask(1, 0)
	checkTask.SetHandler(func(message *taskflow.Message) {
		done <- message
	})

	testDSA.taskflow.AddTask(checkTask)
	testDSA.taskflow.Link(testDSA.dispatcher.task, 0, checkTask, 0)

	// Preparing request
	req := &dsa_pb.BatchPublishRequest{
		Requests: []*dsa_pb.PublishRequest{
			&dsa_pb.PublishRequest{
				EventName: "accountCreated",
				Payload:   []byte(`{"id":1,"name":"fred"}`),
			},
		},
	}

	data, err := proto.Marshal(req)
	if err != nil {
		t.Error(err)
	}

	ctx := taskflow.NewContext()
	testDSA.taskflow.PushWithContext(1, 0, ctx, data)

	// Check results
	message := <-done
	packet := message.Data.(*PipelinePacket)

	tasks := packet.TaskGroup.GetTasks()
	if len(tasks) == 0 {
		t.Fail()
	}

	if tasks[0].EventName != "accountCreated" {
		t.Error("event name is incorrect")
	}

	if len(tasks[0].Payload) != len(req.Requests[0].Payload) {
		t.Error("payload content is incorrect")
	}

	// Packet is done
	packetResult := make(chan *PacketGroup, 1)
	testDSA.OnCompleted(func(privData interface{}, result interface{}, err error) {
		packetResult <- result.(*PacketGroup)
	})
	packet.Done(nil)
	packetGroup := <-packetResult

	if int(packetGroup.completed) != 1 {
		t.Error("counter is incorrect")
	}

	testDSA.taskflow.RemoveTask(checkTask.GetID())
}

func TestEmitter(t *testing.T) {

	// Initializing dispatcher
	err := testDSA.emitter.Init(testDSA)
	if err != nil {
		t.Error()
	}

	testDSA.taskflow.AddTask(testDSA.emitter.task)
	testDSA.taskflow.Link(testDSA.dispatcher.task, 0, testDSA.emitter.task, 0)

	packetCount := 0
	packetBuffer := make(chan *PipelinePacket, 3)
	testDSA.OnEmitted(func(packet *PipelinePacket) {
		packetCount++
		packetBuffer <- packet
	})

	// Preparing request
	req := &dsa_pb.BatchPublishRequest{
		Requests: []*dsa_pb.PublishRequest{
			&dsa_pb.PublishRequest{
				EventName: "accountCreated",
				Payload:   []byte(`{"id":1,"name":"fred"}`),
			},
			&dsa_pb.PublishRequest{
				EventName: "accountCreated",
				Payload:   []byte(`{"id":2,"name":"jhe"}`),
			},
			&dsa_pb.PublishRequest{
				EventName: "accountCreated",
				Payload:   []byte(`{"id":3,"name":"armani"}`),
			},
		},
	}

	data, err := proto.Marshal(req)
	if err != nil {
		t.Error(err)
	}

	ctx := taskflow.NewContext()
	testDSA.taskflow.PushWithContext(1, 0, ctx, data)

	// Packet is done
	packetResult := make(chan *PacketGroup, 1)
	testDSA.OnCompleted(func(privData interface{}, result interface{}, err error) {
		packetResult <- result.(*PacketGroup)
	})

	totalPackets := 0
	for packet := range packetBuffer {
		packet.Done(nil)
		totalPackets++

		if totalPackets == packetCount {
			break
		}
	}

	packetGroup := <-packetResult

	if int(packetGroup.completed) != totalPackets {
		t.Error("counter is incorrect")
	}
}
