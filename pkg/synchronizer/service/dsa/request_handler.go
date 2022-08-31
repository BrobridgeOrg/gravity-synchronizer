package dsa

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/cfsghost/taskflow"

	dsa_pb "github.com/BrobridgeOrg/gravity-api/service/dsa"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/rule"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/task"
	sdf "github.com/BrobridgeOrg/sequential-data-flow"
	"github.com/golang/protobuf/proto"
	"github.com/lithammer/go-jump-consistent-hash"
	log "github.com/sirupsen/logrus"
)

var reqPool = sync.Pool{
	New: func() interface{} {
		return &dsa_pb.BatchPublishRequest{}
	},
}

type RequestHandler struct {
	dsa      *DataSourceAdapter
	task     *taskflow.Task
	incoming *sdf.Flow
}

func NewRequestHandler() *RequestHandler {
	return &RequestHandler{}
}

func (rh *RequestHandler) Init(dsa *DataSourceAdapter) error {

	// Initializing task
	rh.dsa = dsa
	rh.task = taskflow.NewTask(1, 1)

	// Initializing sequential data flow
	opts := sdf.NewOptions()
	opts.BufferSize = 10240
	opts.WorkerCount = rh.dsa.workerCount
	opts.Handler = rh.requestHandler

	rh.incoming = sdf.NewFlow(opts)

	go rh.receiver()

	// Initializing task
	rh.task.SetHandler(rh.handle)

	return nil
}

func (rh *RequestHandler) receiver() {
	for data := range rh.incoming.Output() {

		message := data.(*taskflow.Message)

		if message.Data != nil {
			message.Send(0, message.Data)
		}

		message.Release()
	}
}

func (rh *RequestHandler) handle(message *taskflow.Message) {
	rh.incoming.Push(message)
}

func (rh *RequestHandler) requestHandler(data interface{}, done func(interface{})) {

	message := data.(*taskflow.Message)

	bundle := NewBundle()

	// Parsing request
	input := reqPool.Get().(*dsa_pb.BatchPublishRequest)
	defer reqPool.Put(input)

	err := proto.Unmarshal(message.Data.([]byte), input)
	if err != nil {
		log.Errorf("dsa: Failed to parse request: %v", err)

		if rh.dsa.completionHandler != nil {
			rh.dsa.completionHandler(message.Context.GetPrivData(), nil, ErrUnrecognizedRequest)
		}

		message.Data = nil

		done(message)

		return
	}

	log.Infof("dsa: Incoming requests: %d", len(input.Requests))

	// Check if buffer is full
	if int32(len(input.Requests)*len(rh.dsa.ruleConfig.Rules))+rh.dsa.Pending() > rh.dsa.maxPending {
		log.Warn(ErrMaxPendingTasksExceeded)
		log.Warnf("dsa: max pending: %d", rh.dsa.maxPending)
		log.Warnf("dsa: current pending: %d", rh.dsa.Pending())

		if rh.dsa.completionHandler != nil {
			rh.dsa.completionHandler(message.Context.GetPrivData(), nil, ErrMaxPendingTasksExceeded)
		}

		message.Data = nil

		done(message)

		return
	}

	message.Data = bundle

	// Filtering requests
	for _, req := range input.Requests {

		// Ignore if event name is empty
		if len(req.EventName) == 0 {
			log.Warnf("dsa: ignore unknown event: %s", req.EventName)
			continue
		}

		group := rh.prepare(rh.dsa, req)
		if group == nil {
			// No matched rules
			log.Warnf("dsa: ignore event: %s", req.EventName)
			continue
		}

		// bundle contains multiple task groups
		bundle.AddTaskGroup(group)

		// update pending tasks
		rh.dsa.increaseTaskCount(group.GetTaskCount())
	}

	done(message)
}

func (rh *RequestHandler) prepare(dsa *DataSourceAdapter, req *dsa_pb.PublishRequest) *task.TaskGroup {

	// Parse message
	var message map[string]interface{}
	err := json.Unmarshal(req.Payload, &message)
	if err != nil {
		log.Warnf("dsa: Skip. failed to parse record: %v", err)
		log.Warn(string(req.Payload))
		return nil
	}

	group := task.NewTaskGroup()
	for _, r := range dsa.ruleConfig.Rules {

		// Ignore events
		if r.Event != req.EventName {
			continue
		}

		// Getting primary key based on rule
		primaryKey := rh.findPrimaryKey(r, message)

		// Prepare event
		task := task.NewTask()
		task.Rule = r.ID
		task.PipelineID = jump.HashString(primaryKey, dsa.pipelineCount, jump.NewCRC64())
		task.PrimaryKey = primaryKey
		task.EventName = req.EventName
		task.Payload = req.Payload

		group.AddTask(task)
	}

	// No matches
	if group.GetTaskCount() == 0 {
		return nil
	}

	return group
}

func (rh *RequestHandler) getPrimaryValueAsString(data interface{}) string {

	v := reflect.ValueOf(data)

	switch v.Kind() {
	case reflect.String:
		return data.(string)
	default:
		return fmt.Sprintf("%v", data)
	}
}

func (rh *RequestHandler) findPrimaryKey(r *rule.Rule, message map[string]interface{}) string {

	if r.PrimaryKey != "" {

		val, ok := message[r.PrimaryKey]
		if !ok {
			return ""
		}

		return rh.getPrimaryValueAsString(val)
	}

	return ""
}
