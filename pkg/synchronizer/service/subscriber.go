package synchronizer

import (
	"errors"
	"fmt"
	"sync"

	eventstore "github.com/BrobridgeOrg/EventStore"
	gravity_sdk_types_event "github.com/BrobridgeOrg/gravity-sdk/types/event"
	gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

var pjPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_projection.Projection{}
	},
}

var eventPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_event.Event{}
	},
}

var eventPayloadPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_event.EventPayload{}
	},
}

var systemMessagePool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_event.SystemMessage{}
	},
}

var systemMessage_AwakeMessagePool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_event.SystemMessage_AwakeMessage{}
	},
}

type Subscriber struct {
	synchronizer     *Synchronizer
	id               string
	name             string
	appID            string
	collections      sync.Map
	suspendPipelines sync.Map
	snapshot         *Snapshot
}

func NewSubscriber(id string, name string, appID string) *Subscriber {
	return &Subscriber{
		id:    id,
		name:  name,
		appID: appID,
	}
}

func (sub *Subscriber) Initialize(synchronizer *Synchronizer) error {
	sub.synchronizer = synchronizer
	return nil
}

func (sub *Subscriber) SubscribeToCollections(collections []string) ([]string, error) {

	results := make([]string, 0, len(collections))

	for _, col := range collections {
		if _, ok := sub.collections.Load(col); ok {
			results = append(results, col)
			continue
		}

		log.Info("Subscribed to " + col)

		sub.collections.Store(col, true)
		results = append(results, col)
	}

	return results, nil
}

func (sub *Subscriber) UnsubscribeFromCollections(collections []string) ([]string, error) {

	for _, col := range collections {
		sub.collections.Delete(col)
	}

	return collections, nil
}

func (sub *Subscriber) Awake(pipeline *Pipeline) error {

	connection := sub.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.subscriber.%s", sub.synchronizer.domain, sub.id)

	ev := eventPool.Get().(*gravity_sdk_types_event.Event)
	ev.Type = gravity_sdk_types_event.Event_TYPE_SYSTEM
	ev.EventPayload = nil
	ev.SnapshotInfo = nil
	ev.SystemInfo = systemMessagePool.Get().(*gravity_sdk_types_event.SystemMessage)

	// Payload
	ev.SystemInfo.Type = gravity_sdk_types_event.SystemMessage_TYPE_WAKE
	ev.SystemInfo.AwakeMessage = systemMessage_AwakeMessagePool.Get().(*gravity_sdk_types_event.SystemMessage_AwakeMessage)
	ev.SystemInfo.AwakeMessage.PipelineID = pipeline.id
	ev.SystemInfo.AwakeMessage.Sequence = pipeline.GetLastSequence()

	data, _ := proto.Marshal(ev)
	err := connection.Publish(channel, data)
	systemMessage_AwakeMessagePool.Put(ev.SystemInfo.AwakeMessage)
	systemMessagePool.Put(ev.SystemInfo)
	eventPool.Put(ev)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (sub *Subscriber) Publish(pipeline *Pipeline, events []*eventstore.Event) error {

	connection := sub.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.subscriber.%s", sub.synchronizer.domain, sub.id)

	log.WithFields(log.Fields{
		"channel": channel,
	}).Info("Publishing events")

	// Publish events
	lastSeq := uint64(0)
	for _, event := range events {

		// Parsing event
		pj := pjPool.Get().(*gravity_sdk_types_projection.Projection)
		err := gravity_sdk_types_projection.Unmarshal(event.Data, pj)
		if err != nil {
			log.Error(err)
			pjPool.Put(pj)
			continue
		}

		// Filtering
		_, ok := sub.collections.Load(pj.Collection)
		if !ok {
			pjPool.Put(pj)
			continue
		}
		pjPool.Put(pj)

		// Preparing event
		ev := eventPool.Get().(*gravity_sdk_types_event.Event)
		ev.Type = gravity_sdk_types_event.Event_TYPE_EVENT
		ev.EventPayload = eventPayloadPool.Get().(*gravity_sdk_types_event.EventPayload)
		ev.SnapshotInfo = nil
		ev.SystemInfo = nil

		// Event payload
		ev.EventPayload.State = gravity_sdk_types_event.EventPayload_STATE_NONE
		ev.EventPayload.PipelineID = pipeline.id
		ev.EventPayload.Sequence = event.Sequence
		ev.EventPayload.Data = event.Data

		data, _ := proto.Marshal(ev)
		err = connection.Publish(channel, data)
		eventPayloadPool.Put(ev.EventPayload)
		eventPool.Put(ev)
		if err != nil {
			log.Error(err)
			break
		}

		lastSeq = event.Sequence
	}

	// EOF
	ev := eventPool.Get().(*gravity_sdk_types_event.Event)
	ev.Type = gravity_sdk_types_event.Event_TYPE_EVENT
	ev.EventPayload = eventPayloadPool.Get().(*gravity_sdk_types_event.EventPayload)
	ev.SnapshotInfo = nil
	ev.SystemInfo = nil

	// Event payload
	ev.EventPayload.State = gravity_sdk_types_event.EventPayload_STATE_CHUNK_END
	ev.EventPayload.PipelineID = pipeline.id
	ev.EventPayload.Sequence = lastSeq
	ev.EventPayload.Data = []byte("")

	data, _ := proto.Marshal(ev)
	err := connection.Publish(channel, data)
	eventPayloadPool.Put(ev.EventPayload)
	eventPool.Put(ev)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (sub *Subscriber) Fetch(pipeline *Pipeline, startAt uint64, offset uint64, count int) (int, uint64, error) {

	// Getting events from event store
	events, err := pipeline.eventStore.Fetch(startAt, offset, count)
	if err != nil {
		log.Error(err)
		return 0, 0, errors.New("Failed to fetch data from store")
	}

	if len(events) == 0 {
		return 0, startAt, nil
	}

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
		"startAt":  startAt,
		"offset":   offset,
		"count":    count,
		"total":    len(events),
	}).Info("Fetch events")

	return len(events), events[len(events)-1].Sequence, sub.Publish(pipeline, events)
}
