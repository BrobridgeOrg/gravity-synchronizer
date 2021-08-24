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
		return &gravity_sdk_types_event.Event{}
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
	ev.EventName = "awake"
	ev.SetPayload(map[string]interface{}{
		"pipelineID": pipeline.id,
		"lastSeq":    pipeline.GetLastSequence(),
	})

	data, _ := proto.Marshal(ev)
	err := connection.Publish(channel, data)
	eventPool.Put(ev)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (sub *Subscriber) PullEvents(pipeline *Pipeline, startAt uint64, offset uint64, count int) ([]*eventstore.Event, uint64, error) {

	// Getting events from event store
	events, err := pipeline.eventStore.Fetch(startAt, offset, count)
	if err != nil {
		log.Error(err)
		return nil, 0, errors.New("Failed to fetch data from store")
	}

	if len(events) == 0 {
		return nil, startAt, nil
	}

	return events, events[len(events)-1].Sequence, nil
}
