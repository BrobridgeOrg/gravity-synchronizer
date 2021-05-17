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

type Subscriber struct {
	synchronizer *Synchronizer
	id           string
	name         string
	collections  sync.Map
}

func NewSubscriber(id string, name string) *Subscriber {
	return &Subscriber{
		id:   id,
		name: name,
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

func (sub *Subscriber) Publish(pipeline *Pipeline, events []*eventstore.Event) error {

	connection := sub.synchronizer.eventBus.bus.GetConnection()
	channel := fmt.Sprintf("gravity.subscriber.%s", sub.id)

	log.WithFields(log.Fields{
		"channel": channel,
	}).Info("Publishing events")

	// Publish events
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

		data, _ := proto.Marshal(&gravity_sdk_types_event.Event{
			PipelineID: pipeline.id,
			Sequence:   event.Sequence,
			Data:       event.Data,
		})

		err = connection.Publish(channel, data)
		if err != nil {
			log.Error(err)
			break
		}
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
