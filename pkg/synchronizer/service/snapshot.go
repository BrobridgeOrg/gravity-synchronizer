package synchronizer

import (
	"fmt"
	"sync"

	eventstore "github.com/BrobridgeOrg/EventStore"
	gravity_sdk_types_event "github.com/BrobridgeOrg/gravity-sdk/types/event"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

var snapshotInfoPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_event.SnapshotInfo{}
	},
}

type Snapshot struct {
	id              string
	snapshotManager *SnapshotManager
	view            *eventstore.SnapshotView
}

func NewSnapshot(sm *SnapshotManager, id string, view *eventstore.SnapshotView) *Snapshot {
	return &Snapshot{
		id:              id,
		snapshotManager: sm,
		view:            view,
	}
}

func (snapshot *Snapshot) Close() error {
	return snapshot.view.Release()
}

func (snapshot *Snapshot) Fetch(subscriberID string, collection string, key []byte, offset uint64, count int64) (int, []byte, error) {

	records, err := snapshot.view.Fetch([]byte(collection), key, offset, int(count))
	if err != nil {
		return 0, key, err
	}

	if len(records) == 0 {
		return 0, key, nil
	}

	return len(records), records[len(records)-1].Key, snapshot.publish(subscriberID, collection, records)
}

func (snapshot *Snapshot) publish(subscriberID string, collection string, records []*eventstore.Record) error {

	connection := snapshot.snapshotManager.pipeline.synchronizer.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.subscriber.%s", snapshot.snapshotManager.pipeline.synchronizer.domain, subscriberID)

	// Publish record
	for _, record := range records {

		// Preparing event
		ev := eventPool.Get().(*gravity_sdk_types_event.Event)
		ev.Type = gravity_sdk_types_event.Event_TYPE_SNAPSHOT
		ev.EventPayload = nil
		ev.SnapshotInfo = snapshotInfoPool.Get().(*gravity_sdk_types_event.SnapshotInfo)
		ev.SystemInfo = nil

		// Snapshot information
		ev.SnapshotInfo.PipelineID = snapshot.snapshotManager.pipeline.id
		ev.SnapshotInfo.SnapshotID = snapshot.id
		ev.SnapshotInfo.Collection = collection
		ev.SnapshotInfo.Key = record.Key
		ev.SnapshotInfo.Data = record.Data
		ev.SnapshotInfo.State = gravity_sdk_types_event.SnapshotInfo_STATE_NONE

		data, _ := proto.Marshal(ev)
		err := connection.Publish(channel, data)
		snapshotInfoPool.Put(ev.SnapshotInfo)
		eventPool.Put(ev)
		if err != nil {
			log.Error(err)
			break
		}
	}

	if len(records) > 0 {
		log.WithFields(log.Fields{
			"channel":    channel,
			"subscriber": subscriberID,
			"snapshot":   snapshot.id,
			"count":      len(records),
		}).Info("Publishing snapshot records")
	}

	// EOF
	ev := eventPool.Get().(*gravity_sdk_types_event.Event)
	defer eventPool.Put(ev)
	ev.Type = gravity_sdk_types_event.Event_TYPE_SNAPSHOT
	ev.EventPayload = nil
	ev.SnapshotInfo = snapshotInfoPool.Get().(*gravity_sdk_types_event.SnapshotInfo)
	ev.SystemInfo = nil

	// Snapshot information
	ev.SnapshotInfo.State = gravity_sdk_types_event.SnapshotInfo_STATE_CHUNK_END
	ev.SnapshotInfo.SnapshotID = snapshot.id
	ev.SnapshotInfo.PipelineID = snapshot.snapshotManager.pipeline.id
	ev.SnapshotInfo.Collection = collection
	ev.SnapshotInfo.Key = []byte("")
	ev.SnapshotInfo.Data = []byte("")

	data, _ := proto.Marshal(ev)
	/*
		// No more records
		if len(records) == 0 {

			// Snapshot
			ev.SnapshotInfo.State = gravity_sdk_types_event.SnapshotInfo_STATE_EOF
		}
	*/
	err := connection.Publish(channel, data)
	snapshotInfoPool.Put(ev.SnapshotInfo)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}
