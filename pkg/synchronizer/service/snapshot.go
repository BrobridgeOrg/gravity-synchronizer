package synchronizer

import (
	eventstore "github.com/BrobridgeOrg/EventStore"
)

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

func (snapshot *Snapshot) Pull(subscriberID string, collection string, key []byte, offset uint64, count int64) ([]*eventstore.Record, []byte, error) {

	records, err := snapshot.view.Fetch([]byte(collection), key, offset, int(count))
	if err != nil {
		return nil, key, err
	}

	if len(records) == 0 {
		return nil, key, nil
	}

	return records, records[len(records)-1].Key, nil
}
