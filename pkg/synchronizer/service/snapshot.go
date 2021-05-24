package synchronizer

import eventstore "github.com/BrobridgeOrg/EventStore"

type Snapshot struct {
	id   string
	view *eventstore.SnapshotView
}

func NewSnapshot(id string, view *eventstore.SnapshotView) *Snapshot {
	return &Snapshot{
		id:   id,
		view: view,
	}
}
