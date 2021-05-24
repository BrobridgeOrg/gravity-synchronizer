package synchronizer

import "errors"

type SnapshotManager struct {
	pipeline  *Pipeline
	snapshots map[string]*Snapshot
}

func NewSnapshotManager(pipeline *Pipeline) *SnapshotManager {
	return &SnapshotManager{
		pipeline:  pipeline,
		snapshots: make(map[string]*Snapshot),
	}
}

func (sm *SnapshotManager) CreateSnapshot(snapshotID string) (*Snapshot, error) {

	_, ok := sm.snapshots[snapshotID]
	if ok {
		return nil, errors.New("Snapshot ID exists")
	}

	// Create native snapshot view
	view, err := sm.pipeline.eventStore.CreateSnapshot()
	if err != nil {
		return nil, err
	}

	// Create snapshot object
	snapshot := NewSnapshot(snapshotID, view)
	sm.snapshots[snapshotID] = snapshot

	return snapshot, nil
}

func (sm *SnapshotManager) GetSnapshot(snapshotID string) *Snapshot {

	snapshot, ok := sm.snapshots[snapshotID]
	if ok {
		return snapshot
	}

	return nil
}
