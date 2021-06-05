package synchronizer

import (
	"errors"
	"sync"

	log "github.com/sirupsen/logrus"
)

type SnapshotManager struct {
	pipeline  *Pipeline
	snapshots sync.Map
}

func NewSnapshotManager(pipeline *Pipeline) *SnapshotManager {
	return &SnapshotManager{
		pipeline: pipeline,
	}
}

func (sm *SnapshotManager) CreateSnapshot(snapshotID string) (*Snapshot, error) {

	_, ok := sm.snapshots.Load(snapshotID)
	if ok {
		return nil, errors.New("Snapshot ID exists")
	}

	// Create native snapshot view
	view, err := sm.pipeline.eventStore.CreateSnapshot()
	if err != nil {
		return nil, err
	}

	// Create snapshot object
	snapshot := NewSnapshot(sm, snapshotID, view)
	sm.snapshots.Store(snapshotID, snapshot)

	return snapshot, nil
}

func (sm *SnapshotManager) GetSnapshot(snapshotID string) *Snapshot {

	snapshot, ok := sm.snapshots.Load(snapshotID)
	if ok {
		return snapshot.(*Snapshot)
	}

	return nil
}

func (sm *SnapshotManager) ReleaseSnapshot(snapshotID string) error {

	// Getting existing snapshot
	snapshot := sm.GetSnapshot(snapshotID)
	if snapshot == nil {
		return errors.New("No such snapshot")
	}

	// Remove
	sm.snapshots.Delete(snapshotID)

	// Close
	err := snapshot.Close()
	if err != nil {
		log.Error(err)
		return err
	}

	log.WithFields(log.Fields{
		"snapshot": snapshot.id,
	}).Info("Snapshot was released")

	return nil
}
