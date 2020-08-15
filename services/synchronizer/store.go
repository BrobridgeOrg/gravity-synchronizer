package synchronizer

import (
	"gravity-synchronizer/internal/projection"
	"gravity-synchronizer/internal/transmitter"
)

type StoreConfig struct {
	Stores []Store `json:"store"`
}

type Store struct {
	State       *StateStore
	Transmitter *transmitter.Transmitter
	Collection  string `json:"collection"`
	Database    string `json:"database"`
	Table       string `json:"table"`
}

func NewStore() *Store {
	return &Store{}
}

func (store *Store) IsMatch(pj *projection.Projection) bool {

	if pj.Collection != store.Collection {
		return false
	}

	return true
}

func (store *Store) Handle(event *Event) error {

	// store data
	err := store.Transmitter.ProcessData(store.Table, event.Sequence, &event.Projection)
	if err != nil {
		return err
	}

	// Update state
	store.State.Sequence = event.Sequence
	err = store.State.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) Recovery() error {

	return nil
}
