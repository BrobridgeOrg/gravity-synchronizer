package synchronizer

import "github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"

type EventStore struct {
	synchronizer *Synchronizer
	store        datastore.Store
	name         string
}

func NewEventStore(synchronizer *Synchronizer, name string) *EventStore {
	return &EventStore{
		synchronizer: synchronizer,
		name:         name,
	}
}

func (es *EventStore) Init() error {

	store, err := es.synchronizer.app.GetStoreManager().GetStore(es.name)
	if err != nil {
		return err
	}

	es.store = store

	return nil
}
