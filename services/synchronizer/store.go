package synchronizer

import "gravity-synchronizer/internal/projection"

type StoreConfig struct {
	Stores []Store `json:"store"`
}

type Store struct {
	State      *StateStore
	Collection string `json:"collection"`
	Database   string `json:"database"`
	Table      string `json:"table"`
}

func CreateStore() *Store {
	return &Store{}
}

func (store *Store) IsMatch(pj *projection.Projection) bool {

	if pj.Table != store.Collection {
		return false
	}

	return true
}

func (store *Store) Recovery() error {

	return nil
}
