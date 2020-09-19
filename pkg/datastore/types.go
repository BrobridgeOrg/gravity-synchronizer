package datastore

type Store interface {
	Write([]byte) (uint64, error)
}

type StoreManager interface {
	Init() error
	GetStore(string) (Store, error)
}
