package datastore

import (
	"github.com/tecbot/gorocksdb"
)

type StoreHandler func(uint64, []byte) bool

type Subscription interface {
	Close()
	Watch(*gorocksdb.Iterator)
}

type Store interface {
	Close()
	Write([]byte) (uint64, error)
	GetLastSequenceOfSnapshot() uint64
	GetLastSequence() uint64
	GetDurableState(string) (uint64, error)
	UpdateDurableState(string, uint64) error
	Subscribe(uint64, StoreHandler) (Subscription, error)
}

type StoreManager interface {
	Init() error
	GetStore(string) (Store, error)
}
