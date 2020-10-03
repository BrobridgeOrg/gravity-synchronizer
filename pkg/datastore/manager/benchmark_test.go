package datastore

import (
	"testing"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
)

var manager *Manager
var store datastore.Store

func init() {

	manager = NewManager()
	manager.dbPath = "./bench"
	err := manager.Init()
	if err != nil {
		panic(err)
	}

	// Create a new store for benchmark
	store, err = manager.GetStore("bench")
	if err != nil {
		panic(err)
	}
}

func BenchmarkWrite(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := store.Write([]byte("Benchmark")); err != nil {
			panic(err)
		}
	}
}
