package datastore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"github.com/tecbot/gorocksdb"
)

type Store struct {
	manager   *Manager
	name      string
	db        *gorocksdb.DB
	cfHandles map[string]*gorocksdb.ColumnFamilyHandle
	ro        *gorocksdb.ReadOptions
	wo        *gorocksdb.WriteOptions
	counter   Counter
}

func NewStore(manager *Manager, storeName string) (*Store, error) {

	log.WithFields(log.Fields{
		"name": storeName,
	}).Info("Initializing data store")

	// Initializing options
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetTailing(true)
	wo := gorocksdb.NewDefaultWriteOptions()

	store := &Store{
		manager:   manager,
		name:      storeName,
		cfHandles: make(map[string]*gorocksdb.ColumnFamilyHandle),
		ro:        ro,
		wo:        wo,
	}

	err := store.openDatabase()
	if err != nil {
		return nil, err
	}

	err = store.initializeCounter()
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (store *Store) openDatabase() error {

	dbpath := filepath.Join(store.manager.dbPath, store.name)
	err := os.MkdirAll(dbpath, os.ModePerm)
	if err != nil {
		return err
	}

	// List column families
	cfNames, _ := gorocksdb.ListColumnFamilies(store.manager.options, dbpath)

	if len(cfNames) == 0 {
		cfNames = []string{"default"}
	}

	// Preparing options for column families
	cfOpts := make([]*gorocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts[i] = store.manager.options
	}

	// Open database
	db, cfHandles, err := gorocksdb.OpenDbColumnFamilies(store.manager.options, dbpath, cfNames, cfOpts)
	if err != nil {
		log.Errorf("Failed to open rocks database, %s", err)
		return err
	}

	for i, name := range cfNames {
		store.cfHandles[name] = cfHandles[i]
	}

	store.db = db

	err = store.initializeColumnFamily()
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) initializeColumnFamily() error {

	if _, ok := store.cfHandles["events"]; !ok {
		handle, err := store.db.CreateColumnFamily(store.manager.options, "events")
		if err != nil {
			return err
		}

		store.cfHandles["events"] = handle
	}

	return nil
}

func (store *Store) initializeCounter() error {

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return errors.New("Not found \"events\" column family, so failed to initialize counter.")
	}

	// Initializing counter
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	iter := store.db.NewIteratorCF(ro, cfHandle)
	defer iter.Close()

	// Find last key
	iter.SeekToLast()
	counter := Counter(1)
	if iter.Err() != nil {
		log.Error(iter.Err())
		return iter.Err()
	}

	if iter.Valid() {
		key := iter.Key()
		lastSeq := BytesToUint64(key.Data())
		key.Free()
		counter.SetCount(lastSeq)
	}

	store.counter = counter

	log.WithFields(log.Fields{
		"store":   store.name,
		"lastSeq": counter,
	}).Info("Initialized counter")

	return nil
}

func (store *Store) GetColumnFamailyHandle(name string) (*gorocksdb.ColumnFamilyHandle, error) {

	cfHandle, ok := store.cfHandles[name]
	if !ok {
		return nil, fmt.Errorf("Not found \"%s\" column family", name)
	}

	return cfHandle, nil
}

func (store *Store) Write(data []byte) (uint64, error) {

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return 0, errors.New("Not found \"events\" column family")
	}

	// Getting sequence
	seq := store.counter.Count()
	store.counter.Increase(1)

	// Preparing key-value
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()
	key := Uint64ToBytes(seq)
	batch.PutCF(cfHandle, key, data)

	// Write
	err = store.db.Write(store.wo, batch)
	if err != nil {
		return 0, err
	}

	return seq, nil
}
