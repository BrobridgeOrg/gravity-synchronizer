package datastore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
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

	snapshot      *Snapshot
	subscriptions sync.Map
}

func NewStore(manager *Manager, storeName string) (*Store, error) {

	log.WithFields(log.Fields{
		"name": storeName,
	}).Info("Initializing data store")

	// Initializing options
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	//	ro.SetTailing(true)
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
		store.Close()
		return nil, err
	}

	// Initializing snapshot
	snapshot := NewSnapshot(store)
	err = snapshot.Initialize()
	if err != nil {
		store.Close()
		return nil, err
	}

	store.snapshot = snapshot

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

func (store *Store) Close() {

	store.subscriptions.Range(func(k, v interface{}) bool {
		sub := v.(*Subscription)
		sub.Close()
		return true
	})

	store.db.Close()
}

func (store *Store) assertColumnFamily(name string) (*gorocksdb.ColumnFamilyHandle, error) {

	handle, ok := store.cfHandles[name]
	if !ok {
		handle, err := store.db.CreateColumnFamily(store.manager.options, name)
		if err != nil {
			return nil, err
		}

		store.cfHandles[name] = handle

		return handle, nil
	}

	return handle, nil
}

func (store *Store) initializeColumnFamily() error {

	// Assert events
	_, err := store.assertColumnFamily("events")
	if err != nil {
		return err
	}

	// Assert states
	_, err = store.assertColumnFamily("states")
	if err != nil {
		return err
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
		counter.SetCount(lastSeq + 1)
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
	key := Uint64ToBytes(seq)

	// Write
	err = store.db.PutCF(store.wo, cfHandle, key, data)
	if err != nil {
		return 0, err
	}

	// Parsing data
	pj, err := projection.Unmarshal(data)
	if err == nil {

		// Update snapshot
		store.snapshot.Write(seq, pj)

		// Dispatch event to subscribers
		store.DispatchEvent(seq, pj)
	}

	return seq, nil
}

func (store *Store) DispatchEvent(seq uint64, pj *projection.Projection) {

	// Publish event to all of subscription which is waiting for
	store.subscriptions.Range(func(k, v interface{}) bool {
		sub := v.(*Subscription)
		/*
			log.WithFields(log.Fields{
				"seq":     seq,
				"tailing": sub.tailing,
			}).Info("Publish to all subscibers")
		*/
		if sub.tailing {
			sub.Publish(seq, pj)
		}

		return true
	})
}

func (store *Store) GetLastSequence() uint64 {
	return store.counter.Count() - 1
}

func (store *Store) GetDurableState(durableName string) (uint64, error) {

	cfHandle, err := store.GetColumnFamailyHandle("states")
	if err != nil {
		return 0, errors.New("Not found \"states\" column family")
	}

	// Write
	value, err := store.db.GetCF(store.ro, cfHandle, []byte(durableName))
	if err != nil {
		return 0, err
	}

	defer value.Free()

	if !value.Exists() {
		return 0, nil
	}

	lastSeq := BytesToUint64(value.Data())

	return lastSeq, nil
}

func (store *Store) UpdateDurableState(durableName string, lastSeq uint64) error {

	cfHandle, err := store.GetColumnFamailyHandle("states")
	if err != nil {
		return errors.New("Not found \"states\" column family")
	}

	value := Uint64ToBytes(lastSeq)

	// Write
	err = store.db.PutCF(store.wo, cfHandle, []byte(durableName), value)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) GetLastSequenceOfSnapshot() uint64 {
	return store.snapshot.lastSeq
}

func (store *Store) registerSubscription(sub *Subscription) error {
	store.subscriptions.Store(sub, sub)
	return nil
}

func (store *Store) unregisterSubscription(sub *Subscription) error {
	store.subscriptions.Delete(sub)
	return nil
}

func (store *Store) Subscribe(startAt uint64, fn datastore.StoreHandler) (datastore.Subscription, error) {

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return nil, errors.New("Not found \"events\" column family")
	}

	// Create a new subscription entry
	sub := NewSubscription()

	// Initializing iterator
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetTailing(true)
	iter := store.db.NewIteratorCF(ro, cfHandle)
	if iter.Err() != nil {
		return nil, iter.Err()
	}

	// Register subscription
	store.registerSubscription(sub)

	go func() {
		defer func() {
			iter.Close()
			store.unregisterSubscription(sub)
		}()

		// Seek
		key := Uint64ToBytes(startAt)
		iter.Seek(key)

		// Start watching
		sub.Watch(iter, fn)
	}()

	return datastore.Subscription(sub), nil
}
