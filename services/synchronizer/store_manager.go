package synchronizer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type StoreManager struct {
	EventHandler *EventHandler
	Stores       []*Store
}

func CreateStoreManager(eventHandler *EventHandler) *StoreManager {

	return &StoreManager{
		EventHandler: eventHandler,
		Stores:       make([]*Store, 0),
	}
}

func (sm *StoreManager) Initialize() error {

	statePath := viper.GetString("state_store.path")

	log.WithFields(log.Fields{
		"path": statePath,
	}).Info("Initializing state path ...")

	// Ensure path existing
	err := os.MkdirAll(statePath, os.ModePerm)
	if err != nil {
		return err
	}

	// Load store settings
	configFile := viper.GetString("rules.storeconfig")

	log.WithFields(log.Fields{
		"configFile": configFile,
	}).Info("Loading store settings ...")

	config, err := sm.LoadStoreFile(configFile)
	if err != nil {
		return err
	}

	// Store settings
	for _, store := range config.Stores {

		// state file
		fileName := fmt.Sprintf(
			"%s.%s.%s.%s.state.seq",
			sm.EventHandler.GetApp().GetClientID(),
			store.Collection,
			store.Database,
			store.Table,
		)

		stateFilePath := filepath.Join(statePath, fileName)

		s := CreateStore()
		s.Collection = store.Collection
		s.Database = store.Database
		s.Table = store.Table
		s.State = CreateStateStore(stateFilePath)

		// Initializing and recoverying state for this store
		err := s.State.Initialize()
		if err != nil {
			log.Error(err)
			return err
		}

		log.WithFields(log.Fields{
			"collection": s.Collection,
			"database":   s.Database,
			"table":      s.Table,
			"state.seq":  s.State.Sequence,
		}).Info("Loaded store")

		sm.Stores = append(sm.Stores, s)
	}

	return nil
}

func (sm *StoreManager) Recovery() error {

	for _, store := range sm.Stores {

		if store.State.Sequence > 0 {
			continue
		}

		err := store.Recovery()
		if err != nil {
			return err
		}
	}
	return nil
}

func (sm *StoreManager) LoadStoreFile(filename string) (*StoreConfig, error) {

	// Open and read config file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	// Parse config
	var config StoreConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}

func (sm *StoreManager) GetStoreByCollection(collection string) (*Store, error) {

	for _, store := range sm.Stores {
		if store.Collection == collection {
			return store, nil
		}
	}

	return nil, errors.New("No sush store")
}
