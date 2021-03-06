package synchronizer

import (
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type StoreConfig struct {
	Stores map[string]StoreEntry `json:"stores"`
}

type StoreEntry struct {
	Collection string `json:"collection"`
	Database   string `json:"database"`
	Table      string `json:"table"`
}

type StoreManager struct {
	synchronizer *Synchronizer
	stores       []*Store
}

func NewStoreManager(synchronizer *Synchronizer) *StoreManager {
	return &StoreManager{
		synchronizer: synchronizer,
		stores:       make([]*Store, 0),
	}
}

func (sm *StoreManager) Init() error {

	// Load store settings
	configFile := viper.GetString("rules.store")

	log.WithFields(log.Fields{
		"configFile": configFile,
	}).Info("Loading configuration file for collection store...")

	config, err := sm.LoadConfigFile(configFile)
	if err != nil {
		return err
	}

	for storeName, entry := range config.Stores {
		s, err := sm.LoadStore(storeName, &entry)
		if err != nil {
			log.Error(err)
			continue
		}

		log.WithFields(log.Fields{
			"name":       storeName,
			"collection": s.Collection,
			"database":   s.Database,
			"table":      s.Table,
		}).Info("  Loaded configuration")

		sm.stores = append(sm.stores, s)
	}

	return nil
}

func (sm *StoreManager) LoadConfigFile(filename string) (*StoreConfig, error) {

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

func (sm *StoreManager) LoadStore(name string, entry *StoreEntry) (*Store, error) {

	s := &Store{
		Name:           name,
		Collection:     entry.Collection,
		Database:       entry.Database,
		Table:          entry.Table,
		Transmitter:    sm.synchronizer.transmitterMgr.GetTransmitter(entry.Database),
		TriggerManager: sm.synchronizer.triggerMgr,
	}

	s.Init()

	return s, nil
}

func (sm *StoreManager) AddEventSource(eventStore *EventStore) error {

	log.WithFields(log.Fields{
		"source": eventStore.id,
	}).Info("Adding event source")

	for _, s := range sm.stores {

		err := s.AddEventSource(eventStore)
		if err != nil {
			log.Error(err)
			return err
		}
	}

	return nil
}

func (sm *StoreManager) RemoveEventSource(id uint64) error {

	for _, s := range sm.stores {
		s.RemoveEventSource(id)
	}

	return nil
}
