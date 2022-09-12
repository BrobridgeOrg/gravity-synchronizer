package synchronizer

import (
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Store struct {
	Name       string
	Collection string `json:"collection"`
}

type StoreConfig struct {
	Stores map[string]StoreEntry `json:"stores"`
}

type StoreEntry struct {
	Collection string `json:"collection"`
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
		Name:       name,
		Collection: entry.Collection,
	}

	return s, nil
}
