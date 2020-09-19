package datastore

import (
	"os"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/app"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/tecbot/gorocksdb"
)

type Manager struct {
	app    app.App
	dbPath string
	stores map[string]*Store

	options *gorocksdb.Options
}

func NewManager(a app.App) *Manager {
	return &Manager{
		app:    a,
		stores: make(map[string]*Store),
	}
}

func (manager *Manager) Init() error {

	// Preparing database path
	manager.dbPath = viper.GetString("datastore.path")

	log.WithFields(log.Fields{
		"path": manager.dbPath,
	}).Info("Loading data store")

	err := os.MkdirAll(manager.dbPath, os.ModePerm)
	if err != nil {
		return err
	}

	// Well, I am not really sure what i am writing right here. hope it won't get any troubles. :-S
	options := gorocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	options.SetEnablePipelinedWrite(true)
	options.SetAllowConcurrentMemtableWrites(true)
	options.SetOptimizeFiltersForHits(true)
	options.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(2))

	blockBasedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	blockBasedTableOptions.SetBlockSizeDeviation(5)
	blockBasedTableOptions.SetBlockSize(32 * 1024)
	blockBasedTableOptions.SetCacheIndexAndFilterBlocks(true)
	blockBasedTableOptions.SetCacheIndexAndFilterBlocksWithHighPriority(true)
	blockBasedTableOptions.SetPinL0FilterAndIndexBlocksInCache(true)
	//	blockBasedTableOptions.SetIndexType(gorocksdb.KHashSearchIndexType)
	options.SetBlockBasedTableFactory(blockBasedTableOptions)

	env := gorocksdb.NewDefaultEnv()
	env.SetBackgroundThreads(4)
	options.SetMaxBackgroundCompactions(4)
	options.SetEnv(env)

	manager.options = options

	return nil
}

func (manager *Manager) GetStore(storeName string) (datastore.Store, error) {

	if store, ok := manager.stores[storeName]; ok {
		return datastore.Store(store), nil
	}

	store, err := NewStore(manager, storeName)
	if err != nil {
		return nil, err

	}

	manager.stores[storeName] = store

	return datastore.Store(store), nil
}
