package synchronizer

import (
	"encoding/json"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type DatabaseConfig struct {
	Databases map[string]DatabaseInfo `json:"databases"`
}

type DatabaseInfo struct {
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Secure   bool   `json:"secure"`
	Username string `json:"username"`
	Password string `json:"password"`
	DbName   string `json:"dbname"`
}

type DatabaseManager struct {
	databases map[string]*Database
}

func CreateDatabaseManager() *DatabaseManager {
	return &DatabaseManager{
		databases: make(map[string]*Database),
	}
}

func (dm *DatabaseManager) Initialize() error {

	config, err := dm.LoadDatabaseConfig(viper.GetString("rules.dbconfig"))
	if err != nil {
		return err
	}

	// Initializing database connections
	for dbName, info := range config.Databases {

		log.WithFields(log.Fields{
			"name": dbName,
		}).Info("Initializing database adaptor")

		_, err := dm.InitDatabase(dbName, &info)
		if err != nil {
			log.Error(err)
		}
	}

	return nil
}

func (dm *DatabaseManager) LoadDatabaseConfig(filename string) (*DatabaseConfig, error) {

	// Open configuration file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	// Read
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var config DatabaseConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}

func (dm *DatabaseManager) InitDatabase(dbname string, info *DatabaseInfo) (*Database, error) {

	if db, ok := dm.databases[dbname]; ok {
		return db, nil
	}

	db, err := OpenDatabase(dbname, info)
	if err != nil {
		return nil, err

	}

	dm.databases[dbname] = db

	return db, nil
}

func (dm *DatabaseManager) GetDatabase(dbname string) *Database {

	if db, ok := dm.databases[dbname]; ok {
		return db
	}

	return nil
}
