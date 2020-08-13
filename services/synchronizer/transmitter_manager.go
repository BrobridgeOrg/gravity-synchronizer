package synchronizer

import (
	"encoding/json"
	"gravity-synchronizer/internal/transmitter"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type TransmitterConfig struct {
	Transmitters map[string]TransmitterInfo `json:"transmitters"`
}

type TransmitterInfo struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type TransmitterManager struct {
	transmitters map[string]*transmitter.Transmitter
}

func NewTransmitterManager() *TransmitterManager {
	return &TransmitterManager{
		transmitters: make(map[string]*transmitter.Transmitter),
	}
}

func (tm *TransmitterManager) Initialize() error {

	config, err := tm.LoadTransmitterConfig(viper.GetString("rules.transmitter"))
	if err != nil {
		return err
	}

	// Initializing transmitter connections
	for transmitterName, info := range config.Transmitters {

		log.WithFields(log.Fields{
			"name": transmitterName,
		}).Info("Initializing transmitter")

		_, err := tm.InitTransmitter(transmitterName, &info)
		if err != nil {
			log.Error(err)
		}
	}

	return nil
}

func (tm *TransmitterManager) LoadTransmitterConfig(filename string) (*TransmitterConfig, error) {

	// Open configuration file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	// Read
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var config TransmitterConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}

func (tm *TransmitterManager) InitTransmitter(transmitterName string, info *TransmitterInfo) (*transmitter.Transmitter, error) {

	if db, ok := tm.transmitters[transmitterName]; ok {
		return db, nil
	}

	t := transmitter.NewTransmitter(transmitterName, info.Host, info.Port)
	err := t.Init()
	if err != nil {
		return nil, err
	}

	tm.transmitters[transmitterName] = t

	return t, nil
}

func (tm *TransmitterManager) GetTransmitter(transmitterName string) *transmitter.Transmitter {

	if db, ok := tm.transmitters[transmitterName]; ok {
		return db
	}

	return nil
}
