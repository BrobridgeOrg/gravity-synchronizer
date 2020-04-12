package synchronizer

import (
	"encoding/json"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type TriggerManager struct {
	EventHandler *EventHandler
	Triggers     []*Trigger
}

func CreateTriggerManager(eventHandler *EventHandler) *TriggerManager {

	return &TriggerManager{
		EventHandler: eventHandler,
		Triggers:     make([]*Trigger, 0),
	}
}

func (tm *TriggerManager) Initialize() error {

	configFile := viper.GetString("rules.triggerconfig")

	log.WithFields(log.Fields{
		"configFile": configFile,
	}).Info("Loading trigger settings ...")

	config, err := tm.LoadTriggerFile(configFile)
	if err != nil {
		return err
	}

	// Store triggers
	for _, trigger := range config.Triggers {

		log.WithFields(log.Fields{
			"event": trigger.Condition.Event,
		}).Info("Loaded trigger")

		t := trigger
		tm.Triggers = append(tm.Triggers, &t)
	}

	return nil
}

func (tm *TriggerManager) LoadTriggerFile(filename string) (*TriggerConfig, error) {

	// Open and read trigger file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	// Parse triggers
	var config TriggerConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}
