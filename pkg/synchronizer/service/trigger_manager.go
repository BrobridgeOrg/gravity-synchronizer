package synchronizer

import (
	"io/ioutil"
	"os"

	gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type TriggerConfig struct {
	Triggers []Trigger `json:"trigger"`
}

type TriggerManager struct {
	synchronizer *Synchronizer
	Triggers     []*Trigger
}

func NewTriggerManager(synchronizer *Synchronizer) *TriggerManager {

	return &TriggerManager{
		synchronizer: synchronizer,
		Triggers:     make([]*Trigger, 0),
	}
}

func (tm *TriggerManager) Initialize() error {

	configFile := viper.GetString("rules.trigger")

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
			"event":    trigger.Condition.Event,
			"exporter": trigger.Action.Exporter,
		}).Info("Loading trigger...")

		// Getting exporter for trigger
		ex := tm.synchronizer.exporterMgr.GetExporter(trigger.Action.Exporter)
		if ex == nil {
			log.WithFields(log.Fields{
				"exporter": trigger.Action.Exporter,
			}).Error("No such exporter")
			continue
		}

		t := trigger
		t.Action.ExporterInstance = ex

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

func (tm *TriggerManager) Handle(storeName string, pj *gravity_sdk_types_projection.Projection, rawData []byte) error {

	for _, trigger := range tm.Triggers {
		if !trigger.IsMatch(storeName, pj) {
			continue
		}

		trigger.Handle(storeName, pj, rawData)
	}

	return nil
}
