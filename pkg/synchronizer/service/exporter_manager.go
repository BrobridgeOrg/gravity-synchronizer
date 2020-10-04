package synchronizer

import (
	"encoding/json"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type ExporterConfig struct {
	Exporters map[string]ExporterInfo `json:"exporters"`
}

type ExporterInfo struct {
	Host    string `json:"host"`
	Port    int    `json:"port"`
	Channel string `json:"channel"`
}

type ExporterManager struct {
	exporters map[string]*Exporter
}

func NewExporterManager() *ExporterManager {
	return &ExporterManager{
		exporters: make(map[string]*Exporter),
	}
}

func (dm *ExporterManager) Initialize() error {

	config, err := dm.LoadExporterConfig(viper.GetString("rules.exporter"))
	if err != nil {
		return err
	}

	// Initializing exporters
	for name, info := range config.Exporters {

		log.WithFields(log.Fields{
			"name":    name,
			"host":    info.Host,
			"port":    info.Port,
			"channel": info.Channel,
		}).Info("Initializing exporter")

		ex := &Exporter{
			name:    name,
			host:    info.Host,
			port:    info.Port,
			channel: info.Channel,
		}

		err := dm.InitExporter(name, ex)
		if err != nil {
			log.Error(err)
		}
	}

	return nil
}

func (dm *ExporterManager) LoadExporterConfig(filename string) (*ExporterConfig, error) {

	// Open configuration file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	// Read
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var config ExporterConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}

func (dm *ExporterManager) InitExporter(name string, ex *Exporter) error {

	if _, ok := dm.exporters[name]; ok {
		return nil
	}

	err := ex.Initialize()
	if err != nil {
		return err
	}

	dm.exporters[name] = ex

	return nil
}

func (dm *ExporterManager) GetExporter(name string) *Exporter {

	if ex, ok := dm.exporters[name]; ok {
		return ex
	}

	return nil
}
