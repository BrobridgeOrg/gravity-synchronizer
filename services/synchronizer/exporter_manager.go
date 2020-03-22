package synchronizer

import (
	"encoding/json"
	"gravity-synchronizer/internal/exporter"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type ExporterConfig struct {
	Exporters map[string]ExporterInfo `json:"exporters"`
}

type ExporterInfo struct {
	Type   string                 `json:"type"`
	Host   string                 `json:"host"`
	Port   int                    `json:"port"`
	Params map[string]interface{} `json:"params"`
}

type ExporterManager struct {
	exporters map[string]*exporter.Exporter
}

func CreateExporterManager() *ExporterManager {
	return &ExporterManager{
		exporters: make(map[string]*exporter.Exporter),
	}
}

func (dm *ExporterManager) Initialize() error {

	config, err := dm.LoadExporterConfig(viper.GetString("rules.exporterconfig"))
	if err != nil {
		return err
	}

	// Initializing database connections
	for name, info := range config.Exporters {

		log.WithFields(log.Fields{
			"name": name,
		}).Info("Initializing exporter")

		_, err := dm.InitExporter(name, &info)
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

func (dm *ExporterManager) InitExporter(name string, info *ExporterInfo) (*exporter.Exporter, error) {

	if db, ok := dm.exporters[name]; ok {
		return db, nil
	}

	ex := exporter.CreateExporter()
	err := ex.Connect(info.Type, info.Host, info.Port, info.Params)
	if err != nil {
		return nil, err

	}

	dm.exporters[name] = ex

	return ex, nil
}

func (dm *ExporterManager) GetExporter(name string) *exporter.Exporter {

	if ex, ok := dm.exporters[name]; ok {
		return ex
	}

	return nil
}
