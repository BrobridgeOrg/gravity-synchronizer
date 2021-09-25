package collection

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type Config struct {
	Collections map[string]*CollectionConfig `json:"collections"`
}

type CollectionConfig struct {
	Schema map[string]interface{} `json:"schema"`
}

func LoadConfigFile(filename string) (*Config, error) {

	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var config Config
	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func (c *Config) GetCollectionConfig(name string) *CollectionConfig {
	if v, ok := c.Collections[name]; ok {
		return v
	}

	return nil
}
