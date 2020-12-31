package data_handler

import (
	"io/ioutil"
	"os"
)

type RuleConfig struct {
	Rules []*Rule `json:"rules"`
}

type Rule struct {
	Event      string          `json:"event"`
	Collection string          `json:"collection"`
	Method     string          `json:"method"`
	PrimaryKey string          `json:"primaryKey"`
	Mapping    []MappingConfig `json:"mapping"`
}

type MappingConfig struct {
	Source  string `json:"source"`
	Target  string `json:"target"`
	Primary bool   `json:"primary"`
}

func loadRuleFile(filename string) (*RuleConfig, error) {

	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var config RuleConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}

func (processor *Processor) LoadRuleFile(filename string) error {

	// Load rule config
	config, err := loadRuleFile(filename)
	if err != nil {
		return err
	}

	processor.ruleConfig = config

	return nil
}
