package rule

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type RuleConfig struct {
	Rules []*Rule `json:"rules"`
}

type Rule struct {
	ID            string          `json:"id"`
	Event         string          `json:"event"`
	Collection    string          `json:"collection"`
	Method        string          `json:"method"`
	PrimaryKey    string          `json:"primaryKey"`
	Mapping       []MappingConfig `json:"mapping"`
	HandlerConfig *HandlerConfig  `json:"handler,omitempty"`
	Handler       *Handler
}

type MappingConfig struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

func LoadRuleFile(filename string) (*RuleConfig, error) {

	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var config RuleConfig
	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		return nil, err
	}

	for _, rule := range config.Rules {
		if len(rule.ID) == 0 {
			// Generate rule ID
			rule.ID = rule.Event + "_" + rule.Collection + "_" + rule.PrimaryKey
		}

		rule.Handler = NewHandler(rule.HandlerConfig)
	}

	return &config, nil
}

func (rc *RuleConfig) Get(id string) *Rule {

	for _, rule := range rc.Rules {

		// Ignore events if no rule matched
		if rule.ID == id {
			return rule
		}
	}

	return nil
}
