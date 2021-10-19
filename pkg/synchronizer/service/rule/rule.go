package rule

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"github.com/BrobridgeOrg/schemer"
	log "github.com/sirupsen/logrus"
)

type RuleConfig struct {
	Rules []*Rule `json:"rules"`
}

type Rule struct {
	ID            string                 `json:"id"`
	Event         string                 `json:"event"`
	Collection    string                 `json:"collection"`
	Method        string                 `json:"method"`
	PrimaryKey    string                 `json:"primaryKey"`
	Mapping       []MappingConfig        `json:"mapping"`
	HandlerConfig *HandlerConfig         `json:"handler,omitempty"`
	Schema        map[string]interface{} `json:"schema,omitempty"`
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

		rule.Method = strings.ToUpper(rule.Method)
		rule.Handler = NewHandler(rule.HandlerConfig)

		// Initializing event payload schema
		if rule.Schema != nil {
			schema := schemer.NewSchema()
			err := schemer.Unmarshal(rule.Schema, schema)
			if err != nil {
				log.Error(err)
				continue
			}

			rule.Handler.Transformer.SetSourceSchema(schema)
		}
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
