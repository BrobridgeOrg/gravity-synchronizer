package synchronizer

import (
	"encoding/json"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type RuleEngine struct {
	EventHandler *EventHandler
	Rules        []*Rule `json:"rules"`
}

func CreateRuleEngine(eventHandler *EventHandler) *RuleEngine {

	return &RuleEngine{
		EventHandler: eventHandler,
		Rules:        make([]*Rule, 0),
	}
}

func (re *RuleEngine) Initialize() error {

	config, err := re.LoadRuleFile(viper.GetString("rules.ruleconfig"))
	if err != nil {
		return err
	}

	// Store rules
	for _, rule := range config.Rules {

		log.WithFields(log.Fields{
			"event":    rule.Event,
			"database": rule.Database,
		}).Info("Loaded rule")

		r := rule
		re.Rules = append(re.Rules, &r)
	}

	return nil
}

func (re *RuleEngine) LoadRuleFile(filename string) (*RuleConfig, error) {

	// Open and read rule file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	// Parse rules
	var config RuleConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}
