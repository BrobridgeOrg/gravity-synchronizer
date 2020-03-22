package synchronizer

import "gravity-synchronizer/internal/projection"

type RuleConfig struct {
	Rules []Rule `json:"rules"`
}

type Rule struct {
	Event    string   `json:"event"`
	Database string   `json:"database"`
	Exporter []string `json:"exporter"`
}

func (rule *Rule) Match(pj *projection.Projection) bool {

	if pj.EventName == rule.Event {
		return true
	}

	return false
}
