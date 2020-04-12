package synchronizer

import "gravity-synchronizer/internal/projection"

type TriggerConfig struct {
	Triggers []Trigger `json:"trigger"`
}

type Trigger struct {
	Condition Condition `json:"condition"`
	Action    Action    `json:"action"`
}

type Condition struct {
	Collection string `json:"collection"`
	Event      string `json:"event"`
}

type Action struct {
	Type     string `json:"type"`
	Exporter string `json:"exporter"`
}

func (trigger *Trigger) IsMatch(pj *projection.Projection) bool {

	if len(trigger.Condition.Collection) > 0 {
		if pj.Collection != trigger.Condition.Collection {
			return false
		}
	}

	if len(trigger.Condition.Event) > 0 {
		if pj.EventName != trigger.Condition.Event {
			return false
		}
	}

	return true
}
