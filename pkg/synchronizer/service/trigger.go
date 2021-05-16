package synchronizer

import gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"

type Trigger struct {
	Condition Condition `json:"condition"`
	Action    Action    `json:"action"`
}

type Condition struct {
	Store      string `json:"store"`
	Collection string `json:"collection"`
	Event      string `json:"event"`
}

type Action struct {
	Type             string `json:"type"`
	Exporter         string `json:"exporter"`
	ExporterInstance *Exporter
}

func (trigger *Trigger) IsMatch(storeName string, pj *gravity_sdk_types_projection.Projection) bool {

	if len(trigger.Condition.Store) > 0 {
		if storeName != trigger.Condition.Store {
			return false
		}
	}

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

func (trigger *Trigger) Handle(storeName string, pj *gravity_sdk_types_projection.Projection, rawData []byte) error {
	return trigger.Action.ExporterInstance.Send(pj, rawData)
}
