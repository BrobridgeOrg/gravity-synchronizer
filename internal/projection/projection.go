package projection

import "encoding/json"

type Field struct {
	Name    string      `json:"name"`
	Value   interface{} `json:"value"`
	Primary bool        `json:"primary"`
}

type Projection struct {
	EventName  string  `json:"event"`
	Collection string  `json:"collection"`
	Method     string  `json:"method"`
	Fields     []Field `json:"fields"`
}

type JSONResult struct {
	EventName  string                 `json:"event"`
	Collection string                 `json:"table"`
	Payload    map[string]interface{} `json:"payload"`
}

func (pj *Projection) ToJSON() ([]byte, error) {

	result := JSONResult{
		EventName:  pj.EventName,
		Collection: pj.Collection,
		Payload:    make(map[string]interface{}),
	}

	for _, field := range pj.Fields {
		result.Payload[field.Name] = field.Value
	}

	return json.Marshal(result)
}
