package projection

import (
	"sync"

	jsoniter "github.com/json-iterator/go"
)

type Field struct {
	Name    string      `json:"name"`
	Value   interface{} `json:"value"`
	Primary bool        `json:"primary"`
}

type Projection struct {
	EventName  string            `json:"event"`
	Collection string            `json:"collection"`
	Method     string            `json:"method"`
	PrimaryKey string            `json:"primaryKey"`
	Fields     []Field           `json:"fields"`
	Meta       map[string][]byte `json:"meta"`
}

type JSONResult struct {
	EventName  string                 `json:"event"`
	Collection string                 `json:"table"`
	Payload    map[string]interface{} `json:"payload"`
}

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var pool = sync.Pool{
	New: func() interface{} {
		return &JSONResult{}
	},
}

func Unmarshal(data []byte, pj *Projection) error {

	err := json.Unmarshal(data, pj)
	if err != nil {
		return err
	}

	return nil
}

func (pj *Projection) ToJSON() ([]byte, error) {

	// Allocation
	result := pool.Get().(*JSONResult)
	result.EventName = pj.EventName
	result.Collection = pj.Collection
	result.Payload = make(map[string]interface{})

	for _, field := range pj.Fields {
		result.Payload[field.Name] = field.Value
	}

	data, err := json.Marshal(result)

	pool.Put(result)

	return data, err
}
