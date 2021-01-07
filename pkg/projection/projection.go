package projection

import (
	"bytes"
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
	Collection string                 `json:"collection"`
	Payload    map[string]interface{} `json:"payload"`
	Meta       map[string][]byte      `json:"meta"`
}

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var pool = sync.Pool{
	New: func() interface{} {
		return &JSONResult{}
	},
}

func Unmarshal(data []byte, pj *Projection) error {

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(pj)
	if err != nil {
		return nil
	}

	/*

		err := json.Unmarshal(data, pj)
		if err != nil {
			return err
		}
	*/
	return nil
}

func (pj *Projection) ToJSON() ([]byte, error) {

	// Allocation
	result := pool.Get().(*JSONResult)
	result.EventName = pj.EventName
	result.Collection = pj.Collection
	result.Payload = make(map[string]interface{})
	result.Meta = pj.Meta

	for _, field := range pj.Fields {
		switch field.Value.(type) {
		case jsoniter.Number:

			if n, err := field.Value.(jsoniter.Number).Int64(); err == nil {
				result.Payload[field.Name] = n
			} else if f, err := field.Value.(jsoniter.Number).Float64(); err == nil {
				result.Payload[field.Name] = f
			}
		default:
			result.Payload[field.Name] = field.Value
		}
	}

	data, err := json.Marshal(result)

	pool.Put(result)

	return data, err
}
