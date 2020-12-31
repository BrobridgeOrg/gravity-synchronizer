package data_handler

import "sync"

type Projection struct {
	EventName  string            `json:"event"`
	Collection string            `json:"collection"`
	Method     string            `json:"method"`
	PrimaryKey string            `json:"primaryKey"`
	Fields     []Field           `json:"fields"`
	Meta       map[string][]byte `json:"meta"`
}

var projectionPool = sync.Pool{
	New: func() interface{} {
		return &Projection{}
	},
}
