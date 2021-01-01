package request

import (
	"sync"
)

var customRequestPool = sync.Pool{
	New: func() interface{} {
		return &CustomRequest{}
	},
}

func NewCustomRequest(getDataFn func() []byte, errorFn func(error) error, respondFn func() error) Request {
	request := customRequestPool.Get().(*CustomRequest)
	request.getDataFn = getDataFn
	request.errorFn = errorFn
	request.respondFn = respondFn

	return Request(request)
}

type CustomRequest struct {
	getDataFn func() []byte
	errorFn   func(error) error
	respondFn func() error
}

func (request *CustomRequest) Free() {
	customRequestPool.Put(request)
}

func (request *CustomRequest) Data() []byte {
	return request.getDataFn()
}

func (request *CustomRequest) Error(err error) error {
	return request.errorFn(err)
}

func (request *CustomRequest) Respond() error {
	return request.respondFn()
}
