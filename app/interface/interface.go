package app

import "github.com/nats-io/stan.go"

type EventBusImpl interface {
	Emit(string, []byte) error
	On(string, func(*stan.Msg)) error
}

type AppImpl interface {
	GetEventBus() EventBusImpl
}
