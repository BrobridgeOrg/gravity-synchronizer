package app

import "github.com/nats-io/stan.go"

type EventBusImpl interface {
	Emit(string, []byte) error
	On(string, func(*stan.Msg), uint64) error
}

type AppImpl interface {
	GetClientID() string
	GetEventBus() EventBusImpl
}
