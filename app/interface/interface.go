package app

import "github.com/nats-io/stan.go"

type EventBusImpl interface {
	Emit(string, []byte) error
	On(string, func(*stan.Msg), uint64) error
	RegisterRecoveryHandler(func()) int
	UnregisterRecoveryHandler(id int)
}

type AppImpl interface {
	GetClientID() string
	GetEventBus() EventBusImpl
}
