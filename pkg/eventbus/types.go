package eventbus

import (
	nats "github.com/nats-io/nats.go"
)

type EventBus interface {
	Connect() error
	Close()
	GetConnection() *nats.Conn
}
