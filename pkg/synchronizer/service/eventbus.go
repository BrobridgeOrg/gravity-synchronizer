package synchronizer

import (
	"fmt"
	"time"

	eventbus "github.com/BrobridgeOrg/gravity-synchronizer/pkg/eventbus/service"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	DefaultPingInterval        = 10
	DefaultMaxPingsOutstanding = 3
	DefaultMaxReconnects       = -1
)

type EventBus struct {
	synchronizer        *Synchronizer
	bus                 *eventbus.EventBus
	host                string
	port                int
	clusterID           string
	durableName         string
	channel             string
	pingInterval        int64
	maxPingsOutstanding int
	maxReconnects       int
}

func NewEventBus(synchronizer *Synchronizer) *EventBus {
	return &EventBus{
		synchronizer: synchronizer,
	}
}

func (eb *EventBus) Initialize() error {

	// default settings
	viper.SetDefault("eventbus.pingInterval", DefaultPingInterval)
	viper.SetDefault("eventbus.maxPingsOutstanding", DefaultMaxPingsOutstanding)
	viper.SetDefault("eventbus.maxReconnects", DefaultMaxReconnects)

	// Read configs
	eb.host = viper.GetString("eventbus.host")
	eb.port = viper.GetInt("eventbus.port")
	eb.pingInterval = viper.GetInt64("eventbus.pingInterval")
	eb.maxPingsOutstanding = viper.GetInt("eventbus.maxPingsOutstanding")
	eb.maxReconnects = viper.GetInt("eventbus.maxReconnects")

	address := fmt.Sprintf("%s:%d", eb.host, eb.port)

	log.WithFields(log.Fields{
		"address":             address,
		"pingInterval":        time.Duration(eb.pingInterval) * time.Second,
		"maxPingsOutstanding": eb.maxPingsOutstanding,
		"maxReconnects":       eb.maxReconnects,
	}).Info("Initializing eventbus connections")

	options := eventbus.Options{
		PingInterval:        time.Duration(eb.pingInterval),
		MaxPingsOutstanding: eb.maxPingsOutstanding,
		MaxReconnects:       eb.maxReconnects,
	}

	// Create a new instance connector
	eb.bus = eventbus.NewEventBus(
		address,
		eventbus.EventBusHandler{
			Reconnect: func(natsConn *nats.Conn) {
				/*
					err := source.InitSubscription()
					if err != nil {
						log.Error(err)
						return
					}
				*/
				log.Warn("re-connected to event server")
			},
			Disconnect: func(natsConn *nats.Conn) {
				log.Error("event server was disconnected")
			},
		},
		options,
	)

	// Connect
	err := eb.bus.Connect()
	if err != nil {
		return err
	}

	return nil
}
