package eventbus

import (
	"time"

	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
)

type EventBus struct {
	host              string
	clusterID         string
	clientName        string
	durableName       string
	client            stan.Conn
	natsConn          *nats.Conn
	reconnectHandler  func(natsConn *nats.Conn)
	disconnectHandler func(natsConn *nats.Conn)
}

func CreateConnector(host string, clusterID string, clientName string, durableName string, reconnectHandler func(natsConn *nats.Conn), disconnectHandler func(natsConn *nats.Conn)) *EventBus {
	return &EventBus{
		host:              host,
		clusterID:         clusterID,
		clientName:        clientName,
		durableName:       durableName,
		natsConn:          nil,
		reconnectHandler:  reconnectHandler,
		disconnectHandler: disconnectHandler,
	}
}

func (eb *EventBus) Connect() error {

	log.WithFields(log.Fields{
		"host":        eb.host,
		"clientName":  eb.clientName,
		"clusterID":   eb.clusterID,
		"durableName": eb.durableName,
	}).Info("Connecting to event server")

	if eb.natsConn == nil {
		// Create NATS connection
		nc, err := nats.Connect(eb.host,
			nats.PingInterval(10*time.Second),
			nats.MaxPingsOutstanding(3),
			nats.MaxReconnects(-1),
			nats.ReconnectHandler(eb.reconnectHandler),
			nats.DisconnectHandler(eb.disconnectHandler),
		)
		if err != nil {
			return err
		}

		eb.natsConn = nc
	}

	// Connect to queue server
	sc, err := stan.Connect(
		eb.clusterID,
		eb.clientName,
		stan.NatsConn(eb.natsConn),
	)
	if err != nil {
		return err
	}

	eb.client = sc

	return nil
}

func (eb *EventBus) Close() {
	eb.client.Close()
}

func (eb *EventBus) Emit(eventName string, data []byte) error {

	if err := eb.client.Publish(eventName, data); err != nil {
		return err
	}

	return nil
}

func (eb *EventBus) On(eventName string, fn func(*stan.Msg), seq uint64) error {

	if seq == 0 {
		if _, err := eb.client.Subscribe(eventName, fn, stan.DurableName(eb.durableName), stan.SetManualAckMode()); err != nil {
			return err
		}

		return nil
	}

	if _, err := eb.client.Subscribe(eventName, fn, stan.SetManualAckMode(), stan.StartAtSequence(seq)); err != nil {
		return err
	}

	return nil
}
