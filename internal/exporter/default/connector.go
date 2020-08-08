package default_exporter

import (
	"context"
	"gravity-synchronizer/internal/exporter/default/pool"
	"gravity-synchronizer/internal/projection"
	"time"

	exporter "github.com/BrobridgeOrg/gravity-api/service/exporter"

	stan "github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Connector struct {
	host    string
	channel string
	client  stan.Conn

	pool *pool.GRPCPool

	// channels
	output chan *projection.Projection
}

func NewConnector(host string, params map[string]interface{}) *Connector {

	channel, ok := params["channel"]
	if !ok {
		return nil
	}

	return &Connector{
		host:    host,
		channel: channel.(string),
		output:  make(chan *projection.Projection, 1024),
	}
}

func (connector *Connector) Init() error {

	log.WithFields(log.Fields{
		"host": connector.host,
	}).Info("Connecting to exporter")

	options := &pool.Options{
		InitCap:     8,
		MaxCap:      16,
		DialTimeout: time.Second * 20,
		IdleTimeout: time.Second * 60,
	}

	// Initialize connection pool
	p, err := pool.NewGRPCPool(connector.host, options, grpc.WithInsecure())
	if err != nil {
		return err
	}

	if p == nil {
		return err
	}

	connector.pool = p

	go func() {

		for {
			select {
			case pj := <-connector.output:
				go connector.send(pj)
			}
		}
	}()

	return nil
}

func (connector *Connector) Close() {
}

func (connector *Connector) Emit(eventName string, data []byte) error {

	conn, err := connector.pool.Get()
	if err != nil {
		return err
	}

	client := exporter.NewExporterClient(conn)
	connector.pool.Put(conn)

	reply, err := client.SendEvent(context.Background(), &exporter.SendEventRequest{
		Channel: eventName,
		Payload: data,
	})

	if err != nil {
		log.Error(err)
		return err
	}

	if !reply.Success {
		log.WithFields(log.Fields{
			"reason": reply.Reason,
		}).Error("Exporter error")
	}

	return nil
}

func (connector *Connector) Send(sequence uint64, pj *projection.Projection) error {
	connector.output <- pj
	return nil
}

func (connector *Connector) send(pj *projection.Projection) error {

	// Genereate JSON string
	data, err := pj.ToJSON()
	if err != nil {
		return err
	}

	return connector.Emit(connector.channel, data)
}
