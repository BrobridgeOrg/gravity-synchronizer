package synchronizer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	exporter "github.com/BrobridgeOrg/gravity-api/service/exporter"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
	grpc_connection_pool "github.com/cfsghost/grpc-connection-pool"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const DefaultChannel = "default"

var sendEventRequestPool = sync.Pool{
	New: func() interface{} {
		return &exporter.SendEventRequest{}
	},
}

type Exporter struct {
	name    string
	host    string
	port    int
	channel string
	pool    *grpc_connection_pool.GRPCPool
	output  chan *projection.Projection
}

func (ex *Exporter) Initialize() error {

	ex.output = make(chan *projection.Projection, 4096)

	if len(ex.channel) == 0 {
		ex.channel = DefaultChannel
	}

	log.WithFields(log.Fields{
		"name":    ex.name,
		"host":    ex.host,
		"port":    ex.port,
		"channel": ex.channel,
	}).Info("  Connecting to exporter...")

	options := &grpc_connection_pool.Options{
		InitCap:     8,
		MaxCap:      16,
		DialTimeout: time.Second * 20,
	}

	host := fmt.Sprintf("%s:%d", ex.host, ex.port)

	// Initialize connection pool
	p, err := grpc_connection_pool.NewGRPCPool(host, options, grpc.WithInsecure())
	if err != nil {
		return err
	}

	if p == nil {
		return err
	}

	// Register initializer for stream
	p.SetStreamInitializer("sendEvent", func(conn *grpc.ClientConn) (interface{}, error) {
		client := exporter.NewExporterClient(conn)
		return client.SendEventStream(context.Background())
	})

	ex.pool = p

	return ex.InitWorkers()
}

func (ex *Exporter) InitWorkers() error {

	// Multiplexing
	for i := 0; i < 4; i++ {

		go func() {

			for {
				pj := <-ex.output
				ex.send(pj)
			}
		}()
	}

	return nil
}

func (ex *Exporter) Send(pj *projection.Projection) error {
	ex.output <- pj
	return nil
}

func (ex *Exporter) send(pj *projection.Projection) error {

	// Genereate JSON string
	data, err := pj.ToJSON()
	if err != nil {
		return err
	}

	return ex.Emit(ex.channel, data)
}

func (ex *Exporter) Emit(eventName string, data []byte) error {

	// Preparing request
	request := sendEventRequestPool.Get().(*exporter.SendEventRequest)
	request.Channel = eventName
	request.Payload = data

	// Getting stream from pool
	err := ex.pool.GetStream("sendEvent", func(s interface{}) error {
		// Send request
		err := s.(exporter.Exporter_SendEventStreamClient).Send(request)
		if err != nil {
			log.Error(err)
		}

		return err
	})
	sendEventRequestPool.Put(request)
	if err != nil {
		log.Error("Failed to get connection: %v", err)
		return errors.New("Cannot connect to exporter")
	}

	return nil
}
