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
	output  chan []byte
}

func (ex *Exporter) Initialize() error {

	ex.output = make(chan []byte, 102400)

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
		InitCap:     1,
		MaxCap:      1,
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

	go ex.dispatcher()

	return nil
}

func (ex *Exporter) dispatcher() {

	for data := range ex.output {
		ex.emit(ex.channel, data)
	}
}

func (ex *Exporter) Send(pj *projection.Projection, rawData []byte) error {

	if rawData != nil && pj == nil {
		data := make([]byte, len(rawData))
		copy(data, rawData)
		ex.output <- data
		return nil
	}

	// Genereate JSON string
	data, err := pj.ToJSON()
	if err != nil {
		return err
	}

	ex.output <- data

	return nil
}

func (ex *Exporter) emit(channelName string, data []byte) error {

	// Preparing request
	request := sendEventRequestPool.Get().(*exporter.SendEventRequest)
	request.Channel = channelName
	request.Payload = data

	// Getting stream from pool
	err := ex.pool.GetStream("sendEvent", func(s interface{}) error {
		// Send request
		return s.(exporter.Exporter_SendEventStreamClient).Send(request)
	})
	sendEventRequestPool.Put(request)
	if err != nil {
		log.Error("Failed to get connection: %v", err)
		return errors.New("Cannot connect to exporter")
	}

	return nil
}
