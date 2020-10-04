package synchronizer

import (
	"context"
	"errors"
	"fmt"
	"time"

	exporter "github.com/BrobridgeOrg/gravity-api/service/exporter"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service/projection"
	grpc_connection_pool "github.com/cfsghost/grpc-connection-pool"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const DefaultChannel = "default"

type Exporter struct {
	name    string
	host    string
	port    int
	channel string
	pool    *grpc_connection_pool.GRPCPool
	output  chan *projection.Projection
}

func (ex *Exporter) Initialize() error {

	ex.output = make(chan *projection.Projection, 1024)

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

	ex.pool = p

	go func() {

		for {
			select {
			case pj := <-ex.output:
				go ex.send(pj)
			}
		}
	}()

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

	conn, err := ex.pool.Get()
	if err != nil {
		log.Error(err)
		return err
	}

	client := exporter.NewExporterClient(conn)

	// Preparing context and timeout settings
	grpcCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	reply, err := client.SendEvent(grpcCtx, &exporter.SendEventRequest{
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
		return errors.New(reply.Reason)
	}

	return nil
}
