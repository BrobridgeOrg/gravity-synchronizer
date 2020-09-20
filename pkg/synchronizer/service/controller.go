package synchronizer

import (
	"context"
	"errors"
	"fmt"
	"time"

	controller "github.com/BrobridgeOrg/gravity-api/service/controller"
	grpc_connection_pool "github.com/cfsghost/grpc-connection-pool"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func (synchronizer *Synchronizer) initControllerConnection() error {

	host := viper.GetString("controller.host")
	port := viper.GetInt("controller.port")
	address := fmt.Sprintf("%s:%d", host, port)

	log.WithFields(log.Fields{
		"address": address,
	}).Info("Initializing controller connection")

	options := &grpc_connection_pool.Options{
		InitCap:     8,
		MaxCap:      16,
		DialTimeout: time.Second * 20,
	}

	// Initialize connection pool
	p, err := grpc_connection_pool.NewGRPCPool(address, options, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}

	if p == nil {
		return err
	}

	synchronizer.controllerConns = p

	return nil
}

func (synchronizer *Synchronizer) RegisterClient() error {

	// register synchronizer
	grpcConn, err := synchronizer.controllerConns.Get()
	if err != nil {
		return err
	}

	client := controller.NewControllerClient(grpcConn)

	request := &controller.RegisterRequest{
		ClientID: synchronizer.clientID,
	}

	reply, err := client.Register(context.Background(), request)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	log.Info("Registered client")

	return nil
}

func (synchronizer *Synchronizer) GetPipelines() ([]uint64, error) {

	// register synchronizer
	grpcConn, err := synchronizer.controllerConns.Get()
	if err != nil {
		return nil, err
	}

	client := controller.NewControllerClient(grpcConn)

	request := &controller.GetPipelinesRequest{
		ClientID: synchronizer.clientID,
	}

	reply, err := client.GetPipelines(context.Background(), request)
	if err != nil {
		return nil, err
	}

	return reply.Pipelines, nil
}
