package instance

import (
	"errors"
	"fmt"

	grpc_server "github.com/BrobridgeOrg/gravity-synchronizer/pkg/grpc_server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func (a *AppInstance) initGRPCServer() error {

	// expose port
	port := viper.GetInt("service.port")

	if port == 0 {
		return errors.New("Required service port for gRPC server")
	}

	host := fmt.Sprintf(":%d", port)

	// Initializing GRPC server
	return a.grpcServer.Init(host)
}

func (a *AppInstance) runGRPCServer() error {
	err := a.grpcServer.Serve()
	if err != nil {
		log.Error(err)
		return err
	}

	return err
}

func (a *AppInstance) GetGRPCServer() grpc_server.Server {
	return grpc_server.Server(a.grpcServer)
}
