package server

import (
	"net"

	synchronizer "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	app "github.com/BrobridgeOrg/gravity-synchronizer/pkg/app"
	synchronizer_service "github.com/BrobridgeOrg/gravity-synchronizer/pkg/grpc_server/services/synchronizer"
	"google.golang.org/grpc"

	log "github.com/sirupsen/logrus"
	"github.com/soheilhy/cmux"
)

type Server struct {
	app      app.App
	instance *grpc.Server
	listener net.Listener
	host     string
}

func NewServer(a app.App) *Server {
	return &Server{
		app:      a,
		instance: &grpc.Server{},
	}
}

func (server *Server) Init(host string) error {

	// Put it to mux
	mux, err := server.app.GetMuxManager().AssertMux("grpc", host)
	if err != nil {
		return err
	}

	// Preparing listener
	lis := mux.MatchWithWriters(
		cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"),
	)

	server.host = host
	server.listener = lis
	server.instance = grpc.NewServer()

	// Services
	synchronizerService := synchronizer_service.NewService(server.app)
	synchronizer.RegisterSynchronizerServer(server.instance, synchronizerService)

	return nil
}

func (server *Server) Serve() error {

	log.WithFields(log.Fields{
		"host": server.host,
	}).Info("Starting GRPC server")

	// Starting server
	if err := server.instance.Serve(server.listener); err != cmux.ErrListenerClosed {
		log.Error(err)
		return err
	}

	return nil
}

func (server *Server) GetApp() app.App {
	return server.app
}
