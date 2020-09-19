package instance

import (
	store_manager "github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore/manager"
	grpc_server "github.com/BrobridgeOrg/gravity-synchronizer/pkg/grpc_server/server"
	mux_manager "github.com/BrobridgeOrg/gravity-synchronizer/pkg/mux_manager/manager"
	synchronizer_service "github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done         chan bool
	muxManager   *mux_manager.MuxManager
	grpcServer   *grpc_server.Server
	synchronizer *synchronizer_service.Synchronizer
	storeManager *store_manager.Manager
}

func NewAppInstance() *AppInstance {

	a := &AppInstance{
		done: make(chan bool),
	}

	a.synchronizer = synchronizer_service.NewSynchronizer(a)
	a.muxManager = mux_manager.NewMuxManager(a)
	a.grpcServer = grpc_server.NewServer(a)
	a.storeManager = store_manager.NewManager(a)

	return a
}

func (a *AppInstance) Init() error {

	log.Info("Starting application")

	// Initializing datastore
	err := a.initStoreManager()
	if err != nil {
		return err
	}

	err = a.initSynchronizer()
	if err != nil {
		return err
	}

	// Initializing GRPC server
	err = a.initGRPCServer()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	// GRPC
	go func() {
		err := a.runGRPCServer()
		if err != nil {
			log.Error(err)
		}
	}()

	err := a.runMuxManager()
	if err != nil {
		return err
	}

	<-a.done

	return nil
}
