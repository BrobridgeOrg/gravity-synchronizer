package app

import (
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/grpc_server"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/mux_manager"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer"
)

type App interface {
	GetStoreManager() datastore.StoreManager
	GetGRPCServer() grpc_server.Server
	GetMuxManager() mux_manager.Manager
	GetSynchronizer() synchronizer.Synchronizer
}
