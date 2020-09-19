package manager

import (
	"net"

	app "github.com/BrobridgeOrg/gravity-synchronizer/pkg/app"
	log "github.com/sirupsen/logrus"
	"github.com/soheilhy/cmux"
)

type MuxManager struct {
	app       app.App
	instances map[string]cmux.CMux
}

func NewMuxManager(a app.App) *MuxManager {
	return &MuxManager{
		app:       a,
		instances: make(map[string]cmux.CMux),
	}
}

func (mm *MuxManager) CreateMux(name string, host string) (cmux.CMux, error) {

	// Start to listen on port
	lis, err := net.Listen("tcp", host)
	if err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{
		"host": host,
	}).Info("Starting server on " + host)

	m := cmux.New(lis)

	mm.instances[name] = m

	return m, nil
}

func (mm *MuxManager) Serve() error {

	for _, mux := range mm.instances {

		go func(mux cmux.CMux) {
			err := mux.Serve()
			if err != nil {
				log.Error(err)
			}
		}(mux)
	}

	return nil
}

func (mm *MuxManager) AssertMux(name string, host string) (cmux.CMux, error) {

	if mux, ok := mm.instances[name]; ok {
		return mux, nil
	}

	mux, err := mm.CreateMux(name, host)
	if err != nil {
		return nil, err
	}

	return mux, nil
}

func (mm *MuxManager) GetMux(name string) cmux.CMux {
	return mm.instances[name]
}
