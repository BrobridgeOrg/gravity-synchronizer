package instance

import (
	"runtime"

	synchronizer_service "github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer/service"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done         chan bool
	synchronizer *synchronizer_service.Synchronizer
}

func NewAppInstance() *AppInstance {

	a := &AppInstance{
		done: make(chan bool),
	}

	a.synchronizer = synchronizer_service.NewSynchronizer(a)

	return a
}

func (a *AppInstance) Init() error {

	log.WithFields(log.Fields{
		"max_procs": runtime.GOMAXPROCS(32),
	}).Info("Starting application")

	err := a.initSynchronizer()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	<-a.done

	return nil
}
