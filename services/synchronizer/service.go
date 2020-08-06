package synchronizer

import (
	"github.com/prometheus/common/log"

	app "gravity-synchronizer/app/interface"
)

type Service struct {
	app          app.AppImpl
	eventHandler *EventHandler
}

func CreateService(a app.AppImpl) *Service {

	eventHandler := NewEventHandler(a)
	if eventHandler == nil {
		return nil
	}

	err := eventHandler.Initialize()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Preparing service
	service := &Service{
		app:          a,
		eventHandler: eventHandler,
	}

	return service
}
