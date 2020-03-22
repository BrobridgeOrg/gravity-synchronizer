package synchronizer

import (
	"github.com/prometheus/common/log"

	app "gravity-synchronizer/app/interface"
	pb "gravity-synchronizer/pb"
)

type Service struct {
	app          app.AppImpl
	eventHandler *EventHandler
}

func CreateService(a app.AppImpl) *Service {

	eventHandler := CreateEventHandler(a)
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

func (service *Service) GetSnapshot(in *pb.GetSnapshotRequest, stream pb.Synchronizer_GetSnapshotServer) error {
	/*
		db := service.dbMgr.GetDatabase("users")
		if db == nil {
			return nil
		}

		err := db.FetchSnapshot(stream)
		if err != nil {
			return err
		}
	*/
	return nil
}
