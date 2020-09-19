package synchronizer

import (
	app "github.com/BrobridgeOrg/gravity-synchronizer/pkg/app"
)

type Service struct {
	app app.App
}

func NewService(a app.App) *Service {

	service := &Service{
		app: a,
	}

	return service
}
