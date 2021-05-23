package app

import (
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/synchronizer"
)

type App interface {
	GetSynchronizer() synchronizer.Synchronizer
}
