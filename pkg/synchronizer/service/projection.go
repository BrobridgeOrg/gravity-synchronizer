package synchronizer

import (
	"sync"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
)

var projectionPool = sync.Pool{
	New: func() interface{} {
		return &projection.Projection{}
	},
}
