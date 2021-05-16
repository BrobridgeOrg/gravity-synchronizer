package synchronizer

import (
	"sync"

	gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"
)

var projectionPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_projection.Projection{}
	},
}
