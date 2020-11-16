package datastore

import (
	"runtime"
	"sync"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
	"github.com/cfsghost/gosharding"
)

type SnapshotScheduler struct {
	shard *gosharding.Shard
}

type SnapshotRequest struct {
	Store    *Store
	Sequence uint64
	Data     []byte
	//	Data     *projection.Projection
}

var snapshotRequestPool = sync.Pool{
	New: func() interface{} {
		return &SnapshotRequest{}
	},
}

func NewSnapshotScheduler() *SnapshotScheduler {
	ss := &SnapshotScheduler{}
	ss.initialize()
	return ss
}

func (ss *SnapshotScheduler) initialize() error {

	// Initializing shard
	options := gosharding.NewOptions()
	options.PipelineCount = 32
	options.BufferSize = 10240
	options.Handler = func(id int32, data interface{}) {

		req := data.(*SnapshotRequest)

		// Parsing data
		pj := projectionPool.Get().(*projection.Projection)
		err := projection.Unmarshal(req.Data, pj)
		if err != nil {
			projectionPool.Put(pj)
			return
		}

		// Store to database
		req.Store.snapshot.handle(req.Sequence, pj)

		// Release
		projectionPool.Put(pj)
		snapshotRequestPool.Put(req)

		runtime.Gosched()
	}

	// Create shard with options
	ss.shard = gosharding.NewShard(options)

	return nil
}

func (ss *SnapshotScheduler) Request(store *Store, seq uint64, data []byte) error {

	// Create a new snapshot request
	req := snapshotRequestPool.Get().(*SnapshotRequest)
	req.Store = store
	req.Sequence = seq
	req.Data = data

	ss.shard.PushKV(store.name, req)

	return nil
}
