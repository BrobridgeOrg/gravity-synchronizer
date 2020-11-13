package datastore

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
	log "github.com/sirupsen/logrus"
	"github.com/tecbot/gorocksdb"
)

type StoreHandler func(uint64, *projection.Projection) bool

var eventPool = &sync.Pool{
	New: func() interface{} {
		return &Event{}
	},
}

type Event struct {
	Sequence uint64
	Data     *projection.Projection
}

type Subscription struct {
	lastSequence uint64
	newTriggered chan struct{}
	close        chan struct{}
	queue        chan *Event
	tailing      bool
	watchFn      datastore.StoreHandler
}

func NewSubscription(startAt uint64, fn datastore.StoreHandler) *Subscription {
	return &Subscription{
		lastSequence: startAt,
		newTriggered: make(chan struct{}),
		close:        make(chan struct{}),
		queue:        make(chan *Event, 102400),
		watchFn:      fn,
	}
}

func (sub *Subscription) Close() {
	sub.close <- struct{}{}
	close(sub.queue)
}

func (sub *Subscription) Watch(iter *gorocksdb.Iterator) {

	defer close(sub.close)

	for _ = range sub.newTriggered {

		lastSeq := atomic.LoadUint64((*uint64)(&sub.lastSequence))
		iter.Seek(Uint64ToBytes(lastSeq))
		if !iter.Valid() {
			//			break
			continue
		}

		// If we get record which is the same with last seq, find next one
		key := iter.Key()
		seq := BytesToUint64(key.Data())
		key.Free()
		if seq == lastSeq {
			iter.Next()
		}

		// No more data
		if !iter.Valid() {
			continue
			//			return
		}

		for ; iter.Valid(); iter.Next() {

			select {
			case <-sub.close:
				return
			default:
				key := iter.Key()
				value := iter.Value()
				seq := BytesToUint64(key.Data())

				/*
					// Find next
					lastSeq := atomic.LoadUint64((*uint64)(&sub.lastSequence))
					if seq == lastSeq {
						// Release
						key.Free()
						value.Free()
						continue
					}
						// Not need to get data from database
						if seq < lastSeq {
							// Release
							key.Free()
							value.Free()
							return
						}
				*/
				sub.lastSequence = seq
				//atomic.StoreUint64((*uint64)(&sub.lastSequence), seq)

				// Parsing data
				pj, err := projection.Unmarshal(value.Data())
				if err != nil {
					key.Free()
					value.Free()
					continue
				}

				// Release
				key.Free()
				value.Free()

				// Invoke data handler
				quit := sub.handle(seq, pj)
				if quit {
					return
				}
			}
		}
	}
}

func (sub *Subscription) Trigger() error {

	select {
	case sub.newTriggered <- struct{}{}:
	default:
		return nil
	}

	return nil
}

func (sub *Subscription) handle(seq uint64, data *projection.Projection) bool {

	for {

		select {
		case <-sub.close:
			return true
		default:
			success := sub.watchFn(seq, data)
			if success {
				return false
			}

			log.Warn("Failed to process. Trying to do again in second")

			time.Sleep(time.Second)
		}
	}
}
