package datastore

import (
	"time"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
	log "github.com/sirupsen/logrus"
	"github.com/tecbot/gorocksdb"
)

type StoreHandler func(uint64, *projection.Projection) bool

type Subscription struct {
	lastSequence uint64
	newTriggered chan struct{}
	close        chan struct{}
	watchFn      datastore.StoreHandler
}

func NewSubscription(startAt uint64, fn datastore.StoreHandler) *Subscription {
	return &Subscription{
		lastSequence: startAt,
		newTriggered: make(chan struct{}, 1),
		close:        make(chan struct{}),
		watchFn:      fn,
	}
}

func (sub *Subscription) Close() {
	close(sub.newTriggered)
	sub.close <- struct{}{}
}

func (sub *Subscription) Watch(iter *gorocksdb.Iterator) {

	for _ = range sub.newTriggered {

		iter.Seek(Uint64ToBytes(sub.lastSequence))
		if !iter.Valid() {
			//			break
			continue
		}

		// If we get record which is the same with last seq, find next one
		key := iter.Key()
		seq := BytesToUint64(key.Data())
		key.Free()
		if seq == sub.lastSequence {
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
			}

			// Getting sequence number
			key := iter.Key()
			seq := BytesToUint64(key.Data())
			key.Free()

			sub.lastSequence = seq

			// Parsing data
			value := iter.Value()
			pj := projectionPool.Get().(*projection.Projection)
			err := projection.Unmarshal(value.Data(), pj)
			value.Free()
			if err != nil {
				continue
			}

			// Invoke data handler
			quit := sub.handle(seq, pj)
			projectionPool.Put(pj)
			if quit {
				return
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
		}

		success := sub.watchFn(seq, data)
		if success {
			return false
		}

		log.Warn("Failed to process. Trying to do again in second")

		<-time.After(1 * time.Second)
	}
}
