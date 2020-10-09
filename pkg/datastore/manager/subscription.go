package datastore

import (
	"sync"
	"time"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
	"github.com/prometheus/common/log"
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
	close   chan struct{}
	queue   chan *Event
	tailing bool
}

func NewSubscription() *Subscription {
	return &Subscription{
		close: make(chan struct{}),
		queue: make(chan *Event, 1024),
	}
}

func (sub *Subscription) Close() {
	sub.close <- struct{}{}
}

func (sub *Subscription) Watch(iter *gorocksdb.Iterator, fn datastore.StoreHandler) {

	for ; iter.Valid(); iter.Next() {

		select {
		case <-sub.close:
			return
		default:
			key := iter.Key()
			value := iter.Value()
			seq := BytesToUint64(key.Data())

			// Parsing data
			pj, err := projection.Unmarshal(value.Data())
			if err != nil {
				continue
			}

			// Release
			key.Free()
			value.Free()

			// Invoke data handler
			quit := sub.handle(seq, pj, fn)
			if quit {
				return
			}
		}
	}

	sub.tailing = true

	// Receving real-time events
	for {
		select {
		case <-sub.close:
			return
		case event := <-sub.queue:
			// Invoke data handler
			quit := sub.handle(event.Sequence, event.Data, fn)
			if quit {
				return
			}

			eventPool.Put(event)
		}
	}
}

func (sub *Subscription) Publish(seq uint64, data *projection.Projection) error {

	// Allocate
	event := eventPool.Get().(*Event)
	event.Sequence = seq
	event.Data = data

	sub.queue <- event

	return nil
}

func (sub *Subscription) handle(seq uint64, data *projection.Projection, fn datastore.StoreHandler) bool {

	for {

		select {
		case <-sub.close:
			return true
		default:
			success := fn(seq, data)
			if success {
				return false
			}

			log.Warn("Failed to process. Trying to do again in second")

			time.Sleep(time.Second)
		}
	}
}
