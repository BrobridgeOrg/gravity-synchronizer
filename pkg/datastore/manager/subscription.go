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
	lastSequence uint64
	close        chan struct{}
	queue        chan *Event
	tailing      bool
}

func NewSubscription() *Subscription {
	return &Subscription{
		close: make(chan struct{}),
		queue: make(chan *Event, 102400),
	}
}

func (sub *Subscription) Close() {
	sub.close <- struct{}{}
	close(sub.queue)
}

func (sub *Subscription) Watch(iter *gorocksdb.Iterator, fn datastore.StoreHandler) {

	defer close(sub.close)

	for {

		for ; iter.Valid(); iter.Next() {

			select {
			case <-sub.close:
				return
			default:
				key := iter.Key()
				value := iter.Value()
				seq := BytesToUint64(key.Data())

				sub.lastSequence = seq

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
				quit := sub.handle(seq, pj, fn)
				if quit {
					return
				}
			}
		}

		sub.tailing = true

		// Trying to get data from store again to make sure it is tailing
		iter.Seek(Uint64ToBytes(sub.lastSequence))
		if !iter.Valid() {
			// Weird. It seems that message was deleted already somehow
			break
		}

		iter.Next()
		if !iter.Valid() {
			// No data anymore
			break
		}
	}

	// Receving real-time events
	for event := range sub.queue {

		// Ignore old messages
		if event.Sequence <= sub.lastSequence {
			continue
		}

		sub.lastSequence = event.Sequence

		// Invoke data handler
		quit := sub.handle(event.Sequence, event.Data, fn)
		eventPool.Put(event)
		if quit {
			return
		}
	}
}

func (sub *Subscription) Publish(seq uint64, data *projection.Projection) error {

	if !sub.tailing {
		return nil
	}

	if seq <= sub.lastSequence {
		return nil
	}

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
