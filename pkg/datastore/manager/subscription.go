package datastore

import (
	"time"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/datastore"
	"github.com/prometheus/common/log"
	"github.com/tecbot/gorocksdb"
)

type StoreHandler func(uint64, []byte) bool

type Event struct {
	Sequence uint64
	Data     []byte
}

type Subscription struct {
	close   chan struct{}
	queue   chan *Event
	tailing bool
}

func NewSubscription() *Subscription {
	return &Subscription{
		close: make(chan struct{}),
		queue: make(chan *Event, 1000),
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

			// Invoke data handler
			quit := sub.handle(seq, value.Data(), fn)

			// Release
			key.Free()
			value.Free()

			if quit {
				return
			}
		}
	}

	sub.tailing = true

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
		}
	}
}

func (sub *Subscription) Publish(seq uint64, data []byte) error {

	sub.queue <- &Event{
		Sequence: seq,
		Data:     data,
	}

	return nil
}

func (sub *Subscription) handle(seq uint64, data []byte, fn datastore.StoreHandler) bool {

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
