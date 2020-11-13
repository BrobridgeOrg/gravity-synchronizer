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
		/*
			// Trying to get data from store again to make sure it is tailing
			iter.Seek(Uint64ToBytes(sub.lastSequence))
			if !iter.Valid() {
				// Weird. It seems that message was deleted already somehow
				log.WithFields(log.Fields{
					"seq": sub.lastSequence,
				}).Error("message was missing somehow")
				break
			}

			iter.Next()
			if !iter.Valid() {
				// No data anymore
				//			break
				continue
			}
		*/
	}
	/*
		// Receving real-time events
		for event := range sub.queue {

			// Ignore old messages
			if event.Sequence <= sub.lastSequence {
				continue
			}

			sub.lastSequence = event.Sequence

			// Invoke data handler
			quit := sub.handle(event.Sequence, event.Data)
			eventPool.Put(event)
			if quit {
				return
			}
		}
	*/
}

func (sub *Subscription) Publish(seq uint64, data *projection.Projection) error {
	/*
			if !sub.tailing {
				return nil
			}
		if seq <= sub.lastSequence {
			return nil
		}
	*/

	select {
	case sub.newTriggered <- struct{}{}:
	default:
		return nil
	}
	/*
		lastSeq := atomic.LoadUint64((*uint64)(&sub.lastSequence))
		log.Info(seq, lastSeq)
		if seq != lastSeq+1 {
			return nil
		}

		sub.tailing = true
		atomic.StoreUint64((*uint64)(&sub.lastSequence), seq)
		//	sub.lastSequence = seq

		sub.handle(seq, data)
	*/
	/*
		// Allocate
		event := eventPool.Get().(*Event)
		event.Sequence = seq
		event.Data = data

		sub.queue <- event
	*/
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
