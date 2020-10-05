package datastore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
	log "github.com/sirupsen/logrus"
	"github.com/tecbot/gorocksdb"
)

var LastSequenceKey = []byte("lastSeq")

type Record struct {
	Sequence uint64
	Data     *projection.Projection
}

type Snapshot struct {
	store   *Store
	lastSeq uint64
	close   chan struct{}
	queue   chan *Record
}

func NewSnapshot(store *Store) *Snapshot {
	return &Snapshot{
		store: store,
		close: make(chan struct{}),
		queue: make(chan *Record, 1024),
	}
}

func (snapshot *Snapshot) Initialize() error {

	// Assert snapshot
	_, err := snapshot.store.assertColumnFamily("snapshot")
	if err != nil {
		return err
	}

	// Assert snapshot states
	stateHandle, err := snapshot.store.assertColumnFamily("snapshot_states")
	if err != nil {
		return err
	}

	// Getting last sequence number of snapshot
	value, err := snapshot.store.db.GetCF(snapshot.store.ro, stateHandle, LastSequenceKey)
	if err != nil {
		return err
	}

	lastSeq := BytesToUint64(value.Data())
	value.Free()

	snapshot.lastSeq = lastSeq

	go func() {
		for {
			select {
			case <-snapshot.close:
				return
			case record := <-snapshot.queue:
				// Invoke data handler
				snapshot.handle(record.Sequence, record.Data)
			}
		}
	}()

	return nil
}

func (snapshot *Snapshot) Close() {
	snapshot.close <- struct{}{}
}

func (snapshot *Snapshot) Write(seq uint64, data *projection.Projection) {
	snapshot.queue <- &Record{
		Sequence: seq,
		Data:     data,
	}
}

func (snapshot *Snapshot) getPrimaryKeyData(data *projection.Projection) ([]byte, error) {

	for _, field := range data.Fields {
		if field.Primary == true {

			// Getting value of primary key
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			err := enc.Encode(field.Value)
			if err != nil {
				return nil, err
			}

			return buf.Bytes(), nil
		}
	}

	return nil, nil
}

func (snapshot *Snapshot) handle(seq uint64, data *projection.Projection) {

	cfHandle, err := snapshot.store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		log.Error("Not found \"snapshot\" column family")
	}

	// Getting data of primary key
	primaryKey, err := snapshot.getPrimaryKeyData(data)
	if primaryKey == nil {
		// Ignore record which has no primary key
		return
	}

	value, err := snapshot.store.db.GetCF(snapshot.store.ro, cfHandle, primaryKey)
	if err != nil {
		log.Error(err)
		return
	}

	defer value.Free()

	// Not found
	if value.Size() > 0 {
		orig, err := projection.Unmarshal(value.Data())
		if err != nil {

			// Original data is unrecognized, so using new data instead
			newData, _ := data.ToJSON()

			// Write to database
			snapshot.writeData(cfHandle, seq, primaryKey, newData)

			return
		}

		newData := snapshot.merge(orig, data)

		// Write to database
		snapshot.writeData(cfHandle, seq, primaryKey, newData)

		return
	}

	// Convert data to json string
	newData, _ := data.ToJSON()

	// Write to database
	snapshot.writeData(cfHandle, seq, primaryKey, newData)
}

func (snapshot *Snapshot) writeData(cfHandle *gorocksdb.ColumnFamilyHandle, seq uint64, key []byte, data []byte) {

	// Write to database
	err := snapshot.store.db.PutCF(snapshot.store.wo, cfHandle, key, data)
	if err != nil {
		log.Error(err)
	}

	// Update snapshot state
	seqData := Uint64ToBytes(seq)
	err = snapshot.store.db.PutCF(snapshot.store.wo, cfHandle, LastSequenceKey, seqData)
	if err != nil {
		log.Error(err)
	}

	snapshot.lastSeq = seq
}

func (snapshot *Snapshot) merge(origData *projection.Projection, updates *projection.Projection) []byte {

	result := make(map[string]interface{})

	for _, field := range origData.Fields {
		result[field.Name] = field.Value
	}

	for _, field := range updates.Fields {
		result[field.Name] = field.Value
	}

	// convert to json
	data, _ := json.Marshal(&result)

	return data
}
