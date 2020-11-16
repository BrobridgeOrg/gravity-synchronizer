package datastore

import (
	"bytes"
	"encoding/gob"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/tecbot/gorocksdb"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var LastSequenceKey = []byte("lastSeq")

type Snapshot struct {
	store   *Store
	lastSeq uint64
}

func NewSnapshot(store *Store) *Snapshot {
	return &Snapshot{
		store: store,
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

	if value.Size() > 0 {
		snapshot.lastSeq = BytesToUint64(value.Data())
	}

	value.Free()

	return nil
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

	stateHandle, err := snapshot.store.GetColumnFamailyHandle("snapshot_states")
	if err != nil {
		log.Error("Not found \"snapshot_states\" column family")
	}

	// Getting data of primary key
	primaryKey, err := snapshot.getPrimaryKeyData(data)
	if primaryKey == nil {
		// Ignore record which has no primary key
		return
	}

	// collection name as prefix
	key := bytes.Join([][]byte{
		[]byte(data.Collection),
		primaryKey,
	}, []byte("-"))

	if data.Method == "delete" {
		snapshot.store.db.DeleteCF(snapshot.store.wo, cfHandle, key)
		return
	}

	value, err := snapshot.store.db.GetCF(snapshot.store.ro, cfHandle, key)
	if err != nil {
		log.Error(err)
		return
	}

	// Not found
	if value.Size() > 0 {

		// Parsing original data which from database
		orig := projectionPool.Get().(*projection.Projection)
		err := projection.Unmarshal(value.Data(), orig)
		if err != nil {

			// Original data is unrecognized, so using new data instead
			newData, _ := data.ToJSON()

			// Write to database
			snapshot.writeData(cfHandle, stateHandle, seq, key, newData)

			return
		}

		newData := snapshot.merge(orig, data)

		// Release projection data
		projectionPool.Put(orig)

		// Write to database
		snapshot.writeData(cfHandle, stateHandle, seq, key, newData)

		value.Free()
		return
	}

	// Convert data to json string
	newData, _ := data.ToJSON()

	// Write to database
	snapshot.writeData(cfHandle, stateHandle, seq, key, newData)

	value.Free()
}

func (snapshot *Snapshot) writeData(cfHandle *gorocksdb.ColumnFamilyHandle, stateHandle *gorocksdb.ColumnFamilyHandle, seq uint64, key []byte, data []byte) {

	// Write to database
	err := snapshot.store.db.PutCF(snapshot.store.wo, cfHandle, key, data)
	if err != nil {
		log.Error(err)
	}

	// Update snapshot state
	seqData := Uint64ToBytes(seq)
	err = snapshot.store.db.PutCF(snapshot.store.wo, stateHandle, LastSequenceKey, seqData)
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
