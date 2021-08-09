package synchronizer

import (
	"bytes"
	"encoding/gob"
	"sync"

	eventstore "github.com/BrobridgeOrg/EventStore"
	gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"
	gravity_sdk_types_snapshot_record "github.com/BrobridgeOrg/gravity-sdk/types/snapshot_record"
	log "github.com/sirupsen/logrus"
)

var snapshotRecordPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_snapshot_record.SnapshotRecord{}
	},
}

type SnapshotHandler struct {
}

func NewSnapshotHandler() *SnapshotHandler {
	return &SnapshotHandler{}
}

func (snapshot *SnapshotHandler) getPrimaryKeyData(data *gravity_sdk_types_projection.Projection) ([]byte, error) {

	for _, field := range data.Fields {
		if field.Name == data.PrimaryKey {
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

func (snapshot *SnapshotHandler) handle(request *eventstore.SnapshotRequest) error {

	// Parsing original data which from database
	newData := projectionPool.Get().(*gravity_sdk_types_projection.Projection)
	err := gravity_sdk_types_projection.Unmarshal(request.Data, newData)

	// Getting data of primary key
	primaryKey, err := snapshot.getPrimaryKeyData(newData)
	if primaryKey == nil {
		projectionPool.Put(newData)
		// Ignore record which has no primary key
		return nil
	}

	// Preparing record
	newRecord := snapshotRecordPool.Get().(*gravity_sdk_types_snapshot_record.SnapshotRecord)
	newRecord.SetPayload(newData.GetPayload())

	// Release projection data
	projectionPool.Put(newData)

	data, err := newRecord.ToBytes()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Release record
	snapshotRecordPool.Put(newRecord)

	// Upsert to snapshot
	err = request.Upsert(StrToBytes(newData.Collection), primaryKey, data, func(origin []byte, newValue []byte) []byte {

		// Preparing original record
		originRecord := snapshotRecordPool.Get().(*gravity_sdk_types_snapshot_record.SnapshotRecord)
		err := gravity_sdk_types_snapshot_record.Unmarshal(origin, originRecord)
		if err != nil {
			return origin
		}

		// Preparing new record
		newRecord := snapshotRecordPool.Get().(*gravity_sdk_types_snapshot_record.SnapshotRecord)
		err = gravity_sdk_types_snapshot_record.Unmarshal(newValue, newRecord)
		if err != nil {
			return origin
		}

		// Merged new data to original data
		updatedData := snapshot.merge(originRecord, newRecord)

		// Release record
		snapshotRecordPool.Put(originRecord)
		snapshotRecordPool.Put(newRecord)

		return updatedData
	})

	if err != nil {
		return err
	}

	return nil
}

func (snapshot *SnapshotHandler) merge(origRecord *gravity_sdk_types_snapshot_record.SnapshotRecord, updates *gravity_sdk_types_snapshot_record.SnapshotRecord) []byte {

	// Pre-allocate map to store data
	origPayload := origRecord.Payload.AsMap()
	updatesPayload := updates.Payload.AsMap()
	result := make(map[string]interface{}, len(origPayload)+len(updatesPayload))

	for name, value := range origPayload {
		result[name] = value
	}

	for name, value := range updatesPayload {
		result[name] = value
	}

	//	origRecord.Payload = result
	origRecord.SetPayload(result)
	data, _ := origRecord.ToBytes()

	return data
}
