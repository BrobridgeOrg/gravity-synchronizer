package synchronizer

import (
	"sync"

	eventstore "github.com/BrobridgeOrg/EventStore"
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	gravity_sdk_types_snapshot_record "github.com/BrobridgeOrg/gravity-sdk/types/snapshot_record"
	log "github.com/sirupsen/logrus"
)

var recordPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_record.Record{}
	},
}

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

func (snapshot *SnapshotHandler) handle(request *eventstore.SnapshotRequest) error {

	// Parsing original data which from database
	newData := recordPool.Get().(*gravity_sdk_types_record.Record)
	defer recordPool.Put(newData)
	err := gravity_sdk_types_record.Unmarshal(request.Data, newData)

	// Getting data of primary key
	primaryKeyValue, err := newData.GetPrimaryKeyValue()
	if err != nil {
		// Ignore
		log.Errorf("snapshot_handler: failed to get primary key: %v", err)
		return nil
	}

	if primaryKeyValue == nil {
		// Ignore record which has no primary key
		return nil
	}

	primaryKey, err := primaryKeyValue.GetBytes()
	if err != nil {
		// Ignore
		log.Error(err)
		return nil
	}

	// Delete snapshot record
	if newData.Method == gravity_sdk_types_record.Method_DELETE {
		return request.Delete(StrToBytes(newData.Table), primaryKey)
	}

	// Preparing record
	newRecord := snapshotRecordPool.Get().(*gravity_sdk_types_snapshot_record.SnapshotRecord)
	newRecord.Payload = newData.GetPayload()

	data, err := newRecord.ToBytes()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Release record
	snapshotRecordPool.Put(newRecord)

	// Upsert to snapshot
	err = request.Upsert(StrToBytes(newData.Table), primaryKey, data, snapshot.upsert)
	if err != nil {
		return err
	}

	return nil
}

func (snapshot *SnapshotHandler) upsert(origin []byte, newValue []byte) []byte {

	// Preparing new record
	newRecord := snapshotRecordPool.Get().(*gravity_sdk_types_snapshot_record.SnapshotRecord)
	defer snapshotRecordPool.Put(newRecord)

	err := gravity_sdk_types_snapshot_record.Unmarshal(newValue, newRecord)
	if err != nil {
		log.Errorf("data_handler: failed to parse new record: %v", err)
		return origin
	}

	// Preparing original record
	originRecord := snapshotRecordPool.Get().(*gravity_sdk_types_snapshot_record.SnapshotRecord)
	defer snapshotRecordPool.Put(originRecord)

	err = gravity_sdk_types_snapshot_record.Unmarshal(origin, originRecord)
	if err != nil {
		log.Warnf("data_handler: failed to parse original record: %v", err)
	}

	// Merged new data to original data
	updatedData := snapshot.merge(originRecord, newRecord)

	return updatedData
}

func (snapshot *SnapshotHandler) applyChanges(orig *gravity_sdk_types_record.Value, changes *gravity_sdk_types_record.Value) {

	if orig == nil || changes == nil {
		return
	}

	if changes.Type != gravity_sdk_types_record.DataType_MAP {
		return
	}

	for _, field := range changes.Map.Fields {

		// Getting specifc field
		f := gravity_sdk_types_record.GetField(orig.Map.Fields, field.Name)
		if f == nil {

			// new field
			f = &gravity_sdk_types_record.Field{
				Name:  field.Name,
				Value: field.Value,
			}

			orig.Map.Fields = append(orig.Map.Fields, f)

			continue
		}

		// check type to update
		switch f.Value.Type {
		case gravity_sdk_types_record.DataType_ARRAY:
			f.Value.Array = field.Value.Array
		case gravity_sdk_types_record.DataType_MAP:
			snapshot.applyChanges(f.Value, field.Value)
		default:
			// update value
			f.Value.Value = field.Value.Value
		}
	}

}

func (snapshot *SnapshotHandler) merge(origRecord *gravity_sdk_types_snapshot_record.SnapshotRecord, updates *gravity_sdk_types_snapshot_record.SnapshotRecord) []byte {

	if origRecord.Payload == nil {
		origRecord.Payload = &gravity_sdk_types_record.Value{
			Type: gravity_sdk_types_record.DataType_MAP,
			Map:  &gravity_sdk_types_record.MapValue{},
		}
	}

	snapshot.applyChanges(origRecord.Payload, updates.Payload)

	data, _ := origRecord.ToBytes()

	return data
}
