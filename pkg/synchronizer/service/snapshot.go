package synchronizer

import (
	"bytes"
	"encoding/gob"

	eventstore "github.com/BrobridgeOrg/EventStore"
	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"
)

type SnapshotHandler struct {
}

func NewSnapshotHandler() *SnapshotHandler {
	return &SnapshotHandler{}
}

func (snapshot *SnapshotHandler) getPrimaryKeyData(data *projection.Projection) ([]byte, error) {

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
	newData := projectionPool.Get().(*projection.Projection)
	err := projection.Unmarshal(request.Data, newData)

	// Getting data of primary key
	primaryKey, err := snapshot.getPrimaryKeyData(newData)
	if primaryKey == nil {
		projectionPool.Put(newData)
		// Ignore record which has no primary key
		return nil
	}

	// Upsert to snapshot
	err = request.Upsert([]byte(newData.Collection), primaryKey, func(origin []byte) ([]byte, error) {

		originData := projectionPool.Get().(*projection.Projection)
		err := projection.Unmarshal(origin, originData)
		if err != nil {
			projectionPool.Put(originData)
			return nil, err
		}

		// Merged new data to original data
		updatedData := snapshot.merge(originData, newData)
		projectionPool.Put(originData)

		return updatedData, nil
	})

	if err != nil {
		return err
	}

	// Release projection data
	projectionPool.Put(newData)

	return nil
}

func (snapshot *SnapshotHandler) merge(origData *projection.Projection, updates *projection.Projection) []byte {

	// Pre-allocate map to store data
	result := make(map[string]interface{}, len(origData.Fields)+len(updates.Fields))

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
