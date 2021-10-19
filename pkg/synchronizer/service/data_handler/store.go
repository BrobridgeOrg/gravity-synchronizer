package data_handler

import (
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/cfsghost/taskflow"
)

type Store struct {
	dataHandler *DataHandler
	task        *taskflow.Task
}

func NewStore() *Store {
	return &Store{}
}

func (store *Store) Init(dataHandler *DataHandler) error {

	// Initializing task
	store.dataHandler = dataHandler
	store.task = taskflow.NewTask(1, 1)

	// Initializing task handler
	store.task.SetHandler(store.handle)

	return nil
}

func (store *Store) handle(message *taskflow.Message) {

	record := message.Data.(*gravity_sdk_types_record.Record)

	// Convert to packet
	data, _ := gravity_sdk_types_record.Marshal(record)

	// Release projection object
	recordPool.Put(record)

	tr := message.Context.GetPrivData().(*TaskRequest)

	err := store.dataHandler.storeHandler(tr.PrivData, data)
	if err != nil {
		// Failed
		tr.Done(err)
		return
	}

	// Complete
	tr.Done(nil)
}
