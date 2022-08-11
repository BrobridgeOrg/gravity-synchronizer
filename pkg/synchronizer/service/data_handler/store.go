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

	tr := message.Context.GetPrivData().(*TaskRequest)
	defer message.Release()

	record := message.Data.(*gravity_sdk_types_record.Record)
	defer recordPool.Put(record)

	// Convert to packet
	data, err := gravity_sdk_types_record.Marshal(record)
	if err != nil {
		tr.Done(err)
		return
	}

	err = store.dataHandler.storeHandler(tr.PrivData, data)
	if err != nil {
		tr.Done(err)
		return
	}

	// Complete
	tr.Done(nil)
}
