package data_handler

import (
	"github.com/cfsghost/taskflow"

	gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"
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

	pj := message.Data.(*gravity_sdk_types_projection.Projection)

	// Convert to packet
	data, _ := gravity_sdk_types_projection.Marshal(pj)

	// Release projection object
	projectionPool.Put(pj)

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
