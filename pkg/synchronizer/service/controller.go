package synchronizer

import (
	synchronizer_manager "github.com/BrobridgeOrg/gravity-sdk/synchronizer_manager"
	log "github.com/sirupsen/logrus"
)

func (synchronizer *Synchronizer) RegisterClient() error {

	opts := synchronizer_manager.NewOptions()
	opts.Key = synchronizer.keyring.Get("gravity")

	sm := synchronizer_manager.NewSynchronizerManagerWithClient(synchronizer.gravityClient, opts)
	err := sm.Register(synchronizer.clientID)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"id": synchronizer.clientID,
	}).Info("Registered synchronizer to controller")

	return nil
}

func (synchronizer *Synchronizer) GetPipelines() ([]uint64, error) {

	opts := synchronizer_manager.NewOptions()
	opts.Key = synchronizer.keyring.Get("gravity")

	sm := synchronizer_manager.NewSynchronizerManagerWithClient(synchronizer.gravityClient, opts)
	return sm.GetPipelines(synchronizer.clientID)
}
