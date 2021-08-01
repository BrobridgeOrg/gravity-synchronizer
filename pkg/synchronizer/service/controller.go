package synchronizer

import (
	"github.com/BrobridgeOrg/gravity-sdk/authenticator"
	synchronizer_manager "github.com/BrobridgeOrg/gravity-sdk/synchronizer_manager"
)

type Controller struct {
	synchronizer *Synchronizer
}

func NewController(syncronizer *Synchronizer) *Controller {
	return &Controller{
		synchronizer: syncronizer,
	}
}

func (controller *Controller) GetSynchronizerManager() *synchronizer_manager.SynchronizerManager {

	opts := synchronizer_manager.NewOptions()
	opts.Domain = controller.synchronizer.domain
	opts.Key = controller.synchronizer.keyring.Get("gravity")

	return synchronizer_manager.NewSynchronizerManagerWithClient(controller.synchronizer.gravityClient, opts)
}

func (controller *Controller) GetAuthenticator() *authenticator.Authenticator {

	opts := authenticator.NewOptions()
	opts.Domain = controller.synchronizer.domain
	opts.Key = controller.synchronizer.keyring.Get("gravity")

	return authenticator.NewAuthenticatorWithClient(controller.synchronizer.gravityClient, opts)
}
