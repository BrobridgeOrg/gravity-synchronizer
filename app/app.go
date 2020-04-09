package app

import (
	"gravity-synchronizer/app/eventbus"
	app "gravity-synchronizer/app/interface"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/sony/sonyflake"
	"github.com/spf13/viper"
)

type App struct {
	clientID string
	eventbus *eventbus.EventBus
}

func CreateApp() *App {

	clientID := viper.GetString("event_store.client_name")
	if len(clientID) == 0 {

		// Genereate a unique ID for instance
		flake := sonyflake.NewSonyflake(sonyflake.Settings{})
		id, err := flake.NextID()
		if err != nil {
			return nil
		}

		clientID = strconv.FormatUint(id, 16)
	}

	return &App{
		clientID: clientID,
		eventbus: eventbus.CreateConnector(
			viper.GetString("event_store.host"),
			viper.GetString("event_store.cluster_id"),
			clientID,
			viper.GetString("event_store.durable_name"),
		),
	}
}

func (a *App) Init() error {

	log.WithFields(log.Fields{
		"a_id": a.clientID,
	}).Info("Starting application")

	// Connect to event server
	err := a.eventbus.Connect()
	if err != nil {
		return err
	}

	return nil
}

func (a *App) Uninit() {
}

func (a *App) Run() error {

	port := strconv.Itoa(viper.GetInt("service.port"))
	err := a.InitGRPCServer(":" + port)
	if err != nil {
		return err
	}

	return nil
}

func (a *App) GetEventBus() app.EventBusImpl {
	return app.EventBusImpl(a.eventbus)
}
