package synchronizer

import (
	"fmt"
	"time"

	"github.com/BrobridgeOrg/gravity-sdk/core"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	DefaultPingInterval        = 10
	DefaultMaxPingsOutstanding = 3
	DefaultMaxReconnects       = -1
)

func (synchronizer *Synchronizer) initializeClient() error {

	// default settings
	viper.SetDefault("eventbus.pingInterval", DefaultPingInterval)
	viper.SetDefault("eventbus.maxPingsOutstanding", DefaultMaxPingsOutstanding)
	viper.SetDefault("eventbus.maxReconnects", DefaultMaxReconnects)

	// Read configs
	host := viper.GetString("eventbus.host")
	port := viper.GetInt("eventbus.port")
	pingInterval := viper.GetInt64("eventbus.pingInterval")
	maxPingsOutstanding := viper.GetInt("eventbus.maxPingsOutstanding")
	maxReconnects := viper.GetInt("eventbus.maxReconnects")

	// Preparing options
	options := core.NewOptions()
	options.PingInterval = time.Duration(pingInterval) * time.Second
	options.MaxPingsOutstanding = maxPingsOutstanding
	options.MaxReconnects = maxReconnects

	address := fmt.Sprintf("%s:%d", host, port)

	log.WithFields(log.Fields{
		"address":             address,
		"pingInterval":        options.PingInterval,
		"maxPingsOutstanding": options.MaxPingsOutstanding,
		"maxReconnects":       options.MaxReconnects,
	}).Info("Connecting to gravity...")

	// Connect
	return synchronizer.gravityClient.Connect(address, options)
}
