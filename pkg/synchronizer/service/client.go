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
	DefaultAccessKey           = ""
)

func (synchronizer *Synchronizer) initializeClient() error {

	// default domain and access key
	viper.SetDefault("gravity.domain", "gravity")
	viper.SetDefault("gravity.accessKey", DefaultAccessKey)

	// default settings
	viper.SetDefault("gravity.pingInterval", DefaultPingInterval)
	viper.SetDefault("gravity.maxPingsOutstanding", DefaultMaxPingsOutstanding)
	viper.SetDefault("gravity.maxReconnects", DefaultMaxReconnects)

	// Read configs
	domain := viper.GetString("gravity.domain")
	accessKey := viper.GetString("gravity.accessKey")
	host := viper.GetString("gravity.host")
	port := viper.GetInt("gravity.port")
	pingInterval := viper.GetInt64("gravity.pingInterval")
	maxPingsOutstanding := viper.GetInt("gravity.maxPingsOutstanding")
	maxReconnects := viper.GetInt("gravity.maxReconnects")

	// Preparing options
	options := core.NewOptions()
	options.PingInterval = time.Duration(pingInterval) * time.Second
	options.MaxPingsOutstanding = maxPingsOutstanding
	options.MaxReconnects = maxReconnects

	address := fmt.Sprintf("%s:%d", host, port)

	log.WithFields(log.Fields{
		"domain":              domain,
		"address":             address,
		"pingInterval":        options.PingInterval,
		"maxPingsOutstanding": options.MaxPingsOutstanding,
		"maxReconnects":       options.MaxReconnects,
	}).Info("Connecting to gravity...")

	synchronizer.domain = domain

	// Initializing keyring
	keyInfo := synchronizer.keyring.Put("gravity", accessKey)
	keyInfo.Permission().AddPermissions([]string{"SYSTEM"})

	// Connect
	return synchronizer.gravityClient.Connect(address, options)
}
