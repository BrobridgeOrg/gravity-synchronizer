package main

import (
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	app "gravity-synchronizer/app"
)

func init() {

	// From the environment
	viper.SetEnvPrefix("GRAVITY_SYNCHRONIZER")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// From config file
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.AddConfigPath("./config")

	if err := viper.ReadInConfig(); err != nil {
		log.Error(err)
		log.Warn("Failed to load configuration file")
	}
}

func main() {

	// Initializing application
	a := app.CreateApp()

	err := a.Init()
	if err != nil {
		log.Fatal(err)
		return
	}

	// Starting application
	err = a.Run()
	if err != nil {
		log.Fatal(err)
		return
	}
}
