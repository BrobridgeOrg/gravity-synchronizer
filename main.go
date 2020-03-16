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
		log.Warn("No configuration file was loaded")
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
