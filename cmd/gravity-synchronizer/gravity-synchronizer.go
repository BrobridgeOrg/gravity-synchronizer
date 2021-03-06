package main

import (
	"os"
	"os/signal"
	"runtime"
	"runtime/trace"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	app "github.com/BrobridgeOrg/gravity-synchronizer/pkg/app/instance"
)

func init() {

	// From the environment
	viper.SetEnvPrefix("GRAVITY_SYNCHRONIZER")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// From config file
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.AddConfigPath("./configs")

	if err := viper.ReadInConfig(); err != nil {
		log.Warn("No configuration file was loaded")
	}
	runtime.GOMAXPROCS(8)

	// Using environment variable to enable debug mode
	DEBUG_MODE := os.Getenv("DEBUG_MODE")
	if DEBUG_MODE != "" {
		go func() {

			host, _ := os.Hostname()
			f, err := os.Create("/data/" + host + "-trace.out")
			//f, err := os.Create("cpu-profile.prof")
			if err != nil {
				log.Fatal(err)
			}

			trace.Start(f)

			//		pprof.StartCPUProfile(f)
			//		defer pprof.StopCPUProfile()

			sig := make(chan os.Signal, 1)
			signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM)
			<-sig
			trace.Stop()
			os.Exit(0)
		}()
	}
}

func main() {

	// Initializing application
	a := app.NewAppInstance()

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
