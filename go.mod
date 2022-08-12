module gravity-synchronizer

go 1.15

require (
	github.com/BrobridgeOrg/EventStore v0.0.24
	github.com/BrobridgeOrg/broc v0.0.2
	github.com/BrobridgeOrg/gravity-api v0.2.25
	github.com/BrobridgeOrg/gravity-exporter-nats v0.0.0-20211027080937-4b988b57c4e8 // indirect
	github.com/BrobridgeOrg/gravity-sdk v0.0.50
	github.com/BrobridgeOrg/gravity-synchronizer v0.0.0-00010101000000-000000000000
	github.com/BrobridgeOrg/schemer v0.0.11
	github.com/BrobridgeOrg/sequential-data-flow v0.0.1
	github.com/cfsghost/gosharding v0.0.3
	github.com/cfsghost/parallel-chunked-flow v0.0.6
	github.com/cfsghost/taskflow v0.0.3
	github.com/golang/protobuf v1.5.2
	github.com/json-iterator/go v1.1.12
	github.com/lithammer/go-jump-consistent-hash v1.0.2
	github.com/nats-io/jsm.go v0.0.27
	github.com/nats-io/nats.go v1.13.1-0.20220121202836-972a071d373d
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/prometheus/common v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.3.0
	github.com/spf13/viper v1.10.1
	github.com/stretchr/testify v1.7.0 // indirect
	go.uber.org/ratelimit v0.2.0
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa // indirect
)

replace github.com/BrobridgeOrg/gravity-synchronizer => ./

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api

//replace github.com/BrobridgeOrg/gravity-sdk => ../gravity-sdk

//replace github.com/BrobridgeOrg/broc => ../../broc

//replace github.com/cfsghost/grpc-connection-pool => /Users/fred/works/opensource/grpc-connection-pool
//replace github.com/cfsghost/gosharding => /Users/fred/works/opensource/gosharding
//replace github.com/cfsghost/parallel-chunked-flow => /Users/fred/works/opensource/parallel-chunked-flow
//replace github.com/cfsghost/taskflow => /Users/fred/works/opensource/taskflow

//replace github.com/BrobridgeOrg/EventStore => /Users/fred/works/Brobridge/EventStore
//replace github.com/BrobridgeOrg/schemer => /Users/fred/works/Brobridge/schemer
