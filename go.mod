module gravity-synchronizer

go 1.15

require (
	github.com/BrobridgeOrg/EventStore v0.1.2
	github.com/BrobridgeOrg/broc v0.0.2
	github.com/BrobridgeOrg/gravity-api v0.2.26
	github.com/BrobridgeOrg/gravity-sdk v1.0.5
	github.com/BrobridgeOrg/gravity-synchronizer v0.0.0-00010101000000-000000000000
	github.com/BrobridgeOrg/schemer v0.0.12
	github.com/BrobridgeOrg/sequential-data-flow v0.0.2
	github.com/cfsghost/gosharding v0.0.3
	github.com/cfsghost/taskflow v0.0.3
	github.com/golang/protobuf v1.5.2
	github.com/json-iterator/go v1.1.12
	github.com/lithammer/go-jump-consistent-hash v1.0.2
	github.com/nats-io/nats.go v1.16.0
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.3.0
	github.com/spf13/viper v1.10.1
	github.com/stretchr/testify v1.8.0
	go.uber.org/ratelimit v0.2.0
)

replace github.com/BrobridgeOrg/gravity-synchronizer => ./

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api

//replace github.com/BrobridgeOrg/gravity-sdk => ../sdk/v1/gravity-sdk

//replace github.com/BrobridgeOrg/broc => ../../broc

//replace github.com/cfsghost/grpc-connection-pool => /Users/fred/works/opensource/grpc-connection-pool
//replace github.com/cfsghost/gosharding => /Users/fred/works/opensource/gosharding
//replace github.com/cfsghost/parallel-chunked-flow => /Users/fred/works/opensource/parallel-chunked-flow
//replace github.com/cfsghost/taskflow => /Users/fred/works/opensource/taskflow

//replace github.com/BrobridgeOrg/EventStore => /Users/fred/works/Brobridge/EventStore
//replace github.com/BrobridgeOrg/schemer => /Users/fred/works/Brobridge/schemer
