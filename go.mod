module gravity-synchronizer

go 1.15

require (
	github.com/BrobridgeOrg/EventStore v0.0.8
	github.com/BrobridgeOrg/gravity-adapter-native v0.0.0-20210522184808-9bf6adc07616 // indirect
	github.com/BrobridgeOrg/gravity-api v0.2.15
	github.com/BrobridgeOrg/gravity-sdk v0.0.10
	github.com/BrobridgeOrg/gravity-synchronizer v0.0.0-00010101000000-000000000000
	github.com/cfsghost/gosharding v0.0.3
	github.com/cfsghost/parallel-chunked-flow v0.0.6
	github.com/golang/protobuf v1.4.2
	github.com/json-iterator/go v1.1.10
	github.com/lithammer/go-jump-consistent-hash v1.0.1
	github.com/nats-io/nats.go v1.10.0
	github.com/sirupsen/logrus v1.7.0
	github.com/spf13/viper v1.7.1
)

replace github.com/BrobridgeOrg/gravity-synchronizer => ./

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api

//replace github.com/BrobridgeOrg/gravity-sdk => ../gravity-sdk

//replace github.com/cfsghost/grpc-connection-pool => /Users/fred/works/opensource/grpc-connection-pool
//replace github.com/cfsghost/gosharding => /Users/fred/works/opensource/gosharding
//replace github.com/cfsghost/parallel-chunked-flow => /Users/fred/works/opensource/parallel-chunked-flow

//replace github.com/BrobridgeOrg/EventStore => /Users/fred/works/Brobridge/EventStore
