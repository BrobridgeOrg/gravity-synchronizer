module gravity-synchronizer

go 1.15

require (
	github.com/BrobridgeOrg/EventStore v0.0.6
	github.com/BrobridgeOrg/gravity-api v0.2.12
	github.com/BrobridgeOrg/gravity-data-handler v0.0.0-20201221072337-a33b67c8d037
	github.com/BrobridgeOrg/gravity-sdk v0.0.3
	github.com/BrobridgeOrg/gravity-synchronizer v0.0.0-00010101000000-000000000000
	github.com/cfsghost/gosharding v0.0.3
	github.com/cfsghost/grpc-connection-pool v0.6.0
	github.com/cfsghost/parallel-chunked-flow v0.0.6
	github.com/facebookgo/ensure v0.0.0-20200202191622-63f1cf65ac4c // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20200203212716-c811ad88dec4 // indirect
	github.com/golang/protobuf v1.4.2
	github.com/json-iterator/go v1.1.10
	github.com/lithammer/go-jump-consistent-hash v1.0.1
	github.com/nats-io/nats.go v1.10.0
	github.com/prometheus/common v0.15.0
	github.com/sirupsen/logrus v1.7.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/viper v1.7.1
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	go.uber.org/automaxprocs v1.3.0
	google.golang.org/grpc v1.32.0
	google.golang.org/protobuf v1.25.0
)

replace github.com/BrobridgeOrg/gravity-synchronizer => ./

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api

//replace github.com/BrobridgeOrg/gravity-sdk => ../gravity-sdk

//replace github.com/cfsghost/grpc-connection-pool => /Users/fred/works/opensource/grpc-connection-pool
//replace github.com/cfsghost/gosharding => /Users/fred/works/opensource/gosharding
//replace github.com/cfsghost/parallel-chunked-flow => /Users/fred/works/opensource/parallel-chunked-flow

//replace github.com/BrobridgeOrg/EventStore => /Users/fred/works/Brobridge/EventStore
