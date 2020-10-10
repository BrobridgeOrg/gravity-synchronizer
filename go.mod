module gravity-synchronizer

go 1.13

require (
	github.com/BrobridgeOrg/gravity-api v0.0.0-20201005083130-0c244aa1bdae
	github.com/BrobridgeOrg/gravity-synchronizer v0.0.0-00010101000000-000000000000
	github.com/cfsghost/grpc-connection-pool v0.1.0
	github.com/golang/protobuf v1.4.2
	github.com/json-iterator/go v1.1.6
	github.com/nats-io/nats.go v1.10.0
	github.com/prometheus/common v0.7.0
	github.com/sirupsen/logrus v1.6.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/viper v1.7.1
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	google.golang.org/grpc v1.31.1
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api

replace github.com/BrobridgeOrg/gravity-synchronizer => ./

//replace github.com/cfsghost/grpc-connection-pool => /Users/fred/works/opensource/grpc-connection-pool
