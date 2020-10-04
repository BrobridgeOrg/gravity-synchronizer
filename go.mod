module gravity-synchronizer

go 1.13

require (
	github.com/BrobridgeOrg/gravity-api v0.0.0-20200913143851-ae88deb076cf
	github.com/BrobridgeOrg/gravity-controller v0.0.0-20200913144403-5b1922d13bd5
	github.com/BrobridgeOrg/gravity-synchronizer v0.0.0-00010101000000-000000000000
	github.com/cfsghost/grpc-connection-pool v0.0.0-20200903182758-f64b83c701d7
	github.com/golang/protobuf v1.4.2
	github.com/nats-io/nats.go v1.10.0
	github.com/nats-io/stan.go v0.7.0
	github.com/prometheus/common v0.7.0
	github.com/sirupsen/logrus v1.6.0
	github.com/soheilhy/cmux v0.1.4
	github.com/sony/sonyflake v1.0.0
	github.com/spf13/viper v1.7.1
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	google.golang.org/grpc v1.31.1
	google.golang.org/protobuf v1.25.0
)

replace github.com/BrobridgeOrg/gravity-api => ../gravity-api

replace github.com/BrobridgeOrg/gravity-synchronizer => ./
