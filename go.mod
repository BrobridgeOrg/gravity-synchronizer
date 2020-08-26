module gravity-synchronizer

go 1.13

require (
	github.com/BrobridgeOrg/gravity-api v0.0.0-20200813150216-4f3ba18b8084
	github.com/BrobridgeOrg/gravity-presenter-rest v0.0.0-20200822094512-c7ecfe038349 // indirect
	github.com/armon/consul-api v0.0.0-20180202201655-eb2c6b5be1b6 // indirect
	github.com/denisenkom/go-mssqldb v0.0.0-20200206145737-bbfc9a55622e
	github.com/flyaways/pool v1.0.1 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/godror/godror v0.14.0
	github.com/golang/protobuf v1.4.2
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.3.0
	github.com/mattn/go-oci8 v0.0.8
	github.com/nats-io/nats-streaming-server v0.17.0 // indirect
	github.com/nats-io/nats.go v1.10.0
	github.com/nats-io/stan.go v0.6.0
	github.com/prometheus/common v0.7.0
	github.com/sirupsen/logrus v1.6.0
	github.com/sony/sonyflake v1.0.0
	github.com/spf13/viper v1.7.1
	github.com/syndtr/goleveldb v1.0.0
	github.com/xordataexchange/crypt v0.0.3-0.20170626215501-b2862e3d0a77 // indirect
	google.golang.org/grpc v1.31.0
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
