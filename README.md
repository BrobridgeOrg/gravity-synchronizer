# Gravity Synchronizer

Synchronizer is core component for event sourcing, event store and writing data to specific database.

## Build

build synchronizer with the simple command in the following:

```shell
go build ./cmd/gravity-synchronizer
```

## Benchmark

Synchronizer do data process works which includes persistent message, snapshot, message routing and so on. It requires to have a way to do benchmark for performance evaluation.

### Persistent message

Persistent message is based on RocksDB, here is the benchmark with command in the following:

```shell
go test -bench=. ./pkg/datastore/manager
```

Results:
```shell
$ go test -bench=. ./pkg/datastore/manager
INFO[0000] Loading data store                            path=./bench
INFO[0000] Initializing data store                       name=bench
INFO[0000] Initialized counter                           lastSeq=9600985 store=bench
goos: darwin
goarch: amd64
pkg: gravity-synchronizer/pkg/datastore/manager
BenchmarkWrite-16    	  230128	      4963 ns/op
PASS
ok  	gravity-synchronizer/pkg/datastore/manager	1.658s
```

## License

Licensed under the MIT License

## Authors

Copyright(c) 2020 Fred Chien <<fred@brobridge.com>>
