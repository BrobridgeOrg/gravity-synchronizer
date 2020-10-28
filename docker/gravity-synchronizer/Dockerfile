FROM golang:1.15.3-alpine3.12 AS builder
WORKDIR /
COPY . .

RUN apk add --update git build-base zlib-dev  bzip2-dev snappy-dev lz4-dev zstd-dev

RUN tar -Jxf /docker/gravity-synchronizer/rocksdb.tar.xz -C /docker/gravity-synchronizer/

RUN CGO_CFLAGS="-I/docker/gravity-synchronizer/rocksdb/include" CGO_LDFLAGS="-L/docker/gravity-synchronizer/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" CGO_ENABLED=1 go build -installsuffix cgo -o /gravity-synchronizer /cmd/gravity-synchronizer/gravity-synchronizer.go



FROM alpine:3.12
COPY --from=builder /gravity-synchronizer /gravity-synchronizer
COPY ./configs/config.toml /configs/config.toml
COPY ./rules /rules
COPY ./docker/gravity-synchronizer/startup.sh /startup.sh

RUN apk add --update --no-cache zlib-dev  bzip2-dev snappy-dev lz4-dev zstd-dev

EXPOSE 44448
CMD ["/bin/sh", "/startup.sh"]
