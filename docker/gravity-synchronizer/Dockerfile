FROM golang:1.12.1-alpine AS builder
WORKDIR $GOPATH/src/github.com/BrobridgeOrg/gravity-synchronizer/
COPY . .

RUN apk add --update git
RUN CGO_ENABLED=0 GO111MODULE=on go build -installsuffix cgo -o /gravity-synchronizer

FROM alpine:3.8
COPY --from=builder /gravity-synchronizer /gravity-synchronizer
COPY ./config/config.toml /config.toml
COPY ./rules /rules
COPY ./docker/gravity-synchronizer/startup.sh /startup.sh
CMD ["/bin/sh", "/startup.sh"]
