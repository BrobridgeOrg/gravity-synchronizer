FROM golang:1.15.12-alpine3.12 AS builder
#FROM golang:1.15.15-alpine3.13 AS builder
WORKDIR /
COPY . .

RUN apk add --update build-base && apk upgrade --available
RUN go build -o /gravity-synchronizer /cmd/gravity-synchronizer/gravity-synchronizer.go



FROM alpine:3.15.6
RUN apk update && apk upgrade --available
WORKDIR /
COPY --from=builder /gravity-synchronizer /gravity-synchronizer
COPY ./configs/config.toml /configs/config.toml
COPY ./rules /rules
COPY ./docker/gravity-synchronizer/startup.sh /startup.sh

RUN chmod 700 -R /rules && chown -R 1001  /rules
RUN mkdir /datastore && chmod 700 -R /datastore && chown -R 1001 /datastore
USER 1001

CMD ["sh", "/startup.sh"]
