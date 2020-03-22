package exporter

import "gravity-synchronizer/internal/projection"

type ConnectorImpl interface {
	Connect() error
	Close()
	Send(uint64, *projection.Projection) error
}
