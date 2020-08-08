package exporter

import "gravity-synchronizer/internal/projection"

type ConnectorImpl interface {
	Init() error
	Close()
	Send(uint64, *projection.Projection) error
}
