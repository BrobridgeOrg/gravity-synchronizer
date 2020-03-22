package exporter

import (
	"errors"
	"fmt"
	natssc "gravity-synchronizer/internal/exporter/nats-streaming"

	"gravity-synchronizer/internal/projection"
)

type Exporter struct {
	Info      *ExporterInfo
	Connector ConnectorImpl
}

type ExporterInfo struct {
	Type   string
	Host   string
	Port   int
	Params map[string]interface{}
}

func CreateExporter() *Exporter {
	return &Exporter{}
}

func (exporter *Exporter) Connect(etype string, host string, port int, params map[string]interface{}) error {

	exporter.Info = &ExporterInfo{
		Type:   etype,
		Host:   host,
		Port:   port,
		Params: make(map[string]interface{}),
	}

	for key, value := range params {
		exporter.Info.Params[key] = value
	}

	switch etype {
	case "nats-streaming":
		uri := fmt.Sprintf("%s:%d", host, port)
		connector := natssc.CreateConnector(uri, params)
		exporter.Connector = ConnectorImpl(connector)
		err := connector.Connect()
		if err != nil {
			return err
		}
	default:
		return errors.New(fmt.Sprintf("No such type \"%s\" supported.", etype))
	}

	return nil
}

func (exporter *Exporter) Send(sequence uint64, pj *projection.Projection) error {
	return exporter.Connector.Send(sequence, pj)
}
