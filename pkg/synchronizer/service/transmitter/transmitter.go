package transmitter

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/BrobridgeOrg/gravity-synchronizer/pkg/projection"

	grpc_connection_pool "github.com/cfsghost/grpc-connection-pool"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	transmitter "github.com/BrobridgeOrg/gravity-api/service/transmitter"
)

var (
	NotUnsignedIntegerErr = errors.New("Not unisgned integer")
	NotIntegerErr         = errors.New("Not integer")
	NotFloatErr           = errors.New("Not float")

	TransmitterErr = errors.New("Transmitter error")
)

var recordPool = sync.Pool{
	New: func() interface{} {
		return &transmitter.Record{}
	},
}

type Transmitter struct {
	name    string
	host    string
	port    int
	channel string
	pool    *grpc_connection_pool.GRPCPool
}

func NewTransmitter(name string, host string, port int) *Transmitter {
	return &Transmitter{
		name: name,
		host: host,
		port: port,
	}
}

func (t *Transmitter) Init() error {

	host := fmt.Sprintf("%s:%d", t.host, t.port)

	log.WithFields(log.Fields{
		"name": t.name,
		"host": host,
	}).Info("Connecting to transmitter")

	options := &grpc_connection_pool.Options{
		InitCap:     8,
		MaxCap:      16,
		DialTimeout: time.Second * 20,
	}

	// Initialize connection pool
	p, err := grpc_connection_pool.NewGRPCPool(host, options, grpc.WithInsecure())
	if err != nil {
		return err
	}

	if p == nil {
		return err
	}

	t.pool = p

	return nil
}

func (t *Transmitter) Insert(table string, data map[string]interface{}, callback func(error)) error {

	// Prepare record
	record := recordPool.Get().(*transmitter.Record)
	record.Table = table
	record.Method = transmitter.Method_INSERT
	record.Fields = make([]*transmitter.Field, 0, len(data))

	for key, value := range data {

		// Convert value to protobuf format
		v, err := t.getValue(value)
		if err != nil {
			log.WithFields(log.Fields{
				"component": "transmitter",
				"action":    "insert",
			}).Error(err)
			continue
		}

		record.Fields = append(record.Fields, &transmitter.Field{
			Name:  key,
			Value: v,
		})
	}

	return t.handle(record)
}

func (t *Transmitter) Truncate(table string) error {

	conn, err := t.pool.Get()
	if err != nil {
		return err
	}

	client := transmitter.NewTransmitterClient(conn)

	reply, err := client.Truncate(context.Background(), &transmitter.TruncateRequest{
		Table: table,
	})
	if err != nil {
		log.WithFields(log.Fields{
			"component": "transmitter",
			"action":    "truncate",
		}).Error(err)
		return err
	}

	if !reply.Success {
		log.WithFields(log.Fields{
			"reason": reply.Reason,
		}).Error("Failed to truncate")
		return TransmitterErr
	}

	return nil
}

func (t *Transmitter) ProcessData(table string, sequence uint64, pj *projection.Projection) error {

	record := recordPool.Get().(*transmitter.Record)
	record.EventName = pj.EventName
	record.Table = table
	record.Fields = make([]*transmitter.Field, 0, len(pj.Fields))
	/*
		record := &transmitter.Record{
			EventName: pj.EventName,
			Table:     table,
			Fields:    make([]*transmitter.Field, 0, len(pj.Fields)),
		}
	*/
	if pj.Method == "delete" {
		record.Method = transmitter.Method_DELETE
	} else if pj.Method == "update" {
		record.Method = transmitter.Method_UPDATE
	} else {
		record.Method = transmitter.Method_INSERT
	}

	for _, field := range pj.Fields {

		// Convert value to protobuf format
		v, err := t.getValue(field.Value)
		if err != nil {
			log.WithFields(log.Fields{
				"component": "transmitter",
				"action":    "ProcessData",
			}).Error(err)
			continue
		}

		record.Fields = append(record.Fields, &transmitter.Field{
			Name:      field.Name,
			Value:     v,
			IsPrimary: field.Primary,
		})
	}

	return t.handle(record)
}

func (t *Transmitter) handle(record *transmitter.Record) error {

	conn, err := t.pool.Get()
	if err != nil {
		log.Error(err)
		return err
	}

	client := transmitter.NewTransmitterClient(conn)

	// Preparing context and timeout settings
	grpcCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Send record
	reply, err := client.Send(grpcCtx, record)
	if err != nil {
		// Release
		recordPool.Put(record)
		log.Error(err)
		return err
	}

	// Release
	recordPool.Put(record)

	if !reply.Success {
		log.WithFields(log.Fields{
			"reason": reply.Reason,
		}).Error("Transmitter error")
		return TransmitterErr
	}

	return nil
}

func (t *Transmitter) getValue(data interface{}) (*transmitter.Value, error) {

	if data == nil {
		return nil, errors.New("data cannnot be nil")
	}

	// Float
	bytes, err := t.getBytesFromFloat(data)
	if err == nil {
		return &transmitter.Value{
			Type:  transmitter.DataType_FLOAT64,
			Value: bytes,
		}, nil
	}

	// Integer
	bytes, err = t.getBytesFromInteger(data)
	if err == nil {
		return &transmitter.Value{
			Type:  transmitter.DataType_INT64,
			Value: bytes,
		}, nil
	}

	// Unsigned integer
	bytes, err = t.getBytesFromUnsignedInteger(data)
	if err == nil {
		return &transmitter.Value{
			Type:  transmitter.DataType_INT64,
			Value: bytes,
		}, nil
	}

	v := reflect.ValueOf(data)

	switch v.Kind() {
	case reflect.Bool:
		data, _ := t.getBytes(data)
		return &transmitter.Value{
			Type:  transmitter.DataType_BOOLEAN,
			Value: data,
		}, nil
	case reflect.String:
		return &transmitter.Value{
			Type:  transmitter.DataType_STRING,
			Value: []byte(data.(string)),
		}, nil
	case reflect.Map:

		// Prepare map value
		value := transmitter.MapValue{
			Fields: make([]*transmitter.Field, 0),
		}

		// Convert each key-value set
		for _, key := range v.MapKeys() {
			ele := v.MapIndex(key)

			// Convert value to protobuf format
			v, err := t.getValue(ele.Interface())
			if err != nil {
				log.Error(err)
				continue
			}

			field := transmitter.Field{
				Name:  key.Interface().(string),
				Value: v,
			}

			value.Fields = append(value.Fields, &field)
		}

		return &transmitter.Value{
			Type: transmitter.DataType_MAP,
			Map:  &value,
		}, nil

	case reflect.Slice:

		// Prepare map value
		value := transmitter.ArrayValue{
			Elements: make([]*transmitter.Value, 0, v.Len()),
		}

		for i := 0; i < v.Len(); i++ {
			ele := v.Index(i)

			// Convert value to protobuf format
			v, err := t.getValue(ele.Interface())
			if err != nil {
				log.Error(err)
				continue
			}

			value.Elements = append(value.Elements, v)
		}

		return &transmitter.Value{
			Type:  transmitter.DataType_ARRAY,
			Array: &value,
		}, nil

	default:
		data, _ := t.getBytes(data)
		return &transmitter.Value{
			Type:  transmitter.DataType_BINARY,
			Value: data,
		}, nil
	}
}

func (t *Transmitter) getBytes(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (t *Transmitter) getBytesFromUnsignedInteger(data interface{}) ([]byte, error) {

	var buf = make([]byte, 8)

	v := reflect.ValueOf(data)
	switch v.Kind() {
	case reflect.Uint:
		binary.LittleEndian.PutUint64(buf, uint64(data.(uint)))
	case reflect.Uint8:
		binary.LittleEndian.PutUint64(buf, uint64(data.(uint8)))
	case reflect.Uint16:
		binary.LittleEndian.PutUint64(buf, uint64(data.(uint16)))
	case reflect.Uint32:
		binary.LittleEndian.PutUint64(buf, uint64(data.(uint32)))
	case reflect.Uint64:
		binary.LittleEndian.PutUint64(buf, data.(uint64))
	default:
		return nil, NotUnsignedIntegerErr
	}

	return buf, nil
}

func (t *Transmitter) getBytesFromInteger(data interface{}) ([]byte, error) {

	var buf = make([]byte, 8)

	v := reflect.ValueOf(data)
	switch v.Kind() {
	case reflect.Int:
		binary.LittleEndian.PutUint64(buf, uint64(data.(int)))
	case reflect.Int8:
		binary.LittleEndian.PutUint64(buf, uint64(data.(int8)))
	case reflect.Int16:
		binary.LittleEndian.PutUint64(buf, uint64(data.(int16)))
	case reflect.Int32:
		binary.LittleEndian.PutUint64(buf, uint64(data.(int32)))
	case reflect.Int64:
		binary.LittleEndian.PutUint64(buf, uint64(data.(int64)))
	default:
		return nil, NotIntegerErr
	}

	return buf, nil
}

func (t *Transmitter) getBytesFromFloat(data interface{}) ([]byte, error) {
	var buf = make([]byte, 8)

	v := reflect.ValueOf(data)
	switch v.Kind() {
	case reflect.Float32:
		binary.LittleEndian.PutUint64(buf, uint64(data.(float32)))
	case reflect.Float64:
		binary.LittleEndian.PutUint64(buf, uint64(data.(float64)))
	default:
		return nil, NotFloatErr
	}

	return buf, nil
}
