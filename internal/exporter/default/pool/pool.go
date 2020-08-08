package pool

import (
	"context"
	"errors"
	"sync"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

var ErrExceeded = errors.New("Maximum number of connections exceeded")

type GRPCPool struct {
	host        string
	options     *Options
	dialOptions []grpc.DialOption
	connections chan *Connection
	pending     chan *grpc.ClientConn
	connCount   uint32

	mutex sync.RWMutex
}

func NewGRPCPool(host string, options *Options, dialOptions ...grpc.DialOption) (*GRPCPool, error) {

	pool := &GRPCPool{
		host:        host,
		options:     options,
		dialOptions: dialOptions,
		connections: make(chan *Connection, options.MaxCap),
		connCount:   0,
	}

	err := pool.init()
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func (pool *GRPCPool) init() error {

	log.WithFields(log.Fields{
		"InitCap": pool.options.InitCap,
		"MaxCap":  pool.options.MaxCap,
	}).Info("Initializing gRPC connection pool ...")

	// Initializing connections
	for i := 0; i < pool.options.InitCap; i++ {

		// Create connection
		connection, err := pool.factory()
		if err != nil {
			return err
		}

		pool.connections <- NewConnection(connection)
	}

	return nil
}

func (pool *GRPCPool) ref() (uint32, error) {

	pool.mutex.Lock()

	// Check pool size
	if pool.connCount >= uint32(pool.options.MaxCap) {
		pool.mutex.Unlock()
		return pool.connCount, ErrExceeded
	}

	// Update counter
	pool.connCount++

	pool.mutex.Unlock()

	return pool.connCount, nil
}

func (pool *GRPCPool) unref() (uint32, error) {
	pool.mutex.Lock()
	pool.connCount--
	pool.mutex.Unlock()

	return pool.connCount, nil
}

func (pool *GRPCPool) factory() (*grpc.ClientConn, error) {

	count, err := pool.ref()
	if err != nil {
		return nil, ErrExceeded
	}

	log.WithFields(log.Fields{
		"count": count,
	}).Info("Establishing gRPC connection ...")

	// Preparing context with timeout options
	ctx, cancel := context.WithTimeout(context.Background(), pool.options.DialTimeout)
	defer cancel()

	connection, err := grpc.DialContext(ctx, pool.host, pool.dialOptions...)
	if err != nil {
		pool.unref()
		return nil, err
	}

	return connection, nil
}

func (pool *GRPCPool) checkConnectionState(connection *grpc.ClientConn) bool {

	state := connection.GetState()

	if state == connectivity.Shutdown || state == connectivity.TransientFailure {

		// this connection doesn't work
		connection.Close()
		/*
			pool.mutex.Lock()
			pool.connCount--
			pool.mutex.Unlock()
		*/
		pool.unref()

		return false
	}

	return true
}

func (pool *GRPCPool) Get() (*grpc.ClientConn, error) {

	pool.mutex.Lock()
	connections := pool.connections
	pool.mutex.Unlock()

	for {

		select {
		case c := <-connections:
			// Getting connection from buffered channel

			if !pool.checkConnectionState(c.connection) {
				continue
			}

			return c.connection, nil
		default:

			// No available connection, so creating a new connection
			c, err := pool.factory()
			if err != nil {
				log.Error(err)
				continue
			}

			pool.Put(c)
		}
	}
}

func (pool *GRPCPool) Put(connection *grpc.ClientConn) error {

	if !pool.checkConnectionState(connection) {
		return nil
	}

	pool.connections <- NewConnection(connection)
	return nil
}
