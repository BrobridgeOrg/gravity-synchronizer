package synchronizer

import (
	"context"
	"encoding/json"
	app "gravity-synchronizer/app/interface"
	pb "gravity-synchronizer/pb"
	"io"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type CacheStore struct {
	app    app.AppImpl
	client *grpc.ClientConn
}

func CreateCacheStore(a app.AppImpl) *CacheStore {
	return &CacheStore{
		app: a,
	}
}

func (ccs *CacheStore) Init() error {

	address := viper.GetString("cache_store.host")

	log.WithFields(log.Fields{
		"address": address,
	}).Info("Connecting to cache server...")

	// Set up a connection to supervisor.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		return err
	}

	ccs.client = conn

	return nil
}

func (ccs *CacheStore) GetSnapshotState(collection string) (uint64, error) {

	log.WithFields(log.Fields{
		"collection": collection,
	}).Info("Getting snapshot state...")

	request := &pb.GetSnapshotStateRequest{
		Collection: collection,
	}

	// Preparing context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// Publish
	resp, err := pb.NewDataSnapshotClient(ccs.client).GetSnapshotState(ctx, request)
	if err != nil {
		return 0, err
	}

	return resp.Sequence, nil
}

func (ccs *CacheStore) FetchSnapshot(collection string, fn func(map[string]interface{}) error) (uint64, error) {

	// Set up a connection to supervisor.
	address := viper.GetString("cache_store.host")
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	request := &pb.GetSnapshotRequest{
		Collection: collection,
	}

	// Preparing context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to cache store
	stream, err := pb.NewDataSnapshotClient(conn).GetSnapshot(ctx, request)
	if err != nil {
		return 0, err
	}

	var seq uint64
	var counter uint64 = 0
	for {
		packet, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error(err)

		}

		seq = packet.Sequence

		counter++

		log.WithFields(log.Fields{
			"counter": counter,
			"seq":     seq,
		}).Info("Retriving Packet...")

		for _, entry := range packet.Entries {

			data := make(map[string]interface{})
			err := json.Unmarshal([]byte(entry.Data), &data)
			if err != nil {
				log.Error(err)
				continue
			}

			err = fn(data)
			if err != nil {
				continue
			}
		}
	}

	return seq, nil
}
