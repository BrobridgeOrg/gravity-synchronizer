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
	app app.AppImpl
}

func CreateCacheStore(a app.AppImpl) *CacheStore {
	return &CacheStore{
		app: a,
	}
}

func (ccs *CacheStore) GetSnapshotState(collection string) (uint64, error) {

	// Set up a connection to supervisor.
	address := viper.GetString("cache_store.host")
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	request := &pb.GetSnapshotStateRequest{
		Collection: collection,
	}

	// Preparing context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Publish
	resp, err := pb.NewDataSnapshotClient(conn).GetSnapshotState(ctx, request)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Connect to cache store
	stream, err := pb.NewDataSnapshotClient(conn).GetSnapshot(ctx, request)
	if err != nil {
		return 0, err
	}

	var seq uint64
	for {
		packet, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error(err)

		}

		seq = packet.Sequence

		for _, entry := range packet.Entries {

			log.Info("Retriving ", entry.Data)

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
