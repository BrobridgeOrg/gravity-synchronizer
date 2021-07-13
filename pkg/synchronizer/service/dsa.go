package synchronizer

import (
	"gravity-synchronizer/pkg/synchronizer/service/dsa"
	"sync"

	dsa_pb "github.com/BrobridgeOrg/gravity-api/service/dsa"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var dsaPublishReplyPool = sync.Pool{
	New: func() interface{} {
		return &dsa_pb.BatchPublishReply{}
	},
}

var SuccessReply, _ = proto.Marshal(&dsa_pb.BatchPublishReply{
	Success: true,
})

func FailureReply(successCount int32, reason string) []byte {
	reply := dsaPublishReplyPool.Get().(*dsa_pb.BatchPublishReply)
	reply.Success = false
	reply.SuccessCount = successCount
	reply.Reason = reason
	resp, _ := proto.Marshal(reply)
	dsaPublishReplyPool.Put(reply)
	return resp
}

func (synchronizer *Synchronizer) initializeDataSourceAdapter() error {

	log.Info("Initializing data source adapter...")

	synchronizer.dsa = dsa.NewDataSourceAdapter()
	synchronizer.dsa.SetRuleConfig(synchronizer.ruleConfig)
	synchronizer.dsa.OnEmitted(func(packet *dsa.PipelinePacket) {
		err := synchronizer.handlePipelinePacket(packet)
		packet.Done(err)
	})

	synchronizer.dsa.OnCompleted(func(privData interface{}, data interface{}, err error) {

		m := privData.(*nats.Msg)

		// Failed
		if err != nil {
			m.Respond(FailureReply(0, err.Error()))
			return
		}

		// Success
		m.Respond(SuccessReply)
	})

	// Setup worker count
	viper.SetDefault("pipeline.workerCount", 16)
	workerCount := viper.GetInt("pipeline.workerCount")
	synchronizer.dsa.SetWorkerCount(workerCount)

	err := synchronizer.dsa.Init()
	if err != nil {
		return err
	}

	err = synchronizer.dsa.InitTasks()
	if err != nil {
		return err
	}

	// Subscribe to quque to receive events
	connection := synchronizer.gravityClient.GetConnection()
	sub, err := connection.QueueSubscribe(synchronizer.domain+".dsa.batch", "synchronizer", func(m *nats.Msg) {
		synchronizer.dsa.PushData(m, m.Data)
	})
	if err != nil {
		return err
	}

	sub.SetPendingLimits(-1, -1)
	connection.Flush()

	return nil
}

func (synchronizer *Synchronizer) handlePipelinePacket(packet *dsa.PipelinePacket) error {

	// Trying to find pipeline from this node
	pipeline, ok := synchronizer.pipelines[uint64(packet.PipelineID)]
	if !ok {
		// TODO: support to redirect packet to external node
		return nil
	}

	// Process whole task group and waiting
	err := synchronizer.dataHandler.PushTaskGroup(pipeline, packet.TaskGroup)
	if err != nil {
		return err
	}

	return nil
}
