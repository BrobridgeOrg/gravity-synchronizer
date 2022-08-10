package synchronizer

import (
	"errors"
	"gravity-synchronizer/pkg/synchronizer/service/dsa"
	"sync"

	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	dsa_pb "github.com/BrobridgeOrg/gravity-api/service/dsa"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type DSARequest struct {
	msg *nats.Msg
	key *keyring.KeyInfo
}

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

	// Data handling
	synchronizer.dsa.OnEmitted(func(packet *dsa.PipelinePacket) {
		err := synchronizer.handlePipelinePacket(packet)
		packet.Done(err)
	})

	// Request is completed
	synchronizer.dsa.OnCompleted(func(privData interface{}, data interface{}, err error) {

		/*
			m := privData.(*nats.Msg)
			// Failed
			if err != nil {
				m.Respond(FailureReply(0, err.Error()))
				return
			}

			// Success
			m.Respond(SuccessReply)
		*/

		request := privData.(*DSARequest)

		// Failed
		if err != nil {
			//			encrypted, _ := request.key.Encryption().Encrypt(FailureReply(0, err.Error()))

			packet := &packet_pb.Packet{
				Error:  true,
				Reason: err.Error(),
			}

			returnedData, _ := proto.Marshal(packet)

			request.msg.Respond(returnedData)
			return
		}

		// Encrypt
		encrypted, _ := request.key.Encryption().Encrypt(SuccessReply)
		packet := &packet_pb.Packet{
			Error:   false,
			Payload: encrypted,
		}

		returnedData, _ := proto.Marshal(packet)

		request.msg.Respond(returnedData)
		//		request.msg.Respond(encrypted)
	})

	// Setup worker count
	viper.SetDefault("dsa.workerCount", 4)
	workerCount := viper.GetInt("dsa.workerCount")
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
		//		synchronizer.dsa.PushData(m, m.Data)
		synchronizer.handleBatchMsg(m, "ADAPTER")
	})
	if err != nil {
		return err
	}

	sub.SetPendingLimits(-1, -1)
	connection.Flush()

	return nil
}

func (synchronizer *Synchronizer) handleBatchMsg(m *nats.Msg, rules ...string) error {

	var packet packet_pb.Packet
	err := proto.Unmarshal(m.Data, &packet)
	if err != nil {
		// invalid request
		return errors.New("InvalidRequest")
	}

	// Using appID to find key info
	keyInfo := synchronizer.keyring.Get(packet.AppID)
	if keyInfo == nil {
		// No such app ID
		return errors.New("NotFoundAppID")
	}

	// check permissions
	if len(rules) > 0 {
		hasPerm := false
		for _, rule := range rules {
			if keyInfo.Permission().Check(rule) {
				hasPerm = true
			}
		}

		// No permission
		if !hasPerm {
			return errors.New("Forbidden")
		}
	}

	// Decrypt
	data, err := keyInfo.Encryption().Decrypt(packet.Payload)
	if err != nil {
		return errors.New("InvalidKey")
	}

	// pass decrypted payload to next handler
	var payload packet_pb.Payload
	err = proto.Unmarshal(data, &payload)
	if err != nil {
		return errors.New("InvalidPayload")
	}

	// Prepare DSA request
	request := &DSARequest{
		msg: m,
		key: keyInfo,
	}

	synchronizer.dsa.PushData(request, payload.Data)
	/*
		// Encrypt
		encrypted, err := keyInfo.Encryption().Encrypt(returnedData.([]byte))
		if err != nil {
			return nil, nil
		}
	*/
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
