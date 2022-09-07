package synchronizer

import (
	"errors"
	"fmt"
	"gravity-synchronizer/pkg/synchronizer/service/dsa"
	"sync"

	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	dsa_pb "github.com/BrobridgeOrg/gravity-api/service/dsa"

	//	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var batchReqPool = sync.Pool{
	New: func() interface{} {
		return &dsa_pb.BatchPublishRequest{}
	},
}

var dsaPublishReplyPool = sync.Pool{
	New: func() interface{} {
		return &dsa_pb.BatchPublishReply{}
	},
}

var pubReqPool = sync.Pool{
	New: func() interface{} {
		return &dsa_pb.PublishRequest{}
	},
}

type DSARequest struct {
	msg *nats.Msg
	key *keyring.KeyInfo
}

type DSAMessage struct {
	msg *nats.Msg
}

var dsaReqPool = sync.Pool{
	New: func() interface{} {
		return &DSARequest{}
	},
}

var dsaMsgPool = sync.Pool{
	New: func() interface{} {
		return &DSAMessage{}
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

		switch v := privData.(type) {
		case *DSARequest:

			request := v
			defer dsaReqPool.Put(request)

			// Failed
			if err != nil {
				//			encrypted, _ := request.key.Encryption().Encrypt(FailureReply(0, err.Error()))

				log.Errorf("synchronizer: failed to process data: %v", err)

				packet := &packet_pb.Packet{
					Error:  true,
					Reason: err.Error(),
				}

				returnedData, _ := proto.Marshal(packet)

				request.msg.Respond(returnedData)
				return
			}

			// Encrypt
			if request.key != nil {
				encrypted, _ := request.key.Encryption().Encrypt(SuccessReply)
				packet := &packet_pb.Packet{
					Error:   false,
					Payload: encrypted,
				}

				returnedData, _ := proto.Marshal(packet)
				request.msg.Respond(returnedData)

				return
			}

			packet := &packet_pb.Packet{
				Error:   false,
				Payload: SuccessReply,
			}

			returnedData, _ := proto.Marshal(packet)
			request.msg.Respond(returnedData)
		case *DSAMessage:
			v.msg.Ack()
			dsaMsgPool.Put(v)
		}
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

	// For events
	err = synchronizer.startDSAEventReceiver()
	if err != nil {
		return err
	}

	// For batch
	err = synchronizer.startDSARequestReceiver()
	if err != nil {
		return err
	}

	return nil
}

func (synchronizer *Synchronizer) startDSAEventReceiver() error {

	nc := synchronizer.gravityClient.GetConnection()
	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	subj := fmt.Sprintf("%s.dsa.event", synchronizer.domain)
	ch := make(chan *nats.Msg)
	sub, err := js.ChanQueueSubscribe(subj, synchronizer.clientID, ch)
	if err != nil {
		return err
	}

	sub.SetPendingLimits(-1, -1)
	nc.Flush()
	defer sub.Unsubscribe()

	for msg := range ch {

		// Parsing single publish request
		preq := pubReqPool.Get().(*dsa_pb.PublishRequest)
		err = proto.Unmarshal(msg.Data, preq)
		if err != nil {
			log.Errorf("dsa: %v", err)
			msg.Ack()
			continue
		}

		// Prepare DSA request
		dsam := dsaMsgPool.Get().(*DSAMessage)
		dsam.msg = msg

		synchronizer.dsa.PushData(dsam, preq)
	}

	return nil
}

func (synchronizer *Synchronizer) startDSARequestReceiver() error {

	// Subscribe to quque to receive events
	// Warning: this is legacy way which is not safe to receive data
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

func (synchronizer *Synchronizer) decodeMsg(m *nats.Msg, rules ...string) ([]byte, *DSARequest, error) {

	var packet packet_pb.Packet
	err := proto.Unmarshal(m.Data, &packet)
	if err != nil {
		// invalid request
		return nil, nil, errors.New("InvalidRequest")
	}

	// Using appID to find key info
	keyInfo := synchronizer.keyring.Get(packet.AppID)
	if keyInfo == nil {
		// No such app ID
		return nil, nil, errors.New("NotFoundAppID")
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
			return nil, nil, errors.New("Forbidden")
		}
	}

	// Decrypt
	data, err := keyInfo.Encryption().Decrypt(packet.Payload)
	if err != nil {
		return nil, nil, errors.New("InvalidKey")
	}

	// pass decrypted payload to next handler
	var payload packet_pb.Payload
	err = proto.Unmarshal(data, &payload)
	if err != nil {
		return nil, nil, errors.New("InvalidPayload")
	}

	// Prepare DSA request
	request := dsaReqPool.Get().(*DSARequest)
	request.msg = m
	request.key = keyInfo

	/*
		// Encrypt
		encrypted, err := keyInfo.Encryption().Encrypt(returnedData.([]byte))
		if err != nil {
			return nil, nil
		}
	*/

	return payload.Data, request, nil
}

func (synchronizer *Synchronizer) handleBatchMsg(m *nats.Msg, rules ...string) error {

	payload, request, err := synchronizer.decodeMsg(m, rules...)
	if err != nil {
		return err
	}

	synchronizer.dsa.PushData(request, payload)
	return nil
}

func (synchronizer *Synchronizer) handlePipelinePacket(packet *dsa.PipelinePacket) error {

	// Trying to find pipeline from this node
	pipeline, ok := synchronizer.pipelines[uint64(packet.PipelineID)]
	if !ok {
		// TODO: support to redirect packet to external node
		log.Warnf("synchronizer: pipeline not found: %d", packet.PipelineID)
		return nil
	}

	// Process whole task group and waiting
	err := synchronizer.dataHandler.PushTaskGroup(pipeline, packet.TaskGroup)
	if err != nil {
		return err
	}

	return nil
}
