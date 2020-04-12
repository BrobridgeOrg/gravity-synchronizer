package natss

import (
	"gravity-synchronizer/internal/projection"
	"strconv"

	stan "github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
	"github.com/sony/sonyflake"
)

type Connector struct {
	host       string
	clusterID  string
	clientName string
	queue      string
	client     stan.Conn
}

func CreateConnector(host string, params map[string]interface{}) *Connector {

	clusterID, ok := params["cluster_id"]
	if !ok {
		return nil
	}

	queue, ok := params["queue"]
	if !ok {
		return nil
	}

	// Genereate a unique ID for instance
	flake := sonyflake.NewSonyflake(sonyflake.Settings{})
	id, err := flake.NextID()
	if err != nil {
		return nil
	}

	idStr := strconv.FormatUint(id, 16)

	return &Connector{
		host:       host,
		clusterID:  clusterID.(string),
		clientName: idStr,
		queue:      queue.(string),
	}
}

func (connector *Connector) Connect() error {

	log.WithFields(log.Fields{
		"host":       connector.host,
		"clientName": connector.clientName,
		"clusterID":  connector.clusterID,
	}).Info("Connecting to NATS Streaming server")

	// Connect to queue server
	sc, err := stan.Connect(connector.clusterID, connector.clientName, stan.NatsURL(connector.host))
	if err != nil {
		return err
	}

	connector.client = sc

	return nil
}

func (connector *Connector) Close() {
	connector.client.Close()
}

func (connector *Connector) Emit(eventName string, data []byte) error {

	if err := connector.client.Publish(eventName, data); err != nil {
		return err
	}

	return nil
}

func (connector *Connector) Send(sequence uint64, pj *projection.Projection) error {

	// Genereate JSON string
	data, err := pj.ToJSON()
	if err != nil {
		return err
	}

	return connector.Emit(connector.queue, data)
}
