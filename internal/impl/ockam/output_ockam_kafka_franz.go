package ockam

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"strconv"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/impl/kafka"
	"github.com/benthosdev/benthos/v4/public/service"
)

func ockamFranzKafkaOutputConfig() *service.ConfigSpec {
	return kafka.FranzKafkaOutputConfig().
		Description(`
Writes a batch of messages to Kafka brokers and waits for acknowledgement before propagating it back to the input.

This output often out-performs the traditional ` + "`kafka`" + ` output as well as providing more useful logs and error messages.
`).
		Field(service.NewStringField("node_name").Optional()).
		Field(service.NewStringField("node_port").Optional()).
		Field(service.NewStringField("broker_address").Default("localhost:9092")).
		Field(service.NewStringField("input_node_address"))
}

func init() {
	err := service.RegisterBatchOutput("ockam_kafka_franz", ockamFranzKafkaOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			output, err = newOckamKafkaFranzOutput(conf, mgr.Logger())
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type ockamFranzKafkaOutput struct {
	*kafka.FranzKafkaWriter
	node Node
}

func newOckamKafkaFranzOutput(conf *service.ParsedConfig, log *service.Logger) (*ockamFranzKafkaOutput, error) {
	// Create a franzKafkaWriter instance that the ockamFranzKafkaOutput will use
	franzKafkaWriter, err := kafka.NewFranzKafkaWriterFromConfig(conf, log)
	if err != nil {
		return nil, err
	}

	nodeName, err := conf.FieldString("node_name")
	if err != nil {
		nodeName = "kafka-output-" + strconv.Itoa(rand.Int())
	}

	nodePort, err := conf.FieldString("node_port")
	if err != nil {
		// If the node_port is not set, find a free port
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, err
		}
		nodePort = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
		_ = listener.Close()
	}

	// Use the first "seed_brokers" field item as the inlet address
	seedBrokers, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	if len(seedBrokers) > 1 {
		return nil, errors.New("seed_brokers must contain only one broker")
	}
	kafkaInletAddress := strings.Split(seedBrokers[0], ",")[0]

	bootstrapServer, err := conf.FieldString("broker_address")
	if err != nil {
		return nil, err
	}

	kafkaInputNodeAddress, err := conf.FieldString("input_node_address")
	if err != nil {
		return nil, err
	}

	_, tls, err := conf.FieldTLSToggled("tls")
	if err != nil {
		tls = false
	}

	nodeConfig := "{" +
		"ticket: \"/Users/adrian/projects/ockam/benthos/internal/impl/ockam/test/output.ticket\", " +
		"name: " + nodeName + ", " +
		"tcp-listener-address: 0.0.0.0:" + nodePort + ", " +
		"kafka-inlet: [{" +
		"from: " + kafkaInletAddress + ", " +
		"to: /secure/api," +
		"consumer: " + kafkaInputNodeAddress + ", " +
		"avoid-publishing: true}], " +
		"kafka-outlet: [{" +
		"bootstrap-server: " + bootstrapServer + ", " +
		"tls: " + strconv.FormatBool(tls) + "}]" +
		"}"
	node := Node{Name: nodeName, Config: nodeConfig, Log: log}
	err = node.Create()
	if err != nil {
		return nil, err
	}
	return &ockamFranzKafkaOutput{
		franzKafkaWriter,
		node,
	}, nil
}

func (k *ockamFranzKafkaOutput) Connect(ctx context.Context) error {
	return k.FranzKafkaWriter.Connect(ctx)
}

func (k *ockamFranzKafkaOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	return k.FranzKafkaWriter.WriteBatch(ctx, batch)
}

func (k *ockamFranzKafkaOutput) Close(ctx context.Context) error {
	err1 := k.FranzKafkaWriter.Close(ctx)
	err2 := k.node.Delete()
	err := errors.Join(err1, err2)
	return err
}
