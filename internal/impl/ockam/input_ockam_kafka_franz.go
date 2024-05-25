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

func ockamFranzKafkaInputConfig() *service.ConfigSpec {
	return kafka.FranzKafkaInputConfig().
		Summary(`An Ockam wrapper around Kafka input using the [Franz Kafka client library](https://github.com/twmb/franz-go).`).
		Field(service.NewStringField("node_name").Optional()).
		Field(service.NewStringField("node_port").Optional()).
		Field(service.NewStringField("broker_address").Default("localhost:9092"))
}

func init() {
	err := service.RegisterBatchInput(
		"ockam_kafka_franz", ockamFranzKafkaInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newOckamKafkaFranzInput(conf, mgr)
			if err != nil {
				return nil, err
			}

			r, err := service.AutoRetryNacksBatchedToggled(conf, i)
			if err != nil {
				return nil, err
			}

			return conf.WrapBatchInputExtractTracingSpanMapping("ockam_kafka_franz", r)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func newOckamKafkaFranzInput(conf *service.ParsedConfig, mgr *service.Resources) (*ockamFranzKafkaInput, error) {
	// Create a franzKafkaReader instance that the ockamFranzKafkaInput will use
	franzKafkaReader, err := kafka.NewFranzKafkaReaderFromConfig(conf, mgr)
	if err != nil {
		return nil, err
	}

	nodeName, err := conf.FieldString("node_name")
	if err != nil {
		nodeName = "kafka-input-" + strconv.Itoa(rand.Int())
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

	_, tls, err := conf.FieldTLSToggled("tls")
	if err != nil {
		tls = false
	}

	nodeConfig := "{" +
		"ticket: \"/Users/adrian/projects/ockam/benthos/internal/impl/ockam/test/input.ticket\", " +
		"name: " + nodeName + ", " +
		"tcp-listener-address: 0.0.0.0:" + nodePort + ", " +
		"kafka-inlet: [{" +
		"from: " + kafkaInletAddress + ", " +
		"to: /secure/api, " +
		"avoid-publishing: true}], " +
		"kafka-outlet: [{" +
		"bootstrap-server: " + bootstrapServer + ", " +
		"tls: " + strconv.FormatBool(tls) + "}]" +
		"}"
	node := Node{Name: nodeName, Config: nodeConfig, Log: mgr.Logger()}
	err = node.Create()
	if err != nil {
		return nil, err
	}
	return &ockamFranzKafkaInput{
		franzKafkaReader,
		node,
	}, nil
}

type ockamFranzKafkaInput struct {
	*kafka.FranzKafkaReader
	node Node
}

func (k *ockamFranzKafkaInput) Connect(ctx context.Context) error {
	return k.FranzKafkaReader.Connect(ctx)
}

func (k *ockamFranzKafkaInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return k.FranzKafkaReader.ReadBatch(ctx)
}

func (k *ockamFranzKafkaInput) Close(ctx context.Context) error {
	err1 := k.FranzKafkaReader.Close(ctx)
	err2 := k.node.Delete()
	return errors.Join(err1, err2)
}
