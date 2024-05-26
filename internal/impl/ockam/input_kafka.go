package ockam

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/impl/kafka"
	"github.com/benthosdev/benthos/v4/public/service"
)

func ockamKafkaInputConfig() *service.ConfigSpec {
	return kafka.FranzKafkaInputConfig().
		Summary(`Ockam`).
		Field(service.NewStringField("consumer_identifier")).
		Field(service.NewStringField("producer_identifier")).
		Field(service.NewStringField("ockam_node_address").Optional())
}

// this function is, almost, an exact copy of the init() function in ../kafka/input_kafka_franz.go
func init() {
	err := service.RegisterBatchInput("ockam_kafka", ockamKafkaInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newOckamKafkaInput(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, i.kafkaReader)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type ockamKafkaInput struct {
	kafkaReader *kafka.FranzKafkaReader
	node        Node
}

func newOckamKafkaInput(conf *service.ParsedConfig, mgr *service.Resources) (*ockamKafkaInput, error) {
	kafkaReader, err := kafka.NewFranzKafkaReaderFromConfig(conf, mgr)
	if err != nil {
		return nil, err
	}

	_, tls, err := conf.FieldTLSToggled("tls")
	if err != nil {
		tls = false
	}

	consumerIdentifier, err := conf.FieldString("consumer_identifier")
	if err != nil {
		return nil, err
	}

	producerIdentifier, err := conf.FieldString("producer_identifier")
	if err != nil {
		return nil, err
	}

	nodeAddress, err := conf.FieldString("ockam_node_address")
	if err != nil {
		// Check if the default port 4000 is already in use.
		// If it is, use a random port.
		nodeAddress = "0.0.0.0:4000"
		listener, err := net.Listen("tcp", nodeAddress)
		_ = listener.Close()
		if err != nil {
			listener, err := net.Listen("tcp", "0.0.0.0:0")
			if err != nil {
				return nil, err
			}
			nodeAddress = listener.Addr().String()
		}
	} else {
		// Check if the specified address is already in use.
		listener, err := net.Listen("tcp", nodeAddress)
		_ = listener.Close()
		if err != nil {
			return nil, err
		}
	}
	mgr.Logger().Infof("The Ockam consumer is listening at %v", nodeAddress)

	// Find a free port to use for the kafka-inlet
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	kafkaInletPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	kafkaInletAddress := "127.0.0.1:" + kafkaInletPort
	_ = listener.Close()

	// Override SeedBrokers list that would be used by kafka_franz to be the kafka inlet address
	kafkaReader.SeedBrokers = []string{kafkaInletAddress}

	// Use the first "seed_brokers" field item as the bootstrapServer argument for Ockam.
	seedBrokers, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	bootstrapServer := strings.Split(seedBrokers[0], ",")[0]

	nodeConfig := map[string]interface{}{
		"identity":             "consumer",
		"tcp-listener-address": nodeAddress,
		"kafka-inlet": map[string]interface{}{
			"from":             kafkaInletAddress,
			"to":               "/secure/api",
			"avoid-publishing": true,
			"allow-producer":   "(= subject.identifier \"" + producerIdentifier + "\")",
			"allow":            "(= subject.identifier \"" + consumerIdentifier + "\")"},
		"kafka-outlet": map[string]interface{}{
			"bootstrap-server": bootstrapServer,
			"tls":              tls,
			"allow":            "(= subject.identifier \"" + consumerIdentifier + "\")"}}

	node, err := NewNode(nodeConfig, mgr.Logger())
	if err != nil {
		return nil, err
	}

	return &ockamKafkaInput{kafkaReader, *node}, nil
}

func (o *ockamKafkaInput) Connect(ctx context.Context) error {
	return o.kafkaReader.Connect(ctx)
}

func (o *ockamKafkaInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return o.kafkaReader.ReadBatch(ctx)
}

func (o *ockamKafkaInput) Close(ctx context.Context) error {
	return errors.Join(o.kafkaReader.Close(ctx), o.node.Delete())
}
