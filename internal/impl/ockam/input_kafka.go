package ockam

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/impl/kafka"
	"github.com/benthosdev/benthos/v4/public/service"
)

func ockamKafkaInputConfig() *service.ConfigSpec {
	return kafka.FranzKafkaInputConfig().
		Summary(`Ockam`).
		Field(service.NewStringField("consumer_identifier").Optional()).
		Field(service.NewStringField("producer_identifier").Optional()).
		Field(service.NewStringField("ockam_node_address").Optional().Default("127.0.0.1:4000"))
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

	consumerIdentifier, err := conf.FieldString("consumer_identifier")
	if err != nil {
		consumerIdentifier, err = GetOrCreateIdentifier("consumer")
		if err != nil {
			return nil, err
		}
	}

	producerIdentifier, err := conf.FieldString("producer_identifier")
	if err != nil {
		producerIdentifier, err = GetOrCreateIdentifier("producer")
		if err != nil {
			return nil, err
		}
	}

	_, tls, err := conf.FieldTLSToggled("tls")
	if err != nil {
		tls = false
	}

	nodeAddress, err := conf.FieldString("ockam_node_address")
	if err != nil {
		return nil, err
	}
	reuseNode := LocalAddressIsTaken(nodeAddress)
	mgr.Logger().Infof("The Ockam consumer is listening at %v", nodeAddress)

	var nodeName string
	var kafkaInletAddress string
	if reuseNode {
		mgr.Logger().Infof("Found existing node running at %v", nodeAddress)

		// If the node is already running, point to its kafka inlet address
		name, address, err := GetKafkaInletAddressFrom(nodeAddress)
		if err != nil {
			return nil, err
		}
		nodeName = *name
		kafkaInletAddress = *address
		mgr.Logger().Infof("Reusing the existing Ockam node %v", nodeName)
	} else {
		// Otherwise, find a free port to use for the kafka-inlet
		kafkaInletAddress, err = GetFreeLocalAddress()
		if err != nil {
			return nil, err
		}
	}

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

	var node *Node
	if reuseNode {
		config, _ := json.Marshal(nodeConfig)
		node = &Node{GetOckamBin(), nodeName, string(config)}
	} else {
		node, err = NewNode(nodeConfig, mgr.Logger())
		if err != nil {
			return nil, err
		}
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
	_ = o.node.Delete()
	return o.kafkaReader.Close(ctx)
}
