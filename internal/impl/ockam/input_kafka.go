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
		Field(service.NewStringField("ockam_ticket")).
		Field(service.NewStringField("ockam_consumer_port").Optional())
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

	ticket, err := conf.FieldString("ockam_ticket")
	if err != nil {
		return nil, err
	}

	nodePort, err := conf.FieldString("ockam_consumer_port")
	if err != nil {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		nodePort = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
		_ = listener.Close()
	}
	mgr.Logger().Infof("The Ockam consumer is listening on port %v", nodePort)

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

	nodeConfig := `
		{
			"ticket": "` + ticket + `",
			"tcp-listener-address": "0.0.0.0:` + nodePort + `",

			"kafka-inlet": [{
				"from": "` + kafkaInletAddress + `",
				"to": "/secure/api",
				"avoid-publishing": true
			}],

			"kafka-outlet": [{
				"bootstrap-server": "` + bootstrapServer + `",
				"tls": ` + strconv.FormatBool(tls) + `
			}]
		}
	`
	node, err := NewNode(nodeConfig)
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
