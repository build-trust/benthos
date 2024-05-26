package ockam

import (
	"context"
	"net"
	"strconv"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/impl/kafka"
	"github.com/benthosdev/benthos/v4/public/service"
)

func ockamKafkaOutputConfig() *service.ConfigSpec {
	return kafka.FranzKafkaOutputConfig().
		Summary("Ockam").
		Field(service.NewStringField("consumer_identifier").Optional()).
		Field(service.NewStringField("producer_identifier").Optional()).
		Field(service.NewStringField("ockam_route_to_consumer").Optional())
}

// this function is, almost, an exact copy of the init() function in ../kafka/output_kafka_franz.go
func init() {
	err := service.RegisterBatchOutput("ockam_kafka", ockamKafkaOutputConfig(),
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
			output, err = newOckamKafkaOutput(conf, mgr.Logger())
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type ockamKafkaOutput struct {
	kafkaWriter *kafka.FranzKafkaWriter
	node        Node
}

func newOckamKafkaOutput(conf *service.ParsedConfig, log *service.Logger) (*ockamKafkaOutput, error) {
	kafkaWriter, err := kafka.NewFranzKafkaWriterFromConfig(conf, log)
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

	routeToConsumer, err := conf.FieldString("ockam_route_to_consumer")
	if err != nil {
		routeToConsumer = "/dnsaddr/localhost/tcp/4000"
	}

	// Find a free port to use for the kafka-inlet
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	kafkaInletPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	kafkaInletAddress := "127.0.0.1:" + kafkaInletPort
	_ = listener.Close()

	// Override seed_brokers conf that would be used by kafka_franz to be the kafka inlet address
	kafkaWriter.SeedBrokers = []string{kafkaInletAddress}

	// Use the first "seed_brokers" field item as the bootstrapServer argument for Ockam.
	seedBrokers, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	bootstrapServer := strings.Split(seedBrokers[0], ",")[0]

	nodeConfig := map[string]interface{}{
		"identity": "producer",
		"kafka-inlet": map[string]interface{}{
			"from":             kafkaInletAddress,
			"to":               "/secure/api",
			"avoid-publishing": true,
			"consumer":         routeToConsumer,
			"allow-consumer":   "(= subject.identifier \"" + consumerIdentifier + "\")",
			"allow":            "(= subject.identifier \"" + producerIdentifier + "\")"},
		"kafka-outlet": map[string]interface{}{
			"bootstrap-server": bootstrapServer,
			"tls":              tls,
			"allow":            "(= subject.identifier \"" + producerIdentifier + "\")"}}
	node, err := NewNode(nodeConfig, log)
	if err != nil {
		return nil, err
	}

	return &ockamKafkaOutput{kafkaWriter, *node}, nil
}

func (o *ockamKafkaOutput) Connect(ctx context.Context) error {
	return o.kafkaWriter.Connect(ctx)
}

func (o *ockamKafkaOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	return o.kafkaWriter.WriteBatch(ctx, batch)
}

func (o *ockamKafkaOutput) Close(ctx context.Context) error {
	_ = o.node.Delete()
	return o.kafkaWriter.Close(ctx)
}
