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

func ockamKafkaOutputConfig() *service.ConfigSpec {
	return kafka.FranzKafkaOutputConfig().
		Summary("Ockam").
		Field(service.NewStringField("ockam_ticket")).
		Field(service.NewStringField("ockam_route_to_consumer"))
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

	_, tls, err := conf.FieldTLSToggled("tls")
	if err != nil {
		tls = false
	}

	ticket, err := conf.FieldString("ockam_ticket")
	if err != nil {
		return nil, err
	}

	routeToConsumer, err := conf.FieldString("ockam_route_to_consumer")
	if err != nil {
		return nil, err
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

	nodeConfig := `
		{
			"ticket": "` + ticket + `",

			"kafka-inlet": [{
				"from": "` + kafkaInletAddress + `",
				"to": "/secure/api",
				"consumer": "` + routeToConsumer + `",
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

	return &ockamKafkaOutput{kafkaWriter, *node}, nil
}

func (o *ockamKafkaOutput) Connect(ctx context.Context) error {
	return o.kafkaWriter.Connect(ctx)
}

func (o *ockamKafkaOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	return o.kafkaWriter.WriteBatch(ctx, batch)
}

func (o *ockamKafkaOutput) Close(ctx context.Context) error {
	return errors.Join(o.kafkaWriter.Close(ctx), o.node.Delete())
}
