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
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary(`An Ockam wrapper around Kafka input using the [Franz Kafka client library](https://github.com/twmb/franz-go).`).
		Field(service.NewStringField("node_name").Optional()).
		Field(service.NewStringField("node_port").Optional()).
		Field(service.NewStringField("inlet_address").Optional()).
		Field(service.NewStringListField("seed_brokers").
			Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"})).
		Field(service.NewStringListField("topics").
			Description(`
A list of topics to consume from. Multiple comma separated topics can be listed in a single element. When a ` + "`consumer_group`" + ` is specified partitions are automatically distributed across consumers of a topic, otherwise all partitions are consumed.

Alternatively, it's possible to specify explicit partitions to consume from with a colon after the topic name, e.g. ` + "`foo:0`" + ` would consume the partition 0 of the topic foo. This syntax supports ranges, e.g. ` + "`foo:0-10`" + ` would consume partitions 0 through to 10 inclusive.

Finally, it's also possible to specify an explicit offset to consume from by adding another colon after the partition, e.g. ` + "`foo:0:10`" + ` would consume the partition 0 of the topic foo starting from the offset 10. If the offset is not present (or remains unspecified) then the field ` + "`start_from_oldest`" + ` determines which offset to start from.`).
			Example([]string{"foo", "bar"}).
			Example([]string{"things.*"}).
			Example([]string{"foo,bar"}).
			Example([]string{"foo:0", "bar:1", "bar:3"}).
			Example([]string{"foo:0,bar:1,bar:3"}).
			Example([]string{"foo:0-5"})).
		Field(service.NewBoolField("regexp_topics").
			Description("Whether listed topics should be interpreted as regular expression patterns for matching multiple topics. When topics are specified with explicit partitions this field must remain set to `false`.").
			Default(false)).
		Field(service.NewStringField("consumer_group").
			Description("An optional consumer group to consume as. When specified the partitions of specified topics are automatically distributed across consumers sharing a consumer group, and partition offsets are automatically committed and resumed under this name. Consumer groups are not supported when specifying explicit partitions to consume from in the `topics` field.").
			Optional()).
		Field(service.NewStringField("client_id").
			Description("An identifier for the client connection.").
			Default("benthos").
			Advanced()).
		Field(service.NewStringField("rack_id").
			Description("A rack identifier for this client.").
			Default("").
			Advanced()).
		Field(service.NewIntField("checkpoint_limit").
			Description("Determines how many messages of the same partition can be processed in parallel before applying back pressure. When a message of a given offset is delivered to the output the offset is only allowed to be committed when all messages of prior offsets have also been delivered, this ensures at-least-once delivery guarantees. However, this mechanism also increases the likelihood of duplicates in the event of crashes or server faults, reducing the checkpoint limit will mitigate this.").
			Default(1024).
			Advanced()).
		Field(service.NewAutoRetryNacksToggleField()).
		Field(service.NewDurationField("commit_period").
			Description("The period of time between each commit of the current partition offsets. Offsets are always committed during shutdown.").
			Default("5s").
			Advanced()).
		Field(service.NewBoolField("start_from_oldest").
			Description("Determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset. The setting is applied when creating a new consumer group or the saved offset no longer exists.").
			Default(true).
			Advanced()).
		Field(service.NewTLSToggledField("tls")).
		Field(kafka.SaslField()).
		Field(service.NewBoolField("multi_header").Description("Decode headers into lists to allow handling of multiple values with the same key").Default(false).Advanced()).
		Field(service.NewBatchPolicyField("batching").
			Description("Allows you to configure a [batching policy](/docs/configuration/batching) that applies to individual topic partitions in order to batch messages together before flushing them for processing. Batching can be beneficial for performance as well as useful for windowed processing, and doing so this way preserves the ordering of topic partitions.").
			Advanced()).
		LintRule(`
let has_topic_partitions = this.topics.any(t -> t.contains(":"))
root = if $has_topic_partitions {
  if this.consumer_group.or("") != "" {
    "this input does not support both a consumer group and explicit topic partitions"
  } else if this.regexp_topics {
    "this input does not support both regular expression topics and explicit topic partitions"
  }
}
`)
}

func newOckamFranzKafkaInput(conf *service.ParsedConfig, mgr *service.Resources) (*ockamFranzKafkaInput, error) {
	// Create a kafkaReader instance that the ockamFranzKafkaInput will use
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
			panic(err)
		}
		nodePort = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
		_ = listener.Close()
	}

	// Use the first "seed_brokers" field item as the bootstrap server
	seedBrokers, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	bootstrapServer := strings.Split(seedBrokers[0], ",")[0]

	kafkaInletAddress, err := conf.FieldString("inlet_address")
	if err != nil {
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			panic(err)
		}
		port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
		kafkaInletAddress = "0.0.0.0:" + port
		_ = listener.Close()
	}

	_, tls, err := conf.FieldTLSToggled("tls")
	if err != nil {
		tls = false
	}

	nodeConfig := "{" +
		"name:" + nodeName + "," +
		"tcp-listener-address: 0.0.0.0:" + nodePort + "," +
		"kafka-inlet: {" +
		"from: " + kafkaInletAddress + "," +
		"to: self" +
		"avoid-publishing: true}" +
		"kafka-outlet: {" +
		"bootstrap-server:" + bootstrapServer + "," +
		"tls:" + strconv.FormatBool(tls) + "}" +
		"}"
	node := Node{Name: nodeName, Config: nodeConfig}
	err = node.Create()
	if err != nil {
		return nil, err
	}
	return &ockamFranzKafkaInput{
		franzKafkaReader,
		node,
	}, nil
}

func init() {
	err := service.RegisterBatchInput(
		"ockam_kafka_franz", ockamFranzKafkaInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newOckamFranzKafkaInput(conf, mgr)
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

type ockamFranzKafkaInput struct {
	*kafka.FranzKafkaReader
	node Node
}

func (k *ockamFranzKafkaInput) Connect(ctx context.Context) error {
	if !k.node.IsRunning() {
		return errors.New("the Ockam Node is not running")
	}
	return k.FranzKafkaReader.Connect(ctx)
}

func (k *ockamFranzKafkaInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return k.FranzKafkaReader.ReadBatch(ctx)
}

func (k *ockamFranzKafkaInput) Close(ctx context.Context) error {
	_ = k.node.Delete()
	return k.FranzKafkaReader.Close(ctx)
}
