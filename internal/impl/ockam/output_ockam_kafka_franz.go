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
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("3.61.0").
		Summary("Am Ockam wrapper around Kafka output using the [Franz Kafka client library](https://github.com/twmb/franz-go).").
		Description(`
Writes a batch of messages to Kafka brokers and waits for acknowledgement before propagating it back to the input.

This output often out-performs the traditional ` + "`kafka`" + ` output as well as providing more useful logs and error messages.
`).
		Field(service.NewStringField("node_name").Optional()).
		Field(service.NewStringField("node_port").Optional()).
		Field(service.NewStringField("inlet_address").Optional()).
		Field(service.NewStringField("output_node_address")).
		Field(service.NewStringListField("seed_brokers").
			Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"})).
		Field(service.NewInterpolatedStringField("topic").
			Description("A topic to write messages to.")).
		Field(service.NewInterpolatedStringField("key").
			Description("An optional key to populate for each message.").Optional()).
		Field(service.NewStringAnnotatedEnumField("partitioner", map[string]string{
			"murmur2_hash": "Kafka's default hash algorithm that uses a 32-bit murmur2 hash of the key to compute which partition the record will be on.",
			"round_robin":  "Round-robin's messages through all available partitions. This algorithm has lower throughput and causes higher CPU load on brokers, but can be useful if you want to ensure an even distribution of records to partitions.",
			"least_backup": "Chooses the least backed up partition (the partition with the fewest amount of buffered records). Partitions are selected per batch.",
			"manual":       "Manually select a partition for each message, requires the field `partition` to be specified.",
		}).
			Description("Override the default murmur2 hashing partitioner.").
			Advanced().Optional()).
		Field(service.NewInterpolatedStringField("partition").
			Description("An optional explicit partition to set for each message. This field is only relevant when the `partitioner` is set to `manual`. The provided interpolation string must be a valid integer.").
			Example(`${! meta("partition") }`).
			Optional()).
		Field(service.NewStringField("client_id").
			Description("An identifier for the client connection.").
			Default("benthos").
			Advanced()).
		Field(service.NewStringField("rack_id").
			Description("A rack identifier for this client.").
			Default("").
			Advanced()).
		Field(service.NewBoolField("idempotent_write").
			Description("Enable the idempotent write producer option. This requires the `IDEMPOTENT_WRITE` permission on `CLUSTER` and can be disabled if this permission is not available.").
			Default(true).
			Advanced()).
		Field(service.NewMetadataFilterField("metadata").
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of batches to be sending in parallel at any given time.").
			Default(10)).
		Field(service.NewDurationField("timeout").
			Description("The maximum period of time to wait for message sends before abandoning the request and retrying").
			Default("10s").
			Advanced()).
		Field(service.NewBatchPolicyField("batching")).
		Field(service.NewStringField("max_message_bytes").
			Description("The maximum space in bytes than an individual message may take, messages larger than this value will be rejected. This field corresponds to Kafka's `max.message.bytes`.").
			Advanced().
			Default("1MB").
			Example("100MB").
			Example("50mib")).
		Field(service.NewStringEnumField("compression", "lz4", "snappy", "gzip", "none", "zstd").
			Description("Optionally set an explicit compression type. The default preference is to use snappy when the broker supports it, and fall back to none if not.").
			Optional().
			Advanced()).
		Field(service.NewTLSToggledField("tls")).
		Field(kafka.SaslField()).
		LintRule(`
root = if this.partitioner == "manual" {
  if this.partition.or("") == "" {
    "a partition must be specified when the partitioner is set to manual"
  }
} else if this.partition.or("") != "" {
  "a partition cannot be specified unless the partitioner is set to manual"
}`)
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
			return nil, err
		}
		port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
		kafkaInletAddress = "0.0.0.0:" + port
		_ = listener.Close()
	}

	kafkaOutputNodeAddress, err := conf.FieldString("output_node_address")
	if err != nil {
		return nil, err
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
		"to: self," +
		"consumer: " + kafkaOutputNodeAddress + "," +
		"avoid-publishing: true}," +
		"kafka-outlet: {" +
		"bootstrap-server:" + bootstrapServer + "," +
		"tls:" + strconv.FormatBool(tls) + "}" +
		"}"
	node := Node{Name: nodeName, Config: nodeConfig}
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
	if !k.node.IsRunning() {
		return errors.New("the Ockam Node is not running")
	}
	return k.FranzKafkaWriter.Connect(ctx)
}
