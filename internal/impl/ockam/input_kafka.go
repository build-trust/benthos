package ockam

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/impl/kafka"
	"github.com/benthosdev/benthos/v4/public/service"
)

// this function is, almost, an exact copy of the init() function in ../kafka/input_kafka_franz.go
func init() {
	err := service.RegisterBatchInput("ockam_kafka", ockamKafkaInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newOckamKafkaInput(conf, mgr, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, i)
		})
	if err != nil {
		panic(err)
	}
}

func ockamKafkaInputConfig() *service.ConfigSpec {
	return kafka.FranzKafkaInputConfig().
		Summary(`Ockam`).
		Field(service.NewStringField("ockam_identity_name").Optional()).
		Field(service.NewStringField("ockam_node_address").Default("127.0.0.1:6262")).
		Field(service.NewStringField("ockam_allow_producer").Default("self")).
		Field(service.NewStringField("ockam_enrollment_ticket").Optional()).
		Field(service.NewStringField("ockam_relay").Optional()).
		Field(service.NewStringListField("seed_brokers").
			Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"}).Default(GetRpkBrokersList()))
}

// Return the list of brokers if the RPK_BROKERS environment variable is defined
func GetRpkBrokersList() []string {
	brokers := os.Getenv("RPK_BROKERS")
	if brokers == "" {
		return []string{"localhost:9092"}
	}
	return strings.Split(brokers, ",")
}

//------------------------------------------------------------------------------

type ockamKafkaInput struct {
	node        node
	kafkaReader *kafka.FranzKafkaReader
}

func newOckamKafkaInput(conf *service.ParsedConfig, mgr *service.Resources, log *service.Logger) (*ockamKafkaInput, error) {
	_, err := setupCommand()
	if err != nil {
		return nil, err
	}

	// --- Create Ockam Node ----

	var ticket string
	if conf.Contains("ockam_enrollment_ticket") {
		ticket, err = conf.FieldString("ockam_enrollment_ticket")
		if err != nil {
			return nil, err
		}
	}

	var relay string
	if conf.Contains("ockam_relay") {
		relay, err = conf.FieldString("ockam_relay")
		if err != nil {
			return nil, err
		}
	}

	var identityName string
	if conf.Contains("ockam_identity_name") {
		identityName, err = conf.FieldString("ockam_identity_name")
		if err != nil {
			return nil, err
		}
	}

	address, err := conf.FieldString("ockam_node_address")
	if err != nil {
		return nil, err
	}
	if localTCPAddressIsTaken(address) {
		return nil, errors.New("ockam_node_address '" + address + "' is already in use")
	}

	n, err := newNode(identityName, address, ticket, relay)
	if err != nil {
		return nil, err
	}

	// --- Create Ockam Kafka Inlet ----

	allowProducer, err := conf.FieldString("ockam_allow_producer")
	if err != nil {
		return nil, err
	}

	kafkaInletAddress, err := findAvailableLocalTCPAddress()
	if err != nil {
		return nil, err
	}
	err = n.createKafkaInlet("benthos-kafka-inlet", kafkaInletAddress, "self", true, "self", "self", allowProducer, "")
	if err != nil {
		return nil, err
	}

	// ---- Create Ockam Kafka Outlet ----

	_, tls, err := conf.FieldTLSToggled("tls")
	if err != nil {
		tls = false
	}
	// TODO: Handle other tls fields in kafka franz

	kafkaReader, err := kafka.NewFranzKafkaReaderFromConfig(conf, mgr)
	if err != nil {
		return nil, err
	}

	// Use the first "seed_brokers" field item as the bootstrapServer argument for Ockam.
	seedBrokers, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	if len(seedBrokers) > 1 {
		mgr.Logger().Warn("ockam_kafka input only supports one seed broker")
	}
	// If the environment variable is defined, use that definition instead
	if GetRpkBrokersList() != nil {
		seedBrokers = GetRpkBrokersList()
	}

	bootstrapServer := strings.Split(seedBrokers[0], ",")[0]
	log.Info("bootstrap server " + bootstrapServer)
	// TODO: Handle more that one seed brokers

	kafkaOutletName := "benthos-kafka-outlet"
	err = n.createKafkaOutlet(kafkaOutletName, bootstrapServer, tls, "self")
	if err != nil {
		return nil, err
	}

	// Override the list of SeedBrokers that would be used by kafka_franz, set it to the address of the kafka inlet
	kafkaReader.SeedBrokers = []string{kafkaInletAddress}
	return &ockamKafkaInput{*n, kafkaReader}, nil
}

func (o *ockamKafkaInput) Connect(ctx context.Context) error {
	return o.kafkaReader.Connect(ctx)
}

func (o *ockamKafkaInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return o.kafkaReader.ReadBatch(ctx)
}

func (o *ockamKafkaInput) Close(ctx context.Context) error {
	return errors.Join(o.kafkaReader.Close(ctx), o.node.delete())
}
