package ockam

import (
	"context"
	"math/rand"

	"github.com/benthosdev/benthos/v4/public/service"
)

func newOckamKafkaInput(conf *service.ParsedConfig) (service.Input, error) {
	return service.AutoRetryNacks(&ockamKafkaInput{}), nil
}

func init() {
	err := service.RegisterInput(
		"ockam_kafka_input", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newOckamKafkaInput(conf)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type ockamKafkaInput struct{}

func (g *ockamKafkaInput) Connect(ctx context.Context) error {
	return nil
}

func (g *ockamKafkaInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	b := make([]byte, 10)
	for k := range b {
		b[k] = byte((rand.Int() % 94) + 32)
	}
	return service.NewMessage(b), func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

func (g *ockamKafkaInput) Close(ctx context.Context) error {
	return nil
}
