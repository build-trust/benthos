package ockam

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterOutput(
		"ockam_kafka_output", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			return &ockamKafkaOutput{}, 1, nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type ockamKafkaOutput struct{}

func (b *ockamKafkaOutput) Connect(ctx context.Context) error {
	return nil
}

func (b *ockamKafkaOutput) Write(ctx context.Context, msg *service.Message) error {
	return nil
}

func (b *ockamKafkaOutput) Close(ctx context.Context) error {
	return nil
}
