#!/bin/bash

## Official image
# docker pull jeffail/benthos:latest
# docker run --rm -it \
#   --network redpandanet \
#   --name benthos_consumer \
#   -v $(pwd)/consumer.yaml:/benthos.yaml \
#   jeffail/benthos

## Local image
# docker run --rm \
#   --network redpandanet \
#   --name benthos_consumer \
#   -v $(pwd)/consumer.yaml:/benthos.yaml \
#   local/benthos

## Local dev binary
../../../target/bin/benthos -c consumer.yaml
