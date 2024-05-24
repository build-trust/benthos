#!/bin/bash

# docker pull jeffail/benthos:latest
# docker run --rm -it \
#   --network redpandanet \
#   --name benthos_consumer \
#   -v $(pwd)/consumer.yaml:/benthos.yaml \
#   jeffail/benthos

docker run --rm \
  --network redpandanet \
  --name benthos_consumer \
  -v $(pwd)/consumer.yaml:/benthos.yaml \
  ghcr.io/benthosdev/benthos
