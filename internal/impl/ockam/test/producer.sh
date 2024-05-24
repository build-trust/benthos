#!/bin/bash

# Official image
# docker pull jeffail/benthos:latest
# docker run --rm -it \
#   --network redpandanet \
#   --name benthos_producer \
#   -v $(pwd)/producer.yaml:/benthos.yaml \
#   jeffail/benthos

## Local image
# docker run --rm \
#   --network redpandanet \
#   --name benthos_producer \
#   -v $(pwd)/producer.yaml:/benthos.yaml \
#   local/benthos

## Local dev binary
../../../target/bin/benthos -c producer.yaml
