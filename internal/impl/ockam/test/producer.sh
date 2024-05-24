#!/bin/bash

docker pull jeffail/benthos:latest

docker run --rm -it \
  --network redpandanet \
  --name benthos_producer \
  -v $(pwd)/producer.yaml:/benthos.yaml \
  jeffail/benthos
