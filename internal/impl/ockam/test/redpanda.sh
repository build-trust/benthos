#!/bin/bash

docker pull vectorized/redpanda:latest
docker network create -d bridge redpandanet
docker run \
  --rm \
  --network redpandanet \
  --name redpanda \
  -p 9092:9092 \
  vectorized/redpanda redpanda start \
  --reserve-memory 0M \
  --overprovisioned \
  --smp 1 \
  --memory 1G \
  --advertise-kafka-addr localhost:9092
