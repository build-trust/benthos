## Temporary stuff

Enroll and create tickets for producer and consumer:

```bash
ockam enroll --authorization-code-flow
ockam project ticket --usage-count 100 --expires-in 5d  > producer.ticket
ockam project ticket --usage-count 100 --expires-in 5d  > consumer.ticket
```

## Compile ockam from latest dev

```
cargo build --bin ockam
sudo cp ./target/debug/ockam /opt/homebrew/bin/ockam
```

## Run each script in separate terminals

```bash
./redpanda.sh
./consumer.sh
./producer.sh
```
