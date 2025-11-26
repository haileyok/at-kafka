# at-kafka

A small service that receives events from the AT firehose and produces them to Kafka. Supports standard JSON outputs as well as [Osprey](https://github.com/roostorg/osprey)
formatted events.

## Usage

### Docker Compose

The included `docker-compose.yml` provides a complete local stack. Edit the environment variables in the file to customize:

```yaml
environment:
  ATKAFKA_RELAY_HOST: "wss://bsky.network"
  ATKAFKA_BOOTSTRAP_SERVERS: "kafka:29092"
  ATKAFKA_OUTPUT_TOPIC: "atproto-events"
  ATKAFKA_OSPREY_COMPATIBLE: "false"
```

Then start:

```bash
docker compose up -d        # Start services
```

### Docker Run

For standard mode:

```bash
docker run -d \
  -e ATKAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e ATKAFKA_OUTPUT_TOPIC=atproto-events \
  -p 2112:2112 \
  ghcr.io/haileyok/at-kafka:latest
```

For Osprey-compatible mode:

```bash
docker run -d \
  -e ATKAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e ATKAFKA_OUTPUT_TOPIC=atproto-events \
  -e ATKAFKA_OSPREY_COMPATIBLE=true \
  -p 2112:2112 \
  ghcr.io/haileyok/at-kafka:latest
```

## Configuration

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--relay-host` | `ATKAFKA_RELAY_HOST` | `wss://bsky.network` | AT Protocol relay host to connect to |
| `--bootstrap-servers` | `ATKAFKA_BOOTSTRAP_SERVERS` | (required) | Comma-separated list of Kafka bootstrap servers |
| `--output-topic` | `ATKAFKA_OUTPUT_TOPIC` | (required) | Kafka topic to publish events to |
| `--osprey-compatible` | `ATKAFKA_OSPREY_COMPATIBLE` | `false` | Enable Osprey-compatible event format |

## Event Structure

### Standard Mode

Events are structured similarly to the raw AT Protocol firehose, with one key difference: **commit events are split into individual operation events**.

#### Operation Event
```json
{
  "did": "did:plc:...",
  "timestamp": "2024-01-01T12:00:00.000Z",
  "operation": {
    "action": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "some-rkey",
    "uri": "at://did:plc:123/app.bsky.feed.post/some-rkey",
    "cid": "bafyrei...",
    "path": "app.bsky.feed.post/...",
    "record": {
      "text": "Hello world!",
      "$type": "app.bsky.feed.post",
      "createdAt": "2024-01-01T12:00:00.000Z"
    }
  }
}
```

#### Account Event
```json
{
  "did": "did:plc:...",
  "timestamp": "2024-01-01T12:00:00.000Z",
  "account": {
    "active": true,
    "seq": 12345,
    "status": "active"
  }
}
```

#### Identity Event
```json
{
  "did": "did:plc:...",
  "timestamp": "2024-01-01T12:00:00.000Z",
  "identity": {
    "seq": 12345,
    "handle": "user.bsky.social"
  }
}
```

### Osprey-Compatible Mode

When `--osprey-compatible` is enabled, events are wrapped in the Osprey event format:

```json
{
  "data": {
    "action_name": "operation#create",
    "action_id": 1234567890,
    "data": {
      "did": "did:plc:...",
      "timestamp": "2024-01-01T12:00:00.000Z",
      "operation": { ... }
    },
    "timestamp": "2024-01-01T12:00:00.000Z",
    "secret_data": {},
    "encoding": "UTF8"
  },
  "send_time": "2024-01-01T12:00:00Z"
}
```

Action names in Osprey mode:
- `operation#create` - Record creation
- `operation#update` - Record update
- `operation#delete` - Record deletion
- `account` - Account status changes
- `identity` - Identity/handle changes
- `info` - Informational messages

## Monitoring

The service exposes Prometheus metrics on the default metrics port.

- `atkafka_handled_events` - Total events that are received on the firehose and handled
- `atkafka_produced_events` - Total messages that are output on the bus
