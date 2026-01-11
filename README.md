# at-kafka

A small service that receives events from ATProto and produces them to Kafka. Supports:
- Firehose events from relay or [Tap](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) (for network backfill)
- Ozone moderation events
- Standard JSON and [Osprey](https://github.com/roostorg/osprey)-compatible event formats

## Usage

### Docker Compose

Three compose files are provided:

```bash
# Firehose relay mode (default)
docker compose up -d

# Firehose tap mode (for backfill)
docker compose -f docker-compose.tap.yml up -d

# Ozone moderation events
docker compose -f docker-compose.ozone.yml up -d
```

#### Firehose Configuration

```yaml
environment:
  # Relay/Tap connection
  ATKAFKA_RELAY_HOST: "wss://bsky.network"
  ATKAFKA_TAP_HOST: "ws://localhost:2480"
  ATKAFKA_DISABLE_ACKS: false

  # Kafka
  ATKAFKA_BOOTSTRAP_SERVERS: "kafka:29092"
  ATKAFKA_OUTPUT_TOPIC: "atproto-events"
  ATKAFKA_OSPREY_COMPATIBLE: false

  # Filtering
  ATKAFKA_WATCHED_SERVICES: "*.bsky.network"
  ATKAFKA_IGNORED_SERVICES: "blacksky.app"
  ATKAFKA_WATCHED_COLLECTIONS: "app.bsky.*"
  ATKAFKA_IGNORED_COLLECTIONS: "fm.teal.*"
```

#### Ozone Configuration

```yaml
environment:
  # Ozone connection
  ATKAFKA_OZONE_PDS_HOST: "https://pds.example.com"
  ATKAFKA_OZONE_IDENTIFIER: "your.handle"
  ATKAFKA_OZONE_PASSWORD: "password"
  ATKAFKA_OZONE_LABELER_DID: "did:plc:..."

  # Kafka
  ATKAFKA_BOOTSTRAP_SERVERS: "kafka:29092"
  ATKAFKA_OUTPUT_TOPIC: "ozone-events"
```

### CLI

```bash
# Firehose modes
atkafka firehose relay --bootstrap-servers localhost:9092 --output-topic events
atkafka firehose tap --tap-host ws://localhost:2480 --bootstrap-servers localhost:9092 --output-topic events

# Ozone mode
atkafka ozone-events \
  --pds-host https://pds.example.com \
  --identifier admin@example.com \
  --password password \
  --labeler-did did:plc:... \
  --bootstrap-servers localhost:9092 \
  --output-topic ozone-events
```

## Event Structure

### Firehose Events

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

### Ozone Events

Ozone events are produced as-is from the `tools.ozone.moderation.queryEvents` API response. Events include moderation actions, reports, and other moderation activity. The cursor is persisted to disk and automatically resumed on restart.

## Monitoring

The service exposes Prometheus metrics on the default metrics port.

- `atkafka_handled_events` - Total events that are received on the firehose and handled
- `atkafka_produced_events` - Total messages that are output on the bus
- `atkafka_plc_requests` - Total number of PLC requests that were made, if applicable, and whether they were cached
- `atkafka_api_requests` - Total number of API requests that were made, if applicable, and whether they were cached
- `atkafka_cache_size` - The size of the PLC and API caches
- `atkafka_acks_sent` - Total acks that were sent to Tap, if applicable
