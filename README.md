# at-kafka

A small service that receives events from the AT firehose and produces them to Kafka. Supports standard JSON outputs as well as [Osprey](https://github.com/roostorg/osprey)
formatted events.

Additionally, at-kafka supports subscribing to [Tap](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) if youare attempting to perform a network backfill.

## Usage

### Docker Compose

The included `docker-compose.yml` provides a complete local stack. Edit the environment variables in the file to customize:

```yaml
environment:
  # For relay mode
  ATKAFKA_RELAY_HOST: "wss://bsky.network" # ATProto relay to subscribe to for events

  # For tap mode
  ATKAFKA_TAP_HOST: "ws://localhost:2480" # Tap websocket host to subscribe to for events
  ATKAFKA_DISABLE_ACKS: false # Whether to disable sending of acks to Tap

  # Kafka configuration
  ATKAFKA_BOOTSTRAP_SERVERS: "kafka:29092" # Kafka bootstrap servers, comma separated
  ATKAFKA_OUTPUT_TOPIC: "atproto-events" # The output topic for events
  ATKAFKA_OSPREY_COMPATIBLE: false # Whether to produce Osprey-compatible events

  # Match only Blacksky PDS users
  ATKAFKA_MATCHED_SERVICES: "blacksky.app" # A comma-separated list of PDSes to emit events for
  # OR ignore anyone on Bluesky PBC PDSes
  ATKAFKA_IGNORED_SERVICES: "*.bsky.network" # OR a comma-separated list of PDSes to _not_ emit events for

  # Match only Teal.fm records
  ATKAFKA_MATCHED_COLLECTIONS: "fm.teal.*" # A comma-separated list of collections to emit events for
  # OR ignore all Bluesky records
  ATKAFKA_IGNORED_COLLECTIONS: "app.bsky.*" # OR a comma-separated list of collections to ignore events for
```

Then start:

```bash
# For normal mode
docker compose up -d

# For tap mode
docker compose -f docker-compose.tap.yml up -d

```

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
- `atkafka_plc_requests` - Total number of PLC requests that were made, if applicable, and whether they were cached
- `atkafka_api_requests` - Total number of API requests that were made, if applicable, and whether they were cached
- `atkafka_cache_size` - The size of the PLC and API caches
- `atkafka_acks_sent` - Total acks that were sent to Tap, if applicable
