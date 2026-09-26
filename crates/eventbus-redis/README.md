# eventbus-redis

[![Crates.io](https://img.shields.io/crates/v/eventbus-redis.svg)](https://crates.io/crates/eventbus-redis)
[![Docs.rs](https://docs.rs/eventbus-redis/badge.svg)](https://docs.rs/eventbus-redis)

Redis Streams `StreamBackend` for the [eventbus-contract](https://crates.io/crates/eventbus-contract) workspace, plus JSON and Redis field-level stream codecs.

## Use via the facade (recommended)

```toml
[dependencies]
eventbus-contract = { version = "0.2", features = ["redis"] }
```

```rust
use eventbus_contract::core::stream::StreamBusOptions;
use eventbus_contract::redis::stream_bus_from_client;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = redis::Client::open("redis://127.0.0.1/")?;
let bus = stream_bus_from_client(client, StreamBusOptions::default()).await?;
# Ok(()) }
```

## Features

- `default = []` - base Redis support over `redis-rs` `tokio-comp`.
- `tls` - enable `rediss://` connections via `redis/tls-native-tls` + `redis/tokio-native-tls-comp`. Use `rediss://` URLs in production and ensure CA validation.
- `watermill` - add `WatermillStreamCodec` and `AutoDetectRedisStreamCodec::with_watermill()` for reading Go Watermill canonical Redis Stream entries.

## Wire format

`JsonCodec` is the default. It encodes each `Message` inside a `{"message":{...}}` envelope stored in the `"message"` field of each Stream entry. Existing Redis users keep this behavior unless they register another stream codec.

For Watermill-produced streams, enable the feature and register the codec per stream:

```toml
[dependencies]
eventbus-redis = { version = "0.2", features = ["watermill"] }
```

```rust
use std::sync::Arc;
use eventbus_redis::{RedisBackend, WatermillStreamCodec};

# fn configure(backend: &RedisBackend) {
backend.set_stream_read_codec("mapset.mosaic", Arc::new(WatermillStreamCodec));
# }
```

Watermill canonical entries are decoded from the whole Redis field map:

```text
_watermill_message_uuid = raw UTF-8 string
metadata                 = msgpack map<string,string>
payload                  = raw bytes
```

`_watermill_message_uuid` becomes `Message.uid` and the fallback idempotency key, `metadata` becomes `Message.headers`, `payload` becomes `Message.payload` without base64 or JSON decoding, and the subscribed Redis stream name becomes `Message.topic`. Empty `metadata` fields are accepted as empty metadata. When writing with `WatermillStreamCodec`, typed `Message` fields such as `content_type`, `event_version`, and `idempotency_key` are copied into metadata unless an explicit header already exists.

For mixed streams where the read side should accept either eventbus JSON entries or Watermill entries, opt in per stream:

```rust
# use eventbus_redis::RedisBackend;
# fn configure(backend: &RedisBackend) {
backend.set_auto_detect_read_stream("mapset.mixed");
# }
```

Auto-detect is read-only. It tries matching codecs in order and can continue to later codecs if an earlier matching codec fails to decode. Writes still use the stream write codec, defaulting to the eventbus JSON `"message"` field.

## Connection / TLS / auth

`RedisBackend::from_client(client).await` and `stream_bus_from_client(client, options).await`
create a reconnecting command connection and lazily open a dedicated blocking-read connection for each
`(stream, group, consumer)`. Idle readers cannot hold up publishing, ACKs, or other consumers.
The client selects the URL, database, TLS and authentication settings.
Commands interrupted by a disconnect return an error; subsequent calls use the reconnected
transport. Failed publishes are not automatically replayed because the server may already
have appended them. Application retries still require idempotent consumers.

`RedisBackend::new(connection)`, `with_codec(connection, codec)` and
`stream_bus_from_connection` remain available. Since a cloned `MultiplexedConnection`
shares its socket, these constructors use nonblocking reads with a 10 ms polling interval.
For a custom command connection or codec, use `.with_read_client(client)` to enable
dedicated readers; the client must address the same Redis server and database.
Externally supplied command connections are not automatically reconnected; use the client
constructor when the backend should own connection recovery.

Subscription close, abort and task exit release its read connection, reclaim cursor and
subscription-specific codec registration. Stream/group codec registrations persist.
`remove_subscription_read_codec(stream, group, consumer)` explicitly removes an override
and restores normal codec fallback. Re-register subscription overrides before reusing a
consumer name after closing it.

`close()` and `abort()` wait for backend cleanup. Finish them before shutting down
the Tokio runtime; asynchronous cleanup cannot run after the runtime has stopped.

The crate does not require, default to, or downgrade TLS.

## Delivery state

`attempt` combines the envelope's retry count with the current entry's Redis PEL delivery
count. Reclaiming an unacknowledged entry therefore consumes another attempt, including
across consumer/backend restarts. `first_received` is stored in a per-stream/group Redis
hash; `last_received` comes from the PEL idle time and Redis server clock.

The metadata key is `eventbus:received:<stream-byte-length>:<stream>:<group-byte-length>:<group>`.
This namespace is reserved for the backend. ACK removes the corresponding metadata in the
same Lua script; reclaim also removes metadata for pending entries deleted from the stream
(Redis 7+). If an administrator deletes a whole stream or consumer group externally, they
must also delete its metadata hash. Do not give the hash a shorter lifetime than pending entries.

Redis does not retain the first-delivery timestamp itself. For legacy pending entries, or a
crash between delivery and the first metadata write, the first receipt available to this
backend is used as the initial timestamp; subsequent reclaims preserve it.

In addition to stream commands, Redis ACLs must allow scripting (`EVALSHA`, `SCRIPT LOAD`)
and the script commands `TIME`, `XPENDING`, `HSET`, `HSETNX`, `HGET`, and `HDEL` on the
metadata namespace. This backend uses a standalone `redis::Client`; it does not provide
Redis Cluster routing.

## Performance checks

Use a disposable Redis instance for the opt-in integration checks and round-trip benchmark:

```sh
EVENTBUS_REDIS_URL=redis://127.0.0.1:6379/ cargo test -p eventbus-redis --test connections -- --ignored
EVENTBUS_REDIS_URL=redis://127.0.0.1:6379/ cargo bench -p eventbus-redis --bench roundtrip
```

The benchmark measures publish-to-completed-ACK latency using the default subscription
and ACK settings. It deletes each acknowledged entry outside the timed interval, so stream
history does not grow with the number of iterations. It measures latency, not maximum
concurrent throughput.

## License

MIT
