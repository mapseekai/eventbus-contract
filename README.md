# eventbus-contract

Object-safe event-bus contract for Rust, with a Redis Streams backend and an
in-process backend for tests.

Minimum supported Rust version: **1.86** (checked in CI with all features and targets).

## Documentation

The project guide is published at
[mapseekai.github.io/eventbus-contract](https://mapseekai.github.io/eventbus-contract/).
Use it for installation, backend guides, delivery semantics, configuration
constraints, and contributor documentation. Exact API signatures remain on
[docs.rs](https://docs.rs/eventbus-contract/0.2.3/eventbus_contract/).

```toml
[dependencies]
eventbus-contract = { version = "0.2", features = ["redis"] }
```

Enable Watermill Redis Stream compatibility through the facade when consuming
Go Watermill streams:

```toml
[dependencies]
eventbus-contract = { version = "0.2.3", features = ["redis-watermill"] }
```

```rust
use eventbus_contract::prelude::*;
```

## Workspace layout

| Crate | What it provides | Published in 0.2.x |
|---|---|:---:|
| `eventbus-core` | Object-safe traits (`Publisher`, `Subscriber`, `Handler`, `Delivery`, `DeliveryControl`), value types, generic `StreamBus`. | ✅ |
| `eventbus-memory` | In-process `StreamBackend` behind the default-on `test-utils` feature. | ✅ |
| `eventbus-redis` | Production `RedisBackend` over `XADD` / `XREADGROUP` / `XACK` / `XAUTOCLAIM`, with optional Watermill Redis Stream codecs. | ✅ |
| `eventbus-outbox` | Transactional outbox + dispatcher + dead-letter + idempotency traits. | 🛑 0.3.0 (awaiting reference impl) |
| `eventbus-integration` | DDD integration-event helpers (`IntegrationEvent`, `MessageFactory`, `EventPublisher`). | 🛑 0.3.0 (awaiting reference impl) |
| `eventbus-contract` | Facade — re-exports the published crates behind feature flags. | ✅ |

## Features (facade)

- `memory` (default) — in-process backend for tests / dev.
- `redis` — Redis Streams backend.
- `redis-watermill` - Redis Streams backend plus Watermill canonical entry decoding.
- `tracing` — `tracing` instrumentation on hot paths.

`outbox` and `integration` features are reserved for 0.3.0. The trait crates
live in this workspace today but are not yet published to crates.io because
they have no reference implementation. Once a concrete impl ships
(e.g. `eventbus-postgres-outbox`), the trait crates and their facade
features will land together.

## Quickstart

```rust
use std::sync::Arc;

use eventbus_contract::core::stream::{StreamBus, StreamBusOptions};
use eventbus_contract::memory::MemoryStreamBackend;
use eventbus_contract::prelude::*;

#[tokio::main]
async fn main() -> Result<(), EventBusError> {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default())?;

    struct Echo;
    impl Handler for Echo {
        fn handle(&self, d: Box<dyn DeliveryHandle>)
            -> BoxFuture<'_, Result<(), EventBusError>>
        {
            Box::pin(async move { d.ack().await })
        }
    }

    let cfg = SubscriptionConfig::builder(
        Topic::new("orders")?,
        ConsumerGroup::new("svc-a")?,
    ).max_in_flight(8).build()?;

    let _sub = bus.subscribe(cfg, Echo).await?;
    Ok(())
}
```

## Migrating from `eventbus-contract` 0.1

See [`MIGRATION-0.2.md`](./MIGRATION-0.2.md). Highlights: crate rename,
`Handler` takes `Box<dyn DeliveryHandle>`, `Delivery`/`DeliveryControl` split,
`ack`/`nack`/`retry` consume `Box<Self>`, newtype IDs, builder-driven
`SubscriptionConfig`, `BatchOutcome` from `publish_batch`.

## Changelog

See [`CHANGELOG.md`](./CHANGELOG.md).

## License

MIT.
