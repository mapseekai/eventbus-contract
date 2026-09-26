# Changelog

## 0.2.3 - 2026-09-26

- Reconnect Redis command connections created with `from_client` after connection loss.
- Make subscription close cancellation-safe and allow abort during graceful shutdown.
- Reject unsupported `OrderingMode::Key` at subscription time.
- Validate topic, group, and consumer names during deserialization; preserve their string wire format.
- Isolate overflowing delivery counters as malformed entries instead of panicking.
- Preserve Redis delivery counts and first-receipt metadata across backend restarts; atomically
  remove receipt metadata on ACK. Redis deployments now need the scripting/hash ACLs described
  in the Redis crate README.
- Raise the minimum supported Rust version to 1.86 and check it in CI.
- Simplify batch scheduling, ACK draining, and redundant runtime/error wrappers.

## 0.2.2 - 2026-09-08

Patch release focused on subscription lifecycle correctness and Redis
connection isolation.

### Stream subscriptions

- Reworked idle-message reclaiming to respect in-flight capacity and avoid
  reclaim/read races while preserving prompt reclaim under long read waits.
- Reject retry backoffs that would outlast the reclaim idle timeout.
- Ensure consumer resources are cleaned up on close, abort, and background
  task exit, including subscriptions that never reach their first poll.

### Redis

- Added `RedisBackend::from_client` and `stream_bus_from_client`, which lazily
  create dedicated blocking-read connections per consumer so idle readers do
  not block publishing or acknowledgements.
- Added cleanup for per-consumer reader connections and codec registrations.
- Added opt-in Redis connection regressions and a publish-to-ack round-trip
  benchmark.

### Validation

- Added regression coverage for retry timing, reclaim capacity, abandoned
  deliveries, and `Send` guarantees on public async trait methods.
- CI now checks all features, ignored Redis connection regressions, and
  workspace-wide Clippy targets.

## 0.2.1 - 2026-06-12

Patch release focused on Redis Stream interoperability and publish readiness.

### Redis

- Added Redis field-level stream codecs so the Redis backend can read and write
  formats beyond the default eventbus JSON `message` field.
- Added the optional `watermill` feature with `WatermillStreamCodec` for Go
  Watermill Redis Stream entries (`_watermill_message_uuid`, `metadata`,
  `payload`).
- Added stream/group/subscription read codec registration plus stream-level
  write codec registration on `RedisBackend`.
- Added Watermill-aware auto-detection for mixed eventbus JSON and Watermill
  streams.
- Hardened Redis entry decoding by enforcing raw field-size limits while
  converting Redis fields, before handing the field map to codecs.
- Fixed Watermill edge cases: empty `metadata` fields are accepted, typed
  `Message` fields are written into Watermill metadata, and auto-detect can
  continue after one matching codec fails to decode.

### Facade

- Added the `redis-watermill` feature to `eventbus-contract`, forwarding to
  `eventbus-redis/watermill`.

## 0.2.0 — 2026-05-07

Breaking refactor. See [`MIGRATION-0.2.md`](./MIGRATION-0.2.md).

### Architecture

- Workspace split: `eventbus-core` + `eventbus-memory` + `eventbus-redis`
  + `eventbus-outbox` + `eventbus-integration` + `eventbus-contract` (facade).
- Single-crate users should depend on `eventbus-contract` and pick backends
  via feature flags: `memory` (default), `redis`, `tracing`.
- `eventbus-outbox` and `eventbus-integration` are workspace-only in 0.2.0;
  they will be published to crates.io in 0.3.0 alongside a reference
  implementation crate (e.g. `eventbus-postgres-outbox`).

### Public API — breaking

- `Publisher` / `Subscriber` / `Handler` / `Subscription` / `Delivery` /
  `DeliveryInspector` are now object-safe (return `BoxFuture`).
- `PublisherExt::publish_iter` and `SubscriberExt::subscribe_with` provide
  monomorphic shortcuts for code that does not need `dyn`.
- `Delivery` is read-only; new `DeliveryControl` consumes `Box<Self>` for
  `ack` / `nack` / `retry`. `Handler::handle` now receives
  `Box<dyn DeliveryHandle>`.
- `nack` / `retry` reason changed from `&dyn Error` to `BoxedError`.
- New newtype IDs: `Topic`, `ConsumerGroup`, `ConsumerName`, `MessageId`
  — `#[repr(transparent)]`, parse-don't-validate.
- `SubscriptionConfig` fields are private; build via
  `SubscriptionConfig::builder(Topic, ConsumerGroup).build()?`.
- `publish_batch` returns `Ok(BatchOutcome)` with one `Result` per input
  — no fail-fast.
- `AppendRequest` / `OutboxMessageRecord` / `DeadLetterMessageRecord` payload
  is now `bytes::Bytes`.
- `ErrorScope` adds `Drop` and `HandlerPanic`; `ErrorObserver` adds
  `on_panic`.
- `StreamSubscription::abort()` — immediate abort without graceful drain.

### Correctness

- Limiter race fixed: `consume_loop` now acquires permits before fetching
  from the backend so the in-flight cap is never exceeded.
- `EventBusError::source(context, inner)` replaces ad-hoc
  `Internal(format!())` at sites with a causal chain.
- Subscription `Drop` fires `on_error(ErrorScope::Drop, …)` exactly once
  when the subscription was not closed gracefully.
- Per-entry `FetchedEntry::Malformed` reporting prevents poison-pill loops
  on undecodable PEL entries — the bus acks, surfaces to the observer,
  and routes a synthetic envelope to the dead-letter topic.

### Observability

- New `tracing` feature instruments `publish`, `subscribe`, `consume_loop`,
  and `process_single_message`.
- New tests: `crates/eventbus-core/tests/observer.rs`,
  `finalize_typesafe.rs`, `poison_pill.rs`, `subscribe_validation.rs`,
  plus the `dyn_safety` compile-time witness.
- New Criterion bench `crates/eventbus-core/benches/publish.rs` —
  `cargo bench -p eventbus-core --no-run` compiles clean.

## 0.1.x

See git history for `eventbus-contract` 0.1.
