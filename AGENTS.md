# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Commands

```bash
# Build
cargo build
cargo build --features redis-backend   # include Redis backend

# Test
cargo test                              # all tests (unit + integration with MemoryStreamBackend)
cargo test --features redis-backend     # also compile redis_backend module
cargo test <test_name>                  # single test by name
cargo test -- --nocapture               # show println! output

# Lint / format
cargo clippy
cargo fmt
```

Integration tests in `tests/` use `MemoryStreamBackend` and require no external services to run.

## Architecture

This is a pure Rust **event bus contract library** (`eventbus-contract`). It defines traits and types for event-driven messaging. The concrete transport is pluggable — only one backend (`RedisStreamBus`) is implemented, with a `MemoryStreamBackend` for testing.

### Layer 1 — Core traits (`src/eventbus/mod.rs`)

The public API surface. Implement these traits to build publishers, consumers, and handlers:

- `Publisher` — `publish` / `publish_batch`
- `Subscriber` — `subscribe(cfg, handler) -> Subscription`
- `Handler` — `handle(delivery)` — user-supplied message processor
- `Delivery` — wraps a received `Message`; exposes `ack` / `nack` / `retry`
- `Bus = Publisher + Subscriber` (blanket impl)
- `Codec` — pluggable serialization (serialize/deserialize)

`Message` is the canonical envelope: `uid`, `topic`, `key`, `kind`, `source`, `occurred_at`, `headers`, `payload` (bytes), plus optional trace/idempotency/expiry fields.

`SubscriptionConfig` drives consumer behaviour: call `normalize_and_validate()` before use — it fills defaults then checks consistency.

### Layer 2 — Contract types (`src/contract/mod.rs`)

Value objects and policies with validation:

- `DeliveryGuarantee`: `AtMostOnce | AtLeastOnce | ExactlyOnce`
- `AckMode`: `Manual | AutoOnReceive | AutoOnHandlerSuccess`
- `OrderingMode`: `None | Key`
- `ConsumerBalanceMode`: `Competing | FanOut`
- `BackpressurePolicy`: `max_in_flight`, `max_pending_acks`, `max_batch_size`, `overflow_strategy`

### Layer 3 — Redis Stream backend (`src/redis_stream/`)

`RedisStreamBus<B: StreamBackend>` implements `Publisher + Subscriber`. The `StreamBackend` trait decouples the bus logic from the actual stream store:

- `MemoryStreamBackend` — in-process, used by all integration tests
- `RedisBackend` — real Redis Streams, compiled only with `--features redis-backend`

`RedisStreamBusOptions` configures blocking poll timeouts, idle-claim timeouts, and consumer group start position.

### Supporting modules

| Module | Purpose |
|---|---|
| `outbox/` | `OutboxStore` trait + `OutboxStatus` state machine (`Pending → Processing → Sent/Failed/Dead`). Used for transactional outbox pattern. |
| `outbox/dead_letter.rs` | `DeadLetterStore`, `DeadLetterPolicy`, `DeadLetterDecision` — configurable handling of exhausted retries |
| `idempotency/` | `IdempotencyStore` (simple dedup) and `IdempotencyClaimStore` (lease-based dedup) traits |
| `integration/` | `IntegrationEvent` + `MessageFactory` + `EventPublisher` — thin DDD integration-event helpers |
| `dispatcher/` | `Dispatcher` / `Notifier` / `Listener` traits for outbox-relay workers |
| `message_contract/` | Standard header constants (`HEADER_TRACE_PARENT`, `HEADER_IDEMPOTENCY_KEY`, etc.) and `TraceContext` / `SchemaDescriptor` |
| `delivery_contract/` | `DeliveryInspector`, `DeliveryOutcome`, `DeliveryState` — delivery inspection hooks |
| `consumer/` | `ConsumerMessageRecord` — record type for consumer-side tracking |
| `serde_bytes.rs` | Custom serde module for `Vec<u8>` — serializes payload as base64 string |

## Key Constraints

- `ExactlyOnce` delivery guarantee requires `PublishConfirmation::Persisted` — enforced by `PublishOptions::validate()` and `GuaranteeMatrix::validate()`.
- `BackpressurePolicy` requires `max_pending_acks >= max_in_flight > 0`.
- `SubscriptionConfig` fields `max_in_flight` / `max_pending_acks` must agree with `backpressure` when both are set.
- Traits use `async_fn_in_trait` (allowed via `#![allow(async_fn_in_trait)]`) — no `async-trait` crate.


