//! Redis Stream backend using [`redis-rs`](https://github.com/redis-rs/redis-rs).
//!
//! Enable with the facade's `redis` cargo feature:
//! ```toml
//! eventbus-contract = { version = "0.2", features = ["redis"] }
//! ```
//!
//! Wire format is compatible with the Go `StreamBus` — messages are
//! serialised as JSON inside a `{"message": ...}` envelope stored in the
//! `"message"` field of each Redis Stream entry by default. [`RedisBackend`]
//! also supports field-level codec registration, with reads resolving
//! subscription, then group, then stream, then default, and writes resolving
//! stream, then default.
//! Override the default [`crate::codec::JsonCodec`] via
//! [`RedisBackend::with_codec`] when wire-compat with the Go implementation is
//! not required, or register field-level codecs for specific streams and
//! consumers.
//!
//! # Connection security
//!
//! [`RedisBackend`] accepts a [`redis::Client`] or an already-connected
//! [`MultiplexedConnection`]. The caller chooses the connection URL and TLS / auth
//! settings:
//!
//! - Use a `rediss://` URL (note the double `s`) to negotiate TLS. The
//!   `rustls`/`native-tls` flavours are gated by `redis-rs` features —
//!   pick one in your downstream `Cargo.toml`.
//! - Use a URL of the form `redis://:<password>@host` (or `redis://user:<password>@host`
//!   for ACL) to authenticate.
//!
//! This crate does not require, default to, or downgrade TLS — the
//! [`MultiplexedConnection`] is treated as opaque. Production deployments
//! should connect over `rediss://` and ensure the server certificate is
//! validated against a known CA.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use redis::aio::{ConnectionLike, ConnectionManager, MultiplexedConnection};
use redis::streams::{StreamId, StreamRangeReply, StreamReadReply};
use redis::{FromRedisValue, Value};
use tokio::sync::OnceCell;
use tokio::time::Instant;

use crate::codec::{
    DecodeContext, EncodeContext, EnvelopeStreamCodec, JsonCodec, RedisStreamCodec,
    RedisStreamFields,
};
use eventbus_core::stream::{
    ClaimedMessage, FetchedEntry, StreamBackend, StreamBus, StreamBusOptions,
};
use eventbus_core::{Codec, EventBusError, Message, PartialDeliveryState, HEADER_RETRY_ATTEMPT};

// Keep the existing connection-taking constructors while allowing the client
// constructor to use redis-rs's reconnecting transport.
#[derive(Clone)]
enum CommandConnection {
    Direct(MultiplexedConnection),
    Managed(ConnectionManager),
}

impl ConnectionLike for CommandConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> redis::RedisFuture<'a, Value> {
        match self {
            Self::Direct(conn) => conn.req_packed_command(cmd),
            Self::Managed(conn) => conn.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<Value>> {
        match self {
            Self::Direct(conn) => conn.req_packed_commands(pipeline, offset, count),
            Self::Managed(conn) => conn.req_packed_commands(pipeline, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Direct(conn) => conn.get_db(),
            Self::Managed(conn) => conn.get_db(),
        }
    }
}

/// Pre-decode upper bound on the raw envelope size (8 MiB).
///
/// Stops adversarial / runaway producers from forcing the codec to allocate
/// arbitrarily large structures. Roughly 2× the default 4 MiB payload cap to
/// account for any envelope/encoding overhead (base64 + JSON framing in the
/// default JSON codec).
const MAX_RAW_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const SHARED_READ_POLL_INTERVAL: Duration = Duration::from_millis(10);

// Redis records delivery counts and last-delivery time in the PEL, but not the
// first receipt. Store just that missing fact per group/entry. Reading PEL and
// updating the hash in one script avoids recreating metadata after an ACK.
const DELIVERY_STATE_SCRIPT: &str = r#"
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local states = {}
for i = 3, #ARGV do
    local id = ARGV[i]
    local pending = redis.call('XPENDING', KEYS[1], ARGV[1], id, id, 1)[1]
    if pending then
        local received = string.format('%.0f', now - pending[3])
        if ARGV[2] == 'new' and pending[4] == 1 then
            redis.call('HSET', KEYS[2], id, received)
        else
            redis.call('HSETNX', KEYS[2], id, received)
        end
        states[#states + 1] = {pending[4], redis.call('HGET', KEYS[2], id), received}
    else
        redis.call('HDEL', KEYS[2], id)
        states[#states + 1] = {0, 0, 0}
    end
end
return states
"#;

const ACK_SCRIPT: &str = r#"
local count = 0
for first = 2, #ARGV, 1000 do
    local last = math.min(first + 999, #ARGV)
    count = count + redis.call('XACK', KEYS[1], ARGV[1], unpack(ARGV, first, last))
    redis.call('HDEL', KEYS[2], unpack(ARGV, first, last))
end
return count
"#;

fn delivery_metadata_key(stream: &str, group: &str) -> String {
    format!(
        "eventbus:received:{}:{stream}:{}:{group}",
        stream.len(),
        group.len()
    )
}

/// A [`StreamBackend`] backed by a real Redis connection.
///
/// The wire format is delegated to codecs; the default [`JsonCodec`] wrapped by
/// [`EnvelopeStreamCodec`] matches the Go `StreamBus` envelope so the two
/// implementations interop. Swap in a binary codec via
/// [`RedisBackend::with_codec`] when wire compat is not required and
/// throughput matters, or register field-level codecs for specific streams,
/// groups, and subscriptions.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use eventbus_core::stream::{StreamBus, StreamBusOptions};
/// use eventbus_redis::{stream_bus_from_client, RedisBackend};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = redis::Client::open("redis://127.0.0.1/")?;
///
/// // Option A: convenience constructor with dedicated blocking readers.
/// let bus = stream_bus_from_client(client.clone(), StreamBusOptions::default()).await?;
///
/// // Option B: explicit backend construction.
/// let backend = Arc::new(RedisBackend::from_client(client).await?);
/// let bus = StreamBus::new(backend, StreamBusOptions::default())?;
/// # Ok(())
/// # }
/// ```
///
/// XAUTOCLAIM cursor key — kept as a tuple (rather than a formatted string)
/// so the `DashMap` lookup avoids one allocation per call.
type ReclaimCursorKey = (String, String, String);
type StreamGroupKey = (String, String);
type StreamGroupConsumerKey = (String, String, String);

struct CodecRegistry {
    default_read: Arc<dyn RedisStreamCodec>,
    default_write: Arc<dyn RedisStreamCodec>,
    stream_read: DashMap<String, Arc<dyn RedisStreamCodec>>,
    group_read: DashMap<StreamGroupKey, Arc<dyn RedisStreamCodec>>,
    subscription_read: DashMap<StreamGroupConsumerKey, Arc<dyn RedisStreamCodec>>,
    stream_write: DashMap<String, Arc<dyn RedisStreamCodec>>,
}

impl CodecRegistry {
    fn new(default_codec: Arc<dyn RedisStreamCodec>) -> Self {
        Self {
            default_read: Arc::clone(&default_codec),
            default_write: default_codec,
            stream_read: DashMap::new(),
            group_read: DashMap::new(),
            subscription_read: DashMap::new(),
            stream_write: DashMap::new(),
        }
    }

    fn read_codec(&self, stream: &str, group: &str, consumer: &str) -> Arc<dyn RedisStreamCodec> {
        // The `is_empty` guards keep the common no-override case free of the
        // per-call key `String` allocations — this runs on every fetch /
        // reclaim poll. Tuple keys cannot be looked up by `(&str, ..)`.
        if !self.subscription_read.is_empty() {
            let key = (stream.to_string(), group.to_string(), consumer.to_string());
            if let Some(codec) = self.subscription_read.get(&key) {
                return Arc::clone(codec.value());
            }
        }

        if !self.group_read.is_empty() {
            let key = (stream.to_string(), group.to_string());
            if let Some(codec) = self.group_read.get(&key) {
                return Arc::clone(codec.value());
            }
        }

        if let Some(codec) = self.stream_read.get(stream) {
            return Arc::clone(codec.value());
        }

        Arc::clone(&self.default_read)
    }

    fn write_codec(&self, stream: &str) -> Arc<dyn RedisStreamCodec> {
        self.stream_write
            .get(stream)
            .map(|codec| Arc::clone(codec.value()))
            .unwrap_or_else(|| Arc::clone(&self.default_write))
    }

    fn set_stream_read_codec(&self, stream: &str, codec: Arc<dyn RedisStreamCodec>) {
        self.stream_read.insert(stream.to_string(), codec);
    }

    fn set_group_read_codec(&self, stream: &str, group: &str, codec: Arc<dyn RedisStreamCodec>) {
        self.group_read
            .insert((stream.to_string(), group.to_string()), codec);
    }

    fn set_subscription_read_codec(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        codec: Arc<dyn RedisStreamCodec>,
    ) {
        self.subscription_read.insert(
            (stream.to_string(), group.to_string(), consumer.to_string()),
            codec,
        );
    }

    fn set_stream_write_codec(&self, stream: &str, codec: Arc<dyn RedisStreamCodec>) {
        self.stream_write.insert(stream.to_string(), codec);
    }
}

pub struct RedisBackend {
    conn: CommandConnection,
    read_client: Option<redis::Client>,
    read_connections: DashMap<StreamGroupConsumerKey, Arc<OnceCell<MultiplexedConnection>>>,
    /// Field-level wire-format registry. Defaults to JSON in a `message` field.
    registry: CodecRegistry,
    /// Per-(stream, group, consumer) XAUTOCLAIM start-id cursor.
    ///
    /// DashMap guards are scoped to synchronous lookups and updates; no shard
    /// lock is held across network I/O.
    reclaim_starts: DashMap<ReclaimCursorKey, String>,
}

impl RedisBackend {
    /// Construct a backend using the default [`JsonCodec`] wire format.
    ///
    /// A connection clone shares its socket, so this constructor uses
    /// nonblocking read polling. Prefer [`Self::from_client`] for dedicated
    /// blocking-read connections, automatic command reconnection, and lower
    /// idle polling overhead. An externally supplied connection cannot be
    /// reconnected by this backend without its connection settings.
    pub fn new(conn: MultiplexedConnection) -> Self {
        Self::with_codec(conn, Arc::new(JsonCodec))
    }

    /// Construct a backend with a user-supplied [`Codec`].
    ///
    /// Use this when wire-compat with the Go `StreamBus` is not required and
    /// you want to swap in a binary codec for throughput.
    pub fn with_codec(conn: MultiplexedConnection, codec: Arc<dyn Codec>) -> Self {
        Self {
            conn: CommandConnection::Direct(conn),
            read_client: None,
            read_connections: DashMap::new(),
            registry: CodecRegistry::new(Arc::new(EnvelopeStreamCodec::from_core_codec(codec))),
            reclaim_starts: DashMap::new(),
        }
    }

    /// Connect with an automatically reconnecting command connection and lazily
    /// create one dedicated blocking reader per `(stream, group, consumer)`.
    /// A command interrupted by connection loss still returns an error: it is
    /// not replayed automatically, since an XADD may already have succeeded.
    pub async fn from_client(client: redis::Client) -> Result<Self, EventBusError> {
        let conn = ConnectionManager::new(client.clone())
            .await
            .map_err(|err| EventBusError::source("connect Redis backend", err))?;
        Ok(Self {
            conn: CommandConnection::Managed(conn),
            read_client: Some(client),
            read_connections: DashMap::new(),
            registry: CodecRegistry::new(Arc::new(EnvelopeStreamCodec::default())),
            reclaim_starts: DashMap::new(),
        })
    }

    /// Enable dedicated readers on a backend built with a custom command
    /// connection or codec. The client must select the same server and database.
    #[must_use]
    pub fn with_read_client(mut self, client: redis::Client) -> Self {
        self.read_connections.clear();
        self.read_client = Some(client);
        self
    }

    async fn read_connection(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
    ) -> Result<CommandConnection, EventBusError> {
        let Some(client) = &self.read_client else {
            return Ok(self.conn.clone());
        };
        let key = (stream.to_owned(), group.to_owned(), consumer.to_owned());
        let cell = Arc::clone(
            self.read_connections
                .entry(key)
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .value(),
        );
        // Release the DashMap guard before connecting. OnceCell also prevents
        // simultaneous first reads from opening duplicate connections.
        cell.get_or_try_init(|| async {
            client
                .get_multiplexed_async_connection()
                .await
                .map_err(|err| EventBusError::source("connect Redis reader", err))
        })
        .await
        .cloned()
        .map(CommandConnection::Direct)
    }

    async fn delivery_states(
        &self,
        stream: &str,
        group: &str,
        entries: Vec<FetchedEntry>,
        reclaimed: bool,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        let ids: Vec<&str> = entries
            .iter()
            .filter_map(|entry| match entry {
                FetchedEntry::Decoded(message) => Some(message.id.as_str()),
                FetchedEntry::Malformed { .. } => None,
            })
            .collect();
        if ids.is_empty() {
            return Ok(entries);
        }
        let mut conn = self.conn.clone();
        let states: Vec<(u64, i64, i64)> = redis::Script::new(DELIVERY_STATE_SCRIPT)
            .key(stream)
            .key(delivery_metadata_key(stream, group))
            .arg(group)
            .arg(if reclaimed { "reclaimed" } else { "new" })
            .arg(&ids)
            .invoke_async(&mut conn)
            .await
            .map_err(|err| EventBusError::source("read Redis delivery state", err))?;
        if states.len() != ids.len() {
            return Err(EventBusError::Serialization(
                "incomplete Redis delivery state".into(),
            ));
        }
        let mut states = states.into_iter();
        let mut result = Vec::with_capacity(entries.len());
        for entry in entries {
            let FetchedEntry::Decoded(mut claimed) = entry else {
                result.push(entry);
                continue;
            };
            let (deliveries, first, last) = states.next().expect("validated state count");
            if deliveries == 0 {
                // Another consumer acknowledged this entry before we inspected it.
                continue;
            }
            let attempt = u32::try_from(deliveries)
                .ok()
                .and_then(|n| retry_attempt(&claimed.message).checked_add(n));
            let timestamps = chrono::DateTime::from_timestamp_millis(first)
                .zip(chrono::DateTime::from_timestamp_millis(last));
            match (attempt, timestamps) {
                (Some(attempt), Some((first_received, last_received))) => {
                    claimed.state = PartialDeliveryState {
                        attempt,
                        first_received,
                        last_received,
                        redelivered: reclaimed || deliveries > 1,
                    };
                    result.push(FetchedEntry::Decoded(claimed));
                }
                _ => result.push(FetchedEntry::Malformed {
                    id: claimed.id,
                    error: EventBusError::Serialization("invalid Redis delivery state".into()),
                }),
            }
        }
        Ok(result)
    }

    /// Register a read codec for every consumer reading from `stream`.
    ///
    /// Read resolution prefers subscription-specific codecs first, then
    /// group-level codecs, then this stream-level codec, and finally the
    /// default codec.
    ///
    /// Registrations may be made at any time, including while consumers are
    /// running; they take effect from the next fetch/reclaim poll. Entries
    /// already decoded with the previous codec are not re-decoded.
    pub fn set_stream_read_codec(&self, stream: &str, codec: Arc<dyn RedisStreamCodec>) {
        self.registry.set_stream_read_codec(stream, codec);
    }

    /// Register a read codec for every consumer in the `(stream, group)` pair.
    ///
    /// This takes precedence over stream-level read codecs and falls behind
    /// subscription-specific codecs.
    pub fn set_group_read_codec(
        &self,
        stream: &str,
        group: &str,
        codec: Arc<dyn RedisStreamCodec>,
    ) {
        self.registry.set_group_read_codec(stream, group, codec);
    }

    /// Register a read codec for a single `(stream, group, consumer)` subscription.
    ///
    /// This is the highest-priority read override in the registry.
    pub fn set_subscription_read_codec(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        codec: Arc<dyn RedisStreamCodec>,
    ) {
        self.registry
            .set_subscription_read_codec(stream, group, consumer, codec);
    }

    /// Remove an override, releasing its codec and restoring group/stream/default
    /// fallback on the next read. Also called by `forget_consumer` on shutdown.
    pub fn remove_subscription_read_codec(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
    ) -> bool {
        self.registry
            .subscription_read
            .remove(&(stream.to_owned(), group.to_owned(), consumer.to_owned()))
            .is_some()
    }

    /// Register a write codec for `stream`.
    ///
    /// Writes use the stream-specific codec when present and otherwise fall
    /// back to the default write codec. Registrations take effect from the
    /// next publish.
    pub fn set_stream_write_codec(&self, stream: &str, codec: Arc<dyn RedisStreamCodec>) {
        self.registry.set_stream_write_codec(stream, codec);
    }

    /// Register [`crate::codec::WatermillStreamCodec`] as the read codec for `stream`.
    #[cfg(feature = "watermill")]
    pub fn set_watermill_read_stream(&self, stream: &str) {
        self.set_stream_read_codec(stream, Arc::new(crate::codec::WatermillStreamCodec));
    }

    /// Register a Watermill-aware [`crate::codec::AutoDetectRedisStreamCodec`]
    /// as the read codec for `stream`, accepting both the default envelope
    /// format and Watermill canonical entries.
    #[cfg(feature = "watermill")]
    pub fn set_auto_detect_read_stream(&self, stream: &str) {
        self.set_stream_read_codec(
            stream,
            Arc::new(crate::codec::AutoDetectRedisStreamCodec::with_watermill()),
        );
    }
}

// ---------------------------------------------------------------------------
// Convenience constructor
// ---------------------------------------------------------------------------

/// Create a [`StreamBus`] backed by a real Redis connection.
///
/// Shorthand for wrapping the connection in a [`RedisBackend`] (with the
/// default [`JsonCodec`]) and calling [`StreamBus::new`].
/// Reads use nonblocking polling; prefer [`stream_bus_from_client`] for
/// dedicated blocking connections and lower idle polling overhead.
pub fn stream_bus_from_connection(
    conn: MultiplexedConnection,
    options: StreamBusOptions,
) -> Result<StreamBus<RedisBackend>, EventBusError> {
    StreamBus::new(Arc::new(RedisBackend::new(conn)), options)
}

/// Construct a bus with dedicated blocking readers for each consumer.
pub async fn stream_bus_from_client(
    client: redis::Client,
    options: StreamBusOptions,
) -> Result<StreamBus<RedisBackend>, EventBusError> {
    StreamBus::new(Arc::new(RedisBackend::from_client(client).await?), options)
}

// ---------------------------------------------------------------------------
// StreamBackend implementation
// ---------------------------------------------------------------------------

impl StreamBackend for RedisBackend {
    async fn create_group(
        &self,
        stream: &str,
        group: &str,
        start_id: &str,
    ) -> Result<(), EventBusError> {
        let mut conn = self.conn.clone();
        match redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg(start_id)
            .arg("MKSTREAM")
            .query_async::<()>(&mut conn)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if is_busygroup(&err) => Ok(()),
            Err(err) => Err(EventBusError::source(
                format!("create consumer group for stream {stream}"),
                err,
            )),
        }
    }

    async fn publish(&self, stream: &str, message: Message) -> Result<String, EventBusError> {
        let codec = self.registry.write_codec(stream);
        let fields = codec.encode_fields(EncodeContext { stream }, &message)?;

        let mut conn = self.conn.clone();
        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream).arg("*");
        for (field, bytes) in fields {
            cmd.arg(field).arg(bytes);
        }
        let id: String = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| EventBusError::source(format!("xadd to {stream}"), e))?;

        Ok(id)
    }

    async fn reclaim_idle(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        count: usize,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        let mut conn = self.conn.clone();
        let cursor_key: ReclaimCursorKey =
            (stream.to_string(), group.to_string(), consumer.to_string());
        // Scope the Ref so no shard lock is held across the `.await` below.
        let start = self
            .reclaim_starts
            .get(&cursor_key)
            .map(|entry| entry.value().clone())
            .unwrap_or_else(|| "0-0".to_string());

        // XAUTOCLAIM <stream> <group> <consumer> <min-idle-ms> <start> COUNT <n>
        let raw: Value = redis::cmd("XAUTOCLAIM")
            .arg(stream)
            .arg(group)
            .arg(consumer)
            .arg(min_idle.as_millis() as u64)
            .arg(&start)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut conn)
            .await
            .map_err(|e| EventBusError::source(format!("xautoclaim on {stream}"), e))?;

        let codec = self.registry.read_codec(stream, group, consumer);
        // Redis 7+ reports entries deleted from the stream while still pending.
        if let Value::Array(items) = &raw {
            if let Some(deleted) = items.get(2) {
                let ids: Vec<String> = FromRedisValue::from_redis_value(deleted.clone())
                    .map_err(|err| EventBusError::source("decode deleted pending IDs", err))?;
                if !ids.is_empty() {
                    redis::cmd("HDEL")
                        .arg(delivery_metadata_key(stream, group))
                        .arg(ids)
                        .query_async::<()>(&mut conn)
                        .await
                        .map_err(|err| {
                            EventBusError::source("clean deleted delivery state", err)
                        })?;
                }
            }
        }
        let (next_start, claimed) = parse_autoclaim(raw, stream, codec.as_ref())?;
        let claimed = self.delivery_states(stream, group, claimed, true).await?;
        self.reclaim_starts.insert(cursor_key, next_start);
        Ok(claimed)
    }

    async fn read_new(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
        timeout: Duration,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.read_connection(stream, group, consumer).await?;
        let blocking = self.read_client.is_some() && !timeout.is_zero();
        let deadline = Instant::now() + timeout;
        loop {
            let mut cmd = redis::cmd("XREADGROUP");
            cmd.arg("GROUP")
                .arg(group)
                .arg(consumer)
                .arg("COUNT")
                .arg(count);
            if blocking {
                // Redis interprets BLOCK 0 as infinite, not as a zero timeout.
                cmd.arg("BLOCK").arg(timeout.as_millis().max(1) as u64);
            }
            let result: Result<StreamReadReply, _> = cmd
                .arg("STREAMS")
                .arg(stream)
                .arg(">")
                .query_async(&mut conn)
                .await;
            let reply = match result {
                Ok(reply) => reply,
                Err(err) if is_nil_response(&err) => StreamReadReply { keys: Vec::new() },
                Err(err) => {
                    self.read_connections.remove(&(
                        stream.to_owned(),
                        group.to_owned(),
                        consumer.to_owned(),
                    ));
                    return Err(EventBusError::source(
                        format!("xreadgroup on {stream}"),
                        err,
                    ));
                }
            };
            let codec = self.registry.read_codec(stream, group, consumer);
            let entries: Vec<_> = reply
                .keys
                .into_iter()
                .flat_map(|key| key.ids)
                .map(|entry| decode_entry(stream, entry, false, codec.as_ref()))
                .collect();
            if !entries.is_empty() || blocking || Instant::now() >= deadline {
                return self.delivery_states(stream, group, entries, false).await;
            }
            tokio::time::sleep(
                SHARED_READ_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
    }

    async fn ack(&self, stream: &str, group: &str, message_id: &str) -> Result<(), EventBusError> {
        self.ack_many(stream, group, &[message_id.to_owned()]).await
    }

    /// Acknowledge a batch and remove receipt metadata atomically in one
    /// server round-trip. Large batches are chunked inside the script to
    /// stay within Lua's argument-stack limit.
    async fn ack_many(
        &self,
        stream: &str,
        group: &str,
        message_ids: &[String],
    ) -> Result<(), EventBusError> {
        if message_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.clone();
        let _: i64 = redis::Script::new(ACK_SCRIPT)
            .key(stream)
            .key(delivery_metadata_key(stream, group))
            .arg(group)
            .arg(message_ids)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| EventBusError::source(format!("xack batch on {stream}"), e))?;
        Ok(())
    }

    async fn forget_consumer(&self, stream: &str, group: &str, consumer: &str) {
        let key: ReclaimCursorKey = (stream.to_string(), group.to_string(), consumer.to_string());
        self.reclaim_starts.remove(&key);
        self.read_connections.remove(&key);
        self.remove_subscription_read_codec(stream, group, consumer);
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Decode a single Redis Stream entry (`StreamId`) into a `FetchedEntry`.
///
/// On success returns `Decoded(ClaimedMessage)`; on any per-entry failure
/// (missing field, oversize raw payload, codec error) returns
/// `Malformed { id, error }` so the bus layer can ack + DLQ + observe
/// instead of poisoning the whole batch.
fn decode_entry(
    stream: &str,
    entry: StreamId,
    redelivered: bool,
    codec: &dyn RedisStreamCodec,
) -> FetchedEntry {
    // Consuming the entry lets `entry_fields` move the field bytes out of the
    // redis reply instead of cloning them (payloads can be MiB-sized).
    let id = entry.id;

    let fields = match entry_fields(&id, entry.map) {
        Ok(fields) => fields,
        Err(error) => return FetchedEntry::Malformed { id, error },
    };

    let mut message = match codec.decode_fields(
        DecodeContext {
            stream,
            redis_id: &id,
        },
        &fields,
    ) {
        Ok(m) => m,
        Err(error) => return FetchedEntry::Malformed { id, error },
    };

    // Hoist header values into typed fields once, here at the wire boundary,
    // so consumers can rely on `Message::idempotency_key()` / `schema()` /
    // `trace_context()` without each call re-reading headers.
    message.normalize();

    let Some(attempt) = retry_attempt(&message).checked_add(1) else {
        return FetchedEntry::Malformed {
            id,
            error: EventBusError::Serialization("delivery attempt overflow".into()),
        };
    };
    let now = Utc::now();

    FetchedEntry::Decoded(ClaimedMessage {
        id,
        message: Arc::new(message),
        state: PartialDeliveryState {
            attempt,
            first_received: now,
            last_received: now,
            redelivered,
        },
    })
}

/// Parse the raw `XAUTOCLAIM` response into claimed messages.
///
/// Response shape: `[next-start-id, [entries...], [deleted-ids...]]`
fn parse_autoclaim(
    raw: Value,
    stream: &str,
    codec: &dyn RedisStreamCodec,
) -> Result<(String, Vec<FetchedEntry>), EventBusError> {
    let items = match raw {
        Value::Array(v) if v.len() >= 2 => v,
        Value::Nil => return Ok(("0-0".to_string(), Vec::new())),
        _ => {
            return Err(EventBusError::Serialization(
                "unexpected XAUTOCLAIM response".into(),
            ))
        }
    };
    let mut items = items.into_iter();
    let next_start: String =
        FromRedisValue::from_redis_value(items.next().expect("validated response length"))
            .map_err(|err| EventBusError::source("decode XAUTOCLAIM cursor", err))?;

    // Move the entries into the parser: cloning this Value copies every payload.
    let range: StreamRangeReply =
        FromRedisValue::from_redis_value(items.next().expect("validated response length"))
            .map_err(|err| EventBusError::source("decode XAUTOCLAIM entries", err))?;

    // Per-entry decoding (no `?`) so a corrupt entry becomes `Malformed`
    // instead of poisoning the whole reclaim batch.
    let claimed = range
        .ids
        .into_iter()
        .map(|entry| decode_entry(stream, entry, true, codec))
        .collect();

    Ok((next_start, claimed))
}

fn entry_fields(id: &str, map: HashMap<String, Value>) -> Result<RedisStreamFields, EventBusError> {
    let mut fields = RedisStreamFields::with_capacity(map.len());
    let mut total_raw_bytes = 0usize;

    for (key, val) in map {
        let bytes = redis_value_to_bytes(val)
            .map_err(|err| EventBusError::source(format!("read stream field {key}"), err))?;
        check_raw_field_size(
            id,
            &key,
            bytes.len(),
            &mut total_raw_bytes,
            fields.len() + 1,
        )?;
        fields.insert(key, bytes);
    }

    Ok(fields)
}

fn check_raw_field_size(
    id: &str,
    field: &str,
    field_bytes: usize,
    total_raw_bytes: &mut usize,
    field_count: usize,
) -> Result<(), EventBusError> {
    if field_bytes > MAX_RAW_PAYLOAD_BYTES {
        return Err(EventBusError::Serialization(format!(
            "entry {id} field '{field}' raw payload {field_bytes} bytes exceeds MAX_RAW_PAYLOAD_BYTES {MAX_RAW_PAYLOAD_BYTES}",
        )));
    }

    *total_raw_bytes = (*total_raw_bytes).saturating_add(field_bytes);
    if *total_raw_bytes > MAX_RAW_PAYLOAD_BYTES {
        let total = *total_raw_bytes;
        return Err(EventBusError::Serialization(format!(
            "entry {id} raw payload {total} bytes exceeds MAX_RAW_PAYLOAD_BYTES {MAX_RAW_PAYLOAD_BYTES} across {field_count} fields",
        )));
    }

    Ok(())
}

/// Codecs may produce either text or binary; accept both Redis Value shapes.
fn redis_value_to_bytes(value: Value) -> Result<Vec<u8>, redis::RedisError> {
    match value {
        Value::BulkString(b) => Ok(b),
        Value::SimpleString(s) => Ok(s.into_bytes()),
        other => {
            let s: String = FromRedisValue::from_redis_value(other)?;
            Ok(s.into_bytes())
        }
    }
}

fn retry_attempt(msg: &Message) -> u32 {
    msg.headers
        .get(HEADER_RETRY_ATTEMPT)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Redis returns `ERR BUSYGROUP ...` when a consumer group already exists.
///
/// Prefer the typed [`redis::RedisError::code()`] over string matching so a
/// future redis-rs error-formatting change cannot silently regress this check.
fn is_busygroup(err: &redis::RedisError) -> bool {
    err.code() == Some("BUSYGROUP")
}

/// When XREADGROUP has no new messages Redis returns nil, which surfaces as
/// an `UnexpectedReturnType` deserialization error against `StreamReadReply`.
///
/// `UnexpectedReturnType` can also arise from genuine deserialization bugs;
/// callers should make sure they reach this check only on `XREADGROUP` paths
/// where a nil reply is the expected idle-empty signal.
fn is_nil_response(err: &redis::RedisError) -> bool {
    matches!(err.kind(), redis::ErrorKind::UnexpectedReturnType)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::codec::{EnvelopeStreamCodec, RedisStreamCodec, REDIS_FIELD_MESSAGE};

    #[tokio::test]
    #[ignore = "requires EVENTBUS_REDIS_URL"]
    async fn client_backend_recovers_publish_ack_and_reclaim_after_command_disconnect() {
        let client = redis::Client::open(std::env::var("EVENTBUS_REDIS_URL").unwrap()).unwrap();
        let backend = RedisBackend::from_client(client.clone()).await.unwrap();
        let stream = format!(
            "eventbus-reconnect-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        );
        backend.create_group(&stream, "group", "0").await.unwrap();
        let msg = Message {
            uid: "retry".into(),
            topic: eventbus_core::Topic::new(&stream).unwrap(),
            key: String::new(),
            kind: "test".into(),
            source: "test".into(),
            occurred_at: Utc::now(),
            headers: HashMap::new(),
            payload: bytes::Bytes::new(),
            content_type: None,
            event_version: None,
            idempotency_key: None,
            expires_at: None,
            trace_uid: None,
            correlation_uid: None,
        };
        let original = backend.publish(&stream, msg.clone()).await.unwrap();
        backend
            .read_new(&stream, "group", "first", 1, Duration::ZERO)
            .await
            .unwrap();
        let mut conn = backend.conn.clone();
        let id: i64 = redis::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut conn)
            .await
            .unwrap();
        let mut admin = client.get_multiplexed_async_connection().await.unwrap();
        let killed: usize = redis::cmd("CLIENT")
            .arg("KILL")
            .arg("ID")
            .arg(id)
            .query_async(&mut admin)
            .await
            .unwrap();
        assert_eq!(killed, 1);
        let published = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match backend.publish(&stream, msg.clone()).await {
                    Ok(id) => break id,
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("command connection must reconnect");
        let reclaimed = backend
            .reclaim_idle(&stream, "group", "second", Duration::ZERO, 1)
            .await
            .unwrap();
        assert!(
            matches!(&reclaimed[0], FetchedEntry::Decoded(entry) if entry.id == original && entry.state.attempt == 2)
        );
        backend.ack(&stream, "group", &original).await.unwrap();
        assert_ne!(published, original);
        redis::cmd("DEL")
            .arg(&stream)
            .query_async::<usize>(&mut admin)
            .await
            .unwrap();
    }

    fn assert_stream_backend<T: StreamBackend>() {}

    #[test]
    fn redis_backend_implements_stream_backend() {
        assert_stream_backend::<RedisBackend>();
    }

    fn malformed_id(entry: &FetchedEntry) -> Option<&str> {
        match entry {
            FetchedEntry::Malformed { id, .. } => Some(id.as_str()),
            FetchedEntry::Decoded(_) => None,
        }
    }

    #[test]
    fn decode_entry_reports_invalid_payload_as_malformed() {
        let entry = StreamId {
            id: "1-0".into(),
            map: HashMap::from([(
                REDIS_FIELD_MESSAGE.into(),
                Value::BulkString(b"not-json".to_vec()),
            )]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry(
            "orders.created",
            entry,
            false,
            &EnvelopeStreamCodec::default(),
        );
        assert_eq!(malformed_id(&decoded), Some("1-0"));
    }

    #[test]
    fn parse_autoclaim_surfaces_malformed_entry() {
        let raw = Value::Array(vec![
            Value::BulkString(b"0-0".to_vec()),
            Value::Array(vec![Value::Array(vec![
                Value::BulkString(b"1-0".to_vec()),
                Value::Array(vec![
                    Value::BulkString(REDIS_FIELD_MESSAGE.as_bytes().to_vec()),
                    Value::BulkString(b"not-json".to_vec()),
                ]),
            ])]),
            Value::Array(vec![]),
        ]);

        let (_, entries) = parse_autoclaim(raw, "orders.created", &EnvelopeStreamCodec::default())
            .expect("parse autoclaim");
        assert_eq!(entries.len(), 1);
        assert_eq!(malformed_id(&entries[0]), Some("1-0"));
    }

    #[test]
    fn parse_autoclaim_returns_next_cursor_and_entries() {
        let codec = JsonCodec;
        let bytes = codec
            .encode(&Message {
                uid: "msg-1".into(),
                topic: eventbus_core::Topic::new("orders.created").expect("topic"),
                key: "order-1".into(),
                kind: "orders.created".into(),
                source: "tests".into(),
                occurred_at: Utc::now(),
                headers: HashMap::new(),
                payload: bytes::Bytes::new(),
                content_type: None,
                event_version: None,
                idempotency_key: None,
                expires_at: None,
                trace_uid: None,
                correlation_uid: None,
            })
            .expect("encode message");
        let raw = Value::Array(vec![
            Value::BulkString(b"42-0".to_vec()),
            Value::Array(vec![Value::Array(vec![
                Value::BulkString(b"1-0".to_vec()),
                Value::Array(vec![
                    Value::BulkString(REDIS_FIELD_MESSAGE.as_bytes().to_vec()),
                    Value::BulkString(bytes),
                ]),
            ])]),
            Value::Array(vec![]),
        ]);

        let (cursor, entries) =
            parse_autoclaim(raw, "orders.created", &EnvelopeStreamCodec::default())
                .expect("parse xautoclaim");

        assert_eq!(cursor, "42-0");
        assert_eq!(entries.len(), 1);
        let claimed = match &entries[0] {
            FetchedEntry::Decoded(c) => c,
            FetchedEntry::Malformed { .. } => panic!("expected decoded"),
        };
        assert_eq!(claimed.id, "1-0");
    }

    #[test]
    fn decode_entry_uses_full_field_codec_instead_of_hardcoded_message_field() {
        #[derive(Debug)]
        struct PayloadOnlyCodec;

        impl RedisStreamCodec for PayloadOnlyCodec {
            fn name(&self) -> &str {
                "payload-only"
            }

            fn encode_fields(
                &self,
                _ctx: crate::codec::EncodeContext<'_>,
                _msg: &Message,
            ) -> Result<Vec<(String, Vec<u8>)>, EventBusError> {
                unreachable!("decode-only test")
            }

            fn decode_fields(
                &self,
                ctx: crate::codec::DecodeContext<'_>,
                fields: &crate::codec::RedisStreamFields,
            ) -> Result<Message, EventBusError> {
                let payload = fields.get("payload").expect("payload field").clone();
                Ok(Message {
                    uid: "payload-only-id".into(),
                    topic: eventbus_core::Topic::new(ctx.stream).expect("topic"),
                    key: String::new(),
                    kind: "payload.only".into(),
                    source: "test".into(),
                    occurred_at: Utc::now(),
                    headers: HashMap::new(),
                    payload: bytes::Bytes::from(payload),
                    content_type: None,
                    event_version: None,
                    idempotency_key: None,
                    expires_at: None,
                    trace_uid: None,
                    correlation_uid: None,
                })
            }

            fn can_decode(&self, fields: &crate::codec::RedisStreamFields) -> bool {
                fields.contains_key("payload")
            }
        }

        let entry = StreamId {
            id: "1-0".into(),
            map: HashMap::from([("payload".into(), Value::BulkString(b"raw-payload".to_vec()))]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry("custom.stream", entry, false, &PayloadOnlyCodec);

        let claimed = match decoded {
            FetchedEntry::Decoded(c) => c,
            FetchedEntry::Malformed { error, .. } => panic!("expected decoded, got {error:?}"),
        };
        assert_eq!(claimed.message.topic.as_str(), "custom.stream");
        assert_eq!(
            claimed.message.payload,
            bytes::Bytes::from_static(b"raw-payload")
        );
    }

    #[cfg(feature = "watermill")]
    #[test]
    fn decode_entry_uses_stream_specific_codec_for_watermill_without_message_field() {
        let metadata = HashMap::<String, String>::new();
        let entry = StreamId {
            id: "10-0".into(),
            map: HashMap::from([
                (
                    "_watermill_message_uuid".into(),
                    Value::BulkString(b"wm-stream-specific".to_vec()),
                ),
                (
                    "metadata".into(),
                    Value::BulkString(rmp_serde::to_vec_named(&metadata).expect("metadata")),
                ),
                (
                    "payload".into(),
                    Value::BulkString(b"mosaic-bytes".to_vec()),
                ),
            ]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry(
            "mapset.mosaic",
            entry,
            false,
            &crate::codec::WatermillStreamCodec,
        );

        let claimed = match decoded {
            FetchedEntry::Decoded(c) => c,
            FetchedEntry::Malformed { error, .. } => panic!("expected decoded, got {error:?}"),
        };
        assert_eq!(claimed.message.uid, "wm-stream-specific");
        assert_eq!(claimed.message.topic.as_str(), "mapset.mosaic");
        assert_eq!(
            claimed.message.payload,
            bytes::Bytes::from_static(b"mosaic-bytes")
        );
        assert_eq!(
            claimed.message.idempotency_key.as_deref(),
            Some("wm-stream-specific")
        );
    }

    #[cfg(feature = "watermill")]
    #[test]
    fn decode_entry_uses_auto_detect_codec_for_watermill_without_message_field() {
        let entry = StreamId {
            id: "10-1".into(),
            map: HashMap::from([
                (
                    "_watermill_message_uuid".into(),
                    Value::BulkString(b"wm-auto-entry".to_vec()),
                ),
                ("payload".into(), Value::BulkString(b"mosaic-auto".to_vec())),
            ]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry(
            "mapset.mosaic",
            entry,
            false,
            &crate::codec::AutoDetectRedisStreamCodec::with_watermill(),
        );

        let claimed = match decoded {
            FetchedEntry::Decoded(c) => c,
            FetchedEntry::Malformed { error, .. } => panic!("expected decoded, got {error:?}"),
        };
        assert_eq!(claimed.message.uid, "wm-auto-entry");
        assert_eq!(claimed.message.topic.as_str(), "mapset.mosaic");
        assert_eq!(
            claimed.message.payload,
            bytes::Bytes::from_static(b"mosaic-auto")
        );
    }

    #[test]
    fn decode_entry_preserves_eventbus_json_with_default_codec() {
        let codec = EnvelopeStreamCodec::default();
        let mut fields = codec
            .encode_fields(
                crate::codec::EncodeContext {
                    stream: "native.events",
                },
                &Message {
                    uid: "native-id".into(),
                    topic: eventbus_core::Topic::new("native.events").expect("topic"),
                    key: String::new(),
                    kind: "native.kind".into(),
                    source: "native".into(),
                    occurred_at: Utc::now(),
                    headers: HashMap::new(),
                    payload: bytes::Bytes::from_static(b"native-payload"),
                    content_type: None,
                    event_version: None,
                    idempotency_key: None,
                    expires_at: None,
                    trace_uid: None,
                    correlation_uid: None,
                },
            )
            .expect("encode fields");
        let (_, bytes) = fields.remove(0);
        let entry = StreamId {
            id: "11-0".into(),
            map: HashMap::from([("message".into(), Value::BulkString(bytes))]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry("native.events", entry, false, &codec);
        let claimed = match decoded {
            FetchedEntry::Decoded(c) => c,
            FetchedEntry::Malformed { error, .. } => panic!("expected decoded, got {error:?}"),
        };
        assert_eq!(claimed.message.uid, "native-id");
        assert_eq!(
            claimed.message.payload,
            bytes::Bytes::from_static(b"native-payload")
        );
    }

    #[test]
    fn decode_entry_rejects_total_raw_field_bytes_over_limit() {
        #[derive(Debug)]
        struct NeverCodec;

        impl RedisStreamCodec for NeverCodec {
            fn name(&self) -> &str {
                "never"
            }

            fn encode_fields(
                &self,
                _ctx: crate::codec::EncodeContext<'_>,
                _msg: &Message,
            ) -> Result<Vec<(String, Vec<u8>)>, EventBusError> {
                unreachable!("decode-only test")
            }

            fn decode_fields(
                &self,
                _ctx: crate::codec::DecodeContext<'_>,
                _fields: &crate::codec::RedisStreamFields,
            ) -> Result<Message, EventBusError> {
                panic!("oversize entries must be rejected before codec decode")
            }

            fn can_decode(&self, _fields: &crate::codec::RedisStreamFields) -> bool {
                true
            }
        }

        let half_plus_one = (MAX_RAW_PAYLOAD_BYTES / 2) + 1;
        let entry = StreamId {
            id: "oversize-0".into(),
            map: HashMap::from([
                ("a".into(), Value::BulkString(vec![0; half_plus_one])),
                ("b".into(), Value::BulkString(vec![0; half_plus_one])),
            ]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry("oversize.stream", entry, false, &NeverCodec);
        match decoded {
            FetchedEntry::Malformed { error, .. } => {
                let msg = error.to_string();
                assert!(msg.contains("raw payload"));
                assert!(msg.contains("MAX_RAW_PAYLOAD_BYTES"));
            }
            FetchedEntry::Decoded(_) => panic!("expected malformed oversize entry"),
        }
    }

    #[test]
    fn entry_fields_rejects_single_field_over_limit() {
        let map = HashMap::from([(
            "payload".to_string(),
            Value::BulkString(vec![0; MAX_RAW_PAYLOAD_BYTES + 1]),
        )]);

        let err = entry_fields("oversize-single-0", map).expect_err("oversize field should fail");
        let msg = err.to_string();
        assert!(msg.contains("payload"), "unexpected error: {err}");
        assert!(
            msg.contains("MAX_RAW_PAYLOAD_BYTES"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn entry_fields_rejects_total_field_bytes_over_limit() {
        let half_plus_one = (MAX_RAW_PAYLOAD_BYTES / 2) + 1;
        let map = HashMap::from([
            ("a".to_string(), Value::BulkString(vec![0; half_plus_one])),
            ("b".to_string(), Value::BulkString(vec![0; half_plus_one])),
        ]);

        let err = entry_fields("oversize-total-0", map).expect_err("oversize total should fail");
        let msg = err.to_string();
        assert!(msg.contains("raw payload"), "unexpected error: {err}");
        assert!(msg.contains("2 fields"), "unexpected error: {err}");
    }

    #[test]
    fn registry_prefers_subscription_then_group_then_stream_then_default_read_codec() {
        let registry = CodecRegistry::new(Arc::new(EnvelopeStreamCodec::default()));
        let stream_codec = Arc::new(NamedTestCodec("stream"));
        let group_codec = Arc::new(NamedTestCodec("group"));
        let subscription_codec = Arc::new(NamedTestCodec("subscription"));

        registry.set_stream_read_codec("s", stream_codec);
        registry.set_group_read_codec("s", "g", group_codec);
        registry.set_subscription_read_codec("s", "g", "c", subscription_codec);

        assert_eq!(registry.read_codec("s", "g", "c").name(), "subscription");
        assert_eq!(registry.read_codec("s", "g", "other").name(), "group");
        assert_eq!(registry.read_codec("s", "other", "other").name(), "stream");
        assert_eq!(
            registry.read_codec("other", "other", "other").name(),
            "json"
        );
    }

    #[test]
    fn registry_prefers_stream_write_codec_then_default_write_codec() {
        let registry = CodecRegistry::new(Arc::new(EnvelopeStreamCodec::default()));
        registry.set_stream_write_codec("s", Arc::new(NamedTestCodec("stream-write")));

        assert_eq!(registry.write_codec("s").name(), "stream-write");
        assert_eq!(registry.write_codec("other").name(), "json");
    }

    #[derive(Debug)]
    struct NamedTestCodec(&'static str);

    impl RedisStreamCodec for NamedTestCodec {
        fn name(&self) -> &str {
            self.0
        }

        fn encode_fields(
            &self,
            _ctx: crate::codec::EncodeContext<'_>,
            _msg: &Message,
        ) -> Result<Vec<(String, Vec<u8>)>, EventBusError> {
            Ok(vec![("message".to_string(), self.0.as_bytes().to_vec())])
        }

        fn decode_fields(
            &self,
            _ctx: crate::codec::DecodeContext<'_>,
            _fields: &crate::codec::RedisStreamFields,
        ) -> Result<Message, EventBusError> {
            unreachable!("registry test only checks selection")
        }

        fn can_decode(&self, _fields: &crate::codec::RedisStreamFields) -> bool {
            true
        }
    }
}
