//! Owner-isolated, bounded Pub/Sub subscription session lifecycle.

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fmt,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::transport::{Target, connect_cluster};
use async_trait::async_trait;
use futures_util::StreamExt;
use redis_tower::{
    BinaryPubSubConnection, BinaryPubSubMessage,
    pubsub::MessageKind,
    reconnect::{ConnectionFactory, ReconnectConfig, UrlConnectionFactory},
};
use redis_tower_cluster::{
    BinaryClusterPubSubConnection, BinaryShardedClusterPubSubConnection, MultiplexedClusterClient,
    slot_for_key,
};
use redis_tower_core::{ProtocolVersion, RedisError as TowerError};
use tokio::sync::{Mutex, Notify, RwLock};

/// Default maximum number of sessions held by one manager.
pub const DEFAULT_MAX_PUBSUB_SESSIONS: usize = 64;
/// Default maximum number of sessions owned by one client identity.
pub const DEFAULT_MAX_PUBSUB_SESSIONS_PER_OWNER: usize = 8;
/// Default maximum subscriptions attached to one session.
pub const DEFAULT_MAX_PUBSUB_SUBSCRIPTIONS: usize = 100;
/// Default maximum queued messages per session.
pub const DEFAULT_MAX_PUBSUB_BUFFERED_MESSAGES: usize = 1_000;
/// Default maximum payload size accepted into a session buffer.
pub const DEFAULT_MAX_PUBSUB_MESSAGE_BYTES: usize = 1024 * 1024;
/// Default maximum raw message bytes returned by one read.
pub const DEFAULT_MAX_PUBSUB_READ_BYTES: usize = 256 * 1024;
/// Default maximum duration of one finite read.
pub const DEFAULT_MAX_PUBSUB_READ_DURATION: Duration = Duration::from_secs(30);
/// Default idle lifetime before a session is reclaimed.
pub const DEFAULT_PUBSUB_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Default interval between independent stale-session sweeps.
pub const DEFAULT_PUBSUB_CLEANUP_INTERVAL: Duration = Duration::from_secs(15);
/// Default maximum duration of a connection or subscription mutation.
pub const DEFAULT_PUBSUB_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

const MAX_CHANNEL_BYTES: usize = 64 * 1024;
const MAX_PATTERN_BYTES: usize = 4 * 1024;

/// Limits enforced by the built-in DirectRedis session manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PubSubSessionLimits {
    max_sessions: usize,
    max_sessions_per_owner: usize,
    max_subscriptions_per_session: usize,
    max_buffered_messages: usize,
    max_message_bytes: usize,
    max_read_bytes: usize,
    max_read_duration: Duration,
    idle_timeout: Duration,
    cleanup_interval: Duration,
    operation_timeout: Duration,
}

impl Default for PubSubSessionLimits {
    fn default() -> Self {
        Self {
            max_sessions: DEFAULT_MAX_PUBSUB_SESSIONS,
            max_sessions_per_owner: DEFAULT_MAX_PUBSUB_SESSIONS_PER_OWNER,
            max_subscriptions_per_session: DEFAULT_MAX_PUBSUB_SUBSCRIPTIONS,
            max_buffered_messages: DEFAULT_MAX_PUBSUB_BUFFERED_MESSAGES,
            max_message_bytes: DEFAULT_MAX_PUBSUB_MESSAGE_BYTES,
            max_read_bytes: DEFAULT_MAX_PUBSUB_READ_BYTES,
            max_read_duration: DEFAULT_MAX_PUBSUB_READ_DURATION,
            idle_timeout: DEFAULT_PUBSUB_IDLE_TIMEOUT,
            cleanup_interval: DEFAULT_PUBSUB_CLEANUP_INTERVAL,
            operation_timeout: DEFAULT_PUBSUB_OPERATION_TIMEOUT,
        }
    }
}

impl PubSubSessionLimits {
    pub const fn max_sessions(self) -> usize {
        self.max_sessions
    }

    pub const fn max_sessions_per_owner(self) -> usize {
        self.max_sessions_per_owner
    }

    pub const fn max_subscriptions_per_session(self) -> usize {
        self.max_subscriptions_per_session
    }

    pub const fn max_buffered_messages(self) -> usize {
        self.max_buffered_messages
    }

    pub const fn max_message_bytes(self) -> usize {
        self.max_message_bytes
    }

    pub const fn max_read_bytes(self) -> usize {
        self.max_read_bytes
    }

    pub const fn max_read_duration(self) -> Duration {
        self.max_read_duration
    }

    pub const fn idle_timeout(self) -> Duration {
        self.idle_timeout
    }

    pub const fn cleanup_interval(self) -> Duration {
        self.cleanup_interval
    }

    pub const fn operation_timeout(self) -> Duration {
        self.operation_timeout
    }

    pub const fn with_max_sessions(mut self, value: usize) -> Self {
        self.max_sessions = value;
        self
    }

    pub const fn with_max_sessions_per_owner(mut self, value: usize) -> Self {
        self.max_sessions_per_owner = value;
        self
    }

    pub const fn with_max_subscriptions_per_session(mut self, value: usize) -> Self {
        self.max_subscriptions_per_session = value;
        self
    }

    pub const fn with_max_buffered_messages(mut self, value: usize) -> Self {
        self.max_buffered_messages = value;
        self
    }

    pub const fn with_max_message_bytes(mut self, value: usize) -> Self {
        self.max_message_bytes = value;
        self
    }

    pub const fn with_max_read_bytes(mut self, value: usize) -> Self {
        self.max_read_bytes = value;
        self
    }

    pub const fn with_max_read_duration(mut self, value: Duration) -> Self {
        self.max_read_duration = value;
        self
    }

    pub const fn with_idle_timeout(mut self, value: Duration) -> Self {
        self.idle_timeout = value;
        self
    }

    pub const fn with_cleanup_interval(mut self, value: Duration) -> Self {
        self.cleanup_interval = value;
        self
    }

    pub const fn with_operation_timeout(mut self, value: Duration) -> Self {
        self.operation_timeout = value;
        self
    }

    fn validate(self) -> Result<Self, PubSubSessionError> {
        if self.max_sessions == 0
            || self.max_sessions_per_owner == 0
            || self.max_subscriptions_per_session == 0
            || self.max_buffered_messages == 0
            || self.max_message_bytes == 0
            || self.max_read_bytes == 0
            || self.max_read_duration.is_zero()
            || self.idle_timeout.is_zero()
            || self.cleanup_interval.is_zero()
            || self.operation_timeout.is_zero()
        {
            return Err(PubSubSessionError::new(
                PubSubSessionErrorKind::InvalidRequest,
                "Pub/Sub session limits must all be greater than zero",
            )
            .with_code("INVALID_SESSION_LIMITS"));
        }
        if self.max_sessions_per_owner > self.max_sessions {
            return Err(PubSubSessionError::new(
                PubSubSessionErrorKind::InvalidRequest,
                "max_sessions_per_owner cannot exceed max_sessions",
            )
            .with_code("INVALID_SESSION_LIMITS"));
        }
        Ok(self)
    }
}

/// Opaque owner identity used to isolate sessions between MCP clients or host principals.
///
/// `RedisMcpBuilder::pubsub_sessions` installs a random owner for single-client
/// transports. Multi-client hosts should inject their own owner as a typed
/// Tower-MCP request extension; per-request extensions override the default.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PubSubSessionOwner(Arc<str>);

impl PubSubSessionOwner {
    /// Create an owner from a stable host identity. Empty identities are rejected.
    pub fn new(value: impl Into<String>) -> Result<Self, PubSubSessionError> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 {
            return Err(PubSubSessionError::new(
                PubSubSessionErrorKind::InvalidRequest,
                "Pub/Sub owner identity must contain between 1 and 512 bytes",
            )
            .with_code("INVALID_SESSION_OWNER"));
        }
        Ok(Self(Arc::from(value)))
    }

    pub(crate) fn random() -> Self {
        Self(Arc::from(random_identifier("owner")))
    }

    /// Stable identity bytes for host manager implementations.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PubSubSessionOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PubSubSessionOwner([redacted])")
    }
}

/// Subscription form represented by one Redis command family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum PubSubSubscriptionKind {
    Channel,
    Pattern,
    Sharded,
}

impl PubSubSubscriptionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Channel => "channel",
            Self::Pattern => "pattern",
            Self::Sharded => "sharded",
        }
    }
}

/// One binary-safe subscription attached to a session.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PubSubSubscription {
    pub kind: PubSubSubscriptionKind,
    pub value: Vec<u8>,
}

/// Current bounded state of a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubSessionSnapshot {
    pub session_id: String,
    pub subscriptions: Vec<PubSubSubscription>,
    pub buffered_messages: usize,
    pub max_buffered_messages: usize,
    pub max_message_bytes: usize,
    pub idle_timeout: Duration,
}

/// One binary-safe message removed from a session buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubMessage {
    pub sequence: u64,
    pub kind: PubSubSubscriptionKind,
    pub channel: Vec<u8>,
    pub pattern: Option<Vec<u8>>,
    pub payload: Vec<u8>,
    pub age: Duration,
}

/// Bounds for one finite, cancellation-safe session read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubSubReadRequest {
    pub max_messages: usize,
    pub max_bytes: usize,
    pub wait: Duration,
}

/// Result of one finite session read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubReadResult {
    pub messages: Vec<PubSubMessage>,
    pub remaining_buffered: usize,
    pub timed_out: bool,
    pub dropped_buffer_full_total: u64,
    pub dropped_oversized_total: u64,
}

/// Stable category for a session lifecycle failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PubSubSessionErrorKind {
    InvalidRequest,
    NotFound,
    Quota,
    Timeout,
    Authentication,
    Authorization,
    Connection,
    Server,
    ShuttingDown,
    Other,
}

/// Redacted, transport-independent Pub/Sub session failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubSessionError {
    kind: PubSubSessionErrorKind,
    message: String,
    code: Option<String>,
}

impl PubSubSessionError {
    pub fn new(kind: PubSubSessionErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            code: None,
        }
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub const fn kind(&self) -> PubSubSessionErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }
}

impl fmt::Display for PubSubSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = &self.code {
            write!(formatter, "{code}: {}", self.message)
        } else {
            formatter.write_str(&self.message)
        }
    }
}

impl std::error::Error for PubSubSessionError {}

/// Host boundary for isolated Pub/Sub session storage and connection lifecycle.
///
/// Implementations must treat `(owner, session_id)` as the authorization key,
/// return `NotFound` for foreign handles without revealing whether they exist,
/// and make `read` cancellation-safe when its future is dropped.
#[async_trait]
pub trait PubSubSessionManager: Send + Sync + 'static {
    async fn subscribe(
        &self,
        owner: &PubSubSessionOwner,
        kind: PubSubSubscriptionKind,
        subscriptions: Vec<Vec<u8>>,
    ) -> Result<PubSubSessionSnapshot, PubSubSessionError>;

    async fn read(
        &self,
        owner: &PubSubSessionOwner,
        session_id: &str,
        request: PubSubReadRequest,
    ) -> Result<PubSubReadResult, PubSubSessionError>;

    async fn unsubscribe(
        &self,
        owner: &PubSubSessionOwner,
        session_id: &str,
        kind: PubSubSubscriptionKind,
        subscriptions: Vec<Vec<u8>>,
    ) -> Result<PubSubSessionSnapshot, PubSubSessionError>;

    async fn close(
        &self,
        owner: &PubSubSessionOwner,
        session_id: &str,
    ) -> Result<(), PubSubSessionError>;

    /// Close every session owned by one client or principal.
    async fn close_owner(&self, owner: &PubSubSessionOwner) -> usize;

    /// Close every managed session and reject future creation.
    async fn shutdown(&self);
}

/// Built-in dedicated-connection manager for fixed standalone or Cluster targets.
#[derive(Clone)]
pub struct DirectRedisPubSubSessionManager {
    inner: Arc<DirectManagerInner>,
}

enum DirectTarget {
    Standalone(Target),
    Cluster(Vec<Target>),
}

struct DirectManagerInner {
    target: DirectTarget,
    limits: PubSubSessionLimits,
    sessions: RwLock<HashMap<String, Arc<DirectSession>>>,
    quota: Arc<SessionQuota>,
    shutting_down: AtomicBool,
}

struct DirectSession {
    id: String,
    owner: PubSubSessionOwner,
    connection: Mutex<Option<DirectConnection>>,
    subscriptions: Mutex<BTreeSet<PubSubSubscription>>,
    read_lock: Mutex<()>,
    buffer: Arc<DirectBuffer>,
    last_activity: StdMutex<Instant>,
    closed: AtomicBool,
    limits: PubSubSessionLimits,
    _quota_lease: SessionQuotaLease,
}

#[derive(Default)]
struct SessionQuotaState {
    total: usize,
    by_owner: HashMap<PubSubSessionOwner, usize>,
}

struct SessionQuota {
    state: StdMutex<SessionQuotaState>,
    max_sessions: usize,
    max_sessions_per_owner: usize,
}

struct SessionQuotaLease {
    quota: Arc<SessionQuota>,
    owner: PubSubSessionOwner,
}

impl SessionQuota {
    fn reserve(
        self: &Arc<Self>,
        owner: &PubSubSessionOwner,
    ) -> Result<SessionQuotaLease, PubSubSessionError> {
        let mut state = self.state.lock().map_err(|_| {
            PubSubSessionError::new(
                PubSubSessionErrorKind::Other,
                "Pub/Sub session quota state is unavailable",
            )
            .with_code("SESSION_QUOTA_UNAVAILABLE")
        })?;
        if state.total >= self.max_sessions {
            return Err(PubSubSessionError::new(
                PubSubSessionErrorKind::Quota,
                "global Pub/Sub session quota reached",
            )
            .with_code("SESSION_QUOTA_EXCEEDED"));
        }
        let owner_sessions = state.by_owner.get(owner).copied().unwrap_or_default();
        if owner_sessions >= self.max_sessions_per_owner {
            return Err(PubSubSessionError::new(
                PubSubSessionErrorKind::Quota,
                "per-owner Pub/Sub session quota reached",
            )
            .with_code("OWNER_SESSION_QUOTA_EXCEEDED"));
        }
        state.total += 1;
        state.by_owner.insert(owner.clone(), owner_sessions + 1);
        Ok(SessionQuotaLease {
            quota: self.clone(),
            owner: owner.clone(),
        })
    }
}

impl Drop for SessionQuotaLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.quota.state.lock() {
            state.total = state.total.saturating_sub(1);
            if let Some(owner_sessions) = state.by_owner.get_mut(&self.owner) {
                *owner_sessions = owner_sessions.saturating_sub(1);
                if *owner_sessions == 0 {
                    state.by_owner.remove(&self.owner);
                }
            }
        }
    }
}

struct DirectConnection {
    pumps: std::collections::BTreeMap<u16, PubSubPump>,
    cluster: Option<MultiplexedClusterClient>,
    kind: PubSubSubscriptionKind,
}

struct PubSubPump {
    commands: tokio::sync::mpsc::Sender<Unsubscribe>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for PubSubPump {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Unsubscribe {
    names: Vec<Vec<u8>>,
    reply: tokio::sync::oneshot::Sender<Result<(), TowerError>>,
}

enum PubSubTransport {
    Standalone(
        BinaryPubSubConnection,
        UrlConnectionFactory,
        ReconnectConfig,
    ),
    Cluster(BinaryClusterPubSubConnection),
    Sharded(BinaryShardedClusterPubSubConnection),
}

impl PubSubTransport {
    async fn next_message(&mut self) -> Result<BinaryPubSubMessage, TowerError> {
        match self {
            Self::Standalone(connection, factory, config) => loop {
                match connection.next().await {
                    Some(Ok(message)) => return Ok(message),
                    Some(Err(error))
                        if !matches!(
                            error,
                            TowerError::Connection { .. } | TowerError::ConnectionClosed
                        ) =>
                    {
                        return Err(error);
                    }
                    _ => connection.reconnect_with_backoff(factory, config).await?,
                }
            },
            Self::Cluster(connection) => connection.next_message().await,
            Self::Sharded(connection) => connection.next_message().await,
        }
    }

    async fn unsubscribe(
        &mut self,
        kind: PubSubSubscriptionKind,
        names: &[Vec<u8>],
    ) -> Result<(), TowerError> {
        let names = names.iter().map(Vec::as_slice).collect::<Vec<_>>();
        match self {
            Self::Standalone(connection, ..) => match kind {
                PubSubSubscriptionKind::Channel => connection.unsubscribe_bytes(&names).await,
                PubSubSubscriptionKind::Pattern => connection.punsubscribe_bytes(&names).await,
                PubSubSubscriptionKind::Sharded => connection.sunsubscribe_bytes(&names).await,
            },
            Self::Cluster(connection) => match kind {
                PubSubSubscriptionKind::Channel => connection.unsubscribe_bytes(&names).await,
                PubSubSubscriptionKind::Pattern => connection.punsubscribe_bytes(&names).await,
                PubSubSubscriptionKind::Sharded => Ok(()),
            },
            Self::Sharded(connection) => connection.unsubscribe_bytes(&names).await,
        }
    }
}

fn spawn_pubsub_pump(
    mut transport: PubSubTransport,
    kind: PubSubSubscriptionKind,
    buffer: Arc<DirectBuffer>,
) -> PubSubPump {
    let (commands, mut incoming) = tokio::sync::mpsc::channel::<Unsubscribe>(1);
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                command = incoming.recv() => {
                    let Some(command) = command else { break; };
                    let result = transport.unsubscribe(kind, &command.names).await;
                    let _ = command.reply.send(result);
                }
                message = transport.next_message() => match message {
                    Ok(message) => buffer.accept_message(message),
                    Err(_) => { buffer.notify.notify_one(); break; }
                }
            }
        }
    });
    PubSubPump { commands, task }
}

struct BufferedMessage {
    sequence: u64,
    kind: PubSubSubscriptionKind,
    channel: Vec<u8>,
    pattern: Option<Vec<u8>>,
    payload: Vec<u8>,
    received_at: Instant,
}

#[derive(Default)]
struct DirectBufferState {
    messages: VecDeque<BufferedMessage>,
    dropped_buffer_full: u64,
    dropped_oversized: u64,
}

struct DirectBuffer {
    state: StdMutex<DirectBufferState>,
    notify: Notify,
    next_sequence: AtomicU64,
    max_messages: usize,
    max_message_bytes: usize,
}

impl DirectBuffer {
    fn new(limits: PubSubSessionLimits) -> Self {
        Self {
            state: StdMutex::new(DirectBufferState::default()),
            notify: Notify::new(),
            next_sequence: AtomicU64::new(1),
            max_messages: limits.max_buffered_messages,
            max_message_bytes: limits.max_message_bytes,
        }
    }

    fn accept_message(&self, message: BinaryPubSubMessage) {
        let kind = match message.kind {
            MessageKind::Message => PubSubSubscriptionKind::Channel,
            MessageKind::PMessage => PubSubSubscriptionKind::Pattern,
            MessageKind::SMessage => PubSubSubscriptionKind::Sharded,
        };
        let pattern = message.pattern.map(|value| value.to_vec());
        let channel = Some(message.channel.to_vec());
        let payload = Some(message.payload.to_vec());
        let (Some(channel), Some(payload)) = (channel, payload) else {
            return;
        };
        if payload.len() > self.max_message_bytes || channel.len() > MAX_CHANNEL_BYTES {
            if let Ok(mut state) = self.state.lock() {
                state.dropped_oversized = state.dropped_oversized.saturating_add(1);
            }
            return;
        }
        if pattern
            .as_ref()
            .is_some_and(|value| value.len() > MAX_PATTERN_BYTES)
        {
            if let Ok(mut state) = self.state.lock() {
                state.dropped_oversized = state.dropped_oversized.saturating_add(1);
            }
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            if state.messages.len() == self.max_messages {
                state.messages.pop_front();
                state.dropped_buffer_full = state.dropped_buffer_full.saturating_add(1);
            }
            state.messages.push_back(BufferedMessage {
                sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
                kind,
                channel,
                pattern,
                payload,
                received_at: Instant::now(),
            });
        }
        self.notify.notify_one();
    }

    fn len(&self) -> usize {
        self.state.lock().map_or(0, |state| state.messages.len())
    }

    fn timed_out_result(&self) -> Result<PubSubReadResult, PubSubSessionError> {
        let state = self.state.lock().map_err(|_| {
            PubSubSessionError::new(
                PubSubSessionErrorKind::Other,
                "Pub/Sub message buffer is unavailable",
            )
            .with_code("SESSION_BUFFER_UNAVAILABLE")
        })?;
        Ok(PubSubReadResult {
            messages: Vec::new(),
            remaining_buffered: state.messages.len(),
            timed_out: true,
            dropped_buffer_full_total: state.dropped_buffer_full,
            dropped_oversized_total: state.dropped_oversized,
        })
    }
}

impl DirectRedisPubSubSessionManager {
    /// Create a standalone manager. Connections are opened lazily per session
    /// and forced to RESP3 so subscription pushes never contaminate the normal
    /// request/response connection manager.
    pub fn standalone(url: &str, limits: PubSubSessionLimits) -> Result<Self, PubSubSessionError> {
        let mut target = Target::parse(url)
            .map_err(|error| redacted_target_error(error, "Pub/Sub target configuration failed"))?;
        target.config = target.config.with_protocol(ProtocolVersion::Resp3);
        Self::new(DirectTarget::Standalone(target), limits)
    }

    /// Create a Redis Cluster manager from one or more seed URLs.
    pub fn cluster<I, S>(
        seed_urls: I,
        limits: PubSubSessionLimits,
    ) -> Result<Self, PubSubSessionError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let seed_urls = seed_urls
            .into_iter()
            .map(|url| {
                Target::parse(url.as_ref()).map_err(|error| {
                    redacted_target_error(error, "Pub/Sub Cluster target configuration failed")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if seed_urls.is_empty() {
            return Err(PubSubSessionError::new(
                PubSubSessionErrorKind::InvalidRequest,
                "at least one Redis Cluster seed URL is required",
            )
            .with_code("EMPTY_CLUSTER_SEEDS"));
        }
        for target in &seed_urls {
            target.cluster_builder().map_err(|error| {
                redacted_target_error(error, "Pub/Sub Cluster target configuration failed")
            })?;
        }
        Self::new(DirectTarget::Cluster(seed_urls), limits)
    }

    fn new(target: DirectTarget, limits: PubSubSessionLimits) -> Result<Self, PubSubSessionError> {
        let limits = limits.validate()?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            PubSubSessionError::new(
                PubSubSessionErrorKind::InvalidRequest,
                "DirectRedis Pub/Sub session managers must be created inside a Tokio runtime",
            )
            .with_code("TOKIO_RUNTIME_REQUIRED")
        })?;
        let inner = Arc::new(DirectManagerInner {
            target,
            limits,
            sessions: RwLock::new(HashMap::new()),
            quota: Arc::new(SessionQuota {
                state: StdMutex::new(SessionQuotaState::default()),
                max_sessions: limits.max_sessions,
                max_sessions_per_owner: limits.max_sessions_per_owner,
            }),
            shutting_down: AtomicBool::new(false),
        });
        runtime.spawn(reap_stale_sessions(Arc::downgrade(&inner)));
        Ok(Self { inner })
    }

    /// Configured limits used by this manager.
    pub fn limits(&self) -> PubSubSessionLimits {
        self.inner.limits
    }

    async fn create_connection(
        &self,
        buffer: Arc<DirectBuffer>,
        kind: PubSubSubscriptionKind,
        subscriptions: &[Vec<u8>],
    ) -> Result<DirectConnection, PubSubSessionError> {
        let future = async {
            let mut pumps = std::collections::BTreeMap::new();
            let names = subscriptions.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let cluster = match &self.inner.target {
                DirectTarget::Standalone(target) => {
                    let factory = UrlConnectionFactory::new(target.url.clone())
                        .with_connection_config(target.config.clone());
                    let connection = factory.connect().await?;
                    let mut pubsub = BinaryPubSubConnection::from_connection(connection)?;
                    match kind {
                        PubSubSubscriptionKind::Channel => pubsub.subscribe_bytes(&names).await?,
                        PubSubSubscriptionKind::Pattern => pubsub.psubscribe_bytes(&names).await?,
                        PubSubSubscriptionKind::Sharded => pubsub.ssubscribe_bytes(&names).await?,
                    }
                    let config = ReconnectConfig::default()
                        .connect_timeout(self.inner.limits.operation_timeout);
                    pumps.insert(
                        0,
                        spawn_pubsub_pump(
                            PubSubTransport::Standalone(pubsub, factory, config),
                            kind,
                            buffer.clone(),
                        ),
                    );
                    None
                }
                DirectTarget::Cluster(targets) => {
                    let cluster = connect_cluster(targets)
                        .await
                        .map_err(|error| TowerError::Redis(error.to_string()))?;
                    if kind == PubSubSubscriptionKind::Sharded {
                        let mut groups = std::collections::BTreeMap::<u16, Vec<&[u8]>>::new();
                        for name in &names {
                            groups.entry(slot_for_key(name)).or_default().push(name);
                        }
                        for (slot, names) in groups {
                            let pubsub = cluster.sharded_pubsub_bytes(&names).await?;
                            pumps.insert(
                                slot,
                                spawn_pubsub_pump(
                                    PubSubTransport::Sharded(pubsub),
                                    kind,
                                    buffer.clone(),
                                ),
                            );
                        }
                    } else {
                        let topology = cluster.topology().await;
                        let node = topology
                            .master_addrs()
                            .first()
                            .map(|node| (*node).clone())
                            .ok_or(TowerError::ConnectionClosed)?;
                        let mut pubsub = cluster.pubsub_on_bytes(node).await?;
                        match kind {
                            PubSubSubscriptionKind::Channel => {
                                pubsub.subscribe_bytes(&names).await?
                            }
                            PubSubSubscriptionKind::Pattern => {
                                pubsub.psubscribe_bytes(&names).await?
                            }
                            PubSubSubscriptionKind::Sharded => unreachable!(),
                        }
                        pumps.insert(
                            0,
                            spawn_pubsub_pump(
                                PubSubTransport::Cluster(pubsub),
                                kind,
                                buffer.clone(),
                            ),
                        );
                    }
                    Some(cluster)
                }
            };
            Ok::<_, TowerError>(DirectConnection {
                pumps,
                cluster,
                kind,
            })
        };
        tokio::time::timeout(self.inner.limits.operation_timeout, future)
            .await
            .map_err(|_| operation_timeout("opening Pub/Sub session"))?
            .map_err(|error| redacted_redis_error(error, "opening Pub/Sub session failed"))
    }

    async fn owned_session(
        &self,
        owner: &PubSubSessionOwner,
        session_id: &str,
    ) -> Result<Arc<DirectSession>, PubSubSessionError> {
        validate_session_id(session_id)?;
        let session = self.inner.sessions.read().await.get(session_id).cloned();
        match session {
            Some(session) if session.owner == *owner && !session.closed.load(Ordering::Acquire) => {
                session.touch();
                Ok(session)
            }
            _ => Err(session_not_found()),
        }
    }

    async fn remove_owned(
        &self,
        owner: &PubSubSessionOwner,
        session_id: &str,
    ) -> Result<Arc<DirectSession>, PubSubSessionError> {
        validate_session_id(session_id)?;
        let mut sessions = self.inner.sessions.write().await;
        if !sessions
            .get(session_id)
            .is_some_and(|session| session.owner == *owner)
        {
            return Err(session_not_found());
        }
        let session = sessions.remove(session_id).expect("owned session exists");
        drop(sessions);
        session.close_connection().await;
        Ok(session)
    }
}

impl DirectSession {
    fn touch(&self) {
        if let Ok(mut activity) = self.last_activity.lock() {
            *activity = Instant::now();
        }
    }

    fn idle_for(&self, now: Instant) -> Duration {
        self.last_activity
            .lock()
            .map_or(Duration::ZERO, |activity| {
                now.saturating_duration_since(*activity)
            })
    }

    fn is_idle_expired(&self, now: Instant) -> bool {
        if self.read_lock.try_lock().is_err() || self.connection.try_lock().is_err() {
            return false;
        }
        self.idle_for(now) >= self.limits.idle_timeout
    }

    async fn snapshot(&self) -> PubSubSessionSnapshot {
        PubSubSessionSnapshot {
            session_id: self.id.clone(),
            subscriptions: self.subscriptions.lock().await.iter().cloned().collect(),
            buffered_messages: self.buffer.len(),
            max_buffered_messages: self.limits.max_buffered_messages,
            max_message_bytes: self.limits.max_message_bytes,
            idle_timeout: self.limits.idle_timeout,
        }
    }

    async fn close_connection(&self) {
        self.closed.store(true, Ordering::Release);
        self.connection.lock().await.take();
        self.buffer.notify.notify_one();
    }
}

#[async_trait]
impl PubSubSessionManager for DirectRedisPubSubSessionManager {
    async fn subscribe(
        &self,
        owner: &PubSubSessionOwner,
        kind: PubSubSubscriptionKind,
        subscriptions: Vec<Vec<u8>>,
    ) -> Result<PubSubSessionSnapshot, PubSubSessionError> {
        if self.inner.shutting_down.load(Ordering::Acquire) {
            return Err(PubSubSessionError::new(
                PubSubSessionErrorKind::ShuttingDown,
                "Pub/Sub session manager is shutting down",
            )
            .with_code("SESSION_MANAGER_SHUTTING_DOWN"));
        }
        let subscriptions = validate_subscriptions(
            kind,
            subscriptions,
            self.inner.limits.max_subscriptions_per_session,
        )?;
        // Reserve before any network await so concurrent creation cannot open
        // more dedicated connections than the configured quotas allow.
        let quota_lease = self.inner.quota.reserve(owner)?;
        let buffer = Arc::new(DirectBuffer::new(self.inner.limits));
        let connection = self
            .create_connection(buffer.clone(), kind, &subscriptions)
            .await?;

        let mut sessions = self.inner.sessions.write().await;
        if self.inner.shutting_down.load(Ordering::Acquire) {
            return Err(PubSubSessionError::new(
                PubSubSessionErrorKind::ShuttingDown,
                "Pub/Sub session manager is shutting down",
            )
            .with_code("SESSION_MANAGER_SHUTTING_DOWN"));
        }
        let id = loop {
            let candidate = random_identifier("ps");
            if !sessions.contains_key(&candidate) {
                break candidate;
            }
        };
        let session = Arc::new(DirectSession {
            id: id.clone(),
            owner: owner.clone(),
            connection: Mutex::new(Some(connection)),
            subscriptions: Mutex::new(
                subscriptions
                    .into_iter()
                    .map(|value| PubSubSubscription { kind, value })
                    .collect(),
            ),
            read_lock: Mutex::new(()),
            buffer,
            last_activity: StdMutex::new(Instant::now()),
            closed: AtomicBool::new(false),
            limits: self.inner.limits,
            _quota_lease: quota_lease,
        });
        sessions.insert(id, session.clone());
        drop(sessions);
        Ok(session.snapshot().await)
    }

    async fn read(
        &self,
        owner: &PubSubSessionOwner,
        session_id: &str,
        request: PubSubReadRequest,
    ) -> Result<PubSubReadResult, PubSubSessionError> {
        if request.max_messages == 0 || request.max_bytes == 0 {
            return Err(PubSubSessionError::new(
                PubSubSessionErrorKind::InvalidRequest,
                "Pub/Sub read bounds must be greater than zero",
            )
            .with_code("INVALID_READ_BOUNDS"));
        }
        // Caller values are upper bounds. A manager with tighter resource
        // limits can safely return a smaller page or shorter finite wait.
        let max_messages = request
            .max_messages
            .min(self.inner.limits.max_buffered_messages);
        let max_bytes = request.max_bytes.min(self.inner.limits.max_read_bytes);
        let wait = request.wait.min(self.inner.limits.max_read_duration);
        let session = self.owned_session(owner, session_id).await?;
        let _read = session.read_lock.lock().await;
        let deadline = Instant::now() + wait;
        loop {
            let notified = session.buffer.notify.notified();
            let maybe_result = {
                let mut state = session.buffer.state.lock().map_err(|_| {
                    PubSubSessionError::new(
                        PubSubSessionErrorKind::Other,
                        "Pub/Sub message buffer is unavailable",
                    )
                    .with_code("SESSION_BUFFER_UNAVAILABLE")
                })?;
                if !state.messages.is_empty() || wait.is_zero() {
                    let now = Instant::now();
                    let mut bytes = 0_usize;
                    let mut messages = Vec::new();
                    while messages.len() < max_messages {
                        let Some(front) = state.messages.front() else {
                            break;
                        };
                        let message_bytes = front
                            .channel
                            .len()
                            .saturating_add(front.pattern.as_ref().map_or(0, Vec::len))
                            .saturating_add(front.payload.len());
                        if !messages.is_empty() && bytes.saturating_add(message_bytes) > max_bytes {
                            break;
                        }
                        if messages.is_empty() && message_bytes > max_bytes {
                            state.messages.pop_front();
                            state.dropped_oversized = state.dropped_oversized.saturating_add(1);
                            continue;
                        }
                        let message = state.messages.pop_front().expect("front message exists");
                        bytes = bytes.saturating_add(message_bytes);
                        messages.push(PubSubMessage {
                            sequence: message.sequence,
                            kind: message.kind,
                            channel: message.channel,
                            pattern: message.pattern,
                            payload: message.payload,
                            age: now.saturating_duration_since(message.received_at),
                        });
                    }
                    Some(PubSubReadResult {
                        messages,
                        remaining_buffered: state.messages.len(),
                        timed_out: false,
                        dropped_buffer_full_total: state.dropped_buffer_full,
                        dropped_oversized_total: state.dropped_oversized,
                    })
                } else {
                    None
                }
            };
            if let Some(result) = maybe_result {
                session.touch();
                return Ok(result);
            }
            if session.closed.load(Ordering::Acquire) {
                return Err(session_not_found());
            }
            let now = Instant::now();
            if now >= deadline {
                session.touch();
                return session.buffer.timed_out_result();
            }
            let _ = tokio::time::timeout(deadline.saturating_duration_since(now), notified).await;
        }
    }

    async fn unsubscribe(
        &self,
        owner: &PubSubSessionOwner,
        session_id: &str,
        kind: PubSubSubscriptionKind,
        subscriptions: Vec<Vec<u8>>,
    ) -> Result<PubSubSessionSnapshot, PubSubSessionError> {
        let subscriptions = validate_subscriptions(
            kind,
            subscriptions,
            self.inner.limits.max_subscriptions_per_session,
        )?;
        let session = self.owned_session(owner, session_id).await?;
        let mut connection_guard = session.connection.lock().await;
        let Some(connection) = connection_guard.as_mut() else {
            return Err(session_not_found());
        };
        run_subscription_command(
            connection,
            kind,
            &subscriptions,
            true,
            self.inner.limits.operation_timeout,
        )
        .await?;
        drop(connection_guard);
        let mut current = session.subscriptions.lock().await;
        for value in subscriptions {
            current.remove(&PubSubSubscription { kind, value });
        }
        drop(current);
        session.touch();
        Ok(session.snapshot().await)
    }

    async fn close(
        &self,
        owner: &PubSubSessionOwner,
        session_id: &str,
    ) -> Result<(), PubSubSessionError> {
        self.remove_owned(owner, session_id).await.map(|_| ())
    }

    async fn close_owner(&self, owner: &PubSubSessionOwner) -> usize {
        let removed = {
            let mut sessions = self.inner.sessions.write().await;
            let ids = sessions
                .iter()
                .filter(|(_, session)| session.owner == *owner)
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| sessions.remove(&id))
                .collect::<Vec<_>>()
        };
        let count = removed.len();
        for session in removed {
            session.close_connection().await;
        }
        count
    }

    async fn shutdown(&self) {
        let sessions = {
            let mut sessions = self.inner.sessions.write().await;
            self.inner.shutting_down.store(true, Ordering::Release);
            sessions
                .drain()
                .map(|(_, session)| session)
                .collect::<Vec<_>>()
        };
        for session in sessions {
            session.close_connection().await;
        }
    }
}

async fn reap_stale_sessions(inner: Weak<DirectManagerInner>) {
    loop {
        let Some(inner) = inner.upgrade() else {
            return;
        };
        tokio::time::sleep(inner.limits.cleanup_interval).await;
        if inner.shutting_down.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        let expired = {
            let mut sessions = inner.sessions.write().await;
            let ids = sessions
                .iter()
                .filter(|(_, session)| session.is_idle_expired(now))
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| sessions.remove(&id))
                .collect::<Vec<_>>()
        };
        for session in expired {
            session.close_connection().await;
        }
    }
}

async fn run_subscription_command(
    connection: &mut DirectConnection,
    kind: PubSubSubscriptionKind,
    subscriptions: &[Vec<u8>],
    unsubscribe: bool,
    timeout: Duration,
) -> Result<(), PubSubSessionError> {
    debug_assert!(
        unsubscribe,
        "initial subscriptions are established before exposing a session"
    );
    if kind != connection.kind {
        return Ok(());
    }
    let mut groups = std::collections::BTreeMap::<u16, Vec<Vec<u8>>>::new();
    for name in subscriptions {
        let slot = if connection.cluster.is_some() && kind == PubSubSubscriptionKind::Sharded {
            slot_for_key(name)
        } else {
            0
        };
        groups.entry(slot).or_default().push(name.clone());
    }
    let operation = async {
        for (slot, names) in groups {
            let Some(pump) = connection.pumps.get(&slot) else {
                continue;
            };
            let (reply, result) = tokio::sync::oneshot::channel();
            pump.commands
                .send(Unsubscribe { names, reply })
                .await
                .map_err(|_| TowerError::ConnectionClosed)?;
            result.await.map_err(|_| TowerError::ConnectionClosed)??;
        }
        Ok::<_, TowerError>(())
    };
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| operation_timeout("updating Pub/Sub subscriptions"))?
        .map_err(|error| redacted_redis_error(error, "updating Pub/Sub subscriptions failed"))
}

fn validate_subscriptions(
    kind: PubSubSubscriptionKind,
    subscriptions: Vec<Vec<u8>>,
    max: usize,
) -> Result<Vec<Vec<u8>>, PubSubSessionError> {
    if subscriptions.is_empty() || subscriptions.len() > max {
        return Err(PubSubSessionError::new(
            PubSubSessionErrorKind::InvalidRequest,
            format!("subscriptions must contain between 1 and {max} values"),
        )
        .with_code("INVALID_SUBSCRIPTION_COUNT"));
    }
    let mut unique = BTreeSet::new();
    for value in subscriptions {
        let max_bytes = if kind == PubSubSubscriptionKind::Pattern {
            MAX_PATTERN_BYTES
        } else {
            MAX_CHANNEL_BYTES
        };
        if value.is_empty() || value.len() > max_bytes {
            return Err(PubSubSessionError::new(
                PubSubSessionErrorKind::InvalidRequest,
                format!(
                    "{} subscription values must contain between 1 and {max_bytes} bytes",
                    kind.as_str()
                ),
            )
            .with_code("INVALID_SUBSCRIPTION_VALUE"));
        }
        unique.insert(value);
    }
    if unique.len() > max {
        return Err(PubSubSessionError::new(
            PubSubSessionErrorKind::InvalidRequest,
            format!("subscriptions must contain at most {max} distinct values"),
        )
        .with_code("INVALID_SUBSCRIPTION_COUNT"));
    }
    Ok(unique.into_iter().collect())
}

fn validate_session_id(session_id: &str) -> Result<(), PubSubSessionError> {
    if session_id.len() == 35
        && session_id.starts_with("ps_")
        && session_id[3..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        Ok(())
    } else {
        Err(session_not_found())
    }
}

fn session_not_found() -> PubSubSessionError {
    PubSubSessionError::new(
        PubSubSessionErrorKind::NotFound,
        "Pub/Sub session was not found for this owner",
    )
    .with_code("SESSION_NOT_FOUND")
}

fn operation_timeout(operation: &str) -> PubSubSessionError {
    PubSubSessionError::new(
        PubSubSessionErrorKind::Timeout,
        format!("timed out while {operation}"),
    )
    .with_code("SESSION_OPERATION_TIMEOUT")
}

fn redacted_redis_error(error: TowerError, context: &str) -> PubSubSessionError {
    redacted_target_error(crate::RedisError::from(error), context)
}

fn redacted_target_error(error: crate::RedisError, context: &str) -> PubSubSessionError {
    let kind = match error.kind() {
        crate::RedisErrorKind::Authentication => PubSubSessionErrorKind::Authentication,
        crate::RedisErrorKind::Authorization => PubSubSessionErrorKind::Authorization,
        crate::RedisErrorKind::Connection => PubSubSessionErrorKind::Connection,
        crate::RedisErrorKind::Timeout => PubSubSessionErrorKind::Timeout,
        crate::RedisErrorKind::InvalidRequest => PubSubSessionErrorKind::InvalidRequest,
        crate::RedisErrorKind::Server => PubSubSessionErrorKind::Server,
        _ => PubSubSessionErrorKind::Other,
    };
    PubSubSessionError::new(kind, context).with_code(error.code().unwrap_or("REDIS_SESSION_ERROR"))
}

pub(crate) fn random_identifier(prefix: &str) -> String {
    let value: [u8; 16] = rand::random();
    let mut result = String::with_capacity(prefix.len() + 1 + value.len() * 2);
    result.push_str(prefix);
    result.push('_');
    for byte in value {
        use fmt::Write as _;
        let _ = write!(result, "{byte:02x}");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owners_and_handles_do_not_leak_through_debug() {
        let owner = PubSubSessionOwner::new("private-principal").expect("valid owner");
        assert_eq!(format!("{owner:?}"), "PubSubSessionOwner([redacted])");
        let handle = random_identifier("ps");
        assert!(validate_session_id(&handle).is_ok());
        assert_eq!(handle.len(), 35);
    }

    #[test]
    fn session_limits_fail_closed() {
        let invalid = PubSubSessionLimits::default().with_max_sessions(0);
        assert_eq!(
            invalid.validate().expect_err("zero limit").code(),
            Some("INVALID_SESSION_LIMITS")
        );
    }

    #[test]
    fn subscriptions_are_binary_safe_deduplicated_and_bounded() {
        let values = validate_subscriptions(
            PubSubSubscriptionKind::Channel,
            vec![vec![0xff], b"alpha".to_vec(), vec![0xff]],
            3,
        )
        .expect("valid binary subscriptions");
        assert_eq!(values, vec![b"alpha".to_vec(), vec![0xff]]);
        assert!(
            validate_subscriptions(PubSubSubscriptionKind::Pattern, vec![vec![b'x'; 4097]], 1)
                .is_err()
        );
    }

    #[test]
    fn quota_reservations_bound_pending_and_active_sessions() {
        let quota = Arc::new(SessionQuota {
            state: StdMutex::new(SessionQuotaState::default()),
            max_sessions: 3,
            max_sessions_per_owner: 2,
        });
        let first = PubSubSessionOwner::new("first").expect("valid owner");
        let second = PubSubSessionOwner::new("second").expect("valid owner");
        let first_one = quota.reserve(&first).expect("first lease");
        let _first_two = quota.reserve(&first).expect("second lease");
        assert_eq!(
            quota.reserve(&first).err().expect("per-owner quota").code(),
            Some("OWNER_SESSION_QUOTA_EXCEEDED")
        );
        let _second_one = quota.reserve(&second).expect("global final lease");
        assert_eq!(
            quota.reserve(&second).err().expect("global quota").code(),
            Some("SESSION_QUOTA_EXCEEDED")
        );
        drop(first_one);
        let _second_two = quota.reserve(&second).expect("released lease is reusable");
    }
}
