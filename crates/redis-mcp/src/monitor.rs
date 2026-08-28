//! Owner-isolated, bounded MONITOR streaming sessions.
//!
//! MONITOR converts a connection into an indefinite server-push stream, so it
//! can never be an ordinary request/response tool. This module gives it the
//! same shape as Pub/Sub sessions: an opaque owner-isolated handle over a
//! dedicated connection, a bounded drop-oldest event buffer, finite
//! cancellable reads, idle reaping, and an explicit close. Events are
//! redacted at capture time: client addresses become stable per-session
//! pseudonyms and argument values are omitted unless the session explicitly
//! opted in when it started.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures_util::StreamExt;
use redis::Client;
use tokio::sync::{Mutex, Notify, RwLock};

use crate::pubsub_sessions::{PubSubSessionError, PubSubSessionErrorKind, PubSubSessionOwner};

/// Owner identity shared by every owner-isolated session kind.
///
/// Hosts install one owner per client or principal; Pub/Sub and MONITOR
/// sessions authorize handles against the same identity.
pub type RedisSessionOwner = PubSubSessionOwner;
/// Redacted, transport-independent session failure shared by session kinds.
pub type RedisSessionError = PubSubSessionError;
/// Stable session failure categories shared by session kinds.
pub type RedisSessionErrorKind = PubSubSessionErrorKind;

/// Default maximum number of MONITOR sessions held by one manager.
///
/// MONITOR measurably reduces server throughput, so the global ceiling is
/// intentionally far below the Pub/Sub session quota.
pub const DEFAULT_MAX_MONITOR_SESSIONS: usize = 4;
/// Default maximum number of MONITOR sessions owned by one client identity.
pub const DEFAULT_MAX_MONITOR_SESSIONS_PER_OWNER: usize = 1;
/// Default maximum buffered events per session before drop-oldest applies.
pub const DEFAULT_MAX_MONITOR_BUFFERED_EVENTS: usize = 1_000;
/// Default maximum raw bytes accepted for one captured event.
pub const DEFAULT_MAX_MONITOR_EVENT_BYTES: usize = 8 * 1024;
/// Default maximum raw event bytes returned by one read.
pub const DEFAULT_MAX_MONITOR_READ_BYTES: usize = 256 * 1024;
/// Default maximum duration of one finite read.
pub const DEFAULT_MAX_MONITOR_READ_DURATION: Duration = Duration::from_secs(30);
/// Default idle lifetime before a MONITOR session is reclaimed.
pub const DEFAULT_MONITOR_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Default interval between independent stale-session sweeps.
pub const DEFAULT_MONITOR_CLEANUP_INTERVAL: Duration = Duration::from_secs(5);
/// Default maximum duration of MONITOR connection setup.
pub const DEFAULT_MONITOR_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Limits enforced by the built-in DirectRedis MONITOR session manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct MonitorSessionLimits {
    max_sessions: usize,
    max_sessions_per_owner: usize,
    max_buffered_events: usize,
    max_event_bytes: usize,
    max_read_bytes: usize,
    max_read_duration: Duration,
    idle_timeout: Duration,
    cleanup_interval: Duration,
    operation_timeout: Duration,
}

impl Default for MonitorSessionLimits {
    fn default() -> Self {
        Self {
            max_sessions: DEFAULT_MAX_MONITOR_SESSIONS,
            max_sessions_per_owner: DEFAULT_MAX_MONITOR_SESSIONS_PER_OWNER,
            max_buffered_events: DEFAULT_MAX_MONITOR_BUFFERED_EVENTS,
            max_event_bytes: DEFAULT_MAX_MONITOR_EVENT_BYTES,
            max_read_bytes: DEFAULT_MAX_MONITOR_READ_BYTES,
            max_read_duration: DEFAULT_MAX_MONITOR_READ_DURATION,
            idle_timeout: DEFAULT_MONITOR_IDLE_TIMEOUT,
            cleanup_interval: DEFAULT_MONITOR_CLEANUP_INTERVAL,
            operation_timeout: DEFAULT_MONITOR_OPERATION_TIMEOUT,
        }
    }
}

impl MonitorSessionLimits {
    pub const fn max_sessions(self) -> usize {
        self.max_sessions
    }

    pub const fn max_sessions_per_owner(self) -> usize {
        self.max_sessions_per_owner
    }

    pub const fn max_buffered_events(self) -> usize {
        self.max_buffered_events
    }

    pub const fn max_event_bytes(self) -> usize {
        self.max_event_bytes
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

    pub const fn with_max_buffered_events(mut self, value: usize) -> Self {
        self.max_buffered_events = value;
        self
    }

    pub const fn with_max_event_bytes(mut self, value: usize) -> Self {
        self.max_event_bytes = value;
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

    fn validate(self) -> Result<Self, RedisSessionError> {
        if self.max_sessions == 0
            || self.max_sessions_per_owner == 0
            || self.max_buffered_events == 0
            || self.max_event_bytes == 0
            || self.max_read_bytes == 0
            || self.max_read_duration.is_zero()
            || self.idle_timeout.is_zero()
            || self.cleanup_interval.is_zero()
            || self.operation_timeout.is_zero()
        {
            return Err(RedisSessionError::new(
                RedisSessionErrorKind::InvalidRequest,
                "MONITOR session limits must all be greater than zero",
            )
            .with_code("INVALID_SESSION_LIMITS"));
        }
        if self.max_sessions_per_owner > self.max_sessions {
            return Err(RedisSessionError::new(
                RedisSessionErrorKind::InvalidRequest,
                "max_sessions_per_owner cannot exceed max_sessions",
            )
            .with_code("INVALID_SESSION_LIMITS"));
        }
        Ok(self)
    }
}

/// Options declared when a MONITOR session starts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MonitorSessionOptions {
    /// Capture binary-safe argument values instead of omitting them.
    ///
    /// Argument values routinely contain application data and credentials, so
    /// they are omitted unless the session explicitly opts in.
    pub include_arguments: bool,
}

impl MonitorSessionOptions {
    /// Capture binary-safe argument values instead of omitting them.
    pub const fn with_arguments(mut self, value: bool) -> Self {
        self.include_arguments = value;
        self
    }
}

/// Public state of one MONITOR session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorSessionSnapshot {
    /// Opaque owner-scoped session handle.
    pub session_id: String,
    /// Whether argument values are captured.
    pub include_arguments: bool,
    /// Events currently buffered.
    pub buffered_events: usize,
    /// Buffer capacity before drop-oldest applies.
    pub max_buffered_events: usize,
    /// Maximum raw bytes accepted for one event.
    pub max_event_bytes: usize,
    /// Idle lifetime before the manager reclaims the session.
    pub idle_timeout: Duration,
}

/// One redacted command observation captured from MONITOR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorEvent {
    /// Monotonic per-session sequence number.
    pub sequence: u64,
    /// Server-reported unix timestamp with microseconds, verbatim.
    pub timestamp: String,
    /// Database index the command executed against.
    pub database: i64,
    /// Stable per-session client pseudonym; real addresses are never exposed.
    pub client: String,
    /// The command name, uppercased by the server.
    pub command: Vec<u8>,
    /// Number of arguments observed after the command name.
    pub argument_count: usize,
    /// Binary-safe argument values, present only when the session opted in.
    pub arguments: Option<Vec<Vec<u8>>>,
    /// Buffer residency time when the event was read.
    pub age: Duration,
}

/// Bounds for one finite MONITOR read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonitorReadRequest {
    /// Maximum events removed from the buffer.
    pub max_events: usize,
    /// Maximum raw command and argument bytes removed in this read.
    pub max_bytes: usize,
    /// Finite wait for the first available event.
    pub wait: Duration,
}

impl MonitorReadRequest {
    /// Bound one finite read.
    pub const fn new(max_events: usize, max_bytes: usize, wait: Duration) -> Self {
        Self {
            max_events,
            max_bytes,
            wait,
        }
    }
}

/// Result of one finite MONITOR read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorReadResult {
    /// Events removed from the buffer, oldest first.
    pub events: Vec<MonitorEvent>,
    /// Events still buffered after this read.
    pub remaining_buffered: usize,
    /// Whether the finite wait elapsed without any event.
    pub timed_out: bool,
    /// Events dropped because the buffer was full, since the session started.
    pub dropped_buffer_full_total: u64,
    /// Events dropped because one event exceeded the byte cap.
    pub dropped_oversized_total: u64,
    /// Server lines dropped because they could not be parsed safely.
    pub dropped_unparsed_total: u64,
    /// Whether the dedicated MONITOR connection has ended.
    pub disconnected: bool,
}

/// Host boundary for isolated MONITOR session storage and connection
/// lifecycle.
///
/// Implementations must treat `(owner, session_id)` as the authorization key,
/// return `NotFound` for foreign handles without revealing whether they
/// exist, and make `read` cancellation-safe when its future is dropped.
#[async_trait]
pub trait MonitorSessionManager: Send + Sync + 'static {
    async fn start(
        &self,
        owner: &RedisSessionOwner,
        options: MonitorSessionOptions,
    ) -> Result<MonitorSessionSnapshot, RedisSessionError>;

    async fn read(
        &self,
        owner: &RedisSessionOwner,
        session_id: &str,
        request: MonitorReadRequest,
    ) -> Result<MonitorReadResult, RedisSessionError>;

    async fn close(
        &self,
        owner: &RedisSessionOwner,
        session_id: &str,
    ) -> Result<(), RedisSessionError>;

    /// Close every session owned by one client or principal.
    async fn close_owner(&self, owner: &RedisSessionOwner) -> usize;

    /// Close every managed session and reject future creation.
    async fn shutdown(&self);
}

/// Built-in dedicated-connection MONITOR manager for fixed standalone
/// targets.
///
/// MONITOR is node-local, so Cluster targets must select one node URL
/// explicitly; the manager never fans a stream out across a topology.
#[derive(Clone)]
pub struct DirectRedisMonitorSessions {
    inner: Arc<MonitorManagerInner>,
}

impl fmt::Debug for DirectRedisMonitorSessions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectRedisMonitorSessions")
            .field("limits", &self.inner.limits)
            .finish_non_exhaustive()
    }
}

struct MonitorManagerInner {
    client: Client,
    limits: MonitorSessionLimits,
    sessions: RwLock<HashMap<String, Arc<MonitorSession>>>,
    shutting_down: AtomicBool,
}

struct MonitorSession {
    id: String,
    owner: RedisSessionOwner,
    include_arguments: bool,
    buffer: Arc<MonitorBuffer>,
    pump: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    read_lock: Mutex<()>,
    last_activity: StdMutex<Instant>,
    limits: MonitorSessionLimits,
}

struct BufferedEvent {
    sequence: u64,
    timestamp: String,
    database: i64,
    client: String,
    command: Vec<u8>,
    argument_count: usize,
    arguments: Option<Vec<Vec<u8>>>,
    received_at: Instant,
}

impl BufferedEvent {
    fn raw_bytes(&self) -> usize {
        self.command.len().saturating_add(
            self.arguments
                .as_ref()
                .map_or(0, |arguments| arguments.iter().map(Vec::len).sum()),
        )
    }
}

#[derive(Default)]
struct MonitorBufferState {
    events: VecDeque<BufferedEvent>,
    dropped_buffer_full: u64,
    dropped_oversized: u64,
    dropped_unparsed: u64,
    clients: HashMap<String, usize>,
}

struct MonitorBuffer {
    state: StdMutex<MonitorBufferState>,
    notify: Notify,
    next_sequence: AtomicU64,
    disconnected: AtomicBool,
    include_arguments: bool,
    max_events: usize,
    max_event_bytes: usize,
}

impl MonitorBuffer {
    fn new(limits: MonitorSessionLimits, include_arguments: bool) -> Self {
        Self {
            state: StdMutex::new(MonitorBufferState::default()),
            notify: Notify::new(),
            next_sequence: AtomicU64::new(1),
            disconnected: AtomicBool::new(false),
            include_arguments,
            max_events: limits.max_buffered_events(),
            max_event_bytes: limits.max_event_bytes(),
        }
    }

    fn accept_line(&self, line: &str) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(parsed) = parse_monitor_line(line) else {
            state.dropped_unparsed = state.dropped_unparsed.saturating_add(1);
            return;
        };
        let raw_bytes = parsed
            .command
            .len()
            .saturating_add(parsed.arguments.iter().map(Vec::len).sum::<usize>());
        if raw_bytes > self.max_event_bytes {
            state.dropped_oversized = state.dropped_oversized.saturating_add(1);
            return;
        }
        let next_client = state.clients.len() + 1;
        let client_index = *state
            .clients
            .entry(parsed.client_address)
            .or_insert(next_client);
        if state.events.len() == self.max_events {
            state.events.pop_front();
            state.dropped_buffer_full = state.dropped_buffer_full.saturating_add(1);
        }
        let argument_count = parsed.arguments.len();
        state.events.push_back(BufferedEvent {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            timestamp: parsed.timestamp,
            database: parsed.database,
            client: format!("client-{client_index}"),
            command: parsed.command,
            argument_count,
            arguments: self.include_arguments.then_some(parsed.arguments),
            received_at: Instant::now(),
        });
        drop(state);
        self.notify.notify_one();
    }

    fn mark_disconnected(&self) {
        self.disconnected.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    fn len(&self) -> usize {
        self.state.lock().map_or(0, |state| state.events.len())
    }
}

struct ParsedMonitorLine {
    timestamp: String,
    database: i64,
    client_address: String,
    command: Vec<u8>,
    arguments: Vec<Vec<u8>>,
}

/// Parse one MONITOR line of the form
/// `1700000000.123456 [0 127.0.0.1:51234] "SET" "key" "value"`.
///
/// Lines that do not match the documented shape are rejected rather than
/// partially exposed, so malformed output can never leak unredacted data.
fn parse_monitor_line(line: &str) -> Option<ParsedMonitorLine> {
    let (timestamp, rest) = line.split_once(' ')?;
    if timestamp.is_empty()
        || !timestamp
            .chars()
            .all(|character| character.is_ascii_digit() || character == '.')
    {
        return None;
    }
    let rest = rest.strip_prefix('[')?;
    let (context, rest) = rest.split_once("] ")?;
    let (database, client_address) = context.split_once(' ')?;
    let database = database.parse::<i64>().ok()?;
    let mut values = parse_quoted_values(rest)?;
    if values.is_empty() {
        return None;
    }
    let command = values.remove(0);
    Some(ParsedMonitorLine {
        timestamp: timestamp.to_string(),
        database,
        client_address: client_address.to_string(),
        command,
        arguments: values,
    })
}

/// Parse the space-separated, double-quoted argument rendering MONITOR uses,
/// including `\"`, `\\`, control escapes, and `\xHH` binary escapes.
fn parse_quoted_values(input: &str) -> Option<Vec<Vec<u8>>> {
    let mut values = Vec::new();
    let mut characters = input.chars().peekable();
    loop {
        match characters.next() {
            None => return Some(values),
            Some(' ') => continue,
            Some('"') => {}
            Some(_) => return None,
        }
        let mut value = Vec::new();
        loop {
            match characters.next()? {
                '"' => break,
                '\\' => match characters.next()? {
                    '"' => value.push(b'"'),
                    '\\' => value.push(b'\\'),
                    'n' => value.push(b'\n'),
                    'r' => value.push(b'\r'),
                    't' => value.push(b'\t'),
                    'a' => value.push(0x07),
                    'b' => value.push(0x08),
                    'x' => {
                        let high = characters.next()?.to_digit(16)?;
                        let low = characters.next()?.to_digit(16)?;
                        value.push(((high << 4) | low) as u8);
                    }
                    _ => return None,
                },
                character => {
                    let mut encoded = [0_u8; 4];
                    value.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
                }
            }
        }
        values.push(value);
    }
}

impl MonitorSession {
    fn is_idle_expired(&self, now: Instant) -> bool {
        self.last_activity
            .lock()
            .map(|last| now.saturating_duration_since(*last) >= self.limits.idle_timeout())
            .unwrap_or(false)
    }

    fn touch(&self) {
        if let Ok(mut last) = self.last_activity.lock() {
            *last = Instant::now();
        }
    }

    fn snapshot(&self) -> MonitorSessionSnapshot {
        MonitorSessionSnapshot {
            session_id: self.id.clone(),
            include_arguments: self.include_arguments,
            buffered_events: self.buffer.len(),
            max_buffered_events: self.limits.max_buffered_events(),
            max_event_bytes: self.limits.max_event_bytes(),
            idle_timeout: self.limits.idle_timeout(),
        }
    }

    fn stop_pump(&self) {
        if let Ok(mut pump) = self.pump.lock()
            && let Some(handle) = pump.take()
        {
            handle.abort();
        }
        self.buffer.mark_disconnected();
    }
}

impl DirectRedisMonitorSessions {
    /// Prepare a manager for a fixed standalone Redis URL.
    ///
    /// For Cluster targets, pass the URL of the exact node to observe;
    /// MONITOR is node-local by definition.
    pub fn standalone(url: &str, limits: MonitorSessionLimits) -> Result<Self, RedisSessionError> {
        let limits = limits.validate()?;
        let client = Client::open(url).map_err(|error| {
            RedisSessionError::new(
                RedisSessionErrorKind::InvalidRequest,
                format!("invalid Redis URL: {error}"),
            )
            .with_code("INVALID_REDIS_URL")
        })?;
        let inner = Arc::new(MonitorManagerInner {
            client,
            limits,
            sessions: RwLock::new(HashMap::new()),
            shutting_down: AtomicBool::new(false),
        });
        tokio::spawn(reap_stale_sessions(Arc::downgrade(&inner)));
        Ok(Self { inner })
    }

    async fn owned_session(
        &self,
        owner: &RedisSessionOwner,
        session_id: &str,
    ) -> Result<Arc<MonitorSession>, RedisSessionError> {
        let sessions = self.inner.sessions.read().await;
        sessions
            .get(session_id)
            .filter(|session| &session.owner == owner)
            .cloned()
            .ok_or_else(|| {
                RedisSessionError::new(
                    RedisSessionErrorKind::NotFound,
                    "unknown MONITOR session for this owner",
                )
                .with_code("SESSION_NOT_FOUND")
            })
    }
}

#[async_trait]
impl MonitorSessionManager for DirectRedisMonitorSessions {
    async fn start(
        &self,
        owner: &RedisSessionOwner,
        options: MonitorSessionOptions,
    ) -> Result<MonitorSessionSnapshot, RedisSessionError> {
        if self.inner.shutting_down.load(Ordering::Acquire) {
            return Err(RedisSessionError::new(
                RedisSessionErrorKind::ShuttingDown,
                "the MONITOR session manager is shutting down",
            )
            .with_code("SESSION_MANAGER_SHUTDOWN"));
        }
        {
            let sessions = self.inner.sessions.read().await;
            if sessions.len() >= self.inner.limits.max_sessions() {
                return Err(RedisSessionError::new(
                    RedisSessionErrorKind::Quota,
                    "global MONITOR session quota reached",
                )
                .with_code("SESSION_QUOTA_EXCEEDED"));
            }
            let owned = sessions
                .values()
                .filter(|session| &session.owner == owner)
                .count();
            if owned >= self.inner.limits.max_sessions_per_owner() {
                return Err(RedisSessionError::new(
                    RedisSessionErrorKind::Quota,
                    "per-owner MONITOR session quota reached",
                )
                .with_code("OWNER_SESSION_QUOTA_EXCEEDED"));
            }
        }
        let monitor = tokio::time::timeout(
            self.inner.limits.operation_timeout(),
            self.inner.client.get_async_monitor(),
        )
        .await
        .map_err(|_| {
            RedisSessionError::new(
                RedisSessionErrorKind::Timeout,
                "MONITOR connection setup timed out",
            )
            .with_code("SESSION_SETUP_TIMEOUT")
        })?
        .map_err(|error| {
            RedisSessionError::new(
                RedisSessionErrorKind::Connection,
                format!("MONITOR connection failed: {}", error.category()),
            )
            .with_code("SESSION_CONNECTION_FAILED")
        })?;
        let buffer = Arc::new(MonitorBuffer::new(
            self.inner.limits,
            options.include_arguments,
        ));
        let pump_buffer = buffer.clone();
        let pump = tokio::spawn(async move {
            let mut stream = monitor.into_on_message::<String>();
            while let Some(line) = stream.next().await {
                pump_buffer.accept_line(&line);
            }
            pump_buffer.mark_disconnected();
        });
        let session = Arc::new(MonitorSession {
            id: crate::pubsub_sessions::random_identifier("monitor"),
            owner: owner.clone(),
            include_arguments: options.include_arguments,
            buffer,
            pump: StdMutex::new(Some(pump)),
            read_lock: Mutex::new(()),
            last_activity: StdMutex::new(Instant::now()),
            limits: self.inner.limits,
        });
        let mut sessions = self.inner.sessions.write().await;
        if self.inner.shutting_down.load(Ordering::Acquire) {
            drop(sessions);
            session.stop_pump();
            return Err(RedisSessionError::new(
                RedisSessionErrorKind::ShuttingDown,
                "the MONITOR session manager is shutting down",
            )
            .with_code("SESSION_MANAGER_SHUTDOWN"));
        }
        sessions.insert(session.id.clone(), session.clone());
        drop(sessions);
        Ok(session.snapshot())
    }

    async fn read(
        &self,
        owner: &RedisSessionOwner,
        session_id: &str,
        request: MonitorReadRequest,
    ) -> Result<MonitorReadResult, RedisSessionError> {
        if request.max_events == 0 || request.max_bytes == 0 {
            return Err(RedisSessionError::new(
                RedisSessionErrorKind::InvalidRequest,
                "MONITOR read bounds must be greater than zero",
            )
            .with_code("INVALID_READ_BOUNDS"));
        }
        // Caller values are upper bounds. A manager with tighter resource
        // limits can safely return a smaller page or shorter finite wait.
        let max_events = request
            .max_events
            .min(self.inner.limits.max_buffered_events());
        let max_bytes = request.max_bytes.min(self.inner.limits.max_read_bytes());
        let wait = request.wait.min(self.inner.limits.max_read_duration());
        let session = self.owned_session(owner, session_id).await?;
        session.touch();
        let _read = session.read_lock.lock().await;
        let deadline = Instant::now() + wait;
        loop {
            let notified = session.buffer.notify.notified();
            let maybe_result = {
                let mut state = session.buffer.state.lock().map_err(|_| {
                    RedisSessionError::new(
                        RedisSessionErrorKind::Other,
                        "MONITOR event buffer is unavailable",
                    )
                    .with_code("SESSION_BUFFER_UNAVAILABLE")
                })?;
                let disconnected = session.buffer.disconnected.load(Ordering::Acquire);
                if !state.events.is_empty() || wait.is_zero() || disconnected {
                    let now = Instant::now();
                    let mut bytes = 0_usize;
                    let mut events = Vec::new();
                    while events.len() < max_events {
                        let Some(front) = state.events.front() else {
                            break;
                        };
                        let event_bytes = front.raw_bytes();
                        if !events.is_empty() && bytes.saturating_add(event_bytes) > max_bytes {
                            break;
                        }
                        if events.is_empty() && event_bytes > max_bytes {
                            state.events.pop_front();
                            state.dropped_oversized = state.dropped_oversized.saturating_add(1);
                            continue;
                        }
                        let event = state.events.pop_front().expect("front event exists");
                        bytes = bytes.saturating_add(event_bytes);
                        events.push(MonitorEvent {
                            sequence: event.sequence,
                            timestamp: event.timestamp,
                            database: event.database,
                            client: event.client,
                            command: event.command,
                            argument_count: event.argument_count,
                            arguments: event.arguments,
                            age: now.saturating_duration_since(event.received_at),
                        });
                    }
                    Some(MonitorReadResult {
                        timed_out: events.is_empty() && !disconnected,
                        remaining_buffered: state.events.len(),
                        events,
                        dropped_buffer_full_total: state.dropped_buffer_full,
                        dropped_oversized_total: state.dropped_oversized,
                        dropped_unparsed_total: state.dropped_unparsed,
                        disconnected,
                    })
                } else {
                    None
                }
            };
            if let Some(result) = maybe_result {
                session.touch();
                return Ok(result);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let state = session.buffer.state.lock().map_err(|_| {
                    RedisSessionError::new(
                        RedisSessionErrorKind::Other,
                        "MONITOR event buffer is unavailable",
                    )
                    .with_code("SESSION_BUFFER_UNAVAILABLE")
                })?;
                session.touch();
                return Ok(MonitorReadResult {
                    events: Vec::new(),
                    remaining_buffered: state.events.len(),
                    timed_out: true,
                    dropped_buffer_full_total: state.dropped_buffer_full,
                    dropped_oversized_total: state.dropped_oversized,
                    dropped_unparsed_total: state.dropped_unparsed,
                    disconnected: session.buffer.disconnected.load(Ordering::Acquire),
                });
            }
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    async fn close(
        &self,
        owner: &RedisSessionOwner,
        session_id: &str,
    ) -> Result<(), RedisSessionError> {
        let session = {
            let mut sessions = self.inner.sessions.write().await;
            let owned = sessions
                .get(session_id)
                .is_some_and(|session| &session.owner == owner);
            if !owned {
                return Err(RedisSessionError::new(
                    RedisSessionErrorKind::NotFound,
                    "unknown MONITOR session for this owner",
                )
                .with_code("SESSION_NOT_FOUND"));
            }
            sessions.remove(session_id).expect("checked existence")
        };
        session.stop_pump();
        Ok(())
    }

    async fn close_owner(&self, owner: &RedisSessionOwner) -> usize {
        let removed = {
            let mut sessions = self.inner.sessions.write().await;
            let ids = sessions
                .iter()
                .filter(|(_, session)| &session.owner == owner)
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| sessions.remove(&id))
                .collect::<Vec<_>>()
        };
        let count = removed.len();
        for session in removed {
            session.stop_pump();
        }
        count
    }

    async fn shutdown(&self) {
        self.inner.shutting_down.store(true, Ordering::Release);
        let removed = {
            let mut sessions = self.inner.sessions.write().await;
            sessions
                .drain()
                .map(|(_, session)| session)
                .collect::<Vec<_>>()
        };
        for session in removed {
            session.stop_pump();
        }
    }
}

async fn reap_stale_sessions(inner: Weak<MonitorManagerInner>) {
    loop {
        let Some(inner) = inner.upgrade() else {
            return;
        };
        tokio::time::sleep(inner.limits.cleanup_interval()).await;
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
            session.stop_pump();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_and_binary_monitor_lines() {
        let parsed = parse_monitor_line(
            r#"1700000000.123456 [0 127.0.0.1:51234] "SET" "key" "value with space""#,
        )
        .expect("simple line parses");
        assert_eq!(parsed.timestamp, "1700000000.123456");
        assert_eq!(parsed.database, 0);
        assert_eq!(parsed.client_address, "127.0.0.1:51234");
        assert_eq!(parsed.command, b"SET");
        assert_eq!(
            parsed.arguments,
            vec![b"key".to_vec(), b"value with space".to_vec()]
        );

        let binary =
            parse_monitor_line(r#"1700000000.000001 [2 [::1]:40000] "SET" "bin" "\xff\x00\"\\\n""#)
                .expect("binary line parses");
        assert_eq!(binary.database, 2);
        assert_eq!(binary.client_address, "[::1]:40000");
        assert_eq!(
            binary.arguments,
            vec![b"bin".to_vec(), vec![0xff, 0x00, b'"', b'\\', b'\n']]
        );
    }

    #[test]
    fn rejects_malformed_monitor_lines() {
        assert!(parse_monitor_line("OK").is_none());
        assert!(parse_monitor_line("1700000000.1 [0 addr] unquoted").is_none());
        assert!(parse_monitor_line(r#"ts [0 addr] "SET""#).is_none());
        assert!(parse_monitor_line(r#"1700000000.1 [zero addr] "SET""#).is_none());
        assert!(parse_monitor_line(r#"1700000000.1 [0 addr] "SET" "unterminated"#).is_none());
    }

    #[test]
    fn buffer_redacts_addresses_and_omits_arguments_by_default() {
        let buffer = MonitorBuffer::new(MonitorSessionLimits::default(), false);
        buffer.accept_line(r#"1700000000.1 [0 10.1.2.3:1111] "SET" "key" "secret""#);
        buffer.accept_line(r#"1700000000.2 [0 10.9.9.9:2222] "GET" "key""#);
        buffer.accept_line(r#"1700000000.3 [0 10.1.2.3:1111] "DEL" "key""#);
        let state = buffer.state.lock().expect("buffer state");
        let events = state.events.iter().collect::<Vec<_>>();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].client, "client-1");
        assert_eq!(events[1].client, "client-2");
        assert_eq!(events[2].client, "client-1");
        assert_eq!(events[0].argument_count, 2);
        assert!(events[0].arguments.is_none());
        assert_eq!(events[0].command, b"SET");
    }

    #[test]
    fn buffer_drops_oldest_oversized_and_unparsed_with_counters() {
        let limits = MonitorSessionLimits::default()
            .with_max_buffered_events(2)
            .with_max_event_bytes(16);
        let buffer = MonitorBuffer::new(limits, true);
        buffer.accept_line(r#"1.0 [0 a:1] "GET" "one""#);
        buffer.accept_line(r#"2.0 [0 a:1] "GET" "two""#);
        buffer.accept_line(r#"3.0 [0 a:1] "GET" "three""#);
        buffer.accept_line(&format!(r#"4.0 [0 a:1] "SET" "k" "{}""#, "x".repeat(64)));
        buffer.accept_line("not a monitor line");
        let state = buffer.state.lock().expect("buffer state");
        assert_eq!(state.events.len(), 2);
        assert_eq!(state.events[0].sequence, 2);
        assert_eq!(state.dropped_buffer_full, 1);
        assert_eq!(state.dropped_oversized, 1);
        assert_eq!(state.dropped_unparsed, 1);
    }

    #[test]
    fn limits_reject_zero_and_inverted_values() {
        let zero = MonitorSessionLimits::default()
            .with_max_event_bytes(0)
            .validate()
            .expect_err("zero event bytes");
        assert_eq!(zero.code(), Some("INVALID_SESSION_LIMITS"));
        let inverted = MonitorSessionLimits::default()
            .with_max_sessions(1)
            .with_max_sessions_per_owner(2)
            .validate()
            .expect_err("per-owner above global");
        assert_eq!(inverted.code(), Some("INVALID_SESSION_LIMITS"));
    }
}
