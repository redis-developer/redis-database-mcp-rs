//! Secure Streamable HTTP serving for the standalone server.

use std::{
    collections::{HashMap, HashSet},
    future::{Future, IntoFuture},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Router,
    extract::{Request, State},
    http::{Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use redis_mcp::PubSubSessionOwner;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tower::limit::ConcurrencyLimitLayer;
use tower_mcp::{HttpTransport, ProtocolSupport, SessionHandle, transport::http::SessionConfig};
use tracing::{info, warn};

use crate::{
    config::{HttpConfig, SecretString},
    runtime::{ServerRuntime, ServerSessions},
};

static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct RequestPolicy {
    bearer_tokens: Arc<[SecretString]>,
    request_timeout: std::time::Duration,
    sessions: ServerSessions,
    transport_sessions: SessionHandle,
    session_owners: Arc<tokio::sync::Mutex<HashMap<String, PubSubSessionOwner>>>,
}

impl RequestPolicy {
    fn new(
        config: &HttpConfig,
        sessions: ServerSessions,
        transport_sessions: SessionHandle,
        session_owners: Arc<tokio::sync::Mutex<HashMap<String, PubSubSessionOwner>>>,
    ) -> Self {
        Self {
            bearer_tokens: config.bearer_tokens.clone().into(),
            request_timeout: config.request_timeout,
            sessions,
            transport_sessions,
            session_owners,
        }
    }
}

/// Serve the runtime until Ctrl-C or SIGTERM, then stop accepting connections,
/// bound the drain of existing HTTP streams, and close every Redis-backed
/// session.
pub(crate) async fn run(
    runtime: ServerRuntime,
    config: &HttpConfig,
) -> Result<(), tower_mcp::BoxError> {
    serve_with_shutdown(runtime, config, shutdown_signal()).await
}

#[cfg(unix)]
async fn shutdown_signal() {
    let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
    match terminate {
        Ok(mut terminate) => {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    if let Err(error) = result {
                        warn!(%error, "cannot listen for Ctrl-C; stopping HTTP server");
                    }
                }
                _ = terminate.recv() => {}
            }
        }
        Err(error) => {
            warn!(%error, "cannot listen for SIGTERM; waiting for Ctrl-C only");
            if let Err(error) = tokio::signal::ctrl_c().await {
                warn!(%error, "cannot listen for Ctrl-C; stopping HTTP server");
            }
        }
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!(%error, "cannot listen for Ctrl-C; stopping HTTP server");
    }
}

async fn serve_with_shutdown<F>(
    runtime: ServerRuntime,
    config: &HttpConfig,
    shutdown: F,
) -> Result<(), tower_mcp::BoxError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let protocols = ProtocolSupport::try_new(["2025-11-25", "2026-07-28"])?;
    let sessions = runtime.sessions;
    let cleanup_interval = session_cleanup_interval(config.session_ttl);
    let session_config = SessionConfig::with_ttl(config.session_ttl)
        .max_sessions(config.max_sessions)
        .cleanup_interval(cleanup_interval);
    let transport = HttpTransport::new(runtime.router)
        .protocol_support(protocols)
        .allowed_origins(config.allowed_origins.clone())
        // tower-mcp accepts arbitrary non-local Host values when its list is
        // empty for backwards compatibility. Supplying the bind address makes
        // the safe loopback default fail closed against DNS rebinding.
        .allowed_hosts(effective_allowed_hosts(config))
        .max_body_size(config.max_body_bytes)
        .session_config(session_config)
        .bridge_extension::<PubSubSessionOwner>();

    let session_owners = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let (app, session_handle) = transport.into_router_at_with_handle("/mcp");
    let policy = RequestPolicy::new(
        config,
        sessions.clone(),
        session_handle.clone(),
        session_owners.clone(),
    );
    let app = app
        .layer(ConcurrencyLimitLayer::new(config.max_concurrency))
        // Added last so authentication happens before admission and the
        // request timeout includes any wait for a concurrency permit.
        .layer(middleware::from_fn_with_state(policy, enforce_policy));

    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .map_err(|error| format!("cannot bind MCP HTTP listener: {error}"))?;
    info!(address = %config.bind, endpoint = "/mcp", "MCP HTTP transport listening");
    let (reaper_stop, reaper_stopped) = tokio::sync::oneshot::channel();
    let reaper = tokio::spawn(reap_expired_owners(
        session_handle,
        sessions.clone(),
        session_owners,
        cleanup_interval,
        reaper_stopped,
    ));
    let result = drain_server(listener, app, shutdown, config.drain_timeout).await;
    let _ = reaper_stop.send(());
    let _ = reaper.await;
    sessions.shutdown().await;
    result
}

fn session_cleanup_interval(ttl: std::time::Duration) -> std::time::Duration {
    (ttl / 4).clamp(
        std::time::Duration::from_millis(10),
        std::time::Duration::from_secs(60),
    )
}

async fn reap_expired_owners(
    session_handle: SessionHandle,
    sessions: ServerSessions,
    session_owners: Arc<tokio::sync::Mutex<HashMap<String, PubSubSessionOwner>>>,
    interval: std::time::Duration,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = &mut stop => return,
        }

        // Hold the binding lock while taking the transport snapshot so a
        // freshly bound live session cannot be mistaken for an expired one.
        let expired = {
            let mut owners = session_owners.lock().await;
            let live = session_handle
                .list_sessions()
                .await
                .into_iter()
                .map(|session| session.id)
                .collect::<HashSet<_>>();
            let mut expired = Vec::new();
            owners.retain(|session_id, owner| {
                if live.contains(session_id) {
                    true
                } else {
                    expired.push(owner.clone());
                    false
                }
            });
            expired
        };
        for owner in expired {
            sessions.close_owner(&owner).await;
        }
    }
}

async fn drain_server<F>(
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: F,
    drain_timeout: std::time::Duration,
) -> Result<(), tower_mcp::BoxError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (draining_tx, mut draining_rx) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.await;
            let _ = draining_tx.send(());
        })
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => {
            result.map_err(|error| format!("MCP HTTP server failed: {error}").into())
        }
        _ = &mut draining_rx => {
            match tokio::time::timeout(drain_timeout, &mut server).await {
                Ok(result) => result.map_err(|error| format!("MCP HTTP server failed: {error}").into()),
                Err(_) => {
                    warn!(?drain_timeout, "HTTP graceful drain timed out");
                    Ok(())
                }
            }
        }
    }
}

async fn enforce_policy(
    State(policy): State<RequestPolicy>,
    mut request: Request,
    next: Next,
) -> Response {
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let mut authenticated_token = None;
    if let Some(supplied) = supplied {
        // Check the entire configured allowlist rather than returning on the
        // first match. The matching credential is also the final-protocol
        // principal, so callers that need isolation receive distinct tokens.
        for expected in policy.bearer_tokens.iter() {
            if bool::from(expected.expose().as_bytes().ct_eq(supplied.as_bytes())) {
                authenticated_token = Some(expected.clone());
            }
        }
    }
    if !policy.bearer_tokens.is_empty() && authenticated_token.is_none() {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            "bearer authentication required",
        )
            .into_response();
    }

    // Legacy sessions use the opaque MCP session ID as stable owner material.
    // Final-protocol requests have no session ID, so a configured bearer token
    // is the stable authenticated principal. Anonymous final requests receive
    // a one-request identity and therefore cannot inherit or reuse another
    // caller's stateful handles.
    let mcp_session_id = request
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let owner = if let Some(session_id) = mcp_session_id.as_deref() {
        let owner = session_owner(authenticated_token.as_ref(), session_id);
        let mut owners = policy.session_owners.lock().await;
        match owners.get(session_id) {
            Some(existing) if existing != &owner => {
                return (
                    StatusCode::FORBIDDEN,
                    "MCP session belongs to a different authenticated principal",
                )
                    .into_response();
            }
            Some(_) => {}
            None => {
                // Unknown IDs are left to the transport to reject and are not
                // retained in the owner map. This prevents authenticated junk
                // IDs from growing state outside the configured session cap.
                let is_live = policy
                    .transport_sessions
                    .list_sessions()
                    .await
                    .iter()
                    .any(|session| session.id == session_id);
                if is_live {
                    owners.insert(session_id.to_string(), owner.clone());
                }
            }
        }
        owner
    } else {
        authenticated_token
            .as_ref()
            .map_or_else(|| fresh_owner("anonymous-request"), principal_owner)
    };
    request.extensions_mut().insert(owner.clone());

    let method = request.method().clone();
    let is_delete = method == Method::DELETE;
    let mut response = if is_delete {
        let delete = async {
            let response = next.run(request).await;
            if response.status().is_success() {
                // Keep the transport binding until Redis cleanup succeeds.
                // If the outer timeout cancels cleanup, the TTL reaper will
                // observe the deleted transport session and retry it.
                policy.sessions.close_owner(&owner).await;
                if let Some(session_id) = mcp_session_id.as_deref() {
                    policy.session_owners.lock().await.remove(session_id);
                }
            }
            response
        };
        match tokio::time::timeout(policy.request_timeout, delete).await {
            Ok(response) => response,
            Err(_) => (StatusCode::REQUEST_TIMEOUT, "request timed out").into_response(),
        }
    } else if method == Method::POST {
        match tokio::time::timeout(policy.request_timeout, next.run(request)).await {
            Ok(response) => response,
            Err(_) => (StatusCode::REQUEST_TIMEOUT, "request timed out").into_response(),
        }
    } else {
        next.run(request).await
    };
    if !is_delete && mcp_session_id.is_none() {
        // Bind a newly created legacy session to the principal that performed
        // initialization before its ID is returned to the caller.
        let created_session_id = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        if let Some(session_id) = created_session_id {
            let created_owner = session_owner(authenticated_token.as_ref(), &session_id);
            let mut owners = policy.session_owners.lock().await;
            if owners
                .get(&session_id)
                .is_some_and(|existing| existing != &created_owner)
            {
                policy
                    .transport_sessions
                    .terminate_session(&session_id)
                    .await;
                response = (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "MCP session identity collision",
                )
                    .into_response();
            } else {
                owners.insert(session_id, created_owner);
            }
        }
    }
    response
}

fn effective_allowed_hosts(config: &HttpConfig) -> Vec<String> {
    if config.allowed_hosts.is_empty() {
        vec![config.bind.to_string()]
    } else {
        config.allowed_hosts.clone()
    }
}

fn fresh_owner(kind: &str) -> PubSubSessionOwner {
    let id = NEXT_OWNER.fetch_add(1, Ordering::Relaxed);
    PubSubSessionOwner::new(format!("http-{kind}-{}-{id}", std::process::id()))
        .expect("generated HTTP owner is non-empty and bounded")
}

fn principal_owner(token: &SecretString) -> PubSubSessionOwner {
    let mut hash = Sha256::new();
    hash.update(token.expose().as_bytes());
    let digest = hash.finalize();
    PubSubSessionOwner::new(format!("http-principal:{}", hex::encode(digest)))
        .expect("SHA-256 HTTP owner is non-empty and bounded")
}

fn session_owner(token: Option<&SecretString>, session_id: &str) -> PubSubSessionOwner {
    let mut hash = Sha256::new();
    if let Some(token) = token {
        hash.update(token.expose().as_bytes());
    }
    hash.update([0]);
    hash.update(session_id.as_bytes());
    let digest = hash.finalize();
    PubSubSessionOwner::new(format!("http-session:{}", hex::encode(digest)))
        .expect("SHA-256 HTTP session owner is non-empty and bounded")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        DEFAULT_HTTP_BIND, DEFAULT_HTTP_DRAIN_TIMEOUT, DEFAULT_HTTP_MAX_BODY_BYTES,
        DEFAULT_HTTP_MAX_CONCURRENCY, DEFAULT_HTTP_MAX_SESSIONS, DEFAULT_HTTP_REQUEST_TIMEOUT,
        DEFAULT_HTTP_SESSION_TTL,
    };

    fn config() -> HttpConfig {
        HttpConfig {
            bind: DEFAULT_HTTP_BIND,
            allow_remote: false,
            bearer_tokens: Vec::new(),
            allowed_hosts: Vec::new(),
            allowed_origins: Vec::new(),
            max_body_bytes: DEFAULT_HTTP_MAX_BODY_BYTES,
            max_concurrency: DEFAULT_HTTP_MAX_CONCURRENCY,
            max_sessions: DEFAULT_HTTP_MAX_SESSIONS,
            session_ttl: DEFAULT_HTTP_SESSION_TTL,
            request_timeout: DEFAULT_HTTP_REQUEST_TIMEOUT,
            drain_timeout: DEFAULT_HTTP_DRAIN_TIMEOUT,
        }
    }

    #[test]
    fn loopback_host_policy_is_not_left_open() {
        assert_eq!(effective_allowed_hosts(&config()), vec!["127.0.0.1:8080"]);
    }

    #[test]
    fn configured_hosts_are_preserved() {
        let mut config = config();
        config.allowed_hosts = vec!["redis.example:8443".to_string()];
        assert_eq!(effective_allowed_hosts(&config), config.allowed_hosts);
    }

    #[test]
    fn authenticated_principals_are_stable_and_isolated() {
        let token_a = SecretString::new("agent-a-secret".to_string()).unwrap();
        let token_b = SecretString::new("agent-b-secret".to_string()).unwrap();
        assert_eq!(principal_owner(&token_a), principal_owner(&token_a));
        assert_ne!(principal_owner(&token_a), principal_owner(&token_b));
        assert!(
            !principal_owner(&token_a)
                .as_str()
                .contains("agent-a-secret")
        );
    }

    #[test]
    fn legacy_session_ids_are_stable_and_isolated() {
        let token = SecretString::new("secret".to_string()).unwrap();
        assert_eq!(
            session_owner(Some(&token), "session-a"),
            session_owner(Some(&token), "session-a")
        );
        assert_ne!(
            session_owner(Some(&token), "session-a"),
            session_owner(Some(&token), "session-b")
        );
    }

    #[test]
    fn cleanup_interval_scales_down_for_short_session_ttls() {
        assert_eq!(
            session_cleanup_interval(std::time::Duration::from_millis(100)),
            std::time::Duration::from_millis(25)
        );
        assert_eq!(
            session_cleanup_interval(std::time::Duration::from_millis(1)),
            std::time::Duration::from_millis(10)
        );
        assert_eq!(
            session_cleanup_interval(std::time::Duration::from_secs(3600)),
            std::time::Duration::from_secs(60)
        );
    }
}
