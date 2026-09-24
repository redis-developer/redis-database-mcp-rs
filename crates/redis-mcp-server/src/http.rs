//! Secure Streamable HTTP serving for the standalone server.

use std::{
    future::{Future, IntoFuture},
    sync::atomic::{AtomicU64, Ordering},
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
use tower_mcp::{HttpTransport, ProtocolSupport};
use tracing::{info, warn};

use crate::{
    config::{HttpConfig, SecretString},
    runtime::{ServerRuntime, ServerSessions},
};

static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);
const CLIENT_ID_HEADER: &str = "x-redis-mcp-client-id";

#[derive(Clone)]
struct RequestPolicy {
    bearer_token: Option<SecretString>,
    request_timeout: std::time::Duration,
    sessions: ServerSessions,
}

impl RequestPolicy {
    fn new(config: &HttpConfig, sessions: ServerSessions) -> Self {
        Self {
            bearer_token: config.bearer_token.clone(),
            request_timeout: config.request_timeout,
            sessions,
        }
    }
}

/// Serve the runtime until Ctrl-C, then stop accepting connections, bound the
/// drain of existing HTTP streams, and close every Redis-backed session.
pub(crate) async fn run(
    runtime: ServerRuntime,
    config: &HttpConfig,
) -> Result<(), tower_mcp::BoxError> {
    serve_with_shutdown(runtime, config, async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "cannot listen for Ctrl-C; stopping HTTP server");
        }
    })
    .await
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
    let transport = HttpTransport::new(runtime.router)
        .protocol_support(protocols)
        .allowed_origins(config.allowed_origins.clone())
        // tower-mcp accepts arbitrary non-local Host values when its list is
        // empty for backwards compatibility. Supplying the bind address makes
        // the safe loopback default fail closed against DNS rebinding.
        .allowed_hosts(effective_allowed_hosts(config))
        .max_body_size(config.max_body_bytes)
        .max_sessions(config.max_sessions)
        .session_ttl(config.session_ttl)
        .bridge_extension::<PubSubSessionOwner>();

    let policy = RequestPolicy::new(config, sessions.clone());
    let app = transport
        .into_router_at("/mcp")
        .layer(middleware::from_fn_with_state(policy, enforce_policy))
        .layer(ConcurrencyLimitLayer::new(config.max_concurrency));

    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .map_err(|error| format!("cannot bind MCP HTTP listener: {error}"))?;
    info!(address = %config.bind, endpoint = "/mcp", "MCP HTTP transport listening");
    let result = drain_server(listener, app, shutdown, config.drain_timeout).await;
    sessions.shutdown().await;
    result
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
    if let Some(expected) = &policy.bearer_token {
        let supplied = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        let authenticated = supplied
            .is_some_and(|value| bool::from(expected.expose().as_bytes().ct_eq(value.as_bytes())));
        if !authenticated {
            return (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                "bearer authentication required",
            )
                .into_response();
        }
    }

    // Legacy sessions use the opaque MCP session ID as stable owner material.
    // Final-protocol requests have no session ID, so a bearer-authenticated
    // client supplies its own stable, non-secret client ID. Anonymous final
    // requests receive a one-request identity and therefore cannot inherit or
    // reuse another caller's stateful handles.
    let mcp_session_id = request
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty());
    let owner = if let Some(session_id) = mcp_session_id {
        session_owner(policy.bearer_token.as_ref(), session_id)
    } else {
        let client_id = request
            .headers()
            .get(CLIENT_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty() && value.len() <= 128);
        match (&policy.bearer_token, client_id) {
            (Some(token), Some(client_id)) => principal_owner(token, client_id),
            _ => fresh_owner("anonymous-request"),
        }
    };
    request.extensions_mut().insert(owner.clone());

    let is_delete = *request.method() == Method::DELETE;
    let response = if matches!(*request.method(), Method::POST | Method::DELETE) {
        match tokio::time::timeout(policy.request_timeout, next.run(request)).await {
            Ok(response) => response,
            Err(_) => (StatusCode::REQUEST_TIMEOUT, "request timed out").into_response(),
        }
    } else {
        next.run(request).await
    };
    if is_delete {
        policy.sessions.close_owner(&owner).await;
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

fn principal_owner(token: &SecretString, client_id: &str) -> PubSubSessionOwner {
    let mut hash = Sha256::new();
    hash.update(token.expose().as_bytes());
    hash.update([0]);
    hash.update(client_id.as_bytes());
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
            bearer_token: None,
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
    fn authenticated_client_ids_are_stable_and_isolated() {
        let token = SecretString::new("secret".to_string()).unwrap();
        assert_eq!(
            principal_owner(&token, "agent-a"),
            principal_owner(&token, "agent-a")
        );
        assert_ne!(
            principal_owner(&token, "agent-a"),
            principal_owner(&token, "agent-b")
        );
        assert!(
            !principal_owner(&token, "agent-a")
                .as_str()
                .contains("secret")
        );
        assert!(
            !principal_owner(&token, "agent-a")
                .as_str()
                .contains("agent-a")
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
}
