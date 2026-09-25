use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use redis_mcp::{
    AccessMode, CapabilityStatus, DirectRedis, DirectRedisBlocking, DirectRedisMonitorSessions,
    DirectRedisPubSubSessionManager, DirectRedisTransactions, MonitorSessionLimits,
    NativeRedisInvocation, OutputBudget, PubSubReadRequest, PubSubSessionLimits,
    PubSubSessionManager, PubSubSessionOwner, PubSubSubscriptionKind, RawCommandPolicy,
    RedisCommand, RedisDeployment, RedisError, RedisExecutor, RedisInvocationEngine, RedisMcp,
    RedisModule, RedisTransactionEngine, RedisTransactionLimits, RedisTransactionOutcome,
    RedisTransactionRequest, RedisValue, RedisVersion, ToolBundle, UnavailableToolPolicy,
};
use tower_mcp::client::{ChannelTransport, McpClient};

#[cfg(unix)]
use redis_mcp::RedisErrorKind;
#[cfg(unix)]
use redis_server_wrapper::{
    Delay, Direction, Error as RedisServerError, FaultProxy, RedisServer, RedisServerHandle,
};

struct TestRedis {
    url: String,
    #[cfg(unix)]
    _managed: Option<ManagedRedis>,
}

impl TestRedis {
    async fn start() -> Option<Self> {
        if let Ok(url) = std::env::var("REDIS_URL") {
            return Some(Self {
                url,
                #[cfg(unix)]
                _managed: None,
            });
        }

        #[cfg(unix)]
        {
            match ManagedRedis::start().await {
                Ok(managed) => Some(Self {
                    url: managed.url(),
                    _managed: Some(managed),
                }),
                Err(RedisServerError::BinaryNotFound { binary }) => {
                    eprintln!(
                        "skipping live Redis test: REDIS_URL is not set and {binary} is not on PATH"
                    );
                    None
                }
                Err(error) => panic!("start wrapper-managed Redis: {error}"),
            }
        }

        #[cfg(not(unix))]
        {
            eprintln!(
                "skipping live Redis test: REDIS_URL is not set and self-hosting requires Unix"
            );
            None
        }
    }
}

#[cfg(unix)]
struct ManagedRedis {
    _server: RedisServerHandle,
    _directory: tempfile::TempDir,
    port: u16,
}

#[cfg(unix)]
impl ManagedRedis {
    async fn start() -> Result<Self, RedisServerError> {
        let directory = tempfile::tempdir().expect("create Redis test directory");
        let server = RedisServer::new()
            .auto_port()
            .bind("127.0.0.1")
            .dir(directory.path())
            .no_stack_modules()
            .start()
            .await?;
        let port = server.port();
        Ok(Self {
            _server: server,
            _directory: directory,
            port,
        })
    }

    fn url(&self) -> String {
        format!("redis://127.0.0.1:{}/", self.port)
    }
}

fn test_key(suffix: &str) -> String {
    format!("redis-mcp:test:{}:{suffix}", std::process::id())
}

fn with_protocol(url: &str, protocol: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}protocol={protocol}")
}

async fn router_client(url: &str, access: AccessMode) -> McpClient {
    router_client_with_timeout(url, access, Duration::from_secs(30)).await
}

async fn router_client_with_timeout(
    url: &str,
    access: AccessMode,
    command_timeout: Duration,
) -> McpClient {
    let executor = DirectRedis::connect(url).await.expect("connect to Redis");
    let router = RedisMcp::builder(executor)
        .access(access)
        .raw_commands(access == AccessMode::Full)
        .command_timeout(command_timeout)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect MCP client");
    client
        .initialize("redis-mcp-live-test", "0")
        .await
        .expect("initialize MCP client");
    client
}

async fn router_client_with_budget(
    url: &str,
    access: AccessMode,
    output_budget: OutputBudget,
) -> McpClient {
    let executor = DirectRedis::connect(url).await.expect("connect to Redis");
    let router = RedisMcp::builder(executor)
        .access(access)
        .raw_commands(access == AccessMode::Full)
        .output_budget(output_budget)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect budget MCP client");
    client
        .initialize("redis-mcp-live-budget-test", "0")
        .await
        .expect("initialize budget MCP client");
    client
}

async fn pubsub_session_router_client(
    url: &str,
    manager: DirectRedisPubSubSessionManager,
) -> McpClient {
    let executor = DirectRedis::connect(url).await.expect("connect to Redis");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::ReadWrite)
        .pubsub_sessions(manager)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect Pub/Sub session MCP client");
    client
        .initialize("redis-mcp-live-pubsub-session-test", "0")
        .await
        .expect("initialize Pub/Sub session MCP client");
    client
}

async fn capability_router_client(
    url: &str,
    access: AccessMode,
) -> (McpClient, redis_mcp::RedisCapabilities) {
    let executor = DirectRedis::connect(url).await.expect("connect to Redis");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover Redis capabilities");
    let router = RedisMcp::builder(executor)
        .access(access)
        .raw_commands(access == AccessMode::Full)
        .capabilities(capabilities.clone())
        .unavailable_tool_policy(UnavailableToolPolicy::Hide)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect capability-aware MCP client");
    client
        .initialize("redis-mcp-live-capability-test", "0")
        .await
        .expect("initialize capability-aware MCP client");
    (client, capabilities)
}

async fn module_router_client(url: &str) -> McpClient {
    let executor = DirectRedis::connect(url).await.expect("connect to Redis");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::ReadOnly)
        .bundles([ToolBundle::Json, ToolBundle::Search])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect module MCP client");
    client
        .initialize("redis-mcp-module-absence-test", "0")
        .await
        .expect("initialize module MCP client");
    client
}

async fn scripting_router_client(
    url: &str,
    access: AccessMode,
) -> (McpClient, redis_mcp::RedisCapabilities) {
    let executor = DirectRedis::connect(url)
        .await
        .expect("connect scripting executor");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover scripting capabilities");
    let router = RedisMcp::builder(executor)
        .access(access)
        .bundles([ToolBundle::Scripting])
        .capabilities(capabilities.clone())
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect scripting MCP client");
    client
        .initialize("redis-mcp-live-scripting-test", "0")
        .await
        .expect("initialize scripting MCP client");
    (client, capabilities)
}

async fn admin_router_client(url: &str) -> McpClient {
    let executor = DirectRedis::connect(url)
        .await
        .expect("connect admin executor");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover admin capabilities");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Admin])
        .capabilities(capabilities)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect admin MCP client");
    client
        .initialize("redis-mcp-live-admin-test", "0")
        .await
        .expect("initialize admin MCP client");
    client
}

async fn coordination_router_client(url: &str, namespace: &str) -> McpClient {
    let executor = DirectRedis::connect(url)
        .await
        .expect("connect coordination executor");
    let blocking =
        DirectRedisBlocking::standalone(url).expect("prepare coordination blocking executor");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Coordination])
        .coordination_config(
            redis_mcp::CoordinationConfig::default()
                .with_namespace(namespace)
                .with_shards(4),
        )
        .blocking(blocking)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect coordination MCP client");
    client
        .initialize("redis-mcp-live-coordination-test", "0")
        .await
        .expect("initialize coordination MCP client");
    client
}

async fn coordination_router_client_with_executor(
    executor: impl RedisExecutor,
    namespace: &str,
) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Coordination])
        .coordination_config(
            redis_mcp::CoordinationConfig::default()
                .with_namespace(namespace)
                .with_shards(4),
        )
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect coordination MCP client");
    client
        .initialize("redis-mcp-live-coordination-test", "0")
        .await
        .expect("initialize coordination MCP client");
    client
}

#[derive(Clone)]
struct AmbiguousAfterSuccess<E> {
    inner: E,
    target_tool: &'static str,
    fail_once: Arc<AtomicBool>,
}

impl<E> AmbiguousAfterSuccess<E> {
    fn new(inner: E, target_tool: &'static str) -> Self {
        Self {
            inner,
            target_tool,
            fail_once: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[async_trait]
impl<E: RedisExecutor> RedisExecutor for AmbiguousAfterSuccess<E> {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let target = command.tool_name() == self.target_tool && command.name() == "EVAL";
        let result = self.inner.execute(command).await?;
        if target && self.fail_once.swap(false, Ordering::SeqCst) {
            Err(RedisError::new(
                RedisErrorKind::Connection,
                "simulated connection loss after Redis committed the command",
            ))
        } else {
            Ok(result)
        }
    }
}

async fn call_structured(
    client: &McpClient,
    tool: &'static str,
    input: serde_json::Value,
) -> serde_json::Value {
    let result = client
        .call_tool(tool, input)
        .await
        .unwrap_or_else(|error| panic!("{tool}: {error}"));
    assert!(!result.is_error, "{tool}: {result:?}");
    result
        .structured_content
        .unwrap_or_else(|| panic!("{tool}: missing structured content"))
}

#[cfg(unix)]
#[tokio::test]
async fn durable_handoff_lifecycle_is_idempotent_owned_and_recoverable() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let namespace = format!("coordtest{}", std::process::id());
    let producer = coordination_router_client(&redis.url, &namespace).await;
    let worker_a = coordination_router_client(&redis.url, &namespace).await;
    let worker_b = coordination_router_client(&redis.url, &namespace).await;

    let publish_input = serde_json::json!({
        "capability": "triage",
        "idempotency_key": "incident-42",
        "payload": {"type": "json", "value": {"incident": 42}},
        "metadata": {"severity": "high"},
        "correlation_id": "trace-42"
    });
    let published =
        call_structured(&producer, "redis_handoff_publish", publish_input.clone()).await;
    assert_eq!(published["created"], true);
    let handle = published["handle"].as_str().unwrap().to_string();
    let shard = published["shard"].as_u64().unwrap();

    let replayed = call_structured(&producer, "redis_handoff_publish", publish_input).await;
    assert_eq!(replayed["created"], false);
    assert_eq!(replayed["handle"], handle);

    let claimed = call_structured(
        &worker_a,
        "redis_handoff_claim",
        serde_json::json!({"capability": "triage", "shard": shard, "worker": "worker-a"}),
    )
    .await;
    assert_eq!(claimed["claimed"], true);
    assert_eq!(claimed["handoff"]["handle"], handle);

    let producer_status = call_structured(
        &producer,
        "redis_handoff_status",
        serde_json::json!({"handle": handle}),
    )
    .await;
    assert_eq!(producer_status["status"], "claimed");
    assert_eq!(producer_status["caller_is_publisher"], true);

    tokio::time::sleep(Duration::from_millis(5)).await;
    let recovered = call_structured(
        &worker_b,
        "redis_handoff_recover",
        serde_json::json!({
            "capability": "triage",
            "shard": shard,
            "worker": "worker-b",
            "min_idle_ms": 1
        }),
    )
    .await;
    assert_eq!(recovered["claimed"], true);
    assert_eq!(recovered["handoff"]["handle"], handle);

    let resource_uri = published["resource_uri"].as_str().unwrap();
    let stale_resource = worker_a
        .read_resource(resource_uri)
        .await
        .expect_err("stale claimant cannot read the handoff resource");
    assert!(
        stale_resource
            .to_string()
            .contains("publisher or current claimant"),
        "{stale_resource}"
    );

    let stale_completion = worker_a
        .call_tool(
            "redis_handoff_complete",
            serde_json::json!({
                "handle": handle,
                "idempotency_key": "stale-completion",
                "result": {"type": "text", "value": "stale"}
            }),
        )
        .await
        .expect("stale completion returns a tool result");
    assert!(stale_completion.is_error);
    let stale_error = serde_json::to_string(&stale_completion).expect("serialize ownership error");
    assert!(stale_error.contains("[Authorization]"), "{stale_error}");
    assert!(stale_error.contains("HANDOFF_NOT_OWNED"), "{stale_error}");

    let completion_input = serde_json::json!({
        "handle": handle,
        "idempotency_key": "completion-42",
        "result": {"type": "json", "value": {"resolution": "restarted"}}
    });
    let completed = call_structured(
        &worker_b,
        "redis_handoff_complete",
        completion_input.clone(),
    )
    .await;
    assert_eq!(completed["status"], "completed");
    assert_eq!(completed["acknowledged"], true);
    assert_eq!(completed["result"]["value"]["resolution"], "restarted");

    let completion_replay = call_structured(
        &worker_b,
        "redis_handoff_complete",
        serde_json::json!({
            "handle": handle,
            "idempotency_key": "completion-42",
            "result": {"type": "text", "value": "must-not-replace-committed-result"}
        }),
    )
    .await;
    assert_eq!(completion_replay["status"], "completed");
    assert_eq!(completion_replay["result"]["type"], "json");
    assert_eq!(
        completion_replay["result"]["value"]["resolution"],
        "restarted"
    );

    let final_status = call_structured(
        &producer,
        "redis_handoff_status",
        serde_json::json!({"handle": handle, "max_events": 10}),
    )
    .await;
    assert_eq!(final_status["status"], "completed");
    assert_eq!(final_status["timeline"].as_array().unwrap().len(), 4);
    let resource = producer
        .read_resource(resource_uri)
        .await
        .expect("publisher reads authorized handoff resource");
    let content = serde_json::to_value(resource.contents.first().expect("resource content"))
        .expect("serialize handoff resource content");
    assert_eq!(content["mimeType"], "application/json");
    let body: serde_json::Value = serde_json::from_str(content["text"].as_str().unwrap())
        .expect("handoff resource is JSON text");
    assert_eq!(body["status"], "completed");

    let empty = call_structured(
        &worker_b,
        "redis_handoff_claim",
        serde_json::json!({
            "capability": "triage",
            "shard": shard,
            "worker": "worker-b",
            "wait_ms": 1
        }),
    )
    .await;
    assert_eq!(empty["claimed"], false);

    for invalid in [
        serde_json::json!({
            "capability": "triage",
            "shard": shard,
            "worker": "worker-b",
            "wait_ms": 5_001
        }),
        serde_json::json!({
            "capability": "triage",
            "shard": 16,
            "worker": "worker-b"
        }),
    ] {
        let result = worker_b
            .call_tool("redis_handoff_claim", invalid)
            .await
            .expect("invalid claim returns a tool result");
        assert!(
            result.is_error,
            "invalid claim unexpectedly succeeded: {result:?}"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn durable_handoff_rejects_valid_envelopes_that_cannot_fit_claim_output() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let namespace = format!("coordbudget{}", std::process::id());
    let executor = DirectRedis::connect(&redis.url)
        .await
        .expect("connect coordination budget executor");
    let blocking = DirectRedisBlocking::standalone(&redis.url)
        .expect("prepare coordination budget blocking executor");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Coordination])
        .coordination_config(
            redis_mcp::CoordinationConfig::default()
                .with_namespace(&namespace)
                .with_shards(4),
        )
        .output_budget(OutputBudget::new(8 * 1024, 100))
        .blocking(blocking)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect coordination budget MCP client");
    client
        .initialize("redis-mcp-live-coordination-budget-test", "0")
        .await
        .expect("initialize coordination budget MCP client");

    let rejected = client
        .call_tool(
            "redis_handoff_publish",
            serde_json::json!({
                "capability": "bounded",
                "idempotency_key": "boundary",
                "payload": {"type": "text", "value": "x".repeat(6 * 1024)}
            }),
        )
        .await
        .expect("oversized prospective response returns a tool result");
    assert!(rejected.is_error, "{rejected:?}");
    assert!(
        serde_json::to_string(&rejected)
            .expect("serialize prospective response error")
            .contains("future claim response")
    );

    // The same idempotency key must still create a new handoff: the rejected
    // envelope was validated before XADD/HSET/SET changed Redis.
    let published = call_structured(
        &client,
        "redis_handoff_publish",
        serde_json::json!({
            "capability": "bounded",
            "idempotency_key": "boundary",
            "payload": {"type": "text", "value": "x".repeat(512)}
        }),
    )
    .await;
    assert_eq!(published["created"], true);
    let claimed = call_structured(
        &client,
        "redis_handoff_claim",
        serde_json::json!({
            "capability": "bounded",
            "shard": published["shard"],
            "worker": "budget-worker"
        }),
    )
    .await;
    assert_eq!(claimed["claimed"], true);
}

#[cfg(unix)]
#[tokio::test]
async fn durable_handoff_acl_is_least_privilege_and_redacts_denied_payloads() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let version = DirectRedis::connect(&redis.url)
        .await
        .expect("connect for coordination ACL discovery")
        .discover_capabilities()
        .await
        .expect("discover coordination ACL capabilities")
        .redis_version()
        .expect("coordination ACL Redis reports its version");
    if version < RedisVersion::new(6, 2, 0) {
        eprintln!("skipping coordination ACL test on Redis {version}");
        return;
    }

    let namespace = format!("coordacl{}", std::process::id());
    let allowed_user = format!("coord_allowed_{}", std::process::id());
    let denied_user = format!("coord_denied_{}", std::process::id());
    let password = "coordination-acl-secret";
    let admin = redis::Client::open(redis.url.as_str()).expect("open coordination ACL admin");
    let mut connection = admin
        .get_multiplexed_async_connection()
        .await
        .expect("connect coordination ACL admin");
    let required_commands = [
        "eval",
        "get",
        "set",
        "xgroup",
        "xadd",
        "hset",
        "hincrby",
        "xreadgroup",
        "exists",
        "hget",
        "xack",
        "hgetall",
        "xrevrange",
        "xautoclaim",
    ];
    let mut create_allowed = redis::cmd("ACL");
    create_allowed
        .arg("SETUSER")
        .arg(&allowed_user)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg("~rmcp:*");
    for command in required_commands {
        create_allowed.arg(format!("+{command}"));
    }
    create_allowed
        .query_async::<()>(&mut connection)
        .await
        .expect("create least-privilege coordination ACL user");
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&denied_user)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg("~rmcp:*")
        .arg("+eval")
        .arg("+get")
        .query_async::<()>(&mut connection)
        .await
        .expect("create denied coordination ACL user");

    let restricted_url = |username: &str| {
        let mut url = redis::parse_redis_url(&redis.url).expect("parse coordination ACL URL");
        url.set_username(username)
            .expect("set coordination ACL username");
        url.set_password(Some(password))
            .expect("set coordination ACL password");
        url
    };
    let allowed_url = restricted_url(&allowed_user);
    let producer = coordination_router_client(allowed_url.as_str(), &namespace).await;
    let worker = coordination_router_client(allowed_url.as_str(), &namespace).await;
    let published = call_structured(
        &producer,
        "redis_handoff_publish",
        serde_json::json!({
            "capability": "acl_work",
            "idempotency_key": "acl-job-1",
            "payload": {"type": "text", "value": "allowed"}
        }),
    )
    .await;
    let claimed = call_structured(
        &worker,
        "redis_handoff_claim",
        serde_json::json!({
            "capability": "acl_work",
            "shard": published["shard"],
            "worker": "acl_worker"
        }),
    )
    .await;
    assert_eq!(claimed["claimed"], true);

    let denied_url = restricted_url(&denied_user);
    let denied_client = coordination_router_client(denied_url.as_str(), &namespace).await;
    let secret_payload = "payload-that-must-not-leak";
    let denied = denied_client
        .call_tool(
            "redis_handoff_publish",
            serde_json::json!({
                "capability": "acl_work",
                "idempotency_key": "acl-denied-job",
                "payload": {"type": "text", "value": secret_payload}
            }),
        )
        .await
        .expect("ACL denial is represented as a tool result");
    assert!(denied.is_error);
    let denied = serde_json::to_string(&denied).expect("serialize coordination ACL denial");
    assert!(denied.contains("Authorization"), "{denied}");
    assert!(!denied.contains(password));
    assert!(!denied.contains(secret_payload));

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&allowed_user)
        .arg(&denied_user)
        .query_async::<()>(&mut connection)
        .await
        .expect("delete coordination ACL users");
}

#[cfg(unix)]
#[tokio::test]
async fn durable_handoff_retries_resolve_ambiguous_publish_and_completion() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let namespace = format!("coordambiguous{}", std::process::id());
    let producer_executor = AmbiguousAfterSuccess::new(
        DirectRedis::connect(&redis.url)
            .await
            .expect("connect ambiguous publish executor"),
        "redis_handoff_publish",
    );
    let producer = coordination_router_client_with_executor(producer_executor, &namespace).await;
    let publish_input = serde_json::json!({
        "capability": "ambiguous_work",
        "idempotency_key": "ambiguous-publish-1",
        "payload": {"type": "text", "value": "work"}
    });
    let first_publish = producer
        .call_tool("redis_handoff_publish", publish_input.clone())
        .await
        .expect("ambiguous publish returns a tool result");
    assert!(first_publish.is_error);
    let published = call_structured(&producer, "redis_handoff_publish", publish_input).await;
    assert_eq!(published["created"], false);

    let worker_executor = AmbiguousAfterSuccess::new(
        DirectRedis::connect(&redis.url)
            .await
            .expect("connect ambiguous completion executor"),
        "redis_handoff_complete",
    );
    let worker = coordination_router_client_with_executor(worker_executor, &namespace).await;
    let claimed = call_structured(
        &worker,
        "redis_handoff_claim",
        serde_json::json!({
            "capability": "ambiguous_work",
            "shard": published["shard"],
            "worker": "ambiguous_worker"
        }),
    )
    .await;
    let completion_input = serde_json::json!({
        "handle": claimed["handoff"]["handle"],
        "idempotency_key": "ambiguous-completion-1",
        "result": {"type": "json", "value": {"ok": true}}
    });
    let first_completion = worker
        .call_tool("redis_handoff_complete", completion_input.clone())
        .await
        .expect("ambiguous completion returns a tool result");
    assert!(first_completion.is_error);
    let completed = call_structured(&worker, "redis_handoff_complete", completion_input).await;
    assert_eq!(completed["status"], "completed");
    assert_eq!(completed["acknowledged"], true);
    assert_eq!(completed["result"]["value"]["ok"], true);

    let status = call_structured(
        &producer,
        "redis_handoff_status",
        serde_json::json!({"handle": published["handle"]}),
    )
    .await;
    assert_eq!(status["status"], "completed");
    assert_eq!(status["attempts"], 1);
    assert_eq!(status["timeline"].as_array().unwrap().len(), 3);
}

#[cfg(unix)]
#[tokio::test]
async fn guarded_admin_configuration_and_flush_run_only_on_an_isolated_server() {
    let managed = match ManagedRedis::start().await {
        Ok(managed) => managed,
        Err(RedisServerError::BinaryNotFound { binary }) => {
            eprintln!("skipping isolated admin test: {binary} is not on PATH");
            return;
        }
        Err(error) => panic!("start isolated admin Redis: {error}"),
    };
    let url = managed.url();
    let client = admin_router_client(&url).await;

    let before = call_structured(
        &client,
        "redis_config_get",
        serde_json::json!({"parameters": ["latency-monitor-threshold"]}),
    )
    .await;
    assert!(
        before["values"]["latency-monitor-threshold"]["value"]
            .as_str()
            .is_some()
    );

    let changed = call_structured(
        &client,
        "redis_config_set",
        serde_json::json!({
            "parameter": "latency-monitor-threshold",
            "value": 25,
            "confirm_service_impact": true,
            "max_cluster_nodes": 1
        }),
    )
    .await;
    assert_eq!(changed["scope"], "standalone");
    let after = call_structured(
        &client,
        "redis_config_get",
        serde_json::json!({"parameters": ["latency-monitor-threshold"]}),
    )
    .await;
    assert_eq!(after["values"]["latency-monitor-threshold"]["value"], "25");

    let redis = redis::Client::open(url.as_str()).expect("open isolated Redis client");
    let mut connection = redis
        .get_multiplexed_async_connection()
        .await
        .expect("connect isolated Redis client");
    let _: () = redis::cmd("SET")
        .arg("admin:isolated:one")
        .arg("one")
        .query_async(&mut connection)
        .await
        .expect("seed first isolated key");
    let _: () = redis::cmd("SET")
        .arg("admin:isolated:two")
        .arg("two")
        .query_async(&mut connection)
        .await
        .expect("seed second isolated key");

    let flushed = call_structured(
        &client,
        "redis_flush",
        serde_json::json!({
            "scope": "database",
            "mode": "sync",
            "confirmation": "FLUSHDB",
            "max_cluster_nodes": 1
        }),
    )
    .await;
    assert_eq!(flushed["scope"], "standalone");
    let remaining: i64 = redis::cmd("DBSIZE")
        .query_async(&mut connection)
        .await
        .expect("read isolated database size");
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn direct_adapter_discovers_bounded_standalone_capabilities() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let executor = DirectRedis::connect(&redis.url)
        .await
        .expect("connect for capability discovery");
    let capabilities = executor
        .discover_capabilities_with_timeout(Duration::from_secs(2))
        .await
        .expect("discover standalone capabilities");

    assert!(capabilities.redis_version().is_some());
    assert_eq!(capabilities.deployment(), RedisDeployment::Standalone);
    assert_eq!(capabilities.command("PING"), CapabilityStatus::Available);
    #[cfg(unix)]
    if redis._managed.is_some() {
        assert_eq!(
            capabilities.module(RedisModule::Json).status(),
            CapabilityStatus::Unavailable
        );
        assert_eq!(
            capabilities.module(RedisModule::Search).status(),
            CapabilityStatus::Unavailable
        );
    }
}

#[tokio::test]
async fn missing_module_commands_return_actionable_errors() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let client = module_router_client(&redis.url).await;

    for (tool, input, capability) in [
        (
            "redis_json_get",
            serde_json::json!({"key": test_key("missing-json")}),
            "RedisJSON",
        ),
        ("redis_ft_list", serde_json::json!({}), "Redis Query Engine"),
    ] {
        let result = client
            .call_tool(tool, input)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        if !result.is_error {
            eprintln!("{capability} is present on the configured Redis; absence assertion skipped");
            continue;
        }
        let text = serde_json::to_string(&result).expect("serialize module error");
        assert!(text.contains("ModuleUnavailable"), "{tool}: {text}");
        assert!(text.contains(capability), "{tool}: {text}");
    }
}

#[tokio::test]
async fn live_redis_round_trip_through_router() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let url = redis.url;

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&url, protocol), AccessMode::Full).await;
        let key = test_key(&format!("router:{protocol}"));
        let set = client
            .call_tool(
                "redis_set",
                serde_json::json!({
                    "key": key,
                    "value": "hello",
                    "expiration": {"type": "seconds", "value": 60}
                }),
            )
            .await
            .expect("set key");
        assert_eq!(set.structured_content.as_ref().unwrap()["applied"], true);

        let get = client
            .call_tool("redis_get", serde_json::json!({"key": key}))
            .await
            .expect("get key");
        let get = get.structured_content.expect("structured GET result");
        assert_eq!(get["exists"], true);
        assert_eq!(get["value"], "hello");
        assert_eq!(get["encoding"], "utf8");

        let delete = client
            .call_tool("redis_del", serde_json::json!({"keys": [key]}))
            .await
            .expect("delete key");
        assert_eq!(delete.structured_content.as_ref().unwrap()["deleted"], 1);
    }
}

#[tokio::test]
async fn live_scripting_is_binary_safe_bounded_and_lifecycle_aware_in_resp2_and_resp3() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let isolated = {
        #[cfg(unix)]
        {
            redis._managed.is_some()
        }
        #[cfg(not(unix))]
        {
            false
        }
    };

    for protocol in ["resp2", "resp3"] {
        let (client, capabilities) =
            scripting_router_client(&with_protocol(&redis.url, protocol), AccessMode::Full).await;
        let version = capabilities
            .redis_version()
            .expect("live Redis reports a version");
        let read_eval_tool = if version >= RedisVersion::new(7, 0, 0) {
            "redis_eval_ro"
        } else {
            "redis_eval"
        };
        let key = test_key(&format!("script:{protocol}"));
        let script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
        let binary = vec![0xff, 0x00, 0x01];

        let eval = call_structured(
            &client,
            "redis_eval",
            serde_json::json!({
                "script": {"value": script},
                "keys": [{"value": key}],
                "arguments": [{"value": BASE64.encode(&binary), "encoding": "base64"}]
            }),
        )
        .await;
        assert_eq!(eval["result"]["encoding"], "base64");
        assert_eq!(eval["result"]["value"], BASE64.encode(&binary));

        let load = call_structured(
            &client,
            "redis_script_load",
            serde_json::json!({"script": {"value": "return ARGV[1]"}}),
        )
        .await;
        let sha1 = load["result"]["value"]
            .as_str()
            .expect("SCRIPT LOAD SHA1")
            .to_string();
        assert_eq!(sha1.len(), 40);
        let exists = call_structured(
            &client,
            "redis_script_exists",
            serde_json::json!({"sha1": [sha1]}),
        )
        .await;
        assert_eq!(exists["result"][0], 1);
        let evalsha = call_structured(
            &client,
            "redis_evalsha",
            serde_json::json!({
                "sha1": sha1,
                "arguments": [{"value": BASE64.encode(&binary), "encoding": "base64"}]
            }),
        )
        .await;
        assert_eq!(evalsha["result"]["value"], BASE64.encode(&binary));

        let cache_miss = client
            .call_tool(
                "redis_evalsha",
                serde_json::json!({"sha1": "0000000000000000000000000000000000000000"}),
            )
            .await
            .expect("NOSCRIPT is a tool result");
        assert!(cache_miss.is_error, "{cache_miss:?}");

        let malformed = client
            .call_tool(
                read_eval_tool,
                serde_json::json!({"script": {"value": "this is not valid lua"}}),
            )
            .await
            .expect("malformed Lua is a tool result");
        assert!(malformed.is_error, "{malformed:?}");

        call_structured(
            &client,
            "redis_eval",
            serde_json::json!({
                "script": {"value": "redis.call('DEL', KEYS[1]); return redis.call('LPUSH', KEYS[1], ARGV[1])"},
                "keys": [{"value": key}],
                "arguments": [{"value": "not-a-string"}]
            }),
        )
        .await;
        let wrong_type = client
            .call_tool(
                read_eval_tool,
                serde_json::json!({
                    "script": {"value": "return redis.call('GET', KEYS[1])"},
                    "keys": [{"value": key}]
                }),
            )
            .await
            .expect("scripting WRONGTYPE is a tool result");
        assert!(wrong_type.is_error, "{wrong_type:?}");
        assert!(
            serde_json::to_string(&wrong_type)
                .expect("serialize scripting WRONGTYPE")
                .contains("WRONGTYPE")
        );

        let large_result = client
            .call_tool(
                read_eval_tool,
                serde_json::json!({
                    "script": {"value": "return string.rep('x', 300000)"}
                }),
            )
            .await
            .expect("large scripting response is a tool result");
        assert!(large_result.is_error, "{large_result:?}");
        assert_eq!(
            large_result
                .meta
                .as_ref()
                .expect("scripting output metadata")["io.redis.mcp/outputLimit"]["code"],
            "output_limit_exceeded"
        );

        if isolated {
            call_structured(
                &client,
                "redis_script_flush",
                serde_json::json!({"mode": "sync"}),
            )
            .await;
            let missing_after_flush = call_structured(
                &client,
                "redis_script_exists",
                serde_json::json!({"sha1": [sha1]}),
            )
            .await;
            assert_eq!(missing_after_flush["result"][0], 0);

            let idle_kill = client
                .call_tool("redis_script_kill", serde_json::json!({}))
                .await
                .expect("idle SCRIPT KILL is a tool result");
            assert!(idle_kill.is_error, "{idle_kill:?}");
        }

        if version < RedisVersion::new(7, 0, 0) {
            continue;
        }

        let library = format!("agentlib_{}_{}", std::process::id(), protocol);
        let function = format!("echo_{}_{}", std::process::id(), protocol);
        let library_code = format!(
            "#!lua name={library}\nredis.register_function{{function_name='{function}', callback=function(keys, args) return args[1] end, flags={{'no-writes'}}}}"
        );
        let loaded = call_structured(
            &client,
            "redis_function_load",
            serde_json::json!({"library_code": {"value": library_code}}),
        )
        .await;
        assert_eq!(loaded["result"]["value"], library);

        let fcall = call_structured(
            &client,
            "redis_fcall_ro",
            serde_json::json!({
                "function": function,
                "arguments": [{"value": BASE64.encode(&binary), "encoding": "base64"}]
            }),
        )
        .await;
        assert_eq!(fcall["result"]["encoding"], "base64");
        assert_eq!(fcall["result"]["value"], BASE64.encode(&binary));

        let listed = call_structured(
            &client,
            "redis_function_list",
            serde_json::json!({"library_name": library}),
        )
        .await;
        assert!(
            listed["result"]
                .as_array()
                .is_some_and(|values| !values.is_empty())
        );
        let stats = call_structured(&client, "redis_function_stats", serde_json::json!({})).await;
        assert!(!stats["result"].is_null());

        if isolated {
            let dump = call_structured(
                &client,
                "redis_function_dump",
                serde_json::json!({"max_bytes": 1048576}),
            )
            .await;
            let payload = dump["result"].clone();
            call_structured(
                &client,
                "redis_function_delete",
                serde_json::json!({"library_name": library}),
            )
            .await;
            call_structured(
                &client,
                "redis_function_restore",
                serde_json::json!({"payload": payload, "policy": "append"}),
            )
            .await;

            let idle_kill = client
                .call_tool("redis_function_kill", serde_json::json!({}))
                .await
                .expect("idle FUNCTION KILL is a tool result");
            assert!(idle_kill.is_error, "{idle_kill:?}");
            call_structured(
                &client,
                "redis_function_flush",
                serde_json::json!({"mode": "sync"}),
            )
            .await;
            let listed = call_structured(
                &client,
                "redis_function_list",
                serde_json::json!({"library_name": library}),
            )
            .await;
            assert_eq!(listed["result"], serde_json::json!([]));
        } else {
            call_structured(
                &client,
                "redis_function_delete",
                serde_json::json!({"library_name": library}),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn redis_eight_modern_surface_is_version_gated_and_live_in_resp2_and_resp3() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let (capability_client, capabilities) =
        capability_router_client(&redis.url, AccessMode::Full).await;
    let version = capabilities
        .redis_version()
        .expect("live Redis reports its version");
    let listed = capability_client
        .list_tools()
        .await
        .expect("list capability-filtered tools")
        .tools
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect::<std::collections::BTreeSet<_>>();

    for (minimum, tools) in [
        (
            RedisVersion::new(8, 0, 0),
            &["redis_vadd", "redis_vsim", "redis_hgetex", "redis_hsetex"][..],
        ),
        (
            RedisVersion::new(8, 2, 0),
            &["redis_vismember", "redis_xackdel", "redis_xdelex"][..],
        ),
        (
            RedisVersion::new(8, 4, 0),
            &[
                "redis_delex",
                "redis_digest",
                "redis_msetex",
                "redis_vrange",
            ][..],
        ),
        (
            RedisVersion::new(8, 8, 0),
            &[
                "redis_arcount",
                "redis_arset",
                "redis_increx",
                "redis_xnack",
            ][..],
        ),
        (
            RedisVersion::new(8, 10, 0),
            &["redis_lmovem", "redis_sdiffcard", "redis_sunioncard"][..],
        ),
    ] {
        for tool in tools {
            assert_eq!(
                listed.contains(*tool),
                version >= minimum,
                "{tool} visibility on Redis {version}"
            );
        }
    }

    if version < RedisVersion::new(8, 0, 0) {
        return;
    }

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&redis.url, protocol), AccessMode::Full).await;
        let prefix = test_key(&format!("redis-eight:{protocol}"));
        let vector = format!("{prefix}:vectors");
        let hash = format!("{prefix}:hash");
        let string = format!("{prefix}:string");
        let number = format!("{prefix}:number");
        let array = format!("{prefix}:array");
        let ring = format!("{prefix}:ring");
        let stream = format!("{prefix}:stream");
        let list_source = format!("{prefix}:list-source");
        let list_destination = format!("{prefix}:list-destination");
        let mset_one = format!("{prefix}:mset-one");
        let mset_two = format!("{prefix}:mset-two");

        let added = call_structured(
            &client,
            "redis_vadd",
            serde_json::json!({
                "key": vector,
                "vector": {"type": "values", "values": [1, 0]},
                "element": {"value": "alpha"},
                "attributes": {"kind": "primary"}
            }),
        )
        .await;
        assert_eq!(added["changed"], true, "{protocol}");
        call_structured(
            &client,
            "redis_vadd",
            serde_json::json!({
                "key": vector,
                "vector": {"type": "values", "values": [0, 1]},
                "element": {"value": "beta"}
            }),
        )
        .await;
        let cardinality =
            call_structured(&client, "redis_vcard", serde_json::json!({"key": vector})).await;
        assert_eq!(cardinality["cardinality"], 2, "{protocol}");
        let dimensions =
            call_structured(&client, "redis_vdim", serde_json::json!({"key": vector})).await;
        assert_eq!(dimensions["dimensions"], 2, "{protocol}");
        let embedding = call_structured(
            &client,
            "redis_vemb",
            serde_json::json!({"key": vector, "element": {"value": "alpha"}}),
        )
        .await;
        assert_eq!(embedding["exists"], true, "{protocol}");
        assert_eq!(embedding["dimensions"], 2, "{protocol}");
        let attributes = call_structured(
            &client,
            "redis_vgetattr",
            serde_json::json!({"key": vector, "element": {"value": "alpha"}}),
        )
        .await;
        assert_eq!(attributes["attributes"]["kind"], "primary", "{protocol}");
        call_structured(&client, "redis_vinfo", serde_json::json!({"key": vector})).await;
        call_structured(
            &client,
            "redis_vlinks",
            serde_json::json!({"key": vector, "element": {"value": "alpha"}, "with_scores": true}),
        )
        .await;
        call_structured(
            &client,
            "redis_vrandmember",
            serde_json::json!({"key": vector, "count": 2}),
        )
        .await;
        call_structured(
            &client,
            "redis_vsim",
            serde_json::json!({
                "key": vector,
                "query": {"type": "element", "element": {"value": "alpha"}},
                "with_scores": true,
                "count": 2
            }),
        )
        .await;
        let attribute_changed = call_structured(
            &client,
            "redis_vsetattr",
            serde_json::json!({
                "key": vector,
                "element": {"value": "alpha"},
                "attributes": {"kind": "updated"}
            }),
        )
        .await;
        assert_eq!(attribute_changed["changed"], true, "{protocol}");

        let hash_set = call_structured(
            &client,
            "redis_hsetex",
            serde_json::json!({
                "key": hash,
                "expiration": {"type": "seconds", "value": 60},
                "fields": [
                    {"field": {"value": "one"}, "value": {"value": "first"}},
                    {"field": {"value": "two"}, "value": {"value": "second"}}
                ]
            }),
        )
        .await;
        assert_eq!(hash_set["applied"], true, "{protocol}");
        let hash_get = call_structured(
            &client,
            "redis_hgetex",
            serde_json::json!({
                "key": hash,
                "expiration": {"type": "milliseconds", "value": 60000},
                "fields": [{"value": "one"}, {"value": "two"}]
            }),
        )
        .await;
        assert_eq!(hash_get["values"][0]["value"], "first", "{protocol}");
        let hash_deleted = call_structured(
            &client,
            "redis_hgetdel",
            serde_json::json!({"key": hash, "fields": [{"value": "two"}]}),
        )
        .await;
        assert_eq!(hash_deleted["values"][0]["value"], "second", "{protocol}");

        if version >= RedisVersion::new(8, 2, 0) {
            let membership = call_structured(
                &client,
                "redis_vismember",
                serde_json::json!({"key": vector, "element": {"value": "alpha"}}),
            )
            .await;
            assert_eq!(membership["exists"], true, "{protocol}");
        }

        if version >= RedisVersion::new(8, 4, 0) {
            call_structured(
                &client,
                "redis_set",
                serde_json::json!({"key": string, "value": "delete-me"}),
            )
            .await;
            let digest =
                call_structured(&client, "redis_digest", serde_json::json!({"key": string})).await;
            assert_eq!(digest["exists"], true, "{protocol}");
            assert_eq!(digest["algorithm"], "xxh3-64-hex", "{protocol}");
            let digest_value = digest["digest"]
                .as_str()
                .expect("DIGEST returns a hex string");
            assert_eq!(digest_value.len(), 16, "{protocol}");
            let deleted = call_structured(
                &client,
                "redis_delex",
                serde_json::json!({
                    "key": string,
                    "condition": {"type": "digest_equals", "digest": digest_value}
                }),
            )
            .await;
            assert_eq!(deleted["deleted"], true, "{protocol}");
            let mset = call_structured(
                &client,
                "redis_msetex",
                serde_json::json!({
                    "entries": [
                        {"key": {"value": mset_one}, "value": {"value": "one"}},
                        {"key": {"value": mset_two}, "value": {"value": "two"}}
                    ],
                    "expiration": {"type": "seconds", "value": 60}
                }),
            )
            .await;
            assert_eq!(mset["applied"], true, "{protocol}");
            let range = call_structured(
                &client,
                "redis_vrange",
                serde_json::json!({
                    "key": vector,
                    "start": {"value": "-"},
                    "end": {"value": "+"},
                    "count": 1
                }),
            )
            .await;
            assert_eq!(range["count"], 1, "{protocol}");
            assert_eq!(range["complete"], false, "{protocol}");
            assert!(range["next_start"].is_object(), "{protocol}");
        }

        for (milliseconds, value) in [(1, "one"), (2, "two"), (3, "three")] {
            call_structured(
                &client,
                "redis_xadd",
                serde_json::json!({
                    "key": stream,
                    "id": {"type": "explicit", "id": {"milliseconds": milliseconds, "sequence": 0}},
                    "fields": [{"field": "value", "value": value}]
                }),
            )
            .await;
        }
        call_structured(
            &client,
            "redis_xgroup_create",
            serde_json::json!({
                "key": stream,
                "group": {"value": "workers"},
                "id": {"type": "beginning"}
            }),
        )
        .await;
        call_structured(
            &client,
            "redis_xreadgroup",
            serde_json::json!({
                "group": {"value": "workers"},
                "consumer": {"value": "worker-1"},
                "streams": [{"key": stream, "offset": {"type": "new"}}],
                "count": 3
            }),
        )
        .await;
        if version >= RedisVersion::new(8, 2, 0) {
            let acked = call_structured(
                &client,
                "redis_xackdel",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": "workers"},
                    "reference_policy": "delete_references",
                    "ids": [{"milliseconds": 2, "sequence": 0}]
                }),
            )
            .await;
            assert_eq!(acked["results"].as_array().unwrap().len(), 1, "{protocol}");
            let deleted = call_structured(
                &client,
                "redis_xdelex",
                serde_json::json!({
                    "key": stream,
                    "reference_policy": "delete_references",
                    "ids": [{"milliseconds": 3, "sequence": 0}]
                }),
            )
            .await;
            assert_eq!(
                deleted["results"].as_array().unwrap().len(),
                1,
                "{protocol}"
            );
        }

        if version >= RedisVersion::new(8, 8, 0) {
            let incremented = call_structured(
                &client,
                "redis_increx",
                serde_json::json!({
                    "key": number,
                    "increment": {"type": "integer", "value": "2"},
                    "upper_bound": "10",
                    "expiration": {"type": "seconds", "value": 60}
                }),
            )
            .await;
            assert_eq!(incremented["value"], "2", "{protocol}");

            let set = call_structured(
                &client,
                "redis_arset",
                serde_json::json!({
                    "key": array,
                    "index": 0,
                    "values": [
                        {"value": "alpha"},
                        {"value": "beta"},
                        {"value": "alphabet"}
                    ]
                }),
            )
            .await;
            assert_eq!(set["new_slots"], 3, "{protocol}");
            call_structured(
                &client,
                "redis_armset",
                serde_json::json!({
                    "key": array,
                    "entries": [{"index": 5, "value": {"value": "gamma"}}]
                }),
            )
            .await;
            assert_eq!(
                call_structured(&client, "redis_arcount", serde_json::json!({"key": array})).await
                    ["count"],
                4,
                "{protocol}"
            );
            assert_eq!(
                call_structured(&client, "redis_arlen", serde_json::json!({"key": array})).await["length"],
                6,
                "{protocol}"
            );
            assert_eq!(
                call_structured(
                    &client,
                    "redis_arget",
                    serde_json::json!({"key": array, "index": 1})
                )
                .await["value"]["value"],
                "beta",
                "{protocol}"
            );
            call_structured(
                &client,
                "redis_argetrange",
                serde_json::json!({"key": array, "start": 0, "end": 5}),
            )
            .await;
            call_structured(
                &client,
                "redis_armget",
                serde_json::json!({"key": array, "indices": [0, 5, 4]}),
            )
            .await;
            let grep = call_structured(
                &client,
                "redis_argrep",
                serde_json::json!({
                    "key": array,
                    "start": "-",
                    "end": "+",
                    "predicates": [{"type": "match", "value": {"value": "alpha"}}],
                    "with_values": true,
                    "limit": 10
                }),
            )
            .await;
            assert_eq!(grep["count"], 2, "{protocol}");
            call_structured(
                &client,
                "redis_arinfo",
                serde_json::json!({"key": array, "full": true}),
            )
            .await;
            let used = call_structured(
                &client,
                "redis_arop",
                serde_json::json!({
                    "key": array,
                    "start": 0,
                    "end": 5,
                    "operation": {"type": "used"}
                }),
            )
            .await;
            assert_eq!(used["result"], 4, "{protocol}");
            let scan = call_structured(
                &client,
                "redis_arscan",
                serde_json::json!({"key": array, "start": 0, "end": 5, "limit": 10}),
            )
            .await;
            assert_eq!(scan["count"], 4, "{protocol}");
            call_structured(
                &client,
                "redis_arseek",
                serde_json::json!({"key": array, "index": 6}),
            )
            .await;
            let inserted = call_structured(
                &client,
                "redis_arinsert",
                serde_json::json!({"key": array, "values": [{"value": "delta"}]}),
            )
            .await;
            assert_eq!(inserted["last_index"], 6, "{protocol}");
            call_structured(
                &client,
                "redis_arlastitems",
                serde_json::json!({"key": array, "count": 2, "reverse": true}),
            )
            .await;
            let next =
                call_structured(&client, "redis_arnext", serde_json::json!({"key": array})).await;
            assert_eq!(next["next_index"], 7, "{protocol}");
            let ring_insert = call_structured(
                &client,
                "redis_arring",
                serde_json::json!({
                    "key": ring,
                    "size": 3,
                    "values": [
                        {"value": "one"}, {"value": "two"},
                        {"value": "three"}, {"value": "four"}
                    ]
                }),
            )
            .await;
            assert_eq!(ring_insert["last_index"], 0, "{protocol}");
            call_structured(
                &client,
                "redis_ardel",
                serde_json::json!({"key": array, "indices": [1]}),
            )
            .await;
            call_structured(
                &client,
                "redis_ardelrange",
                serde_json::json!({"key": array, "ranges": [{"start": 2, "end": 5}]}),
            )
            .await;

            let nacked = call_structured(
                &client,
                "redis_xnack",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": "workers"},
                    "mode": "silent",
                    "ids": [{"milliseconds": 1, "sequence": 0}],
                    "retry_count": 2
                }),
            )
            .await;
            assert_eq!(nacked["released"], 1, "{protocol}");
        }

        if version >= RedisVersion::new(8, 10, 0) {
            call_structured(
                &client,
                "redis_rpush",
                serde_json::json!({
                    "key": list_source,
                    "elements": [
                        {"value": "one"}, {"value": "two"}, {"value": "three"}
                    ]
                }),
            )
            .await;
            let moved = call_structured(
                &client,
                "redis_lmovem",
                serde_json::json!({
                    "source": {"value": list_source},
                    "destination": {"value": list_destination},
                    "from": "right",
                    "to": "left",
                    "amount": {"type": "exactly", "count": 2, "ordering": "bulk"}
                }),
            )
            .await;
            assert_eq!(moved["count"], 2, "{protocol}");
        }

        let removed = call_structured(
            &client,
            "redis_vrem",
            serde_json::json!({"key": vector, "element": {"value": "beta"}}),
        )
        .await;
        assert_eq!(removed["changed"], true, "{protocol}");
        call_structured(
            &client,
            "redis_del",
            serde_json::json!({
                "keys": [
                    vector, hash, string, number, array, ring, stream,
                    list_source, list_destination, mset_one, mset_two
                ]
            }),
        )
        .await;
    }
}

#[tokio::test]
async fn live_pubsub_publish_and_inspection_are_binary_safe_in_resp2_and_resp3() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let capabilities = DirectRedis::connect(&redis.url)
        .await
        .expect("connect for Pub/Sub capability discovery")
        .discover_capabilities()
        .await
        .expect("discover Pub/Sub capabilities");

    for protocol in ["resp2", "resp3"] {
        let url = with_protocol(&redis.url, protocol);
        let redis_client = redis::Client::open(url.as_str()).expect("open Pub/Sub subscriber");
        let mut subscriber = redis_client
            .get_async_pubsub()
            .await
            .expect("connect Pub/Sub subscriber");
        let mut channel = test_key(&format!("pubsub:{protocol}")).into_bytes();
        channel.extend([0xff, 0x00]);
        subscriber
            .subscribe(channel.clone())
            .await
            .expect("subscribe to binary channel");
        let pattern = test_key(&format!("pubsub-pattern:{protocol}:*"));
        subscriber
            .psubscribe(pattern)
            .await
            .expect("subscribe to pattern");

        let client = router_client(&url, AccessMode::ReadWrite).await;
        let channel_base64 = BASE64.encode(&channel);
        let channels = client
            .call_tool(
                "redis_pubsub_channels",
                serde_json::json!({
                    "pattern": {"value": channel_base64, "encoding": "base64"},
                    "limit": 4
                }),
            )
            .await
            .expect("PUBSUB CHANNELS")
            .structured_content
            .expect("structured PUBSUB CHANNELS");
        assert_eq!(channels["count"], 1, "{protocol}");
        assert_eq!(
            channels["channels"][0]["value"], channel_base64,
            "{protocol}"
        );
        assert_eq!(channels["channels"][0]["encoding"], "base64", "{protocol}");

        let counts = client
            .call_tool(
                "redis_pubsub_numsub",
                serde_json::json!({
                    "channels": [{"value": channel_base64, "encoding": "base64"}]
                }),
            )
            .await
            .expect("PUBSUB NUMSUB")
            .structured_content
            .expect("structured PUBSUB NUMSUB");
        assert_eq!(counts["counts"][0]["subscribers"], 1, "{protocol}");

        let patterns = client
            .call_tool("redis_pubsub_numpat", serde_json::json!({}))
            .await
            .expect("PUBSUB NUMPAT")
            .structured_content
            .expect("structured PUBSUB NUMPAT");
        assert!(
            patterns["pattern_count"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );

        let published = client
            .call_tool(
                "redis_publish",
                serde_json::json!({
                    "channel": {"value": channel_base64, "encoding": "base64"},
                    "message": {"value": "/gE=", "encoding": "base64"}
                }),
            )
            .await
            .expect("binary PUBLISH")
            .structured_content
            .expect("structured binary PUBLISH");
        assert_eq!(published["receivers"], 1, "{protocol}");
        assert_eq!(published["delivery"], "global", "{protocol}");

        if capabilities
            .redis_version()
            .is_some_and(|version| version >= RedisVersion::new(7, 0, 0))
        {
            let published = client
                .call_tool(
                    "redis_spublish",
                    serde_json::json!({
                        "channel": {"value": "shard:{events}"},
                        "message": {"value": "hello"}
                    }),
                )
                .await
                .expect("SPUBLISH")
                .structured_content
                .expect("structured SPUBLISH");
            assert_eq!(published["receivers"], 0, "{protocol}");
            let shard_channels = client
                .call_tool(
                    "redis_pubsub_shardchannels",
                    serde_json::json!({"pattern": {"value": "shard:*"}, "limit": 4}),
                )
                .await
                .expect("PUBSUB SHARDCHANNELS")
                .structured_content
                .expect("structured PUBSUB SHARDCHANNELS");
            assert_eq!(shard_channels["count"], 0, "{protocol}");
        }
    }
}

#[tokio::test]
async fn live_pubsub_sessions_are_binary_safe_owner_isolated_and_bounded() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let limits = PubSubSessionLimits::default()
        .with_max_sessions(4)
        .with_max_sessions_per_owner(2)
        .with_max_buffered_messages(2)
        .with_max_message_bytes(4)
        .with_max_read_bytes(256)
        .with_max_read_duration(Duration::from_secs(2))
        .with_idle_timeout(Duration::from_secs(2))
        .with_cleanup_interval(Duration::from_millis(25));
    let manager = DirectRedisPubSubSessionManager::standalone(&redis.url, limits)
        .expect("create standalone Pub/Sub session manager");
    let first = pubsub_session_router_client(&redis.url, manager.clone()).await;
    let second = pubsub_session_router_client(&redis.url, manager.clone()).await;

    let mut channel = test_key("session:binary").into_bytes();
    channel.extend([0xff, 0x00]);
    let channel_base64 = BASE64.encode(&channel);
    let opened = first
        .call_tool(
            "redis_subscribe",
            serde_json::json!({
                "subscriptions": [{"value": channel_base64, "encoding": "base64"}]
            }),
        )
        .await
        .expect("open channel session")
        .structured_content
        .expect("structured channel session");
    let session_id = opened["session_id"]
        .as_str()
        .expect("opaque session id")
        .to_string();
    assert!(session_id.starts_with("ps_"));
    assert_eq!(opened["subscriptions"][0]["value"]["encoding"], "base64");

    let foreign = second
        .call_tool(
            "redis_pubsub_read",
            serde_json::json!({"session_id": session_id, "wait_ms": 0}),
        )
        .await
        .expect("foreign owner read result");
    assert!(foreign.is_error);
    assert!(
        serde_json::to_string(&foreign)
            .expect("serialize foreign-owner failure")
            .contains("SESSION_NOT_FOUND"),
        "{foreign:?}"
    );

    for payload in [[0xff], [0xfe], [0xfd]] {
        let published = first
            .call_tool(
                "redis_publish",
                serde_json::json!({
                    "channel": {"value": channel_base64, "encoding": "base64"},
                    "message": {"value": BASE64.encode(payload), "encoding": "base64"}
                }),
            )
            .await
            .expect("publish buffered message")
            .structured_content
            .expect("structured publish result");
        assert_eq!(published["receivers"], 1);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let read = first
        .call_tool(
            "redis_pubsub_read",
            serde_json::json!({
                "session_id": session_id,
                "max_messages": 2,
                "max_bytes": 256,
                "wait_ms": 1000
            }),
        )
        .await
        .expect("read bounded messages")
        .structured_content
        .expect("structured bounded read");
    assert_eq!(read["returned"], 2);
    assert_eq!(read["dropped_buffer_full_total"], 1);
    assert_eq!(read["messages"][0]["payload"]["value"], "/g==");
    assert_eq!(read["messages"][1]["payload"]["value"], "/Q==");

    first
        .call_tool(
            "redis_publish",
            serde_json::json!({
                "channel": {"value": channel_base64, "encoding": "base64"},
                "message": {"value": "AQIDBAU=", "encoding": "base64"}
            }),
        )
        .await
        .expect("publish oversized message");
    tokio::time::sleep(Duration::from_millis(25)).await;
    let dropped = first
        .call_tool(
            "redis_pubsub_read",
            serde_json::json!({
                "session_id": session_id,
                "max_messages": 1,
                "max_bytes": 64,
                "wait_ms": 25
            }),
        )
        .await
        .expect("read after oversized message")
        .structured_content
        .expect("structured oversized read");
    assert_eq!(dropped["returned"], 0);
    assert_eq!(dropped["timed_out"], true);
    assert_eq!(dropped["dropped_oversized_total"], 1);

    let unsubscribed = first
        .call_tool(
            "redis_pubsub_unsubscribe",
            serde_json::json!({
                "session_id": session_id,
                "kind": "channel",
                "subscriptions": [{"value": channel_base64, "encoding": "base64"}]
            }),
        )
        .await
        .expect("unsubscribe channel")
        .structured_content
        .expect("structured unsubscribe");
    assert_eq!(unsubscribed["subscription_count"], 0);

    let closed = first
        .call_tool(
            "redis_pubsub_close",
            serde_json::json!({"session_id": session_id}),
        )
        .await
        .expect("close session")
        .structured_content
        .expect("structured close");
    assert_eq!(closed["closed"], true);

    let pattern = format!("{}:*", test_key("session:pattern"));
    let matching_channel = format!("{}:one", test_key("session:pattern"));
    let pattern_session = first
        .call_tool(
            "redis_psubscribe",
            serde_json::json!({"subscriptions": [{"value": pattern}]}),
        )
        .await
        .expect("open pattern session")
        .structured_content
        .expect("structured pattern session")["session_id"]
        .as_str()
        .expect("pattern session id")
        .to_string();
    first
        .call_tool(
            "redis_publish",
            serde_json::json!({"channel": {"value": matching_channel}, "message": {"value": "pat"}}),
        )
        .await
        .expect("publish matching pattern message");
    let pattern_read = first
        .call_tool(
            "redis_pubsub_read",
            serde_json::json!({
                "session_id": pattern_session,
                "max_messages": 1,
                "max_bytes": 256,
                "wait_ms": 1000
            }),
        )
        .await
        .expect("read pattern message")
        .structured_content
        .expect("structured pattern read");
    assert_eq!(pattern_read["messages"][0]["kind"], "pattern");
    assert_eq!(pattern_read["messages"][0]["pattern"]["value"], pattern);
    first
        .call_tool(
            "redis_pubsub_close",
            serde_json::json!({"session_id": pattern_session}),
        )
        .await
        .expect("close pattern session");

    let capabilities = DirectRedis::connect(&redis.url)
        .await
        .expect("connect for sharded Pub/Sub capability discovery")
        .discover_capabilities()
        .await
        .expect("discover sharded Pub/Sub capabilities");
    if capabilities
        .redis_version()
        .is_some_and(|version| version >= RedisVersion::new(7, 0, 0))
    {
        let shard_channel = format!("{}:{{slot}}", test_key("session:shard"));
        let shard_session = first
            .call_tool(
                "redis_ssubscribe",
                serde_json::json!({"subscriptions": [{"value": shard_channel}]}),
            )
            .await
            .expect("open sharded session")
            .structured_content
            .expect("structured sharded session")["session_id"]
            .as_str()
            .expect("sharded session id")
            .to_string();
        first
            .call_tool(
                "redis_spublish",
                serde_json::json!({"channel": {"value": shard_channel}, "message": {"value": "shr"}}),
            )
            .await
            .expect("publish sharded message");
        let shard_read = first
            .call_tool(
                "redis_pubsub_read",
                serde_json::json!({
                    "session_id": shard_session,
                    "max_messages": 1,
                    "max_bytes": 256,
                    "wait_ms": 1000
                }),
            )
            .await
            .expect("read sharded message")
            .structured_content
            .expect("structured sharded read");
        assert_eq!(shard_read["messages"][0]["kind"], "sharded");
        first
            .call_tool(
                "redis_pubsub_close",
                serde_json::json!({"session_id": shard_session}),
            )
            .await
            .expect("close sharded session");
    }

    let owner = PubSubSessionOwner::new("quota-owner").expect("valid owner");
    let first_quota = manager
        .subscribe(
            &owner,
            PubSubSubscriptionKind::Pattern,
            vec![b"quota:*".to_vec()],
        )
        .await
        .expect("first direct session");
    let second_quota = manager
        .subscribe(
            &owner,
            PubSubSubscriptionKind::Channel,
            vec![b"quota:second".to_vec()],
        )
        .await
        .expect("second direct session");
    let quota = manager
        .subscribe(
            &owner,
            PubSubSubscriptionKind::Channel,
            vec![b"quota:third".to_vec()],
        )
        .await
        .expect_err("per-owner quota");
    assert_eq!(quota.code(), Some("OWNER_SESSION_QUOTA_EXCEEDED"));
    assert_eq!(manager.close_owner(&owner).await, 2);
    for session_id in [first_quota.session_id, second_quota.session_id] {
        let missing = manager
            .read(
                &owner,
                &session_id,
                PubSubReadRequest {
                    max_messages: 1,
                    max_bytes: 64,
                    wait: Duration::ZERO,
                },
            )
            .await
            .expect_err("owner cleanup removed session");
        assert_eq!(missing.code(), Some("SESSION_NOT_FOUND"));
    }

    let cancellation_owner = PubSubSessionOwner::new("cancellation-owner").expect("valid owner");
    let cancellation_channel = test_key("session:cancellation");
    let cancellation_session = manager
        .subscribe(
            &cancellation_owner,
            PubSubSubscriptionKind::Channel,
            vec![cancellation_channel.as_bytes().to_vec()],
        )
        .await
        .expect("open cancellation-safe session");
    let cancelled = tokio::time::timeout(
        Duration::from_millis(20),
        manager.read(
            &cancellation_owner,
            &cancellation_session.session_id,
            PubSubReadRequest {
                max_messages: 1,
                max_bytes: 256,
                wait: Duration::from_secs(1),
            },
        ),
    )
    .await;
    assert!(
        cancelled.is_err(),
        "pending read should be dropped by timeout"
    );
    first
        .call_tool(
            "redis_publish",
            serde_json::json!({
                "channel": {"value": cancellation_channel},
                "message": {"value": "kept"}
            }),
        )
        .await
        .expect("publish after cancelled read");
    let after_cancellation = manager
        .read(
            &cancellation_owner,
            &cancellation_session.session_id,
            PubSubReadRequest {
                max_messages: 1,
                max_bytes: 256,
                wait: Duration::from_secs(1),
            },
        )
        .await
        .expect("read message after cancellation");
    assert_eq!(after_cancellation.messages[0].payload, b"kept");
    manager
        .close(&cancellation_owner, &cancellation_session.session_id)
        .await
        .expect("close cancellation session");

    let stale_manager = DirectRedisPubSubSessionManager::standalone(
        &redis.url,
        PubSubSessionLimits::default()
            .with_idle_timeout(Duration::from_millis(75))
            .with_cleanup_interval(Duration::from_millis(10)),
    )
    .expect("create stale-session test manager");
    let stale_owner = PubSubSessionOwner::new("stale-owner").expect("valid owner");
    let stale = stale_manager
        .subscribe(
            &stale_owner,
            PubSubSubscriptionKind::Channel,
            vec![b"stale".to_vec()],
        )
        .await
        .expect("open stale session");
    tokio::time::sleep(Duration::from_millis(120)).await;
    let reaped = stale_manager
        .read(
            &stale_owner,
            &stale.session_id,
            PubSubReadRequest {
                max_messages: 1,
                max_bytes: 64,
                wait: Duration::ZERO,
            },
        )
        .await
        .expect_err("idle session was independently reaped");
    assert_eq!(reaped.code(), Some("SESSION_NOT_FOUND"));
    stale_manager.shutdown().await;

    manager.shutdown().await;
    let shutting_down = manager
        .subscribe(
            &owner,
            PubSubSubscriptionKind::Channel,
            vec![b"after-shutdown".to_vec()],
        )
        .await
        .expect_err("shutdown rejects new sessions");
    assert_eq!(shutting_down.code(), Some("SESSION_MANAGER_SHUTTING_DOWN"));
}

#[tokio::test]
async fn live_key_string_semantics_in_resp2_and_resp3() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&redis.url, protocol), AccessMode::Full).await;
        let prefix = test_key(&format!("key-string:{protocol}"));
        let key = format!("{prefix}:value");
        let copy = format!("{prefix}:copy");
        let renamed = format!("{prefix}:renamed");
        let occupied = format!("{prefix}:occupied");
        let restored = format!("{prefix}:restored");
        let counter = format!("{prefix}:counter");

        let initial = client
            .call_tool(
                "redis_set",
                serde_json::json!({
                    "key": key,
                    "value": "old",
                    "expiration": {"type": "seconds", "value": 60}
                }),
            )
            .await
            .expect("initial SET")
            .structured_content
            .expect("structured initial SET");
        assert_eq!(initial["applied"], true);

        let replaced = client
            .call_tool(
                "redis_set",
                serde_json::json!({
                    "key": key,
                    "value": "hello",
                    "condition": "xx",
                    "get": true,
                    "expiration": {"type": "keep_ttl"}
                }),
            )
            .await
            .expect("SET XX GET")
            .structured_content
            .expect("structured SET XX GET");
        assert_eq!(replaced["applied"], true);
        assert_eq!(replaced["previous_exists"], true);
        assert_eq!(replaced["previous_value"], "old");

        let no_op = client
            .call_tool(
                "redis_set",
                serde_json::json!({
                    "key": key,
                    "value": "ignored",
                    "condition": "nx"
                }),
            )
            .await
            .expect("SET NX no-op")
            .structured_content
            .expect("structured SET NX no-op");
        assert_eq!(no_op["applied"], false);

        let range = client
            .call_tool(
                "redis_getrange",
                serde_json::json!({"key": key, "start": 1, "end": 3}),
            )
            .await
            .expect("GETRANGE")
            .structured_content
            .expect("structured GETRANGE");
        assert_eq!(range["value"], "ell");

        let setrange = client
            .call_tool(
                "redis_setrange",
                serde_json::json!({"key": key, "offset": 1, "value": "ipp"}),
            )
            .await
            .expect("SETRANGE")
            .structured_content
            .expect("structured SETRANGE");
        assert_eq!(setrange["length_bytes"], 5);

        let getex = client
            .call_tool(
                "redis_getex",
                serde_json::json!({
                    "key": key,
                    "expiration": {"type": "milliseconds", "value": 60000}
                }),
            )
            .await
            .expect("GETEX")
            .structured_content
            .expect("structured GETEX");
        assert_eq!(getex["value"], "hippo");

        let incremented = client
            .call_tool(
                "redis_incrby",
                serde_json::json!({"key": counter, "amount": 10}),
            )
            .await
            .expect("INCRBY")
            .structured_content
            .expect("structured INCRBY");
        assert_eq!(incremented["value"], 10);
        let decremented = client
            .call_tool("redis_decr", serde_json::json!({"key": counter}))
            .await
            .expect("DECR")
            .structured_content
            .expect("structured DECR");
        assert_eq!(decremented["value"], 9);
        let decremented = client
            .call_tool(
                "redis_decrby",
                serde_json::json!({"key": counter, "amount": 4}),
            )
            .await
            .expect("DECRBY")
            .structured_content
            .expect("structured DECRBY");
        assert_eq!(decremented["value"], 5);
        let float = client
            .call_tool(
                "redis_incrbyfloat",
                serde_json::json!({"key": counter, "amount": 0.5}),
            )
            .await
            .expect("INCRBYFLOAT")
            .structured_content
            .expect("structured INCRBYFLOAT");
        assert_eq!(float["value"], "5.5");

        let touched = client
            .call_tool("redis_touch", serde_json::json!({"keys": [key]}))
            .await
            .expect("TOUCH")
            .structured_content
            .expect("structured TOUCH");
        assert_eq!(touched["touched"], 1);

        let copied = client
            .call_tool(
                "redis_copy",
                serde_json::json!({"source": key, "destination": copy}),
            )
            .await
            .expect("COPY")
            .structured_content
            .expect("structured COPY");
        assert_eq!(copied["copied"], true);
        let copy_no_op = client
            .call_tool(
                "redis_copy",
                serde_json::json!({"source": key, "destination": copy}),
            )
            .await
            .expect("COPY no-op")
            .structured_content
            .expect("structured COPY no-op");
        assert_eq!(copy_no_op["copied"], false);

        let renamed_result = client
            .call_tool(
                "redis_rename",
                serde_json::json!({"source": copy, "destination": renamed}),
            )
            .await
            .expect("RENAME")
            .structured_content
            .expect("structured RENAME");
        assert_eq!(renamed_result["renamed"], true);
        client
            .call_tool(
                "redis_set",
                serde_json::json!({"key": occupied, "value": "occupied"}),
            )
            .await
            .expect("set occupied destination");
        let rename_no_op = client
            .call_tool(
                "redis_renamenx",
                serde_json::json!({"source": renamed, "destination": occupied}),
            )
            .await
            .expect("RENAMENX no-op")
            .structured_content
            .expect("structured RENAMENX no-op");
        assert_eq!(rename_no_op["renamed"], false);

        let object = client
            .call_tool(
                "redis_object_inspect",
                serde_json::json!({"key": key, "operation": "encoding"}),
            )
            .await
            .expect("OBJECT ENCODING")
            .structured_content
            .expect("structured OBJECT ENCODING");
        assert_eq!(object["exists"], true);
        assert!(object["encoding"].as_str().is_some());

        let dump = client
            .call_tool("redis_dump", serde_json::json!({"key": key}))
            .await
            .expect("DUMP")
            .structured_content
            .expect("structured DUMP");
        let payload = dump["payload_base64"]
            .as_str()
            .expect("base64 DUMP payload");
        let restore = client
            .call_tool(
                "redis_restore",
                serde_json::json!({"key": restored, "payload_base64": payload}),
            )
            .await
            .expect("RESTORE")
            .structured_content
            .expect("structured RESTORE");
        assert_eq!(restore["restored"], true);
        let restored_value = client
            .call_tool("redis_get", serde_json::json!({"key": restored}))
            .await
            .expect("GET restored")
            .structured_content
            .expect("structured GET restored");
        assert_eq!(restored_value["value"], "hippo");
        let busy_restore = client
            .call_tool(
                "redis_restore",
                serde_json::json!({"key": restored, "payload_base64": payload}),
            )
            .await
            .expect("RESTORE busy-key result");
        assert!(busy_restore.is_error);
        let replace_restore = client
            .call_tool(
                "redis_restore_replace",
                serde_json::json!({"key": restored, "payload_base64": payload}),
            )
            .await
            .expect("RESTORE REPLACE")
            .structured_content
            .expect("structured RESTORE REPLACE");
        assert_eq!(replace_restore["overwrite_allowed"], true);

        let binary_key = format!("/w{}=", if protocol == "resp2" { "A" } else { "E" });
        client
            .call_tool(
                "redis_set",
                serde_json::json!({
                    "key": binary_key,
                    "key_encoding": "base64",
                    "value": "/wA=",
                    "value_encoding": "base64"
                }),
            )
            .await
            .expect("binary SET");
        let binary = client
            .call_tool(
                "redis_get",
                serde_json::json!({"key": binary_key, "key_encoding": "base64"}),
            )
            .await
            .expect("binary GET")
            .structured_content
            .expect("structured binary GET");
        assert_eq!(binary["encoding"], "base64");
        assert_eq!(binary["value"], "/wA=");
        let deleted_binary = client
            .call_tool(
                "redis_getdel",
                serde_json::json!({"key": binary_key, "key_encoding": "base64"}),
            )
            .await
            .expect("binary GETDEL")
            .structured_content
            .expect("structured binary GETDEL");
        assert_eq!(deleted_binary["value"], "/wA=");

        let deleted = client
            .call_tool(
                "redis_del",
                serde_json::json!({
                    "keys": [key, renamed, occupied, restored, counter]
                }),
            )
            .await
            .expect("clean up key/string live test")
            .structured_content
            .expect("structured cleanup");
        assert_eq!(deleted["deleted"], 5);
    }
}

#[tokio::test]
async fn live_bitmap_geo_and_hll_families_are_typed_bounded_and_binary_safe() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let capabilities = DirectRedis::connect(&redis.url)
        .await
        .expect("connect for specialized data capability discovery")
        .discover_capabilities()
        .await
        .expect("discover specialized data capabilities");
    let redis_seven = capabilities
        .redis_version()
        .is_some_and(|version| version >= RedisVersion::new(7, 0, 0));

    for protocol in ["resp2", "resp3"] {
        let url = with_protocol(&redis.url, protocol);
        let client = router_client(&url, AccessMode::Full).await;
        let prefix = test_key(&format!("bitmap-geo-hll:{protocol}"));
        let bitmap = format!("{prefix}:bitmap");
        let wrong_type = format!("{prefix}:wrong-type");
        let geo = format!("{prefix}:geo");
        let geo_store = format!("{prefix}:geo-store");
        let hll_left = format!("{prefix}:hll-left");
        let hll_right = format!("{prefix}:hll-right");
        let hll_merged = format!("{prefix}:hll-merged");
        let hll_empty = format!("{prefix}:hll-empty");
        let hll_empty_merged = format!("{prefix}:hll-empty-merged");

        let setbit = client
            .call_tool(
                "redis_setbit",
                serde_json::json!({"key": bitmap, "offset": 9, "value": true}),
            )
            .await
            .expect("SETBIT tool result")
            .structured_content
            .expect("structured SETBIT");
        assert_eq!(setbit["previous"], 0);
        assert_eq!(setbit["value"], 1);

        let getbit = client
            .call_tool(
                "redis_getbit",
                serde_json::json!({"key": bitmap, "offset": 9}),
            )
            .await
            .expect("GETBIT tool result")
            .structured_content
            .expect("structured GETBIT");
        assert_eq!(getbit["set"], true);

        let mut bitcount_input = serde_json::json!({"key": bitmap});
        if redis_seven {
            bitcount_input["range"] = serde_json::json!({"start": 8, "end": 15, "unit": "bit"});
        }
        let bitcount = client
            .call_tool("redis_bitcount", bitcount_input)
            .await
            .expect("BITCOUNT tool result")
            .structured_content
            .expect("structured BITCOUNT");
        assert_eq!(bitcount["set_bits"], 1);

        let byte_bitcount = client
            .call_tool(
                "redis_bitcount",
                serde_json::json!({
                    "key": bitmap,
                    "range": {"start": 0, "end": 1, "unit": "byte"}
                }),
            )
            .await
            .expect("byte-range BITCOUNT tool result")
            .structured_content
            .expect("structured byte-range BITCOUNT");
        assert_eq!(byte_bitcount["set_bits"], 1);

        let bitpos = client
            .call_tool(
                "redis_bitpos",
                serde_json::json!({"key": bitmap, "bit": true}),
            )
            .await
            .expect("BITPOS tool result")
            .structured_content
            .expect("structured BITPOS");
        assert_eq!(bitpos["position"], 9);

        let bitfield = client
            .call_tool(
                "redis_bitfield",
                serde_json::json!({
                    "key": bitmap,
                    "operations": [
                        {
                            "operation": "set",
                            "encoding": {"signed": true, "width": 8},
                            "offset": {"kind": "index", "value": 2},
                            "value": "120"
                        },
                        {
                            "operation": "increment",
                            "encoding": {"signed": true, "width": 8},
                            "offset": {"kind": "index", "value": 2},
                            "increment": "20",
                            "overflow": "saturate"
                        }
                    ]
                }),
            )
            .await
            .expect("BITFIELD tool result")
            .structured_content
            .expect("structured BITFIELD");
        assert_eq!(bitfield["results"][1]["value"], "127");

        let bitfield_ro = client
            .call_tool(
                "redis_bitfield_ro",
                serde_json::json!({
                    "key": bitmap,
                    "operations": [{
                        "encoding": {"signed": true, "width": 8},
                        "offset": {"kind": "index", "value": 2}
                    }]
                }),
            )
            .await
            .expect("BITFIELD_RO tool result")
            .structured_content
            .expect("structured BITFIELD_RO");
        assert_eq!(bitfield_ro["results"][0]["value"], "127");

        client
            .call_tool(
                "redis_rpush",
                serde_json::json!({"key": wrong_type, "elements": ["not-a-bitmap"]}),
            )
            .await
            .expect("seed bitmap wrong type");
        let wrong_type_result = client
            .call_tool(
                "redis_getbit",
                serde_json::json!({"key": wrong_type, "offset": 0}),
            )
            .await
            .expect("GETBIT wrong type tool result");
        assert!(wrong_type_result.is_error);
        assert!(
            serde_json::to_string(&wrong_type_result)
                .expect("serialize GETBIT wrong type")
                .contains("WRONGTYPE")
        );

        let binary_member = BASE64.encode([0xff, 0x00]);
        let geoadd = client
            .call_tool(
                "redis_geoadd",
                serde_json::json!({
                    "key": geo,
                    "members": [
                        {"member": "san-francisco", "longitude": "-122.4194", "latitude": "37.7749"},
                        {"member": "oakland", "longitude": "-122.2712", "latitude": "37.8044"},
                        {"member": {"value": binary_member, "encoding": "base64"}, "longitude": "-122.3", "latitude": "37.8"}
                    ]
                }),
            )
            .await
            .expect("GEOADD tool result")
            .structured_content
            .expect("structured GEOADD");
        assert_eq!(geoadd["affected"], 3);
        let conditional_geoadd = client
            .call_tool(
                "redis_geoadd",
                serde_json::json!({
                    "key": geo,
                    "nx": true,
                    "ch": true,
                    "members": [{
                        "member": "san-francisco",
                        "longitude": "-122.4194",
                        "latitude": "37.7749"
                    }]
                }),
            )
            .await
            .expect("conditional GEOADD tool result")
            .structured_content
            .expect("structured conditional GEOADD");
        assert_eq!(conditional_geoadd["affected"], 0);
        assert_eq!(conditional_geoadd["count_mode"], "added_or_changed");

        let distance_result = client
            .call_tool(
                "redis_geodist",
                serde_json::json!({
                    "key": geo, "from": "san-francisco", "to": "oakland",
                    "unit": "kilometers"
                }),
            )
            .await
            .expect("GEODIST tool result");
        assert!(!distance_result.is_error, "{distance_result:?}");
        let distance = distance_result
            .structured_content
            .expect("structured GEODIST");
        assert!(distance["distance"].as_str().is_some());
        assert_eq!(distance["unit"], "kilometers");
        let missing_distance = client
            .call_tool(
                "redis_geodist",
                serde_json::json!({"key": geo, "from": "san-francisco", "to": "missing"}),
            )
            .await
            .expect("missing GEODIST tool result")
            .structured_content
            .expect("structured missing GEODIST");
        assert_eq!(missing_distance["distance"], serde_json::Value::Null);

        let hashes = client
            .call_tool(
                "redis_geohash",
                serde_json::json!({"key": geo, "members": ["san-francisco", "missing"]}),
            )
            .await
            .expect("GEOHASH tool result")
            .structured_content
            .expect("structured GEOHASH");
        assert!(hashes["members"][0]["geohash"].as_str().is_some());
        assert_eq!(hashes["members"][1]["geohash"], serde_json::Value::Null);

        let positions = client
            .call_tool(
                "redis_geopos",
                serde_json::json!({"key": geo, "members": ["san-francisco", "missing"]}),
            )
            .await
            .expect("GEOPOS tool result")
            .structured_content
            .expect("structured GEOPOS");
        assert!(positions["members"][0]["position"]["longitude"].is_string());
        assert_eq!(positions["members"][1]["position"], serde_json::Value::Null);

        let search = client
            .call_tool(
                "redis_geosearch",
                serde_json::json!({
                    "key": geo,
                    "center": {"kind": "member", "member": "san-francisco"},
                    "shape": {"kind": "radius", "radius": "20", "unit": "kilometers"},
                    "sort": "ascending", "count": 10
                }),
            )
            .await
            .expect("GEOSEARCH tool result")
            .structured_content
            .expect("structured GEOSEARCH");
        assert_eq!(search["count"], 3);
        assert!(search["results"][0]["distance"].is_string());
        assert!(search["results"][0]["geohash_integer"].is_string());

        let stored = client
            .call_tool(
                "redis_geosearchstore",
                serde_json::json!({
                    "destination": geo_store, "source": geo,
                    "center": {"kind": "member", "member": "san-francisco"},
                    "shape": {"kind": "box", "width": "50", "height": "50", "unit": "kilometers"},
                    "count": 10, "store_distance": true
                }),
            )
            .await
            .expect("GEOSEARCHSTORE tool result")
            .structured_content
            .expect("structured GEOSEARCHSTORE");
        assert_eq!(stored["stored"], 3);
        assert_eq!(stored["destination_overwritten"], true);

        let initialized = client
            .call_tool("redis_pfadd", serde_json::json!({"key": hll_empty}))
            .await
            .expect("empty PFADD tool result")
            .structured_content
            .expect("structured empty PFADD");
        assert_eq!(initialized["observed"], 0);
        assert_eq!(initialized["key_created"], true);
        assert_eq!(initialized["register_changed"], false);

        let initialized_merge = client
            .call_tool(
                "redis_pfmerge",
                serde_json::json!({"destination": hll_empty_merged}),
            )
            .await
            .expect("empty PFMERGE tool result")
            .structured_content
            .expect("structured empty PFMERGE");
        assert_eq!(initialized_merge["source_count"], 0);
        assert_eq!(initialized_merge["cluster_requires_same_slot"], false);

        for (key, elements) in [
            (&hll_left, serde_json::json!(["alice", "bob"])),
            (
                &hll_right,
                serde_json::json!(["bob", {"value": binary_member, "encoding": "base64"}]),
            ),
        ] {
            let added = client
                .call_tool(
                    "redis_pfadd",
                    serde_json::json!({"key": key, "elements": elements}),
                )
                .await
                .expect("PFADD tool result")
                .structured_content
                .expect("structured PFADD");
            assert_eq!(added["approximate"], true);
        }
        let count = client
            .call_tool(
                "redis_pfcount",
                serde_json::json!({"keys": [hll_left, hll_right]}),
            )
            .await
            .expect("PFCOUNT tool result")
            .structured_content
            .expect("structured PFCOUNT");
        assert_eq!(count["estimated_cardinality"], "3");
        assert_eq!(count["approximate"], true);

        let merged = client
            .call_tool(
                "redis_pfmerge",
                serde_json::json!({"destination": hll_merged, "sources": [hll_left, hll_right]}),
            )
            .await
            .expect("PFMERGE tool result")
            .structured_content
            .expect("structured PFMERGE");
        assert_eq!(merged["destination_overwritten"], true);
        let merged_count = client
            .call_tool("redis_pfcount", serde_json::json!({"keys": [hll_merged]}))
            .await
            .expect("merged PFCOUNT tool result")
            .structured_content
            .expect("structured merged PFCOUNT");
        assert_eq!(merged_count["estimated_cardinality"], "3");

        client
            .call_tool(
                "redis_del",
                serde_json::json!({
                    "keys": [
                        bitmap, wrong_type, geo, geo_store, hll_left, hll_right, hll_merged,
                        hll_empty, hll_empty_merged
                    ]
                }),
            )
            .await
            .expect("clean specialized data keys");
    }

    let large_geo = test_key("geo-output-budget");
    let large_member = "x".repeat(4096);
    let full = router_client(&redis.url, AccessMode::Full).await;
    full.call_tool(
        "redis_geoadd",
        serde_json::json!({
            "key": large_geo,
            "members": [{"member": large_member, "longitude": 0, "latitude": 0}]
        }),
    )
    .await
    .expect("seed oversized GEOSEARCH member");
    let limited =
        router_client_with_budget(&redis.url, AccessMode::ReadOnly, OutputBudget::new(512, 10))
            .await
            .call_tool(
                "redis_geosearch",
                serde_json::json!({
                    "key": large_geo,
                    "center": {"kind": "coordinates", "longitude": 0, "latitude": 0},
                    "shape": {"kind": "radius", "radius": 1, "unit": "kilometers"},
                    "count": 1
                }),
            )
            .await
            .expect("bounded GEOSEARCH output");
    assert!(limited.is_error);
    assert!(
        serde_json::to_string(&limited)
            .expect("serialize bounded GEOSEARCH")
            .contains("output_limit_exceeded")
    );
    full.call_tool("redis_del", serde_json::json!({"keys": [large_geo]}))
        .await
        .expect("delete oversized GEOSEARCH key");
}

#[tokio::test]
async fn live_curated_catalog_round_trip_in_resp2_and_resp3() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let url = redis.url;

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&url, protocol), AccessMode::Full).await;
        let prefix = test_key(&format!("curated:{protocol}"));
        let string_a = format!("{prefix}:a");
        let string_b = format!("{prefix}:b");
        let counter = format!("{prefix}:counter");
        let hash = format!("{prefix}:hash");
        let list = format!("{prefix}:list");
        let set = format!("{prefix}:set");
        let zset = format!("{prefix}:zset");

        let mset = client
            .call_tool(
                "redis_mset",
                serde_json::json!({
                    "entries": [
                        {"key": string_a, "value": "hello"},
                        {"key": string_b, "value": "world"}
                    ]
                }),
            )
            .await
            .expect("mset")
            .structured_content
            .expect("structured mset");
        assert_eq!(mset["stored"], 2);

        let exists = client
            .call_tool(
                "redis_exists",
                serde_json::json!({"keys": [string_a, string_b]}),
            )
            .await
            .expect("exists")
            .structured_content
            .expect("structured exists");
        assert_eq!(exists["existing"], 2);
        assert_eq!(exists["all_exist"], true);

        let mget = client
            .call_tool(
                "redis_mget",
                serde_json::json!({"keys": [string_a, string_b, format!("{prefix}:missing")]}),
            )
            .await
            .expect("mget")
            .structured_content
            .expect("structured mget");
        assert_eq!(mget["values"][0]["value"], "hello");
        assert_eq!(mget["values"][2]["exists"], false);

        let append = client
            .call_tool(
                "redis_append",
                serde_json::json!({"key": string_a, "value": "!"}),
            )
            .await
            .expect("append")
            .structured_content
            .expect("structured append");
        assert_eq!(append["length_bytes"], 6);

        let incr = client
            .call_tool("redis_incr", serde_json::json!({"key": counter}))
            .await
            .expect("incr")
            .structured_content
            .expect("structured incr");
        assert_eq!(incr["value"], 1);

        let expire = client
            .call_tool(
                "redis_expire",
                serde_json::json!({"key": string_b, "seconds": 60}),
            )
            .await
            .expect("expire")
            .structured_content
            .expect("structured expire");
        assert_eq!(expire["applied"], true);
        let persist = client
            .call_tool("redis_persist", serde_json::json!({"key": string_b}))
            .await
            .expect("persist")
            .structured_content
            .expect("structured persist");
        assert_eq!(persist["applied"], true);

        let memory = client
            .call_tool("redis_memory_usage", serde_json::json!({"key": string_a}))
            .await
            .expect("memory usage")
            .structured_content
            .expect("structured memory usage");
        assert_eq!(memory["exists"], true);
        assert!(memory["bytes"].as_u64().is_some_and(|bytes| bytes > 0));

        client
            .call_tool(
                "redis_hset",
                serde_json::json!({"key": hash, "fields": {"name": "Ada", "role": "engineer"}}),
            )
            .await
            .expect("hset");
        let hget = client
            .call_tool(
                "redis_hget",
                serde_json::json!({"key": hash, "field": "name"}),
            )
            .await
            .expect("hget")
            .structured_content
            .expect("structured hget");
        assert_eq!(hget["value"], "Ada");
        let hgetall = client
            .call_tool("redis_hgetall", serde_json::json!({"key": hash}))
            .await
            .expect("hgetall")
            .structured_content
            .expect("structured hgetall");
        assert_eq!(hgetall["count"], 2);

        client
            .call_tool(
                "redis_lpush",
                serde_json::json!({"key": list, "elements": ["first", "second"]}),
            )
            .await
            .expect("lpush");
        let lrange = client
            .call_tool("redis_lrange", serde_json::json!({"key": list}))
            .await
            .expect("lrange")
            .structured_content
            .expect("structured lrange");
        assert_eq!(lrange["elements"][0]["value"], "second");

        client
            .call_tool(
                "redis_sadd",
                serde_json::json!({"key": set, "members": ["beta", "alpha"]}),
            )
            .await
            .expect("sadd");
        let smembers = client
            .call_tool("redis_smembers", serde_json::json!({"key": set}))
            .await
            .expect("smembers")
            .structured_content
            .expect("structured smembers");
        assert_eq!(smembers["members"][0]["value"], "alpha");

        client
            .call_tool(
                "redis_zadd",
                serde_json::json!({
                    "key": zset,
                    "members": [
                        {"score": 2.0, "member": "bob"},
                        {"score": 1.0, "member": "alice"}
                    ]
                }),
            )
            .await
            .expect("zadd");
        let zrange = client
            .call_tool(
                "redis_zrange",
                serde_json::json!({"key": zset, "withscores": true}),
            )
            .await
            .expect("zrange")
            .structured_content
            .expect("structured zrange");
        assert_eq!(zrange["members"][0]["member"], "alice");
        assert_eq!(zrange["members"][0]["score"], "1");

        let cleanup_keys = [string_a, string_b, counter, hash, list, set, zset];
        let cleanup = client
            .call_tool("redis_unlink", serde_json::json!({"keys": cleanup_keys}))
            .await
            .expect("unlink cleanup")
            .structured_content
            .expect("structured unlink");
        assert_eq!(cleanup["unlinked"], cleanup_keys.len());
    }
}

#[tokio::test]
async fn live_list_family_preserves_order_binary_values_and_nil_semantics() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let url = redis.url;

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&url, protocol), AccessMode::Full).await;
        let prefix = test_key(&format!("lists:{protocol}"));
        let list = format!("{prefix}:list");
        let archive = format!("{prefix}:archive");
        let missing = format!("{prefix}:missing");
        let large_pop = format!("{prefix}:large-pop");
        let large_move_source = format!("{prefix}:large-move-source");
        let large_move_destination = format!("{prefix}:large-move-destination");
        let wrong_type = format!("{prefix}:wrong-type");

        let pushed = client
            .call_tool(
                "redis_rpush",
                serde_json::json!({
                    "key": list,
                    "elements": ["a", "needle", "b", "needle"]
                }),
            )
            .await
            .expect("RPUSH")
            .structured_content
            .expect("structured RPUSH");
        assert_eq!(pushed["pushed"], 4);
        assert_eq!(pushed["length"], 4);

        let length = client
            .call_tool("redis_llen", serde_json::json!({"key": list}))
            .await
            .expect("LLEN")
            .structured_content
            .expect("structured LLEN");
        assert_eq!(length["exists"], true);
        assert_eq!(length["length"], 4);

        let indexed = client
            .call_tool(
                "redis_lindex",
                serde_json::json!({"key": list, "index": -1}),
            )
            .await
            .expect("negative LINDEX")
            .structured_content
            .expect("structured LINDEX");
        assert_eq!(indexed["value"], "needle");

        let positions = client
            .call_tool(
                "redis_lpos",
                serde_json::json!({"key": list, "value": "needle", "count": 2}),
            )
            .await
            .expect("LPOS")
            .structured_content
            .expect("structured LPOS");
        assert_eq!(positions["positions"], serde_json::json!([1, 3]));

        let tail = client
            .call_tool(
                "redis_lrange",
                serde_json::json!({"key": list, "start": -2, "stop": -1}),
            )
            .await
            .expect("negative LRANGE")
            .structured_content
            .expect("structured negative LRANGE");
        assert_eq!(tail["elements"][0]["value"], "b");
        assert_eq!(tail["elements"][1]["value"], "needle");
        assert_eq!(tail["page"]["complete"], true);

        let replaced = client
            .call_tool(
                "redis_lset",
                serde_json::json!({
                    "key": list,
                    "index": -2,
                    "value": "/wA=",
                    "value_encoding": "base64"
                }),
            )
            .await
            .expect("binary LSET")
            .structured_content
            .expect("structured LSET");
        assert_eq!(replaced["replaced"], true);
        let binary = client
            .call_tool(
                "redis_lindex",
                serde_json::json!({"key": list, "index": -2}),
            )
            .await
            .expect("binary LINDEX")
            .structured_content
            .expect("structured binary LINDEX");
        assert_eq!(binary["value"], "/wA=");
        assert_eq!(binary["encoding"], "base64");

        let removed = client
            .call_tool(
                "redis_lrem",
                serde_json::json!({"key": list, "count": -1, "value": "needle"}),
            )
            .await
            .expect("negative LREM")
            .structured_content
            .expect("structured LREM");
        assert_eq!(removed["removed"], 1);

        let popped = client
            .call_tool("redis_lpop", serde_json::json!({"key": list, "count": 2}))
            .await
            .expect("counted LPOP")
            .structured_content
            .expect("structured LPOP");
        assert_eq!(popped["popped"], 2);
        assert_eq!(popped["elements"][0]["value"], "a");
        assert_eq!(popped["elements"][1]["value"], "needle");

        client
            .call_tool(
                "redis_rpush",
                serde_json::json!({"key": list, "elements": ["tail-1", "tail-2"]}),
            )
            .await
            .expect("second RPUSH");
        let right = client
            .call_tool("redis_rpop", serde_json::json!({"key": list, "count": 2}))
            .await
            .expect("counted RPOP")
            .structured_content
            .expect("structured RPOP");
        assert_eq!(right["elements"][0]["value"], "tail-2");
        assert_eq!(right["elements"][1]["value"], "tail-1");

        let moved = client
            .call_tool(
                "redis_lmove",
                serde_json::json!({
                    "source": list,
                    "destination": archive,
                    "from": "left",
                    "to": "right"
                }),
            )
            .await
            .expect("LMOVE")
            .structured_content
            .expect("structured LMOVE");
        assert_eq!(moved["moved"], true);
        assert_eq!(moved["value"], "/wA=");
        assert_eq!(moved["encoding"], "base64");

        let trimmed = client
            .call_tool(
                "redis_ltrim",
                serde_json::json!({"key": archive, "start": 1, "stop": 0}),
            )
            .await
            .expect("empty-range LTRIM")
            .structured_content
            .expect("structured LTRIM");
        assert_eq!(trimmed["exists"], false);

        let missing_length = client
            .call_tool("redis_llen", serde_json::json!({"key": missing}))
            .await
            .expect("missing LLEN")
            .structured_content
            .expect("structured missing LLEN");
        assert_eq!(missing_length["exists"], false);
        assert_eq!(missing_length["length"], 0);
        let not_added = client
            .call_tool(
                "redis_xadd",
                serde_json::json!({
                    "key": missing,
                    "no_mkstream": true,
                    "fields": [{"field": "event", "value": "ignored"}]
                }),
            )
            .await
            .expect("XADD NOMKSTREAM on a missing key")
            .structured_content
            .expect("structured XADD NOMKSTREAM");
        assert_eq!(not_added["added"], false);
        assert_eq!(not_added["id"], serde_json::Value::Null);
        let missing_range = client
            .call_tool("redis_lrange", serde_json::json!({"key": missing}))
            .await
            .expect("missing LRANGE")
            .structured_content
            .expect("structured missing LRANGE");
        assert_eq!(missing_range["exists"], false);
        assert_eq!(missing_range["elements"], serde_json::json!([]));
        let missing_index = client
            .call_tool(
                "redis_lindex",
                serde_json::json!({"key": missing, "index": 0}),
            )
            .await
            .expect("missing LINDEX")
            .structured_content
            .expect("structured missing LINDEX");
        assert_eq!(missing_index["list_exists"], false);
        assert_eq!(missing_index["element_exists"], false);
        let missing_positions = client
            .call_tool(
                "redis_lpos",
                serde_json::json!({"key": missing, "value": "needle"}),
            )
            .await
            .expect("missing LPOS")
            .structured_content
            .expect("structured missing LPOS");
        assert_eq!(missing_positions["exists"], false);
        assert_eq!(missing_positions["positions"], serde_json::json!([]));
        let missing_pop = client
            .call_tool(
                "redis_lpop",
                serde_json::json!({"key": missing, "count": 2}),
            )
            .await
            .expect("missing LPOP")
            .structured_content
            .expect("structured missing LPOP");
        assert_eq!(missing_pop["found"], false);
        assert_eq!(missing_pop["elements"], serde_json::json!([]));
        let missing_move = client
            .call_tool(
                "redis_lmove",
                serde_json::json!({
                    "source": missing,
                    "destination": archive,
                    "from": "left",
                    "to": "right"
                }),
            )
            .await
            .expect("missing LMOVE")
            .structured_content
            .expect("structured missing LMOVE");
        assert_eq!(missing_move["moved"], false);
        assert_eq!(missing_move["value"], serde_json::Value::Null);

        let large_value = "x".repeat(128);
        client
            .call_tool(
                "redis_rpush",
                serde_json::json!({"key": large_pop, "elements": [large_value]}),
            )
            .await
            .expect("seed oversized pop");
        let omitted_pop = client
            .call_tool(
                "redis_lpop",
                serde_json::json!({
                    "key": large_pop,
                    "count": 1,
                    "max_returned_bytes": 8
                }),
            )
            .await
            .expect("oversized LPOP")
            .structured_content
            .expect("structured oversized LPOP");
        assert_eq!(omitted_pop["popped"], 1);
        assert_eq!(omitted_pop["element_bytes"], 128);
        assert_eq!(omitted_pop["elements_omitted"], true);
        assert_eq!(omitted_pop["elements"], serde_json::json!([]));

        client
            .call_tool(
                "redis_rpush",
                serde_json::json!({"key": large_move_source, "elements": [large_value]}),
            )
            .await
            .expect("seed oversized move");
        let omitted_move = client
            .call_tool(
                "redis_lmove",
                serde_json::json!({
                    "source": large_move_source,
                    "destination": large_move_destination,
                    "from": "left",
                    "to": "right",
                    "max_value_bytes": 8
                }),
            )
            .await
            .expect("oversized LMOVE")
            .structured_content
            .expect("structured oversized LMOVE");
        assert_eq!(omitted_move["moved"], true);
        assert_eq!(omitted_move["value_bytes"], 128);
        assert_eq!(omitted_move["value_omitted"], true);
        assert_eq!(omitted_move["value"], serde_json::Value::Null);

        client
            .call_tool(
                "redis_set",
                serde_json::json!({"key": wrong_type, "value": "not-a-list"}),
            )
            .await
            .expect("seed wrong-type value");
        let wrong_type_result = client
            .call_tool("redis_llen", serde_json::json!({"key": wrong_type}))
            .await
            .expect("wrong-type LLEN is a tool result");
        assert!(wrong_type_result.is_error);
        assert!(
            serde_json::to_string(&wrong_type_result)
                .expect("serialize wrong-type LLEN")
                .contains("WRONGTYPE")
        );

        client
            .call_tool(
                "redis_unlink",
                serde_json::json!({
                    "keys": [
                        list,
                        archive,
                        missing,
                        large_pop,
                        large_move_source,
                        large_move_destination,
                        wrong_type
                    ]
                }),
            )
            .await
            .expect("clean up list family");
    }
}

#[tokio::test]
async fn live_set_family_preserves_membership_binary_algebra_and_nil_semantics() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let url = redis.url;
    let capabilities = DirectRedis::connect(&url)
        .await
        .expect("connect for set capability discovery")
        .discover_capabilities()
        .await
        .expect("discover set test capabilities");
    let supports_set_cardinality = capabilities
        .redis_version()
        .is_some_and(|version| version >= RedisVersion::new(8, 10, 0));

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&url, protocol), AccessMode::Full).await;
        let prefix = test_key(&format!("sets:{protocol}"));
        let left = format!("{prefix}:{{same}}:left");
        let right = format!("{prefix}:{{same}}:right");
        let missing = format!("{prefix}:{{same}}:missing");
        let difference_store = format!("{prefix}:{{same}}:difference");
        let empty_store = format!("{prefix}:{{same}}:empty");
        let intersection_store = format!("{prefix}:{{same}}:intersection");
        let union_store = format!("{prefix}:{{same}}:union");
        let wrong_type = format!("{prefix}:wrong-type");

        let added = client
            .call_tool(
                "redis_sadd",
                serde_json::json!({
                    "key": left,
                    "members": [
                        "beta",
                        "alpha",
                        {"member": "/w==", "member_encoding": "base64"}
                    ]
                }),
            )
            .await
            .expect("SADD left")
            .structured_content
            .expect("structured SADD left");
        assert_eq!(added["requested"], 3);
        assert_eq!(added["added"], 3);

        client
            .call_tool(
                "redis_sadd",
                serde_json::json!({"key": right, "members": ["beta", "gamma"]}),
            )
            .await
            .expect("SADD right");

        let cardinality = client
            .call_tool("redis_scard", serde_json::json!({"key": left}))
            .await
            .expect("SCARD")
            .structured_content
            .expect("structured SCARD");
        assert_eq!(cardinality["exists"], true);
        assert_eq!(cardinality["cardinality"], 3);

        let present = client
            .call_tool(
                "redis_sismember",
                serde_json::json!({"key": left, "member": "alpha"}),
            )
            .await
            .expect("present SISMEMBER")
            .structured_content
            .expect("structured present SISMEMBER");
        assert_eq!(present["set_exists"], true);
        assert_eq!(present["is_member"], true);

        let absent = client
            .call_tool(
                "redis_sismember",
                serde_json::json!({"key": left, "member": "missing"}),
            )
            .await
            .expect("absent SISMEMBER")
            .structured_content
            .expect("structured absent SISMEMBER");
        assert_eq!(absent["set_exists"], true);
        assert_eq!(absent["is_member"], false);

        let missing_membership = client
            .call_tool(
                "redis_sismember",
                serde_json::json!({"key": missing, "member": "missing"}),
            )
            .await
            .expect("missing-set SISMEMBER")
            .structured_content
            .expect("structured missing-set SISMEMBER");
        assert_eq!(missing_membership["set_exists"], false);
        assert_eq!(missing_membership["is_member"], false);

        let multiple = client
            .call_tool(
                "redis_smismember",
                serde_json::json!({
                    "key": left,
                    "members": [
                        "alpha",
                        "missing",
                        {"member": "/w==", "member_encoding": "base64"}
                    ]
                }),
            )
            .await
            .expect("SMISMEMBER")
            .structured_content
            .expect("structured SMISMEMBER");
        assert_eq!(multiple["count"], 3);
        assert_eq!(multiple["members"][0]["is_member"], true);
        assert_eq!(multiple["members"][1]["is_member"], false);
        assert_eq!(multiple["members"][2]["member_encoding"], "base64");
        assert_eq!(multiple["members"][2]["is_member"], true);

        let members = client
            .call_tool("redis_smembers", serde_json::json!({"key": left}))
            .await
            .expect("SMEMBERS")
            .structured_content
            .expect("structured SMEMBERS");
        assert_eq!(members["ordering"], "byte_sorted");
        assert_eq!(members["members"][0]["value"], "alpha");
        assert_eq!(members["members"][1]["value"], "beta");
        assert_eq!(members["members"][2]["encoding"], "base64");

        let scan = client
            .call_tool(
                "redis_sscan",
                serde_json::json!({"key": left, "cursor": 0, "count": 100}),
            )
            .await
            .expect("SSCAN")
            .structured_content
            .expect("structured SSCAN");
        assert_eq!(scan["exists"], true);
        assert_eq!(scan["ordering"], "byte_sorted_within_page");
        assert_eq!(scan["count"], 3);

        let difference = client
            .call_tool("redis_sdiff", serde_json::json!({"keys": [left, right]}))
            .await
            .expect("SDIFF")
            .structured_content
            .expect("structured SDIFF");
        assert_eq!(difference["members"][0]["value"], "alpha");
        assert_eq!(difference["members"][1]["encoding"], "base64");

        let intersection = client
            .call_tool("redis_sinter", serde_json::json!({"keys": [left, right]}))
            .await
            .expect("SINTER")
            .structured_content
            .expect("structured SINTER");
        assert_eq!(intersection["members"][0]["value"], "beta");

        let union = client
            .call_tool("redis_sunion", serde_json::json!({"keys": [left, right]}))
            .await
            .expect("SUNION")
            .structured_content
            .expect("structured SUNION");
        assert_eq!(union["count"], 4);
        assert_eq!(union["members"][0]["value"], "alpha");
        assert_eq!(union["members"][1]["value"], "beta");
        assert_eq!(union["members"][2]["value"], "gamma");
        assert_eq!(union["members"][3]["encoding"], "base64");

        if supports_set_cardinality {
            let difference_cardinality = client
                .call_tool(
                    "redis_sdiffcard",
                    serde_json::json!({"keys": [left, right], "limit": 1}),
                )
                .await
                .expect("SDIFFCARD")
                .structured_content
                .expect("structured SDIFFCARD");
            assert_eq!(difference_cardinality["cardinality"], 1);
            assert_eq!(difference_cardinality["limit_reached"], true);
            assert!(difference_cardinality.get("members").is_none());

            let union_cardinality = client
                .call_tool(
                    "redis_sunioncard",
                    serde_json::json!({
                        "keys": [left, right],
                        "approximate": true
                    }),
                )
                .await
                .expect("SUNIONCARD")
                .structured_content
                .expect("structured SUNIONCARD");
            assert_eq!(union_cardinality["approximate"], true);
            assert!(
                union_cardinality["cardinality"]
                    .as_u64()
                    .is_some_and(|value| value > 0)
            );
            assert!(union_cardinality.get("members").is_none());
        }

        for (tool, destination, expected) in [
            ("redis_sdiffstore", &difference_store, 2),
            ("redis_sinterstore", &intersection_store, 1),
            ("redis_sunionstore", &union_store, 4),
        ] {
            client
                .call_tool(
                    "redis_sadd",
                    serde_json::json!({"key": destination, "members": ["stale"]}),
                )
                .await
                .expect("seed set store destination");
            let stored = client
                .call_tool(
                    tool,
                    serde_json::json!({
                        "destination": destination,
                        "keys": [left, right]
                    }),
                )
                .await
                .unwrap_or_else(|error| panic!("{tool}: {error}"))
                .structured_content
                .unwrap_or_else(|| panic!("{tool}: structured result"));
            assert_eq!(stored["destination_cardinality"], expected, "{tool}");
            assert_eq!(stored["destination_overwritten"], true, "{tool}");
            assert!(stored.get("members").is_none(), "{tool}");
        }
        let empty = client
            .call_tool(
                "redis_sdiffstore",
                serde_json::json!({"destination": empty_store, "keys": [missing]}),
            )
            .await
            .expect("empty SDIFFSTORE")
            .structured_content
            .expect("structured empty SDIFFSTORE");
        assert_eq!(empty["destination_cardinality"], 0);
        let empty_destination = client
            .call_tool("redis_scard", serde_json::json!({"key": empty_store}))
            .await
            .expect("empty SDIFFSTORE destination")
            .structured_content
            .expect("structured empty SDIFFSTORE destination");
        assert_eq!(empty_destination["exists"], false);

        let removed = client
            .call_tool(
                "redis_srem",
                serde_json::json!({
                    "key": left,
                    "members": ["beta", {"member": "/w==", "member_encoding": "base64"}]
                }),
            )
            .await
            .expect("SREM")
            .structured_content
            .expect("structured SREM");
        assert_eq!(removed["requested"], 2);
        assert_eq!(removed["removed"], 2);
        let repeated = client
            .call_tool(
                "redis_srem",
                serde_json::json!({"key": left, "members": ["beta"]}),
            )
            .await
            .expect("repeated SREM")
            .structured_content
            .expect("structured repeated SREM");
        assert_eq!(repeated["removed"], 0);

        client
            .call_tool(
                "redis_set",
                serde_json::json!({"key": wrong_type, "value": "not-a-set"}),
            )
            .await
            .expect("seed wrong-type value");
        let wrong_type_result = client
            .call_tool("redis_scard", serde_json::json!({"key": wrong_type}))
            .await
            .expect("wrong-type SCARD is a tool result");
        assert!(wrong_type_result.is_error);
        assert!(
            serde_json::to_string(&wrong_type_result)
                .expect("serialize wrong-type SCARD")
                .contains("WRONGTYPE")
        );
        let wrong_type_store = client
            .call_tool(
                "redis_sunionstore",
                serde_json::json!({
                    "destination": union_store,
                    "keys": [left, wrong_type]
                }),
            )
            .await
            .expect("wrong-type SUNIONSTORE is a tool result");
        assert!(wrong_type_store.is_error);
        assert!(
            serde_json::to_string(&wrong_type_store)
                .expect("serialize wrong-type SUNIONSTORE")
                .contains("WRONGTYPE")
        );

        let missing_members = client
            .call_tool("redis_smembers", serde_json::json!({"key": missing}))
            .await
            .expect("missing SMEMBERS")
            .structured_content
            .expect("structured missing SMEMBERS");
        assert_eq!(missing_members["exists"], false);
        assert_eq!(missing_members["members"], serde_json::json!([]));
        let missing_scan = client
            .call_tool("redis_sscan", serde_json::json!({"key": missing}))
            .await
            .expect("missing SSCAN")
            .structured_content
            .expect("structured missing SSCAN");
        assert_eq!(missing_scan["exists"], false);
        assert_eq!(missing_scan["members"], serde_json::json!([]));

        client
            .call_tool(
                "redis_unlink",
                serde_json::json!({
                    "keys": [
                        left,
                        right,
                        missing,
                        difference_store,
                        empty_store,
                        intersection_store,
                        union_store,
                        wrong_type
                    ]
                }),
            )
            .await
            .expect("clean up set family");
    }
}

fn assert_redis_edge_score(value: &serde_json::Value) {
    let score = value.as_str().expect("sorted-set score string");
    assert!(
        matches!(score, "0.1" | "0.10000000000000001"),
        "unexpected Redis score spelling: {score}"
    );
    assert_eq!(score.parse::<f64>().expect("finite Redis score"), 0.1);
}

#[tokio::test]
async fn live_sorted_set_family_preserves_exact_scores_ranges_binary_and_nil_semantics() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let capabilities = DirectRedis::connect(&redis.url)
        .await
        .expect("connect for sorted-set capability discovery")
        .discover_capabilities()
        .await
        .expect("discover sorted-set test capabilities");
    let supports_zintercard = capabilities
        .redis_version()
        .is_some_and(|version| version >= RedisVersion::new(7, 0, 0));
    let supports_zset_count_aggregate = capabilities
        .redis_version()
        .is_some_and(|version| version >= RedisVersion::new(8, 8, 0));

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&redis.url, protocol), AccessMode::Full).await;
        let zset = test_key(&format!("sorted-set-family:{protocol}"));
        let other = test_key(&format!("sorted-set-family:{protocol}:other"));
        let lex = test_key(&format!("sorted-set-family:{protocol}:lex"));
        let difference_store = test_key(&format!("sorted-set-family:{protocol}:difference"));
        let intersection_store = test_key(&format!("sorted-set-family:{protocol}:intersection"));
        let range_store = test_key(&format!("sorted-set-family:{protocol}:range"));
        let union_store = test_key(&format!("sorted-set-family:{protocol}:union"));
        let missing = test_key(&format!("sorted-set-family:{protocol}:missing"));
        let wrong_type = test_key(&format!("sorted-set-family:{protocol}:wrong-type"));

        let added = client
            .call_tool(
                "redis_zadd",
                serde_json::json!({
                    "key": zset,
                    "members": [
                        {"score": "0.10000000000000001", "member": {"member": "/g==", "member_encoding": "base64"}},
                        {"score": "1", "member": "alice"},
                        {"score": 2, "member": "bob"},
                        {"score": "3.0", "member": "carol"}
                    ]
                }),
            )
            .await
            .expect("seed sorted-set family")
            .structured_content
            .expect("structured ZADD");
        assert_eq!(added["requested"], 4);
        assert_eq!(added["affected"], 4);
        client
            .call_tool(
                "redis_zadd",
                serde_json::json!({
                    "key": other,
                    "members": [
                        {"score": 10, "member": "alice"},
                        {"score": 4, "member": "bob"},
                        {"score": 5, "member": "dave"}
                    ]
                }),
            )
            .await
            .expect("seed second sorted set");

        let card = client
            .call_tool("redis_zcard", serde_json::json!({"key": zset}))
            .await
            .expect("ZCARD")
            .structured_content
            .expect("structured ZCARD");
        assert_eq!(card["exists"], true);
        assert_eq!(card["cardinality"], 4);

        let exact_result = client
            .call_tool(
                "redis_zscore",
                serde_json::json!({
                    "key": zset,
                    "member": "/g==",
                    "member_encoding": "base64"
                }),
            )
            .await
            .expect("binary ZSCORE");
        assert!(!exact_result.is_error, "binary ZSCORE: {exact_result:?}");
        let exact = exact_result.structured_content.expect("structured ZSCORE");
        assert_eq!(exact["zset_exists"], true);
        assert_eq!(exact["member_exists"], true);
        assert_redis_edge_score(&exact["score"]);

        let scores = client
            .call_tool(
                "redis_zmscore",
                serde_json::json!({
                    "key": zset,
                    "members": [
                        "alice",
                        "missing",
                        {"member": "/g==", "member_encoding": "base64"}
                    ]
                }),
            )
            .await
            .expect("ZMSCORE")
            .structured_content
            .expect("structured ZMSCORE");
        assert_eq!(scores["zset_exists"], true);
        assert_eq!(scores["members"][0]["score"], "1");
        assert_eq!(scores["members"][1]["member_exists"], false);
        assert_eq!(scores["members"][1]["score"], serde_json::Value::Null);
        assert_eq!(scores["members"][2]["member_encoding"], "base64");
        assert_redis_edge_score(&scores["members"][2]["score"]);

        let rank = client
            .call_tool(
                "redis_zrank",
                serde_json::json!({"key": zset, "member": "alice"}),
            )
            .await
            .expect("ZRANK")
            .structured_content
            .expect("structured ZRANK");
        assert_eq!(rank["rank"], 1);
        let reverse_rank = client
            .call_tool(
                "redis_zrevrank",
                serde_json::json!({"key": zset, "member": "alice"}),
            )
            .await
            .expect("ZREVRANK")
            .structured_content
            .expect("structured ZREVRANK");
        assert_eq!(reverse_rank["rank"], 2);

        let count = client
            .call_tool(
                "redis_zcount",
                serde_json::json!({
                    "key": zset,
                    "min": {"kind": "exclusive", "value": "1"},
                    "max": {"kind": "positive_infinity"}
                }),
            )
            .await
            .expect("ZCOUNT")
            .structured_content
            .expect("structured ZCOUNT");
        assert_eq!(count["count"], 2);

        let rank_page = client
            .call_tool(
                "redis_zrange",
                serde_json::json!({
                    "key": zset,
                    "range": {"kind": "rank", "start": 0, "stop": 1},
                    "withscores": true
                }),
            )
            .await
            .expect("rank ZRANGE")
            .structured_content
            .expect("structured rank ZRANGE");
        assert_eq!(rank_page["count"], 2);
        assert_eq!(rank_page["page"]["continuation"]["start"], 2);
        assert_eq!(rank_page["members"][0]["encoding"], "base64");

        let score_page = client
            .call_tool(
                "redis_zrange",
                serde_json::json!({
                    "key": zset,
                    "range": {
                        "kind": "score",
                        "min": {"kind": "inclusive", "value": "1"},
                        "max": {"kind": "exclusive", "value": "3"},
                        "limit": 1
                    },
                    "withscores": true
                }),
            )
            .await
            .expect("score ZRANGE")
            .structured_content
            .expect("structured score ZRANGE");
        assert_eq!(score_page["members"][0]["member"], "alice");
        assert_eq!(score_page["page"]["continuation"]["offset"], 1);
        let reverse_score = client
            .call_tool(
                "redis_zrange",
                serde_json::json!({
                    "key": zset,
                    "rev": true,
                    "range": {
                        "kind": "score",
                        "min": {"kind": "inclusive", "value": "1"},
                        "max": {"kind": "exclusive", "value": "3"},
                        "limit": 2
                    }
                }),
            )
            .await
            .expect("reverse score ZRANGE")
            .structured_content
            .expect("structured reverse score ZRANGE");
        assert_eq!(reverse_score["members"][0]["member"], "bob");
        assert_eq!(reverse_score["members"][1]["member"], "alice");

        client
            .call_tool(
                "redis_zadd",
                serde_json::json!({
                    "key": lex,
                    "members": [
                        {"score": 0, "member": "apple"},
                        {"score": 0, "member": "banana"},
                        {"score": 0, "member": "cherry"}
                    ]
                }),
            )
            .await
            .expect("seed lex sorted set");
        let lex_page = client
            .call_tool(
                "redis_zrange",
                serde_json::json!({
                    "key": lex,
                    "range": {
                        "kind": "lex",
                        "min": {"kind": "exclusive", "value": "apple"},
                        "max": {"kind": "positive_infinity"},
                        "limit": 1
                    }
                }),
            )
            .await
            .expect("lex ZRANGE")
            .structured_content
            .expect("structured lex ZRANGE");
        assert_eq!(lex_page["members"][0]["member"], "banana");
        assert_eq!(lex_page["page"]["continuation"]["offset"], 1);

        let scan = client
            .call_tool(
                "redis_zscan",
                serde_json::json!({"key": zset, "cursor": 0, "count": 100}),
            )
            .await
            .expect("ZSCAN")
            .structured_content
            .expect("structured ZSCAN");
        assert_eq!(scan["exists"], true);
        assert_eq!(scan["page"]["complete"], true);
        let scanned_binary = scan["members"]
            .as_array()
            .and_then(|members| members.iter().find(|member| member["encoding"] == "base64"))
            .expect("binary ZSCAN member");
        assert_redis_edge_score(&scanned_binary["score"]);

        if supports_zintercard {
            let intersection_cardinality = client
                .call_tool(
                    "redis_zintercard",
                    serde_json::json!({"keys": [zset, other], "limit": 2}),
                )
                .await
                .expect("ZINTERCARD")
                .structured_content
                .expect("structured ZINTERCARD");
            assert_eq!(intersection_cardinality["cardinality"], 2);
            assert_eq!(intersection_cardinality["limit_reached"], true);
            assert!(intersection_cardinality.get("members").is_none());
        }

        let difference = client
            .call_tool(
                "redis_zdiffstore",
                serde_json::json!({
                    "destination": difference_store,
                    "keys": [zset, other]
                }),
            )
            .await
            .expect("ZDIFFSTORE")
            .structured_content
            .expect("structured ZDIFFSTORE");
        assert_eq!(difference["destination_cardinality"], 2);
        assert!(difference.get("members").is_none());

        let intersection = client
            .call_tool(
                "redis_zinterstore",
                serde_json::json!({
                    "destination": intersection_store,
                    "sources": [
                        {"key": zset, "weight": 2},
                        {"key": other, "weight": 3}
                    ],
                    "aggregate": "max"
                }),
            )
            .await
            .expect("weighted ZINTERSTORE")
            .structured_content
            .expect("structured ZINTERSTORE");
        assert_eq!(intersection["destination_cardinality"], 2);
        assert_eq!(intersection["weighted"], true);
        assert_eq!(intersection["aggregate"], "max");

        let union = client
            .call_tool(
                "redis_zunionstore",
                serde_json::json!({
                    "destination": union_store,
                    "sources": [zset, other],
                    "aggregate": "sum"
                }),
            )
            .await
            .expect("ZUNIONSTORE")
            .structured_content
            .expect("structured ZUNIONSTORE");
        assert_eq!(union["destination_cardinality"], 5);
        assert_eq!(union["weighted"], false);
        if supports_zset_count_aggregate {
            let counted_union = client
                .call_tool(
                    "redis_zunionstore",
                    serde_json::json!({
                        "destination": union_store,
                        "sources": [zset, other],
                        "aggregate": "count"
                    }),
                )
                .await
                .expect("COUNT ZUNIONSTORE")
                .structured_content
                .expect("structured COUNT ZUNIONSTORE");
            assert_eq!(counted_union["destination_cardinality"], 5);
            assert_eq!(counted_union["aggregate"], "count");
        }

        let range_stored = client
            .call_tool(
                "redis_zrangestore",
                serde_json::json!({
                    "destination": range_store,
                    "source": zset,
                    "range": {
                        "kind": "score",
                        "min": {"kind": "inclusive", "value": "1"},
                        "max": {"kind": "positive_infinity"},
                        "limit": 2
                    }
                }),
            )
            .await
            .expect("bounded ZRANGESTORE")
            .structured_content
            .expect("structured ZRANGESTORE");
        assert_eq!(range_stored["requested_maximum"], 2);
        assert_eq!(range_stored["destination_cardinality"], 2);

        let increment = client
            .call_tool(
                "redis_zincrby",
                serde_json::json!({"key": zset, "member": "alice", "increment": "0.25"}),
            )
            .await
            .expect("ZINCRBY")
            .structured_content
            .expect("structured ZINCRBY");
        assert_eq!(increment["score"], "1.25");

        client
            .call_tool(
                "redis_zadd",
                serde_json::json!({
                    "key": zset,
                    "members": [{"score": "-1", "member": "xxxxxxxxxxxxxxxx"}]
                }),
            )
            .await
            .expect("seed oversized sorted-set member");
        let omitted_pop = client
            .call_tool(
                "redis_zpopmin",
                serde_json::json!({
                    "key": zset,
                    "count": 1,
                    "max_returned_bytes": 8
                }),
            )
            .await
            .expect("bounded ZPOPMIN")
            .structured_content
            .expect("structured bounded ZPOPMIN");
        assert_eq!(omitted_pop["count"], 1);
        assert_eq!(omitted_pop["member_bytes"], 16);
        assert_eq!(omitted_pop["members_omitted"], true);
        assert_eq!(omitted_pop["members"], serde_json::json!([]));

        let popped_min = client
            .call_tool(
                "redis_zpopmin",
                serde_json::json!({"key": zset, "count": 1}),
            )
            .await
            .expect("ZPOPMIN")
            .structured_content
            .expect("structured ZPOPMIN");
        assert_eq!(popped_min["members"][0]["encoding"], "base64");
        assert_redis_edge_score(&popped_min["members"][0]["score"]);
        let popped_max = client
            .call_tool(
                "redis_zpopmax",
                serde_json::json!({"key": zset, "count": 1}),
            )
            .await
            .expect("ZPOPMAX")
            .structured_content
            .expect("structured ZPOPMAX");
        assert_eq!(popped_max["members"][0]["member"], "carol");

        let removed = client
            .call_tool(
                "redis_zrem",
                serde_json::json!({"key": zset, "members": ["bob", "missing"]}),
            )
            .await
            .expect("ZREM")
            .structured_content
            .expect("structured ZREM");
        assert_eq!(removed["removed"], 1);
        let removed_range = client
            .call_tool(
                "redis_zremrangebyscore",
                serde_json::json!({
                    "key": zset,
                    "min": {"kind": "inclusive", "value": "1"},
                    "max": {"kind": "inclusive", "value": "2"}
                }),
            )
            .await
            .expect("ZREMRANGEBYSCORE")
            .structured_content
            .expect("structured ZREMRANGEBYSCORE");
        assert_eq!(removed_range["removed"], 1);

        let missing_score = client
            .call_tool(
                "redis_zscore",
                serde_json::json!({"key": missing, "member": "missing"}),
            )
            .await
            .expect("missing ZSCORE")
            .structured_content
            .expect("structured missing ZSCORE");
        assert_eq!(missing_score["zset_exists"], false);
        assert_eq!(missing_score["member_exists"], false);
        assert_eq!(missing_score["score"], serde_json::Value::Null);
        for tool in ["redis_zrange", "redis_zscan"] {
            let result = client
                .call_tool(tool, serde_json::json!({"key": missing}))
                .await
                .unwrap_or_else(|error| panic!("{tool}: {error}"))
                .structured_content
                .unwrap_or_else(|| panic!("{tool}: structured missing result"));
            assert_eq!(result["exists"], false, "{tool}");
            assert_eq!(result["members"], serde_json::json!([]), "{tool}");
        }

        client
            .call_tool(
                "redis_set",
                serde_json::json!({"key": wrong_type, "value": "not-a-zset"}),
            )
            .await
            .expect("seed wrong-type sorted-set key");
        let wrong = client
            .call_tool("redis_zcard", serde_json::json!({"key": wrong_type}))
            .await
            .expect("wrong-type ZCARD is a tool result");
        assert!(wrong.is_error);
        assert!(
            serde_json::to_string(&wrong)
                .expect("serialize wrong-type ZCARD")
                .contains("WRONGTYPE")
        );
        let wrong_store = client
            .call_tool(
                "redis_zdiffstore",
                serde_json::json!({
                    "destination": difference_store,
                    "keys": [zset, wrong_type]
                }),
            )
            .await
            .expect("wrong-type ZDIFFSTORE is a tool result");
        assert!(wrong_store.is_error);
        assert!(
            serde_json::to_string(&wrong_store)
                .expect("serialize wrong-type ZDIFFSTORE")
                .contains("WRONGTYPE")
        );

        client
            .call_tool(
                "redis_unlink",
                serde_json::json!({
                    "keys": [
                        zset,
                        other,
                        lex,
                        difference_store,
                        intersection_store,
                        range_store,
                        union_store,
                        missing,
                        wrong_type
                    ]
                }),
            )
            .await
            .expect("clean up sorted-set family");
    }
}

#[tokio::test]
async fn live_stream_family_is_bounded_binary_safe_and_group_aware_in_resp2_and_resp3() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&redis.url, protocol), AccessMode::Full).await;
        let stream = test_key(&format!("stream-family:{protocol}"));
        let missing = test_key(&format!("stream-family:{protocol}:missing"));
        let wrong_type = test_key(&format!("stream-family:{protocol}:wrong-type"));
        let group = "/g==";
        let consumer_one = "/Q==";
        let consumer_two = "/A==";

        let missing_length = client
            .call_tool("redis_xlen", serde_json::json!({"key": missing}))
            .await
            .expect("missing XLEN")
            .structured_content
            .expect("structured missing XLEN");
        assert_eq!(missing_length["exists"], false);
        assert_eq!(missing_length["length"], 0);

        for (milliseconds, value, value_encoding) in [
            (1, "created", "utf8"),
            (2, "/wA=", "base64"),
            (3, "updated", "utf8"),
        ] {
            let added = client
                .call_tool(
                    "redis_xadd",
                    serde_json::json!({
                        "key": stream,
                        "id": {"type": "explicit", "id": {"milliseconds": milliseconds, "sequence": 0}},
                        "fields": [{
                            "field": "/g==",
                            "field_encoding": "base64",
                            "value": value,
                            "value_encoding": value_encoding
                        }]
                    }),
                )
                .await
                .expect("XADD")
                .structured_content
                .expect("structured XADD");
            assert_eq!(added["added"], true);
            assert_eq!(added["id"], format!("{milliseconds}-0"));
        }

        let length = client
            .call_tool("redis_xlen", serde_json::json!({"key": stream}))
            .await
            .expect("XLEN")
            .structured_content
            .expect("structured XLEN");
        assert_eq!(length["exists"], true);
        assert_eq!(length["length"], 3);

        let range = client
            .call_tool(
                "redis_xrange",
                serde_json::json!({"key": stream, "count": 2}),
            )
            .await
            .expect("XRANGE")
            .structured_content
            .expect("structured XRANGE");
        assert_eq!(range["page"]["complete"], false);
        assert_eq!(range["page"]["continuation_id"], "2-0");
        assert_eq!(range["entries"][0]["fields"][0]["field_encoding"], "base64");
        assert_eq!(range["entries"][1]["fields"][0]["value_encoding"], "base64");
        assert_eq!(range["entries"][1]["fields"][0]["value"], "/wA=");

        let reverse = client
            .call_tool(
                "redis_xrevrange",
                serde_json::json!({"key": stream, "count": 2}),
            )
            .await
            .expect("XREVRANGE")
            .structured_content
            .expect("structured XREVRANGE");
        assert_eq!(reverse["entries"][0]["id"], "3-0");
        assert_eq!(reverse["page"]["continuation_id"], "2-0");

        let read = client
            .call_tool(
                "redis_xread",
                serde_json::json!({
                    "streams": [{
                        "key": stream,
                        "offset": {"type": "explicit", "id": {"milliseconds": 1, "sequence": 0}}
                    }],
                    "count": 2
                }),
            )
            .await
            .expect("XREAD")
            .structured_content
            .expect("structured XREAD");
        assert_eq!(read["count"], 2);
        assert_eq!(read["streams"][0]["continuation_id"], "3-0");

        let timed = client
            .call_tool(
                "redis_xread",
                serde_json::json!({
                    "streams": [{"key": missing, "offset": {"type": "latest"}}],
                    "count": 1,
                    "block_ms": 20
                }),
            )
            .await
            .expect("finite blocking XREAD")
            .structured_content
            .expect("structured finite XREAD");
        assert_eq!(timed["timed_out"], true);
        assert_eq!(timed["streams"], serde_json::json!([]));

        let created = client
            .call_tool(
                "redis_xgroup_create",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": group, "encoding": "base64"},
                    "id": {"type": "beginning"}
                }),
            )
            .await
            .expect("XGROUP CREATE")
            .structured_content
            .expect("structured XGROUP CREATE");
        assert_eq!(created["applied"], true);

        let create_consumer = client
            .call_tool(
                "redis_xgroup_createconsumer",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": group, "encoding": "base64"},
                    "consumer": {"value": consumer_one, "encoding": "base64"}
                }),
            )
            .await
            .expect("XGROUP CREATECONSUMER")
            .structured_content
            .expect("structured XGROUP CREATECONSUMER");
        assert_eq!(create_consumer["created"], true);

        let groups = client
            .call_tool("redis_xinfo_groups", serde_json::json!({"key": stream}))
            .await
            .expect("XINFO GROUPS")
            .structured_content
            .expect("structured XINFO GROUPS");
        assert_eq!(groups["groups"][0]["name"], group);
        assert_eq!(groups["groups"][0]["name_encoding"], "base64");

        let consumers_result = client
            .call_tool(
                "redis_xinfo_consumers",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": group, "encoding": "base64"}
                }),
            )
            .await
            .expect("XINFO CONSUMERS");
        assert!(
            !consumers_result.is_error,
            "XINFO CONSUMERS: {consumers_result:?}"
        );
        let consumers = consumers_result
            .structured_content
            .expect("structured XINFO CONSUMERS");
        assert_eq!(consumers["consumers"][0]["name"], consumer_one);
        assert_eq!(consumers["consumers"][0]["name_encoding"], "base64");

        let info = client
            .call_tool("redis_xinfo_stream", serde_json::json!({"key": stream}))
            .await
            .expect("XINFO STREAM")
            .structured_content
            .expect("structured XINFO STREAM");
        assert_eq!(info["length"], 3);
        assert_eq!(info["groups"], 1);
        assert_eq!(info["first_entry"]["id"], "1-0");

        let group_read = client
            .call_tool(
                "redis_xreadgroup",
                serde_json::json!({
                    "group": {"value": group, "encoding": "base64"},
                    "consumer": {"value": consumer_one, "encoding": "base64"},
                    "streams": [{"key": stream, "offset": {"type": "new"}}],
                    "count": 1,
                    "max_returned_bytes": 1
                }),
            )
            .await
            .expect("XREADGROUP")
            .structured_content
            .expect("structured XREADGROUP");
        assert_eq!(group_read["count"], 1);
        assert_eq!(group_read["fields_omitted"], true);
        assert_eq!(group_read["streams"][0]["entries"][0]["id"], "1-0");
        assert_eq!(
            group_read["streams"][0]["entries"][0]["fields"],
            serde_json::json!([])
        );

        let pending = client
            .call_tool(
                "redis_xpending",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": group, "encoding": "base64"}
                }),
            )
            .await
            .expect("XPENDING summary")
            .structured_content
            .expect("structured XPENDING summary");
        assert_eq!(pending["summary"]["count"], 1);
        assert_eq!(
            pending["summary"]["consumers"][0]["name_encoding"],
            "base64"
        );

        let pending_entries = client
            .call_tool(
                "redis_xpending",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": group, "encoding": "base64"},
                    "query": {"type": "entries", "count": 10}
                }),
            )
            .await
            .expect("XPENDING entries")
            .structured_content
            .expect("structured XPENDING entries");
        assert_eq!(pending_entries["entries"][0]["id"], "1-0");

        let claimed = client
            .call_tool(
                "redis_xclaim",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": group, "encoding": "base64"},
                    "consumer": {"value": consumer_two, "encoding": "base64"},
                    "min_idle_time_ms": 0,
                    "ids": [{"milliseconds": 1, "sequence": 0}],
                    "just_id": true
                }),
            )
            .await
            .expect("XCLAIM JUSTID")
            .structured_content
            .expect("structured XCLAIM JUSTID");
        assert_eq!(claimed["ids"], serde_json::json!(["1-0"]));

        let auto_claimed = client
            .call_tool(
                "redis_xautoclaim",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": group, "encoding": "base64"},
                    "consumer": {"value": consumer_one, "encoding": "base64"},
                    "min_idle_time_ms": 0,
                    "start": {"milliseconds": 0, "sequence": 0},
                    "count": 10,
                    "just_id": true
                }),
            )
            .await
            .expect("XAUTOCLAIM JUSTID")
            .structured_content
            .expect("structured XAUTOCLAIM JUSTID");
        assert_eq!(auto_claimed["ids"], serde_json::json!(["1-0"]));
        assert!(auto_claimed["next_start_id"].is_string());

        let acknowledged = client
            .call_tool(
                "redis_xack",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": group, "encoding": "base64"},
                    "ids": [{"milliseconds": 1, "sequence": 0}]
                }),
            )
            .await
            .expect("XACK")
            .structured_content
            .expect("structured XACK");
        assert_eq!(acknowledged["acknowledged"], 1);

        let deleted_consumer = client
            .call_tool(
                "redis_xgroup_delconsumer",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": group, "encoding": "base64"},
                    "consumer": {"value": consumer_two, "encoding": "base64"}
                }),
            )
            .await
            .expect("XGROUP DELCONSUMER")
            .structured_content
            .expect("structured XGROUP DELCONSUMER");
        assert_eq!(deleted_consumer["pending_deleted"], 0);

        let trimmed = client
            .call_tool(
                "redis_xtrim",
                serde_json::json!({
                    "key": stream,
                    "trim": {"type": "max_len", "threshold": 2}
                }),
            )
            .await
            .expect("XTRIM")
            .structured_content
            .expect("structured XTRIM");
        assert_eq!(trimmed["removed"], 1);

        let deleted = client
            .call_tool(
                "redis_xdel",
                serde_json::json!({
                    "key": stream,
                    "ids": [{"milliseconds": 2, "sequence": 0}]
                }),
            )
            .await
            .expect("XDEL")
            .structured_content
            .expect("structured XDEL");
        assert_eq!(deleted["deleted"], 1);

        let destroyed = client
            .call_tool(
                "redis_xgroup_destroy",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": group, "encoding": "base64"}
                }),
            )
            .await
            .expect("XGROUP DESTROY")
            .structured_content
            .expect("structured XGROUP DESTROY");
        assert_eq!(destroyed["applied"], true);
        let missing_group = client
            .call_tool(
                "redis_xpending",
                serde_json::json!({
                    "key": stream,
                    "group": {"value": "missing-group"}
                }),
            )
            .await
            .expect("missing group is represented as a tool result");
        assert!(missing_group.is_error);
        let missing_group =
            serde_json::to_string(&missing_group).expect("serialize missing-group result");
        assert!(
            missing_group.contains("NOGROUP") || missing_group.contains("no such key"),
            "{missing_group}"
        );

        client
            .call_tool(
                "redis_set",
                serde_json::json!({"key": wrong_type, "value": "not-a-stream"}),
            )
            .await
            .expect("seed wrong-type stream key");
        let wrong = client
            .call_tool("redis_xlen", serde_json::json!({"key": wrong_type}))
            .await
            .expect("wrong-type XLEN is a tool result");
        assert!(wrong.is_error);
        assert!(
            serde_json::to_string(&wrong)
                .expect("serialize wrong-type XLEN")
                .contains("WRONGTYPE")
        );

        client
            .call_tool(
                "redis_unlink",
                serde_json::json!({"keys": [stream, missing, wrong_type]}),
            )
            .await
            .expect("clean up stream family");
    }
}

#[tokio::test]
async fn live_hash_family_preserves_semantics_and_binary_data_in_resp2_and_resp3() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&redis.url, protocol), AccessMode::Full).await;
        let hash = test_key(&format!("hash-family:{protocol}"));
        let missing_hash = test_key(&format!("hash-family:{protocol}:missing"));
        let wrong_type = test_key(&format!("hash-family:{protocol}:wrong-type"));

        let seeded = client
            .call_tool(
                "redis_hset",
                serde_json::json!({
                    "key": hash,
                    "fields": {
                        "empty": "",
                        "name": "Ada",
                        "score": "1.25",
                        "visits": "10"
                    }
                }),
            )
            .await
            .expect("seed UTF-8 hash")
            .structured_content
            .expect("structured UTF-8 HSET");
        assert_eq!(seeded["fields_set"], 4);
        assert_eq!(seeded["fields_added"], 4);
        assert_eq!(seeded["fields_updated"], 0);

        let binary = client
            .call_tool(
                "redis_hset",
                serde_json::json!({
                    "key": hash,
                    "entries": [
                        {
                            "field": "/g==",
                            "field_encoding": "base64",
                            "value": "/Q==",
                            "value_encoding": "base64"
                        },
                        {"field": "name", "value": "Grace"}
                    ]
                }),
            )
            .await
            .expect("binary-safe HSET")
            .structured_content
            .expect("structured binary-safe HSET");
        assert_eq!(binary["fields_set"], 2);
        assert_eq!(binary["fields_added"], 1);
        assert_eq!(binary["fields_updated"], 1);

        let empty = client
            .call_tool(
                "redis_hget",
                serde_json::json!({"key": hash, "field": "empty"}),
            )
            .await
            .expect("HGET empty value")
            .structured_content
            .expect("structured empty HGET");
        assert_eq!(empty["hash_exists"], true);
        assert_eq!(empty["field_exists"], true);
        assert_eq!(empty["value"], "");

        let missing_field = client
            .call_tool(
                "redis_hget",
                serde_json::json!({"key": hash, "field": "missing"}),
            )
            .await
            .expect("HGET missing field")
            .structured_content
            .expect("structured missing-field HGET");
        assert_eq!(missing_field["hash_exists"], true);
        assert_eq!(missing_field["field_exists"], false);
        assert_eq!(missing_field["value"], serde_json::Value::Null);

        let missing = client
            .call_tool(
                "redis_hget",
                serde_json::json!({"key": missing_hash, "field": "missing"}),
            )
            .await
            .expect("HGET missing hash")
            .structured_content
            .expect("structured missing-hash HGET");
        assert_eq!(missing["hash_exists"], false);
        assert_eq!(missing["field_exists"], false);

        let exists = client
            .call_tool(
                "redis_hexists",
                serde_json::json!({"key": hash, "field": "name"}),
            )
            .await
            .expect("HEXISTS")
            .structured_content
            .expect("structured HEXISTS");
        assert_eq!(exists["hash_exists"], true);
        assert_eq!(exists["field_exists"], true);

        let length = client
            .call_tool("redis_hlen", serde_json::json!({"key": hash}))
            .await
            .expect("HLEN")
            .structured_content
            .expect("structured HLEN");
        assert_eq!(length["exists"], true);
        assert_eq!(length["length"], 5);

        let selected = client
            .call_tool(
                "redis_hmget",
                serde_json::json!({
                    "key": hash,
                    "fields": [
                        "name",
                        "missing",
                        {"field": "/g==", "field_encoding": "base64"},
                        "empty"
                    ]
                }),
            )
            .await
            .expect("HMGET")
            .structured_content
            .expect("structured HMGET");
        assert_eq!(selected["hash_exists"], true);
        assert_eq!(selected["count"], 4);
        assert_eq!(selected["values"][0]["value"], "Grace");
        assert_eq!(selected["values"][1]["exists"], false);
        assert_eq!(selected["values"][2]["field_encoding"], "base64");
        assert_eq!(selected["values"][2]["value"], "/Q==");
        assert_eq!(selected["values"][2]["value_encoding"], "base64");
        assert_eq!(selected["values"][3]["exists"], true);
        assert_eq!(selected["values"][3]["value"], "");

        let empty_length = client
            .call_tool(
                "redis_hstrlen",
                serde_json::json!({"key": hash, "field": "empty"}),
            )
            .await
            .expect("HSTRLEN empty value")
            .structured_content
            .expect("structured HSTRLEN");
        assert_eq!(empty_length["hash_exists"], true);
        assert_eq!(empty_length["field_exists"], true);
        assert_eq!(empty_length["length_bytes"], 0);

        let keys = client
            .call_tool("redis_hkeys", serde_json::json!({"key": hash}))
            .await
            .expect("HKEYS")
            .structured_content
            .expect("structured HKEYS");
        assert_eq!(keys["count"], 5);
        assert!(keys["fields"].as_array().is_some_and(|fields| {
            fields
                .iter()
                .any(|field| field["value"] == "/g==" && field["encoding"] == "base64")
        }));

        let values = client
            .call_tool("redis_hvals", serde_json::json!({"key": hash}))
            .await
            .expect("HVALS")
            .structured_content
            .expect("structured HVALS");
        assert_eq!(values["count"], 5);
        assert!(values["values"].as_array().is_some_and(|values| {
            values
                .iter()
                .any(|value| value["value"] == "/Q==" && value["encoding"] == "base64")
        }));

        let scan = client
            .call_tool(
                "redis_hscan",
                serde_json::json!({"key": hash, "cursor": 0, "count": 100}),
            )
            .await
            .expect("HSCAN")
            .structured_content
            .expect("structured HSCAN");
        assert_eq!(scan["cursor"], 0);
        assert_eq!(scan["count"], 5);
        assert_eq!(scan["page"]["complete"], true);

        let incremented = client
            .call_tool(
                "redis_hincrby",
                serde_json::json!({"key": hash, "field": "visits", "increment": -3}),
            )
            .await
            .expect("HINCRBY")
            .structured_content
            .expect("structured HINCRBY");
        assert_eq!(incremented["value"], 7);

        let decimal = client
            .call_tool(
                "redis_hincrbyfloat",
                serde_json::json!({"key": hash, "field": "score", "increment": 0.5}),
            )
            .await
            .expect("HINCRBYFLOAT")
            .structured_content
            .expect("structured HINCRBYFLOAT");
        assert_eq!(decimal["value"], "1.75");

        client
            .call_tool(
                "redis_set",
                serde_json::json!({"key": wrong_type, "value": "not-a-hash"}),
            )
            .await
            .expect("seed wrong-type key");
        let wrong_type_result = client
            .call_tool(
                "redis_hget",
                serde_json::json!({"key": wrong_type, "field": "name"}),
            )
            .await
            .expect("wrong type is represented as a tool result");
        assert!(wrong_type_result.is_error);
        let wrong_type_result =
            serde_json::to_string(&wrong_type_result).expect("serialize wrong-type result");
        assert!(
            wrong_type_result.contains("WRONGTYPE"),
            "{wrong_type_result}"
        );

        let deleted = client
            .call_tool(
                "redis_hdel",
                serde_json::json!({
                    "key": hash,
                    "fields": ["name", {"field": "/g==", "field_encoding": "base64"}]
                }),
            )
            .await
            .expect("HDEL")
            .structured_content
            .expect("structured HDEL");
        assert_eq!(deleted["requested"], 2);
        assert_eq!(deleted["deleted"], 2);

        client
            .call_tool("redis_del", serde_json::json!({"keys": [hash, wrong_type]}))
            .await
            .expect("clean up hash family test");
    }
}

#[tokio::test]
async fn live_hash_field_expiration_is_version_gated_and_typed() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let (client, capabilities) = capability_router_client(&redis.url, AccessMode::Full).await;
    let listed = client
        .list_tools()
        .await
        .expect("list capability-aware tools");
    let names = listed
        .tools
        .iter()
        .map(|tool| tool.name.as_ref())
        .collect::<Vec<_>>();
    let field_expiration_tools = [
        "redis_hexpire",
        "redis_hexpire_delete",
        "redis_hpersist",
        "redis_httl",
    ];
    let version = capabilities
        .redis_version()
        .expect("live capability discovery reports Redis version");

    if version < RedisVersion::new(7, 4, 0) {
        for tool in field_expiration_tools {
            assert!(
                !names.contains(&tool),
                "{tool} must be hidden on Redis {version}"
            );
        }
        return;
    }
    for tool in field_expiration_tools {
        assert!(
            names.contains(&tool),
            "{tool} must be exposed on Redis {version}"
        );
    }

    let hash = test_key("hash-field-expiration");
    client
        .call_tool(
            "redis_hset",
            serde_json::json!({
                "key": hash,
                "fields": {
                    "delete-me": "value",
                    "expiring": "value",
                    "persistent": "value"
                }
            }),
        )
        .await
        .expect("seed field-expiration hash");

    let expired = client
        .call_tool(
            "redis_hexpire",
            serde_json::json!({
                "key": hash,
                "seconds": 60,
                "fields": ["expiring", "missing"]
            }),
        )
        .await
        .expect("HEXPIRE")
        .structured_content
        .expect("structured HEXPIRE");
    assert_eq!(expired["expirations_set"], 1);
    assert_eq!(expired["fields_missing"], 1);
    assert_eq!(expired["fields"][0]["status"], "expiration_set");
    assert_eq!(expired["fields"][1]["status"], "field_missing");

    let conditional = client
        .call_tool(
            "redis_hexpire",
            serde_json::json!({
                "key": hash,
                "seconds": 30,
                "condition": "nx",
                "fields": ["expiring"]
            }),
        )
        .await
        .expect("conditional HEXPIRE")
        .structured_content
        .expect("structured conditional HEXPIRE");
    assert_eq!(conditional["condition_not_met"], 1);
    assert_eq!(conditional["fields"][0]["status"], "condition_not_met");

    let ttls = client
        .call_tool(
            "redis_httl",
            serde_json::json!({"key": hash, "fields": ["expiring", "persistent", "missing"]}),
        )
        .await
        .expect("HTTL")
        .structured_content
        .expect("structured HTTL");
    assert_eq!(ttls["hash_exists"], true);
    assert_eq!(ttls["fields"][0]["status"], "expiring");
    assert!(
        ttls["fields"][0]["ttl_seconds"]
            .as_u64()
            .is_some_and(|ttl| ttl <= 60)
    );
    assert_eq!(ttls["fields"][1]["status"], "persistent");
    assert_eq!(ttls["fields"][2]["status"], "field_missing");

    let millisecond_expiration = client
        .call_tool(
            "redis_hexpire",
            serde_json::json!({
                "key": hash,
                "expiration": 5000,
                "mode": "relative_milliseconds",
                "fields": ["expiring"]
            }),
        )
        .await
        .expect("HPEXPIRE")
        .structured_content
        .expect("structured HPEXPIRE");
    assert_eq!(millisecond_expiration["mode"], "relative_milliseconds");
    assert_eq!(millisecond_expiration["expirations_set"], 1);
    let millisecond_ttl = client
        .call_tool(
            "redis_httl",
            serde_json::json!({
                "key": hash,
                "mode": "remaining_milliseconds",
                "fields": ["expiring"]
            }),
        )
        .await
        .expect("HPTTL")
        .structured_content
        .expect("structured HPTTL");
    assert_eq!(millisecond_ttl["mode"], "remaining_milliseconds");
    assert!(
        millisecond_ttl["fields"][0]["value"]
            .as_u64()
            .is_some_and(|ttl| ttl > 0 && ttl <= 5000)
    );

    const FUTURE_UNIX_SECONDS: u64 = 4_102_444_800;
    const FUTURE_UNIX_MILLISECONDS: u64 = 4_102_444_800_000;
    client
        .call_tool(
            "redis_hexpire",
            serde_json::json!({
                "key": hash,
                "expiration": FUTURE_UNIX_SECONDS,
                "mode": "unix_seconds",
                "fields": ["expiring"]
            }),
        )
        .await
        .expect("HEXPIREAT");
    let expiration_time = client
        .call_tool(
            "redis_httl",
            serde_json::json!({
                "key": hash,
                "mode": "unix_seconds",
                "fields": ["expiring"]
            }),
        )
        .await
        .expect("HEXPIRETIME")
        .structured_content
        .expect("structured HEXPIRETIME");
    assert_eq!(expiration_time["fields"][0]["value"], FUTURE_UNIX_SECONDS);

    client
        .call_tool(
            "redis_hexpire",
            serde_json::json!({
                "key": hash,
                "expiration": FUTURE_UNIX_MILLISECONDS,
                "mode": "unix_milliseconds",
                "fields": ["expiring"]
            }),
        )
        .await
        .expect("HPEXPIREAT");
    let expiration_time = client
        .call_tool(
            "redis_httl",
            serde_json::json!({
                "key": hash,
                "mode": "unix_milliseconds",
                "fields": ["expiring"]
            }),
        )
        .await
        .expect("HPEXPIRETIME")
        .structured_content
        .expect("structured HPEXPIRETIME");
    assert_eq!(
        expiration_time["fields"][0]["value"],
        FUTURE_UNIX_MILLISECONDS
    );

    let deleted = client
        .call_tool(
            "redis_hexpire_delete",
            serde_json::json!({
                "key": hash,
                "mode": "relative_milliseconds",
                "fields": ["delete-me", "missing"]
            }),
        )
        .await
        .expect("destructive HPEXPIRE")
        .structured_content
        .expect("structured destructive HPEXPIRE");
    assert_eq!(deleted["deleted"], 1);
    assert_eq!(deleted["fields_missing"], 1);
    assert_eq!(deleted["fields"][0]["status"], "deleted");
    assert_eq!(deleted["fields"][1]["status"], "field_missing");

    let persisted = client
        .call_tool(
            "redis_hpersist",
            serde_json::json!({"key": hash, "fields": ["expiring", "persistent", "missing"]}),
        )
        .await
        .expect("HPERSIST")
        .structured_content
        .expect("structured HPERSIST");
    assert_eq!(persisted["expirations_removed"], 1);
    assert_eq!(persisted["already_persistent"], 1);
    assert_eq!(persisted["fields_missing"], 1);

    client
        .call_tool("redis_del", serde_json::json!({"keys": [hash]}))
        .await
        .expect("clean up field-expiration hash");
}

#[tokio::test]
async fn live_hash_sampling_and_bounded_sort_preserve_semantics_in_resp2_and_resp3() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let discovery = DirectRedis::connect(&redis.url)
        .await
        .expect("connect for HRANDFIELD/SORT discovery");
    let capabilities = discovery
        .discover_capabilities()
        .await
        .expect("discover HRANDFIELD/SORT capabilities");
    let version = capabilities
        .redis_version()
        .expect("live Redis reports its version");
    if version < RedisVersion::new(6, 2, 0) {
        return;
    }

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&redis.url, protocol), AccessMode::Full).await;
        let hash = test_key(&format!("issue-62:{protocol}:hash"));
        let missing = test_key(&format!("issue-62:{protocol}:missing"));
        let list = test_key(&format!("issue-62:{protocol}:list"));
        let stored_list = test_key(&format!("issue-62:{protocol}:stored"));
        let external_list = test_key(&format!("issue-62:{protocol}:external-list"));
        let weight_prefix = test_key(&format!("issue-62:{protocol}:weight"));
        let object_prefix = test_key(&format!("issue-62:{protocol}:object"));
        let weight_a = format!("{weight_prefix}:a");
        let weight_b = format!("{weight_prefix}:b");
        let object_a = format!("{object_prefix}:a");
        let object_b = format!("{object_prefix}:b");
        let wrong_type = test_key(&format!("issue-62:{protocol}:wrong-type"));

        client
            .call_tool(
                "redis_hset",
                serde_json::json!({"key": hash, "fields": {"alpha": "one", "beta": "two"}}),
            )
            .await
            .expect("seed HRANDFIELD hash");
        client
            .call_tool(
                "redis_set",
                serde_json::json!({"key": wrong_type, "value": "not-a-collection"}),
            )
            .await
            .expect("seed #62 wrong-type key");

        let sample = client
            .call_tool(
                "redis_hrandfield",
                serde_json::json!({"key": hash, "count": -8, "with_values": true}),
            )
            .await
            .expect("HRANDFIELD WITHVALUES")
            .structured_content
            .expect("structured HRANDFIELD WITHVALUES");
        assert_eq!(sample["hash_exists"], true, "{protocol}");
        assert_eq!(sample["duplicates_allowed"], true, "{protocol}");
        assert_eq!(sample["returned"], 8, "{protocol}");
        assert!(
            sample["entries"]
                .as_array()
                .is_some_and(|entries| entries.iter().all(|entry| entry["value"].is_string())),
            "{protocol}: {sample}"
        );

        let absent_sample = client
            .call_tool(
                "redis_hrandfield",
                serde_json::json!({"key": missing, "count": 2}),
            )
            .await
            .expect("missing HRANDFIELD")
            .structured_content
            .expect("structured missing HRANDFIELD");
        assert_eq!(absent_sample["hash_exists"], false, "{protocol}");
        assert_eq!(absent_sample["returned"], 0, "{protocol}");

        let wrong_hash = client
            .call_tool(
                "redis_hrandfield",
                serde_json::json!({"key": wrong_type, "count": 1}),
            )
            .await
            .expect("wrong-type HRANDFIELD result");
        assert!(wrong_hash.is_error, "{protocol}: {wrong_hash:?}");

        if version >= RedisVersion::new(7, 0, 0) {
            client
                .call_tool(
                    "redis_lpush",
                    serde_json::json!({"key": list, "elements": ["10", "2", "1"]}),
                )
                .await
                .expect("seed SORT list");
            let sorted = client
                .call_tool("redis_sort", serde_json::json!({"key": list, "count": 2}))
                .await
                .expect("bounded SORT_RO")
                .structured_content
                .expect("structured bounded SORT_RO");
            assert_eq!(sorted["source_exists"], true, "{protocol}");
            assert_eq!(sorted["returned"], 2, "{protocol}");
            assert_eq!(sorted["values"][0]["value"], "1", "{protocol}");
            assert_eq!(sorted["values"][1]["value"], "2", "{protocol}");

            let stored = client
                .call_tool(
                    "redis_sort_store",
                    serde_json::json!({
                        "key": list,
                        "destination": stored_list,
                        "count": 2,
                        "order": "descending"
                    }),
                )
                .await
                .expect("bounded SORT STORE")
                .structured_content
                .expect("structured bounded SORT STORE");
            assert_eq!(stored["stored"], 2, "{protocol}");
            let stored_values = client
                .call_tool("redis_lrange", serde_json::json!({"key": stored_list}))
                .await
                .expect("read SORT STORE destination")
                .structured_content
                .expect("structured SORT STORE destination");
            assert_eq!(stored_values["elements"][0]["value"], "10", "{protocol}");
            assert_eq!(stored_values["elements"][1]["value"], "2", "{protocol}");

            client
                .call_tool(
                    "redis_lpush",
                    serde_json::json!({"key": external_list, "elements": ["b", "a"]}),
                )
                .await
                .expect("seed external-pattern SORT list");
            for (key, value) in [(&weight_a, "2"), (&weight_b, "1")] {
                client
                    .call_tool("redis_set", serde_json::json!({"key": key, "value": value}))
                    .await
                    .expect("seed SORT weight");
            }
            for (key, name) in [(&object_a, "Ada"), (&object_b, "Bob")] {
                client
                    .call_tool(
                        "redis_hset",
                        serde_json::json!({"key": key, "fields": {"name": name}}),
                    )
                    .await
                    .expect("seed SORT object");
            }
            let external = client
                .call_tool(
                    "redis_sort",
                    serde_json::json!({
                        "key": external_list,
                        "by": format!("{weight_prefix}:*"),
                        "get": ["#", format!("{object_prefix}:*->name")],
                        "count": 2
                    }),
                )
                .await
                .expect("standalone external-pattern SORT_RO")
                .structured_content
                .expect("structured standalone external-pattern SORT_RO");
            assert_eq!(external["get_pattern_count"], 2, "{protocol}");
            assert_eq!(external["returned"], 4, "{protocol}");
            assert_eq!(external["values"][0]["value"], "b", "{protocol}");
            assert_eq!(external["values"][1]["value"], "Bob", "{protocol}");
            assert_eq!(external["values"][2]["value"], "a", "{protocol}");
            assert_eq!(external["values"][3]["value"], "Ada", "{protocol}");

            let absent_sort = client
                .call_tool(
                    "redis_sort",
                    serde_json::json!({"key": missing, "count": 2}),
                )
                .await
                .expect("missing SORT_RO")
                .structured_content
                .expect("structured missing SORT_RO");
            assert_eq!(absent_sort["source_exists"], false, "{protocol}");
            assert_eq!(absent_sort["returned"], 0, "{protocol}");

            let wrong_sort = client
                .call_tool(
                    "redis_sort",
                    serde_json::json!({"key": wrong_type, "count": 2}),
                )
                .await
                .expect("wrong-type SORT_RO result");
            assert!(wrong_sort.is_error, "{protocol}: {wrong_sort:?}");
        }

        client
            .call_tool(
                "redis_del",
                serde_json::json!({
                    "keys": [
                        hash, list, stored_list, external_list, weight_a, weight_b,
                        object_a, object_b, wrong_type
                    ]
                }),
            )
            .await
            .expect("clean #62 standalone keys");
    }
}

#[tokio::test]
async fn live_large_collections_are_paged_or_fail_with_stable_budget_errors() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let hash = test_key("budget:hash");
    let list = test_key("budget:list");
    let set = test_key("budget:set");
    let set_other = test_key("budget:set-other");
    let zset = test_key("budget:zset");

    let direct = redis::Client::open(redis.url.as_str()).expect("open direct Redis client");
    let mut connection = direct
        .get_multiplexed_async_connection()
        .await
        .expect("connect direct Redis client");
    let mut seed = redis::pipe();
    for index in 0..600 {
        seed.cmd("HSET")
            .arg(&hash)
            .arg(format!("field:{index:04}"))
            .arg(format!("value:{index:04}"))
            .ignore();
        seed.cmd("SADD")
            .arg(&set)
            .arg(format!("member:{index:04}"))
            .ignore();
        seed.cmd("SADD")
            .arg(&set_other)
            .arg(format!("other:{index:04}"))
            .ignore();
        if index < 200 {
            seed.cmd("LPUSH")
                .arg(&list)
                .arg(format!("element:{index:04}"))
                .ignore();
            seed.cmd("ZADD")
                .arg(&zset)
                .arg(index)
                .arg(format!("member:{index:04}"))
                .ignore();
        }
    }
    seed.query_async::<()>(&mut connection)
        .await
        .expect("seed large collections");

    let client = router_client_with_budget(
        &redis.url,
        AccessMode::Full,
        OutputBudget::new(1_000_000, 100),
    )
    .await;

    for (tool, input, alternative) in [
        (
            "redis_hgetall",
            serde_json::json!({"key": hash}),
            "redis_hscan",
        ),
        (
            "redis_hkeys",
            serde_json::json!({"key": hash}),
            "redis_hscan",
        ),
        (
            "redis_hvals",
            serde_json::json!({"key": hash}),
            "redis_hscan",
        ),
        (
            "redis_smembers",
            serde_json::json!({"key": set}),
            "redis_sscan",
        ),
    ] {
        let result = client
            .call_tool(tool, input)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(result.is_error, "{tool}");
        let error = &result.meta.as_ref().unwrap()["io.redis.mcp/outputLimit"];
        assert_eq!(error["code"], "output_limit_exceeded");
        assert_eq!(error["dimension"], "collection_entries");
        assert!(error["guidance"].as_str().unwrap().contains(alternative));
    }

    let algebra = client
        .call_tool(
            "redis_sunion",
            serde_json::json!({"keys": [set, set_other]}),
        )
        .await
        .expect("large SUNION");
    assert!(algebra.is_error);
    let algebra_error = &algebra.meta.as_ref().unwrap()["io.redis.mcp/outputLimit"];
    assert_eq!(algebra_error["dimension"], "collection_entries");
    assert_eq!(algebra_error["actual"], 1200);
    assert_eq!(algebra_error["limit"], 100);

    let oversized_members = (0..101)
        .map(|index| format!("member:{index:04}"))
        .collect::<Vec<_>>();
    let membership = client
        .call_tool(
            "redis_smismember",
            serde_json::json!({"key": set, "members": oversized_members.clone()}),
        )
        .await
        .expect("oversized SMISMEMBER is a tool result");
    assert!(membership.is_error);
    assert!(
        serde_json::to_string(&membership)
            .expect("serialize oversized SMISMEMBER")
            .contains("configured output limit of 100 entries")
    );
    let scores = client
        .call_tool(
            "redis_zmscore",
            serde_json::json!({"key": zset, "members": oversized_members}),
        )
        .await
        .expect("oversized ZMSCORE is a tool result");
    assert!(scores.is_error);
    assert!(
        serde_json::to_string(&scores)
            .expect("serialize oversized ZMSCORE")
            .contains("configured output limit of 100 entries")
    );
    let score_range = client
        .call_tool(
            "redis_zrange",
            serde_json::json!({
                "key": zset,
                "range": {
                    "kind": "score",
                    "min": {"kind": "negative_infinity"},
                    "max": {"kind": "positive_infinity"},
                    "limit": 101
                }
            }),
        )
        .await
        .expect("oversized score ZRANGE is a tool result");
    assert!(score_range.is_error);

    for (tool, key) in [
        ("redis_hscan", &hash),
        ("redis_sscan", &set),
        ("redis_zscan", &zset),
    ] {
        let page = client
            .call_tool(
                tool,
                serde_json::json!({"key": key, "cursor": 0, "count": 10}),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(!page.is_error, "{tool}: {page:?}");
        let page = page.structured_content.unwrap();
        assert!(page["count"].as_u64().is_some_and(|count| count > 0));
        assert_eq!(page["page"]["complete"], false);
        assert!(page["page"]["continuation"]["cursor"].as_u64().is_some());
    }

    for (tool, key) in [("redis_lrange", &list), ("redis_zrange", &zset)] {
        let arguments = if tool == "redis_zrange" {
            serde_json::json!({
                "key": key,
                "range": {"kind": "rank", "start": 0, "stop": 9}
            })
        } else {
            serde_json::json!({"key": key, "start": 0, "stop": 9})
        };
        let page = client
            .call_tool(tool, arguments)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(!page.is_error, "{tool}: {page:?}");
        let page = page.structured_content.unwrap();
        assert_eq!(page["count"], 10);
        assert_eq!(page["page"]["continuation"]["start"], 10);
    }

    let oversized_pop = client
        .call_tool("redis_lpop", serde_json::json!({"key": list, "count": 101}))
        .await
        .expect("oversized LPOP is a tool result");
    assert!(oversized_pop.is_error);
    assert!(
        serde_json::to_string(&oversized_pop)
            .expect("serialize oversized LPOP")
            .contains("configured output limit of 100 entries")
    );
    let length = client
        .call_tool("redis_llen", serde_json::json!({"key": list}))
        .await
        .expect("LLEN after rejected LPOP")
        .structured_content
        .expect("structured LLEN after rejected LPOP");
    assert_eq!(length["length"], 200);

    let oversized_zpop = client
        .call_tool(
            "redis_zpopmax",
            serde_json::json!({"key": zset, "count": 101}),
        )
        .await
        .expect("oversized ZPOPMAX is a tool result");
    assert!(oversized_zpop.is_error);
    assert!(
        serde_json::to_string(&oversized_zpop)
            .expect("serialize oversized ZPOPMAX")
            .contains("configured output limit of 100 entries")
    );
    let zcard = client
        .call_tool("redis_zcard", serde_json::json!({"key": zset}))
        .await
        .expect("ZCARD after rejected ZPOPMAX")
        .structured_content
        .expect("structured ZCARD after rejected ZPOPMAX");
    assert_eq!(zcard["cardinality"], 200);

    let raw = client
        .call_tool(
            "redis_command",
            serde_json::json!({"command": "HGETALL", "arguments": [hash]}),
        )
        .await
        .expect("raw HGETALL");
    assert!(raw.is_error);
    assert_eq!(
        raw.meta.as_ref().unwrap()["io.redis.mcp/outputLimit"]["code"],
        "output_limit_exceeded"
    );

    redis::cmd("DEL")
        .arg(&[&hash, &list, &set, &set_other, &zset])
        .query_async::<()>(&mut connection)
        .await
        .expect("clean up large collections");
}

#[tokio::test]
async fn live_binary_values_remain_explicit_in_resp2_and_resp3() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let url = redis.url;

    for protocol in ["resp2", "resp3"] {
        let protocol_url = with_protocol(&url, protocol);
        let prefix = test_key(&format!("binary:{protocol}"));
        let string = format!("{prefix}:string");
        let hash = format!("{prefix}:hash");
        let list = format!("{prefix}:list");
        let set = format!("{prefix}:set");
        let zset = format!("{prefix}:zset");

        let redis = redis::Client::open(protocol_url.as_str()).expect("open direct Redis client");
        let mut connection = redis
            .get_multiplexed_async_connection()
            .await
            .expect("connect direct Redis client");
        redis::cmd("SET")
            .arg(&string)
            .arg(b"\xff\0")
            .query_async::<()>(&mut connection)
            .await
            .expect("seed binary string");
        redis::cmd("HSET")
            .arg(&hash)
            .arg(b"\xfe")
            .arg(b"\xfd")
            .query_async::<()>(&mut connection)
            .await
            .expect("seed binary hash");
        redis::cmd("LPUSH")
            .arg(&list)
            .arg(b"\xfc")
            .query_async::<()>(&mut connection)
            .await
            .expect("seed binary list");
        redis::cmd("SADD")
            .arg(&set)
            .arg(b"\xfb")
            .query_async::<()>(&mut connection)
            .await
            .expect("seed binary set");
        redis::cmd("ZADD")
            .arg(&zset)
            .arg(1)
            .arg(b"\xfa")
            .query_async::<()>(&mut connection)
            .await
            .expect("seed binary sorted set");

        let native = RedisInvocationEngine::builder(
            DirectRedis::connect(&protocol_url)
                .await
                .expect("connect native invocation engine"),
        )
        .access(AccessMode::ReadOnly)
        .raw_command_policy(RawCommandPolicy::Classified)
        .build();
        let echoed = native
            .invoke(NativeRedisInvocation::new("ECHO").arg(vec![0xff, 0x00]))
            .await
            .expect("native binary ECHO");
        assert_eq!(echoed, RedisValue::BulkString(vec![0xff, 0x00]));

        let client = router_client(&protocol_url, AccessMode::ReadOnly).await;
        let get = client
            .call_tool("redis_get", serde_json::json!({"key": string}))
            .await
            .expect("binary get")
            .structured_content
            .expect("structured binary get");
        assert_eq!(get["encoding"], "base64");
        assert_eq!(get["value"], "/wA=");

        let hgetall = client
            .call_tool("redis_hgetall", serde_json::json!({"key": hash}))
            .await
            .expect("binary hgetall")
            .structured_content
            .expect("structured binary hgetall");
        assert_eq!(hgetall["entries"][0]["field_encoding"], "base64");
        assert_eq!(hgetall["entries"][0]["value_encoding"], "base64");

        let lrange = client
            .call_tool("redis_lrange", serde_json::json!({"key": list}))
            .await
            .expect("binary lrange")
            .structured_content
            .expect("structured binary lrange");
        assert_eq!(lrange["elements"][0]["encoding"], "base64");

        let smembers = client
            .call_tool("redis_smembers", serde_json::json!({"key": set}))
            .await
            .expect("binary smembers")
            .structured_content
            .expect("structured binary smembers");
        assert_eq!(smembers["members"][0]["encoding"], "base64");

        let membership = client
            .call_tool(
                "redis_sismember",
                serde_json::json!({
                    "key": set,
                    "member": "+w==",
                    "member_encoding": "base64"
                }),
            )
            .await
            .expect("binary SISMEMBER")
            .structured_content
            .expect("structured binary SISMEMBER");
        assert_eq!(membership["is_member"], true);

        let memberships = client
            .call_tool(
                "redis_smismember",
                serde_json::json!({
                    "key": set,
                    "members": [
                        {"member": "+w==", "member_encoding": "base64"},
                        "missing"
                    ]
                }),
            )
            .await
            .expect("binary SMISMEMBER")
            .structured_content
            .expect("structured binary SMISMEMBER");
        assert_eq!(memberships["members"][0]["member_encoding"], "base64");
        assert_eq!(memberships["members"][0]["is_member"], true);
        assert_eq!(memberships["members"][1]["is_member"], false);

        let zrange = client
            .call_tool(
                "redis_zrange",
                serde_json::json!({"key": zset, "withscores": true}),
            )
            .await
            .expect("binary zrange")
            .structured_content
            .expect("structured binary zrange");
        assert_eq!(zrange["members"][0]["encoding"], "base64");
        assert_eq!(zrange["members"][0]["score"], "1");

        let mut cleanup = redis::cmd("DEL");
        for key in [&string, &hash, &list, &set, &zset] {
            cleanup.arg(key);
        }
        cleanup
            .query_async::<()>(&mut connection)
            .await
            .expect("clean up binary keys");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn live_policy_timeouts_remain_authoritative_over_transport() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let target_url = redis::parse_redis_url(&redis.url).expect("parse Redis test URL");
    let target_host = target_url
        .host_str()
        .expect("wrapper-managed Redis URL uses a TCP host");
    let target_port = target_url.port().unwrap_or(6379);
    let proxy = FaultProxy::spawn((target_host, target_port))
        .await
        .expect("start timeout fault proxy");
    let mut proxy_url = target_url.clone();
    proxy_url
        .set_host(Some(&proxy.addr().ip().to_string()))
        .expect("set timeout fault-proxy host");
    proxy_url
        .set_port(Some(proxy.addr().port()))
        .expect("set timeout fault-proxy port");

    // Connect before injecting latency so this measures a command response,
    // not only connection setup. The invocation engine's two-second timeout
    // must remain authoritative over any transport response timeout.
    let invocation = RedisInvocationEngine::builder(
        DirectRedis::connect(proxy_url.as_str())
            .await
            .expect("connect delayed invocation executor"),
    )
    .access(AccessMode::ReadOnly)
    .raw_command_policy(RawCommandPolicy::Classified)
    .command_timeout(Duration::from_secs(2))
    .build();
    proxy.set_delay(
        Direction::UpstreamToClient,
        Delay::Fixed(Duration::from_millis(750)),
    );
    let started = Instant::now();
    let ping = invocation
        .invoke(NativeRedisInvocation::new("PING"))
        .await
        .expect("delayed PING stays within the invocation deadline");
    assert_eq!(ping, RedisValue::SimpleString("PONG".to_string()));
    assert!(
        started.elapsed() >= Duration::from_millis(650),
        "fault proxy did not delay the PING response: {:?}",
        started.elapsed()
    );

    // A transaction uses a fresh dedicated connection and confirms MULTI
    // before sending queued commands. Its complete multi-round-trip attempt
    // is likewise governed by RedisTransactionEngine's duration.
    let transaction_invocation = RedisInvocationEngine::builder(
        DirectRedis::connect(&redis.url)
            .await
            .expect("connect transaction policy executor"),
    )
    .access(AccessMode::ReadOnly)
    .raw_command_policy(RawCommandPolicy::Classified)
    .build();
    let transactions = RedisTransactionEngine::new(
        transaction_invocation,
        DirectRedisTransactions::standalone(proxy_url.as_str())
            .expect("prepare delayed transaction adapter"),
    )
    .with_limits(RedisTransactionLimits::default().with_max_duration(Duration::from_secs(5)));
    let started = Instant::now();
    let outcome = transactions
        .invoke(RedisTransactionRequest::new().command(NativeRedisInvocation::new("PING")))
        .await
        .expect("delayed transaction stays within its configured duration");
    assert!(
        matches!(
            outcome,
            RedisTransactionOutcome::Committed { ref results }
                if results == &[RedisValue::SimpleString("PONG".to_string())]
        ),
        "unexpected delayed transaction outcome: {outcome:?}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(650),
        "fault proxy did not delay the transaction response: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn live_diagnostics_are_structured_bounded_redacted_and_binary_safe() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let redis_client = redis::Client::open(redis.url.as_str()).expect("open diagnostics Redis");
    let mut connection = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("connect diagnostics Redis");
    let key = test_key("diagnostics");
    let binary_key = [test_key("diagnostics-binary").as_bytes(), &[0xff, 0x00]].concat();
    redis::cmd("SET")
        .arg(&key)
        .arg("diagnostic-value")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed diagnostic key");
    redis::cmd("SET")
        .arg(&binary_key)
        .arg("binary-diagnostic-value")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed binary diagnostic key");
    redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("diagnostic-admin-client")
        .query_async::<()>(&mut connection)
        .await
        .expect("name diagnostic admin client");

    for protocol in ["resp2", "resp3"] {
        let url = with_protocol(&redis.url, protocol);
        let client = router_client(&url, AccessMode::Full).await;

        let clients = client
            .call_tool("redis_client_list", serde_json::json!({"max_results": 20}))
            .await
            .expect("CLIENT LIST")
            .structured_content
            .expect("structured CLIENT LIST");
        assert!(clients["returned"].as_u64().is_some_and(|count| count >= 1));
        assert_eq!(clients["clients"][0]["address"], serde_json::Value::Null);
        assert_eq!(clients["clients"][0]["sensitive_fields_redacted"], true);
        let redacted = serde_json::to_string(&clients).expect("serialize redacted clients");
        assert!(!redacted.contains("diagnostic-admin-client"));

        let sensitive = client
            .call_tool(
                "redis_client_list",
                serde_json::json!({
                    "name": "diagnostic-admin-client",
                    "max_results": 20,
                    "include_sensitive": true
                }),
            )
            .await
            .expect("sensitive CLIENT LIST")
            .structured_content
            .expect("structured sensitive CLIENT LIST");
        assert_eq!(sensitive["matched"], 1);
        assert_eq!(
            sensitive["clients"][0]["name"]["value"],
            "diagnostic-admin-client"
        );

        let memory = client
            .call_tool("redis_memory_stats", serde_json::json!({}))
            .await
            .expect("MEMORY STATS")
            .structured_content
            .expect("structured MEMORY STATS");
        assert_eq!(memory["nodes"].as_array().map(Vec::len), Some(1));
        assert!(
            memory["nodes"][0]["fields"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );

        let modules = client
            .call_tool("redis_module_list", serde_json::json!({}))
            .await
            .expect("MODULE LIST")
            .structured_content
            .expect("structured MODULE LIST");
        assert!(modules["modules"].is_array());

        let slowlog = client
            .call_tool("redis_slowlog", serde_json::json!({"limit": 5}))
            .await
            .expect("SLOWLOG GET")
            .structured_content
            .expect("structured SLOWLOG GET");
        assert!(slowlog["entries"].is_array());
        let serialized = serde_json::to_string(&slowlog).expect("serialize SLOWLOG");
        assert!(!serialized.contains("diagnostic-value"));

        let latency = client
            .call_tool(
                "redis_latency_history",
                serde_json::json!({"event": "command", "limit": 5}),
            )
            .await
            .expect("LATENCY HISTORY")
            .structured_content
            .expect("structured LATENCY HISTORY");
        assert!(latency["samples"].is_array());

        let identity = client
            .call_tool("redis_acl_whoami", serde_json::json!({}))
            .await
            .expect("ACL WHOAMI")
            .structured_content
            .expect("structured ACL WHOAMI");
        assert_eq!(identity["identities"][0]["username"]["value"], "default");

        let health = client
            .call_tool("redis_health_check", serde_json::json!({}))
            .await
            .expect("health check")
            .structured_content
            .expect("structured health check");
        assert_eq!(health["status"], "ok");
        assert!(health["nodes"][0]["redis_version"].is_string());

        let connections = client
            .call_tool("redis_connection_summary", serde_json::json!({}))
            .await
            .expect("connection summary")
            .structured_content
            .expect("structured connection summary");
        assert!(
            connections["total"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
        assert_eq!(connections["client_identity_redacted"], true);

        let keyspace = client
            .call_tool("redis_keyspace_summary", serde_json::json!({}))
            .await
            .expect("keyspace summary")
            .structured_content
            .expect("structured keyspace summary");
        assert!(
            keyspace["total_keys"]
                .as_u64()
                .is_some_and(|count| count >= 2)
        );

        let memory_summary = client
            .call_tool("redis_memory_summary", serde_json::json!({}))
            .await
            .expect("memory summary")
            .structured_content
            .expect("structured memory summary");
        assert!(memory_summary["nodes"][0]["total_allocated_bytes"].is_number());

        let binary_key_base64 = BASE64.encode(&binary_key);
        let key_summary = client
            .call_tool(
                "redis_key_summary",
                serde_json::json!({"key": binary_key_base64, "key_encoding": "base64"}),
            )
            .await
            .expect("binary key summary")
            .structured_content
            .expect("structured binary key summary");
        assert_eq!(key_summary["exists"], true);
        assert_eq!(key_summary["key"]["encoding"], "base64");
        assert_eq!(key_summary["key"]["value"], binary_key_base64);

        // The suite shares one keyspace when REDIS_URL is set, so a single
        // SCAN page is not guaranteed to visit this key; follow the
        // continuation cursor until the key appears or the scan completes.
        let mut cursor = 0u64;
        let mut found = false;
        loop {
            let hotkeys = client
                .call_tool(
                    "redis_hotkeys",
                    serde_json::json!({
                        "pattern": key,
                        "count": 100,
                        "max_keys": 256,
                        "top": 5,
                        "cursor": cursor,
                    }),
                )
                .await
                .expect("one-page hotkey sample")
                .structured_content
                .expect("structured hotkey sample");
            if hotkeys["candidates"]
                .as_array()
                .expect("hotkey candidates")
                .iter()
                .any(|candidate| candidate["key"]["value"] == key)
            {
                found = true;
                break;
            }
            if hotkeys["page"]["complete"]
                .as_bool()
                .expect("hotkey page completion flag")
            {
                break;
            }
            cursor = hotkeys["page"]["continuation"]["cursor"]
                .as_u64()
                .expect("hotkey continuation cursor");
        }
        assert!(found, "hotkey scan pages must eventually surface {key}");
    }

    let limited = router_client_with_budget(
        &redis.url,
        AccessMode::ReadOnly,
        OutputBudget::new(512, 100),
    )
    .await
    .call_tool("redis_client_list", serde_json::json!({"max_results": 20}))
    .await
    .expect("bounded CLIENT LIST output");
    assert!(limited.is_error);
    let limited = serde_json::to_string(&limited).expect("serialize bounded CLIENT LIST");
    assert!(limited.contains("output_limit_exceeded"), "{limited}");

    redis::cmd("DEL")
        .arg(&key)
        .arg(&binary_key)
        .query_async::<()>(&mut connection)
        .await
        .expect("delete diagnostics keys");
}

#[cfg(unix)]
#[tokio::test]
async fn live_acl_failures_are_classified_without_leaking_credentials() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let redis_version = DirectRedis::connect(&redis.url)
        .await
        .expect("connect for ACL capability discovery")
        .discover_capabilities()
        .await
        .expect("discover ACL test capabilities")
        .redis_version()
        .expect("ACL test Redis reports its version");
    let password = "mcp-test-secret-42";
    let username = format!("mcp_reader_{}", std::process::id());

    let admin = redis::Client::open(redis.url.as_str()).expect("open admin Redis client");
    let mut connection = admin
        .get_multiplexed_async_connection()
        .await
        .expect("connect admin Redis client");
    let readable_hash = test_key("acl-readable-hash");
    let readable_list = test_key("acl-readable-list");
    let readable_set = test_key("acl-readable-set");
    let readable_zset = test_key("acl-readable-zset");
    let readable_stream = test_key("acl-readable-stream");
    let readable_bitmap = test_key("acl-readable-bitmap");
    redis::cmd("HSET")
        .arg(&readable_hash)
        .arg("name")
        .arg("Ada")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed ACL-readable hash");
    redis::cmd("RPUSH")
        .arg(&readable_list)
        .arg("visible")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed ACL-readable list");
    redis::cmd("SADD")
        .arg(&readable_set)
        .arg("visible")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed ACL-readable set");
    redis::cmd("ZADD")
        .arg(&readable_zset)
        .arg(1)
        .arg("visible")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed ACL-readable sorted set");
    redis::cmd("XADD")
        .arg(&readable_stream)
        .arg("1-0")
        .arg("event")
        .arg("visible")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed ACL-readable stream");
    redis::cmd("SETBIT")
        .arg(&readable_bitmap)
        .arg(7)
        .arg(1)
        .query_async::<()>(&mut connection)
        .await
        .expect("seed ACL-readable bitmap");
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg("~*")
        .arg("+ping")
        .arg("+get")
        .arg("+getrange")
        .arg("+hget")
        .arg("+lindex")
        .arg("+sismember")
        .arg("+zscore")
        .arg("+xlen")
        .arg("+getbit")
        .arg("+exists")
        .arg("+pubsub")
        .query_async::<()>(&mut connection)
        .await
        .expect("create restricted ACL user");

    let mut restricted_url = redis::parse_redis_url(&redis.url).expect("parse Redis test URL");
    restricted_url
        .set_username(&username)
        .expect("set restricted Redis username");
    restricted_url
        .set_password(Some("wrong-password"))
        .expect("set wrong Redis password");
    let authentication = match DirectRedis::connect(restricted_url.as_str()).await {
        Ok(_) => panic!("wrong Redis password unexpectedly authenticated"),
        Err(error) => error,
    };
    assert_eq!(authentication.kind(), RedisErrorKind::Authentication);
    assert!(!authentication.to_string().contains("wrong-password"));

    let session_owner = PubSubSessionOwner::new("acl-owner").expect("valid ACL owner");
    let wrong_password_sessions = DirectRedisPubSubSessionManager::standalone(
        restricted_url.as_str(),
        PubSubSessionLimits::default(),
    )
    .expect("parse wrong-password Pub/Sub target");
    let session_authentication = wrong_password_sessions
        .subscribe(
            &session_owner,
            PubSubSubscriptionKind::Channel,
            vec![b"acl-session-secret-channel".to_vec()],
        )
        .await
        .expect_err("wrong Pub/Sub password is rejected");
    assert_eq!(
        session_authentication.kind(),
        redis_mcp::PubSubSessionErrorKind::Authentication,
        "{session_authentication:?}"
    );
    let session_authentication = session_authentication.to_string();
    assert!(!session_authentication.contains("wrong-password"));
    assert!(!session_authentication.contains("acl-session-secret-channel"));

    restricted_url
        .set_password(Some(password))
        .expect("set restricted Redis password");

    let restricted_sessions = DirectRedisPubSubSessionManager::standalone(
        restricted_url.as_str(),
        PubSubSessionLimits::default(),
    )
    .expect("create restricted Pub/Sub manager");
    let session_authorization = restricted_sessions
        .subscribe(
            &session_owner,
            PubSubSubscriptionKind::Channel,
            vec![b"acl-session-secret-channel".to_vec()],
        )
        .await
        .expect_err("restricted Pub/Sub channel is rejected");
    assert_eq!(
        session_authorization.kind(),
        redis_mcp::PubSubSessionErrorKind::Authorization,
        "{session_authorization:?}"
    );
    let session_authorization = session_authorization.to_string();
    assert!(!session_authorization.contains(password));
    assert!(!session_authorization.contains("acl-session-secret-channel"));
    let native = RedisInvocationEngine::builder(
        DirectRedis::connect(restricted_url.as_str())
            .await
            .expect("connect restricted native engine"),
    )
    .access(AccessMode::ReadWrite)
    .raw_command_policy(RawCommandPolicy::Classified)
    .build();
    let allowed_native = native
        .invoke(NativeRedisInvocation::new("GET").arg(test_key("native-acl-readable")))
        .await
        .expect("ACL-allowed native GET");
    assert_eq!(allowed_native, RedisValue::Nil);
    let denied_native = native
        .invoke(
            NativeRedisInvocation::new("SET")
                .arg(test_key("native-acl-denied"))
                .arg("blocked-native-value"),
        )
        .await
        .expect_err("ACL-denied native SET");
    assert_eq!(denied_native.kind(), RedisErrorKind::Authorization);
    assert!(!denied_native.to_string().contains(password));
    assert!(!denied_native.to_string().contains("blocked-native-value"));

    let client = router_client(restricted_url.as_str(), AccessMode::ReadWrite).await;
    if redis_version >= RedisVersion::new(7, 0, 0) {
        let scripting_executor = DirectRedis::connect(restricted_url.as_str())
            .await
            .expect("connect restricted scripting executor");
        let scripting_router = RedisMcp::builder(scripting_executor)
            .access(AccessMode::ReadOnly)
            .bundles([ToolBundle::Scripting])
            .capabilities(redis_mcp::RedisCapabilities::unknown().with_redis_version(redis_version))
            .build();
        let scripting_client = McpClient::connect(ChannelTransport::new(scripting_router))
            .await
            .expect("connect restricted scripting client");
        scripting_client
            .initialize("redis-mcp-live-scripting-acl-test", "0")
            .await
            .expect("initialize restricted scripting client");
        let denied = scripting_client
            .call_tool(
                "redis_eval_ro",
                serde_json::json!({
                    "script": {"value": "return 'secret-script-body'"}
                }),
            )
            .await
            .expect("ACL-denied EVAL_RO is represented as a tool result");
        assert!(denied.is_error, "{denied:?}");
        let denied = serde_json::to_string(&denied).expect("serialize EVAL_RO ACL denial");
        assert!(denied.contains("[Authorization]"), "{denied}");
        assert!(!denied.contains(password));
        assert!(!denied.contains("secret-script-body"));
    }
    let allowed = client
        .call_tool(
            "redis_get",
            serde_json::json!({"key": test_key("acl-readable")}),
        )
        .await
        .expect("ACL-allowed GET");
    assert!(!allowed.is_error);
    let allowed_range = client
        .call_tool(
            "redis_getrange",
            serde_json::json!({
                "key": test_key("acl-readable-range"),
                "start": 0,
                "end": 7
            }),
        )
        .await
        .expect("ACL-allowed GETRANGE");
    assert!(!allowed_range.is_error);

    let allowed_hash = client
        .call_tool(
            "redis_hget",
            serde_json::json!({"key": readable_hash, "field": "name"}),
        )
        .await
        .expect("ACL-allowed HGET");
    assert!(!allowed_hash.is_error);
    assert_eq!(
        allowed_hash.structured_content.as_ref().unwrap()["value"],
        "Ada"
    );

    if redis_version >= RedisVersion::new(6, 2, 0) {
        let denied_sample = client
            .call_tool(
                "redis_hrandfield",
                serde_json::json!({"key": readable_hash, "count": 1}),
            )
            .await
            .expect("ACL-denied HRANDFIELD is represented as a tool result");
        assert!(denied_sample.is_error);
        let denied_sample =
            serde_json::to_string(&denied_sample).expect("serialize HRANDFIELD ACL denial");
        assert!(denied_sample.contains("[Authorization]"), "{denied_sample}");
        assert!(!denied_sample.contains(password));
    }

    if redis_version >= RedisVersion::new(7, 0, 0) {
        let denied_sort = client
            .call_tool(
                "redis_sort",
                serde_json::json!({"key": readable_list, "count": 1}),
            )
            .await
            .expect("ACL-denied SORT_RO is represented as a tool result");
        assert!(denied_sort.is_error);
        let denied_sort =
            serde_json::to_string(&denied_sort).expect("serialize SORT_RO ACL denial");
        assert!(denied_sort.contains("[Authorization]"), "{denied_sort}");
        assert!(!denied_sort.contains(password));
    }

    if redis_version >= RedisVersion::new(7, 4, 0) {
        let denied_hash_expiration = client
            .call_tool(
                "redis_hexpire",
                serde_json::json!({
                    "key": readable_hash,
                    "expiration": 1000,
                    "mode": "relative_milliseconds",
                    "fields": ["name"]
                }),
            )
            .await
            .expect("ACL-denied HPEXPIRE is represented as a tool result");
        assert!(denied_hash_expiration.is_error);
        let denied_hash_expiration =
            serde_json::to_string(&denied_hash_expiration).expect("serialize HPEXPIRE ACL denial");
        assert!(
            denied_hash_expiration.contains("[Authorization]"),
            "{denied_hash_expiration}"
        );
        assert!(!denied_hash_expiration.contains(password));
    }

    let allowed_list = client
        .call_tool(
            "redis_lindex",
            serde_json::json!({"key": readable_list, "index": 0}),
        )
        .await
        .expect("ACL-allowed LINDEX");
    assert!(!allowed_list.is_error);
    assert_eq!(
        allowed_list.structured_content.as_ref().unwrap()["value"],
        "visible"
    );

    let allowed_set = client
        .call_tool(
            "redis_sismember",
            serde_json::json!({"key": readable_set, "member": "visible"}),
        )
        .await
        .expect("ACL-allowed SISMEMBER");
    assert!(!allowed_set.is_error);
    assert_eq!(
        allowed_set.structured_content.as_ref().unwrap()["is_member"],
        true
    );

    let allowed_sorted_set = client
        .call_tool(
            "redis_zscore",
            serde_json::json!({"key": readable_zset, "member": "visible"}),
        )
        .await
        .expect("ACL-allowed ZSCORE");
    assert!(!allowed_sorted_set.is_error);
    assert_eq!(
        allowed_sorted_set.structured_content.as_ref().unwrap()["score"],
        "1"
    );

    let allowed_stream = client
        .call_tool("redis_xlen", serde_json::json!({"key": readable_stream}))
        .await
        .expect("ACL-allowed XLEN");
    assert!(!allowed_stream.is_error);
    assert_eq!(
        allowed_stream.structured_content.as_ref().unwrap()["length"],
        1
    );

    let allowed_bitmap = client
        .call_tool(
            "redis_getbit",
            serde_json::json!({"key": readable_bitmap, "offset": 7}),
        )
        .await
        .expect("ACL-allowed GETBIT");
    assert!(!allowed_bitmap.is_error);
    assert_eq!(
        allowed_bitmap.structured_content.as_ref().unwrap()["set"],
        true
    );

    let denied_bitmap = client
        .call_tool(
            "redis_setbit",
            serde_json::json!({"key": readable_bitmap, "offset": 123456, "value": true}),
        )
        .await
        .expect("ACL-denied SETBIT is represented as a tool result");
    assert!(denied_bitmap.is_error);
    let denied_bitmap = serde_json::to_string(&denied_bitmap).expect("serialize SETBIT ACL denial");
    assert!(denied_bitmap.contains("[Authorization]"), "{denied_bitmap}");
    assert!(!denied_bitmap.contains(password));
    assert!(!denied_bitmap.contains("123456"));

    let allowed_pubsub_inspection = client
        .call_tool("redis_pubsub_numpat", serde_json::json!({}))
        .await
        .expect("ACL-allowed PUBSUB NUMPAT");
    assert!(!allowed_pubsub_inspection.is_error);

    let denied_publish = client
        .call_tool(
            "redis_publish",
            serde_json::json!({
                "channel": {"value": "acl:denied"},
                "message": {"value": "blocked-pubsub-value"}
            }),
        )
        .await
        .expect("ACL-denied PUBLISH is represented as a tool result");
    assert!(denied_publish.is_error);
    let denied_publish =
        serde_json::to_string(&denied_publish).expect("serialize PUBLISH ACL denial");
    assert!(
        denied_publish.contains("[Authorization]"),
        "{denied_publish}"
    );
    assert!(!denied_publish.contains(password));
    assert!(!denied_publish.contains("blocked-pubsub-value"));

    let full_client = router_client(restricted_url.as_str(), AccessMode::Full).await;
    if redis_version >= RedisVersion::new(7, 4, 0) {
        let denied_expiration_delete = full_client
            .call_tool(
                "redis_hexpire_delete",
                serde_json::json!({"key": readable_hash, "fields": ["name"]}),
            )
            .await
            .expect("ACL-denied destructive HEXPIRE is represented as a tool result");
        assert!(denied_expiration_delete.is_error);
        let denied_expiration_delete = serde_json::to_string(&denied_expiration_delete)
            .expect("serialize destructive HEXPIRE ACL denial");
        assert!(
            denied_expiration_delete.contains("[Authorization]"),
            "{denied_expiration_delete}"
        );
        assert!(!denied_expiration_delete.contains(password));
    }
    let denied_list = full_client
        .call_tool(
            "redis_lpop",
            serde_json::json!({"key": readable_list, "count": 1}),
        )
        .await
        .expect("ACL-denied LPOP is represented as a tool result");
    assert!(denied_list.is_error);
    let denied_list = serde_json::to_string(&denied_list).expect("serialize LPOP ACL denial");
    assert!(denied_list.contains("[Authorization]"), "{denied_list}");
    assert!(!denied_list.contains(password));

    let denied_set_membership = client
        .call_tool(
            "redis_smismember",
            serde_json::json!({"key": readable_set, "members": ["visible"]}),
        )
        .await
        .expect("ACL-denied SMISMEMBER is represented as a tool result");
    assert!(denied_set_membership.is_error);
    let denied_set_membership =
        serde_json::to_string(&denied_set_membership).expect("serialize SMISMEMBER ACL denial");
    assert!(
        denied_set_membership.contains("[Authorization]"),
        "{denied_set_membership}"
    );
    assert!(!denied_set_membership.contains(password));

    let denied_set_remove = full_client
        .call_tool(
            "redis_srem",
            serde_json::json!({"key": readable_set, "members": ["visible"]}),
        )
        .await
        .expect("ACL-denied SREM is represented as a tool result");
    assert!(denied_set_remove.is_error);
    let denied_set_remove =
        serde_json::to_string(&denied_set_remove).expect("serialize SREM ACL denial");
    assert!(
        denied_set_remove.contains("[Authorization]"),
        "{denied_set_remove}"
    );
    assert!(!denied_set_remove.contains(password));

    let denied_set_store = full_client
        .call_tool(
            "redis_sdiffstore",
            serde_json::json!({
                "destination": test_key("acl-set-store"),
                "keys": [readable_set]
            }),
        )
        .await
        .expect("ACL-denied SDIFFSTORE is represented as a tool result");
    assert!(denied_set_store.is_error);
    let denied_set_store =
        serde_json::to_string(&denied_set_store).expect("serialize SDIFFSTORE ACL denial");
    assert!(
        denied_set_store.contains("[Authorization]"),
        "{denied_set_store}"
    );
    assert!(!denied_set_store.contains(password));

    if redis_version >= RedisVersion::new(8, 10, 0) {
        let denied_cardinality = client
            .call_tool(
                "redis_sdiffcard",
                serde_json::json!({"keys": [readable_set]}),
            )
            .await
            .expect("ACL-denied SDIFFCARD is represented as a tool result");
        assert!(denied_cardinality.is_error);
        let denied_cardinality =
            serde_json::to_string(&denied_cardinality).expect("serialize SDIFFCARD ACL denial");
        assert!(
            denied_cardinality.contains("[Authorization]"),
            "{denied_cardinality}"
        );
        assert!(!denied_cardinality.contains(password));
    }

    let denied_sorted_scores = client
        .call_tool(
            "redis_zmscore",
            serde_json::json!({"key": readable_zset, "members": ["visible"]}),
        )
        .await
        .expect("ACL-denied ZMSCORE is represented as a tool result");
    assert!(denied_sorted_scores.is_error);
    let denied_sorted_scores =
        serde_json::to_string(&denied_sorted_scores).expect("serialize ZMSCORE ACL denial");
    assert!(
        denied_sorted_scores.contains("[Authorization]"),
        "{denied_sorted_scores}"
    );
    assert!(!denied_sorted_scores.contains(password));

    let denied_sorted_remove = full_client
        .call_tool(
            "redis_zrem",
            serde_json::json!({"key": readable_zset, "members": ["visible"]}),
        )
        .await
        .expect("ACL-denied ZREM is represented as a tool result");
    assert!(denied_sorted_remove.is_error);
    let denied_sorted_remove =
        serde_json::to_string(&denied_sorted_remove).expect("serialize ZREM ACL denial");
    assert!(
        denied_sorted_remove.contains("[Authorization]"),
        "{denied_sorted_remove}"
    );
    assert!(!denied_sorted_remove.contains(password));

    let denied_sorted_store = full_client
        .call_tool(
            "redis_zunionstore",
            serde_json::json!({
                "destination": test_key("acl-zset-store"),
                "sources": [readable_zset]
            }),
        )
        .await
        .expect("ACL-denied ZUNIONSTORE is represented as a tool result");
    assert!(denied_sorted_store.is_error);
    let denied_sorted_store =
        serde_json::to_string(&denied_sorted_store).expect("serialize ZUNIONSTORE ACL denial");
    assert!(
        denied_sorted_store.contains("[Authorization]"),
        "{denied_sorted_store}"
    );
    assert!(!denied_sorted_store.contains(password));

    let denied_stream_range = client
        .call_tool(
            "redis_xrange",
            serde_json::json!({"key": readable_stream, "count": 1}),
        )
        .await
        .expect("ACL-denied XRANGE is represented as a tool result");
    assert!(denied_stream_range.is_error);
    let denied_stream_range =
        serde_json::to_string(&denied_stream_range).expect("serialize XRANGE ACL denial");
    assert!(
        denied_stream_range.contains("[Authorization]"),
        "{denied_stream_range}"
    );
    assert!(!denied_stream_range.contains(password));

    let denied_stream_add = client
        .call_tool(
            "redis_xadd",
            serde_json::json!({
                "key": readable_stream,
                "fields": [{"field": "event", "value": "blocked-stream-value"}]
            }),
        )
        .await
        .expect("ACL-denied XADD is represented as a tool result");
    assert!(denied_stream_add.is_error);
    let denied_stream_add =
        serde_json::to_string(&denied_stream_add).expect("serialize XADD ACL denial");
    assert!(
        denied_stream_add.contains("[Authorization]"),
        "{denied_stream_add}"
    );
    assert!(!denied_stream_add.contains(password));
    assert!(!denied_stream_add.contains("blocked-stream-value"));

    let denied = client
        .call_tool(
            "redis_set",
            serde_json::json!({"key": test_key("acl-denied"), "value": "blocked"}),
        )
        .await
        .expect("ACL-denied SET is represented as a tool result");
    assert!(denied.is_error);
    let denied = serde_json::to_string(&denied).expect("serialize ACL denial");
    assert!(denied.contains("[Authorization]"), "{denied}");
    assert!(!denied.contains(password));

    let denied_getex = client
        .call_tool(
            "redis_getex",
            serde_json::json!({
                "key": test_key("acl-denied-getex"),
                "expiration": {"type": "seconds", "value": 60}
            }),
        )
        .await
        .expect("ACL-denied GETEX is represented as a tool result");
    assert!(denied_getex.is_error);
    let denied_getex = serde_json::to_string(&denied_getex).expect("serialize GETEX ACL denial");
    assert!(denied_getex.contains("[Authorization]"), "{denied_getex}");
    assert!(!denied_getex.contains(password));

    let denied_hash = client
        .call_tool(
            "redis_hincrby",
            serde_json::json!({
                "key": readable_hash,
                "field": "visits",
                "increment": 123456789
            }),
        )
        .await
        .expect("ACL-denied HINCRBY is represented as a tool result");
    assert!(denied_hash.is_error);
    let denied_hash = serde_json::to_string(&denied_hash).expect("serialize HINCRBY ACL denial");
    assert!(denied_hash.contains("[Authorization]"), "{denied_hash}");
    assert!(!denied_hash.contains(password));
    assert!(!denied_hash.contains("123456789"));

    for (tool, arguments) in [
        ("redis_client_list", serde_json::json!({})),
        ("redis_health_check", serde_json::json!({})),
        ("redis_memory_stats", serde_json::json!({})),
        ("redis_acl_whoami", serde_json::json!({})),
    ] {
        let denied_diagnostic = client
            .call_tool(tool, arguments)
            .await
            .unwrap_or_else(|error| panic!("{tool} ACL denial: {error}"));
        assert!(denied_diagnostic.is_error, "{tool}");
        let denied_diagnostic = serde_json::to_string(&denied_diagnostic)
            .unwrap_or_else(|error| panic!("serialize {tool} ACL denial: {error}"));
        assert!(
            denied_diagnostic.contains("[Authorization]"),
            "{tool}: {denied_diagnostic}"
        );
        assert!(
            !denied_diagnostic.contains(password),
            "{tool}: {denied_diagnostic}"
        );
        assert!(
            !denied_diagnostic.contains(&username),
            "{tool}: {denied_diagnostic}"
        );
    }

    redis::cmd("DEL")
        .arg(&[
            &readable_hash,
            &readable_list,
            &readable_set,
            &readable_zset,
            &readable_stream,
            &readable_bitmap,
        ])
        .query_async::<()>(&mut connection)
        .await
        .expect("delete ACL-readable hash");

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<()>(&mut connection)
        .await
        .expect("delete restricted ACL user");
}

#[cfg(unix)]
#[tokio::test]
async fn live_connection_loss_is_bounded_and_direct_redis_recovers() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let target_url = redis::parse_redis_url(&redis.url).expect("parse Redis test URL");
    let Some(target_host) = target_url.host_str() else {
        eprintln!("skipping reconnect test: Redis URL does not use a TCP host");
        return;
    };
    let target_port = target_url.port().unwrap_or(6379);
    let proxy = FaultProxy::spawn((target_host, target_port))
        .await
        .expect("start Redis fault proxy");
    let mut proxy_url = target_url.clone();
    proxy_url
        .set_host(Some(&proxy.addr().ip().to_string()))
        .expect("set Redis fault-proxy host");
    proxy_url
        .set_port(Some(proxy.addr().port()))
        .expect("set Redis fault-proxy port");
    let client = router_client_with_timeout(
        proxy_url.as_str(),
        AccessMode::ReadOnly,
        Duration::from_millis(250),
    )
    .await;

    let initial = client
        .call_tool("redis_ping", serde_json::json!({}))
        .await
        .expect("initial PING");
    assert!(!initial.is_error);

    proxy.close_after(Direction::UpstreamToClient, 1);
    let disconnected = tokio::time::timeout(
        Duration::from_secs(2),
        client.call_tool("redis_ping", serde_json::json!({})),
    )
    .await
    .expect("library command timeout bounds connection loss")
    .expect("connection loss is represented as a tool result");
    assert!(disconnected.is_error);
    let disconnected = serde_json::to_string(&disconnected).expect("serialize disconnected result");
    assert!(
        disconnected.contains("[Connection]") || disconnected.contains("timed out"),
        "{disconnected}"
    );

    proxy.clear_close_after(Direction::UpstreamToClient);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let result = client
            .call_tool("redis_ping", serde_json::json!({}))
            .await
            .expect("reconnect PING is represented as a tool result");
        if !result.is_error {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "existing DirectRedis connection manager did not recover: {result:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let session_manager = DirectRedisPubSubSessionManager::standalone(
        proxy_url.as_str(),
        PubSubSessionLimits::default()
            .with_max_read_duration(Duration::from_secs(1))
            .with_operation_timeout(Duration::from_millis(500)),
    )
    .expect("create proxied Pub/Sub session manager");
    let session_client =
        pubsub_session_router_client(proxy_url.as_str(), session_manager.clone()).await;
    let session_channel = test_key("session-reconnect");
    let session_id = session_client
        .call_tool(
            "redis_subscribe",
            serde_json::json!({"subscriptions": [{"value": session_channel}]}),
        )
        .await
        .expect("open proxied Pub/Sub session")
        .structured_content
        .expect("structured proxied Pub/Sub session")["session_id"]
        .as_str()
        .expect("proxied Pub/Sub session id")
        .to_string();
    let publisher = redis::Client::open(redis.url.as_str()).expect("open reconnect publisher");
    let mut publisher = publisher
        .get_multiplexed_async_connection()
        .await
        .expect("connect reconnect publisher");
    redis::cmd("PUBLISH")
        .arg(&session_channel)
        .arg("before")
        .query_async::<()>(&mut publisher)
        .await
        .expect("publish before Pub/Sub fault");
    let before = session_client
        .call_tool(
            "redis_pubsub_read",
            serde_json::json!({"session_id": session_id, "wait_ms": 1000}),
        )
        .await
        .expect("read before Pub/Sub fault")
        .structured_content
        .expect("structured read before Pub/Sub fault");
    assert_eq!(before["messages"][0]["payload"]["value"], "before");

    proxy.close_after(Direction::UpstreamToClient, 1);
    redis::cmd("PUBLISH")
        .arg(&session_channel)
        .arg("cut")
        .query_async::<()>(&mut publisher)
        .await
        .expect("publish message that cuts Pub/Sub connection");
    let _bounded = tokio::time::timeout(
        Duration::from_secs(2),
        session_client.call_tool(
            "redis_pubsub_read",
            serde_json::json!({"session_id": session_id, "wait_ms": 100}),
        ),
    )
    .await
    .expect("Pub/Sub read remains bounded during connection loss")
    .expect("connection loss is represented as a Pub/Sub tool result");

    proxy.clear_close_after(Direction::UpstreamToClient);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        redis::cmd("PUBLISH")
            .arg(&session_channel)
            .arg("recovered")
            .query_async::<()>(&mut publisher)
            .await
            .expect("publish while waiting for Pub/Sub resubscription");
        let result = session_client
            .call_tool(
                "redis_pubsub_read",
                serde_json::json!({"session_id": session_id, "wait_ms": 100}),
            )
            .await
            .expect("reconnect read is represented as a tool result");
        if result.structured_content.as_ref().is_some_and(|content| {
            content["messages"].as_array().is_some_and(|messages| {
                messages
                    .iter()
                    .any(|message| message["payload"]["value"] == "recovered")
            })
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "existing Pub/Sub session did not reconnect and resubscribe: {result:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    session_client
        .call_tool(
            "redis_pubsub_close",
            serde_json::json!({"session_id": session_id}),
        )
        .await
        .expect("close reconnected Pub/Sub session");

    let capabilities = DirectRedis::connect(&redis.url)
        .await
        .expect("connect for reconnect capability discovery")
        .discover_capabilities()
        .await
        .expect("discover reconnect capabilities");
    if capabilities
        .redis_version()
        .is_some_and(|version| version >= RedisVersion::new(7, 0, 0))
    {
        let shard_channel = format!("{}:{{slot}}", test_key("session-shard-reconnect"));
        let shard_session = session_client
            .call_tool(
                "redis_ssubscribe",
                serde_json::json!({"subscriptions": [{"value": shard_channel}]}),
            )
            .await
            .expect("open proxied sharded Pub/Sub session")
            .structured_content
            .expect("structured proxied sharded session")["session_id"]
            .as_str()
            .expect("proxied sharded session id")
            .to_string();
        redis::cmd("SPUBLISH")
            .arg(&shard_channel)
            .arg("before-shard")
            .query_async::<()>(&mut publisher)
            .await
            .expect("publish before sharded Pub/Sub fault");
        let before = session_client
            .call_tool(
                "redis_pubsub_read",
                serde_json::json!({"session_id": shard_session, "wait_ms": 1000}),
            )
            .await
            .expect("read before sharded Pub/Sub fault")
            .structured_content
            .expect("structured read before sharded Pub/Sub fault");
        assert_eq!(before["messages"][0]["payload"]["value"], "before-shard");

        proxy.close_after(Direction::UpstreamToClient, 1);
        redis::cmd("SPUBLISH")
            .arg(&shard_channel)
            .arg("cut-shard")
            .query_async::<()>(&mut publisher)
            .await
            .expect("publish message that cuts sharded Pub/Sub connection");
        let _bounded = tokio::time::timeout(
            Duration::from_secs(2),
            session_client.call_tool(
                "redis_pubsub_read",
                serde_json::json!({"session_id": shard_session, "wait_ms": 100}),
            ),
        )
        .await
        .expect("sharded read remains bounded during connection loss")
        .expect("sharded connection loss is represented as a tool result");

        proxy.clear_close_after(Direction::UpstreamToClient);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            redis::cmd("SPUBLISH")
                .arg(&shard_channel)
                .arg("recovered-shard")
                .query_async::<()>(&mut publisher)
                .await
                .expect("publish while waiting for sharded resubscription");
            let result = session_client
                .call_tool(
                    "redis_pubsub_read",
                    serde_json::json!({"session_id": shard_session, "wait_ms": 100}),
                )
                .await
                .expect("sharded reconnect read is represented as a tool result");
            if result.structured_content.as_ref().is_some_and(|content| {
                content["messages"].as_array().is_some_and(|messages| {
                    messages
                        .iter()
                        .any(|message| message["payload"]["value"] == "recovered-shard")
                })
            }) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "existing sharded Pub/Sub session did not reconnect: {result:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        session_client
            .call_tool(
                "redis_pubsub_close",
                serde_json::json!({"session_id": shard_session}),
            )
            .await
            .expect("close reconnected sharded Pub/Sub session");
    }
    session_manager.shutdown().await;

    let fresh = router_client(proxy_url.as_str(), AccessMode::ReadOnly).await;
    let fresh_ping = fresh
        .call_tool("redis_ping", serde_json::json!({}))
        .await
        .expect("fresh DirectRedis PING");
    assert!(!fresh_ping.is_error);
}

async fn transaction_router_client(url: &str, output_budget: Option<OutputBudget>) -> McpClient {
    let executor = DirectRedis::connect(url)
        .await
        .expect("connect transaction executor");
    let transactions =
        DirectRedisTransactions::standalone(url).expect("prepare transaction adapter");
    let mut builder = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .raw_command_policy(RawCommandPolicy::Classified)
        .transactions(transactions);
    if let Some(output_budget) = output_budget {
        builder = builder.output_budget(output_budget);
    }
    let router = builder.build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect transaction MCP client");
    client
        .initialize("redis-mcp-live-transaction-test", "0")
        .await
        .expect("initialize transaction MCP client");
    client
}

#[tokio::test]
async fn live_transactions_commit_reject_and_align_per_command_results() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let client = transaction_router_client(&redis.url, None).await;
    let mut connection = redis::Client::open(redis.url.as_str())
        .expect("open verification client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect verification client");

    let counter = test_key("txn:counter");
    redis::cmd("SET")
        .arg(&counter)
        .arg("5")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed transaction counter");

    let committed = call_structured(
        &client,
        "redis_transaction",
        serde_json::json!({
            "watch": [{"value": counter}],
            "commands": [
                {"command": "INCR", "arguments": [{"value": counter}]},
                {"command": "GET", "arguments": [{"value": counter}]},
            ],
        }),
    )
    .await;
    assert_eq!(committed["status"], "committed");
    assert_eq!(committed["watched"], 1);
    assert_eq!(committed["command_count"], 2);
    let results = committed["results"].as_array().expect("aligned results");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["index"], 0);
    assert_eq!(results[0]["command"], "INCR");
    assert_eq!(results[0]["value"], 6);
    assert_eq!(results[1]["command"], "GET");
    assert_eq!(results[1]["value"]["value"], "6");
    let stored: String = redis::cmd("GET")
        .arg(&counter)
        .query_async(&mut connection)
        .await
        .expect("read committed counter");
    assert_eq!(stored, "6");

    let wrong_type = test_key("txn:wrong-type");
    redis::cmd("SET")
        .arg(&wrong_type)
        .arg("abc")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed non-numeric string");
    let mixed = call_structured(
        &client,
        "redis_transaction",
        serde_json::json!({
            "commands": [
                {"command": "INCR", "arguments": [{"value": wrong_type}]},
                {"command": "APPEND", "arguments": [{"value": wrong_type}, {"value": "!"}]},
            ],
        }),
    )
    .await;
    assert_eq!(mixed["status"], "committed");
    let mixed_results = mixed["results"].as_array().expect("mixed results");
    assert!(
        mixed_results[0]["value"]["server_error"]["code"].is_string(),
        "runtime INCR failure must stay in-band: {mixed:?}"
    );
    assert_eq!(mixed_results[1]["value"], 4);
    let appended: String = redis::cmd("GET")
        .arg(&wrong_type)
        .query_async(&mut connection)
        .await
        .expect("read appended value");
    assert_eq!(appended, "abc!");

    let rejected_key = test_key("txn:rejected");
    let rejected = call_structured(
        &client,
        "redis_transaction",
        serde_json::json!({
            "commands": [
                {"command": "SET", "arguments": [{"value": rejected_key}, {"value": "first"}]},
                {"command": "SET", "arguments": [{"value": rejected_key}]},
            ],
        }),
    )
    .await;
    assert_eq!(rejected["status"], "rejected");
    let failures = rejected["failures"].as_array().expect("queue failures");
    assert!(!failures.is_empty(), "{rejected:?}");
    assert_eq!(failures[0]["index"], 1);
    assert_eq!(failures[0]["command"], "SET");
    assert_eq!(failures[0]["code"], "ERR");
    let rejected_exists: i64 = redis::cmd("EXISTS")
        .arg(&rejected_key)
        .query_async(&mut connection)
        .await
        .expect("verify rejected transaction executed nothing");
    assert_eq!(rejected_exists, 0);

    let binary_key = test_key("txn:binary");
    let binary_value = BASE64.encode([0xff, 0x00, 0x01]);
    let binary = call_structured(
        &client,
        "redis_transaction",
        serde_json::json!({
            "commands": [
                {
                    "command": "SET",
                    "arguments": [
                        {"value": binary_key},
                        {"value": binary_value, "encoding": "base64"},
                    ],
                },
                {"command": "GET", "arguments": [{"value": binary_key}]},
            ],
        }),
    )
    .await;
    assert_eq!(binary["status"], "committed");
    assert_eq!(binary["results"][1]["value"]["encoding"], "base64");
    assert_eq!(binary["results"][1]["value"]["value"], binary_value);

    for (command, code) in [
        ("SUBSCRIBE", "SUBSCRIPTION_COMMAND_UNSUPPORTED"),
        ("MULTI", "TRANSACTION_COMMAND_UNSUPPORTED"),
        ("EVAL", "SCRIPT_COMMAND_UNSUPPORTED"),
        ("BLPOP", "BLOCKING_COMMAND_UNSUPPORTED"),
        ("FLUSHALL", "ADMIN_COMMAND_UNSUPPORTED"),
    ] {
        let blocked = client
            .call_tool(
                "redis_transaction",
                serde_json::json!({
                    "commands": [
                        {"command": "GET", "arguments": [{"value": counter}]},
                        {"command": command, "arguments": [{"value": "argument"}]},
                    ],
                }),
            )
            .await
            .expect("blocked transaction is a tool result");
        assert!(blocked.is_error, "{command}: {blocked:?}");
        assert!(
            serde_json::to_string(&blocked)
                .expect("serialize blocked transaction")
                .contains(code),
            "{command}: {blocked:?}"
        );
    }

    let big_key = test_key("txn:big");
    redis::cmd("SET")
        .arg(&big_key)
        .arg("x".repeat(64 * 1024))
        .query_async::<()>(&mut connection)
        .await
        .expect("seed oversized value");
    let budget_client =
        transaction_router_client(&redis.url, Some(OutputBudget::new(4_096, 1_000))).await;
    let overflow = budget_client
        .call_tool(
            "redis_transaction",
            serde_json::json!({
                "commands": [{"command": "GET", "arguments": [{"value": big_key}]}],
            }),
        )
        .await
        .expect("oversized transaction is a tool result");
    assert!(overflow.is_error, "{overflow:?}");
    assert_eq!(
        overflow.meta.as_ref().expect("transaction output metadata")["io.redis.mcp/outputLimit"]["code"],
        "output_limit_exceeded"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn live_watched_transactions_abort_on_conflicting_writes() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let target_url = redis::parse_redis_url(&redis.url).expect("parse Redis test URL");
    let Some(target_host) = target_url.host_str() else {
        eprintln!("skipping watched transaction test: Redis URL does not use a TCP host");
        return;
    };
    let target_port = target_url.port().unwrap_or(6379);
    let proxy = FaultProxy::spawn((target_host, target_port))
        .await
        .expect("start transaction fault proxy");
    let mut proxy_url = target_url.clone();
    proxy_url
        .set_host(Some(&proxy.addr().ip().to_string()))
        .expect("set transaction fault-proxy host");
    proxy_url
        .set_port(Some(proxy.addr().port()))
        .expect("set transaction fault-proxy port");

    let watched_key = test_key("txn:watched");
    let mut connection = redis::Client::open(redis.url.as_str())
        .expect("open watched-key client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect watched-key client");
    redis::cmd("SET")
        .arg(&watched_key)
        .arg("initial")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed watched key");

    let invocation = RedisInvocationEngine::builder(
        DirectRedis::connect(&redis.url)
            .await
            .expect("connect invocation executor"),
    )
    .access(AccessMode::Full)
    .raw_command_policy(RawCommandPolicy::Classified)
    .build();
    let engine = RedisTransactionEngine::new(
        invocation,
        DirectRedisTransactions::standalone(proxy_url.as_str())
            .expect("prepare proxied transaction adapter"),
    );
    let request = || {
        RedisTransactionRequest::new()
            .watch(watched_key.clone())
            .command(NativeRedisInvocation::new("GET").arg(watched_key.clone()))
    };

    // Widen the WATCH-to-EXEC window so a concurrent writer always conflicts.
    proxy.set_delay(
        Direction::ClientToUpstream,
        Delay::Fixed(Duration::from_millis(120)),
    );
    let writer_url = redis.url.clone();
    let writer_key = watched_key.clone();
    let writer = tokio::spawn(async move {
        let mut connection = redis::Client::open(writer_url.as_str())
            .expect("open conflicting writer")
            .get_multiplexed_async_connection()
            .await
            .expect("connect conflicting writer");
        for iteration in 0_u32.. {
            redis::cmd("SET")
                .arg(&writer_key)
                .arg(iteration)
                .query_async::<()>(&mut connection)
                .await
                .expect("conflicting write");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let aborted = engine
        .invoke(request())
        .await
        .expect("watched transaction under contention");
    writer.abort();
    assert_eq!(aborted, RedisTransactionOutcome::Aborted);
    let unchanged: String = redis::cmd("TYPE")
        .arg(&watched_key)
        .query_async(&mut connection)
        .await
        .expect("watched key still exists");
    assert_eq!(unchanged, "string");

    proxy.clear_delay(Direction::ClientToUpstream);
    let committed = engine
        .invoke(request())
        .await
        .expect("watched transaction without contention");
    match committed {
        RedisTransactionOutcome::Committed { results } => assert_eq!(results.len(), 1),
        other => panic!("uncontended watched transaction must commit: {other:?}"),
    }
}

#[tokio::test]
async fn live_acl_restricted_transactions_reject_at_queue_time() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let mut admin = redis::Client::open(redis.url.as_str())
        .expect("open ACL admin client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect ACL admin client");
    let username = format!("redis-mcp-txn-acl-{}", std::process::id());
    let watchless_username = format!("redis-mcp-txn-watchless-{}", std::process::id());
    let password = "txn-acl-secret";
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg("~*")
        .arg("+multi")
        .arg("+exec")
        .arg("+watch")
        .arg("+get")
        .query_async::<()>(&mut admin)
        .await
        .expect("create transaction ACL user");
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&watchless_username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg("~*")
        .arg("+multi")
        .arg("+exec")
        .arg("+get")
        .query_async::<()>(&mut admin)
        .await
        .expect("create watchless ACL user");

    let key = test_key("txn:acl");
    let base_url = redis.url.clone();
    let engine_for = move |url: String| {
        let base_url = base_url.clone();
        async move {
            let invocation = RedisInvocationEngine::builder(
                DirectRedis::connect(&base_url)
                    .await
                    .expect("connect ACL invocation executor"),
            )
            .access(AccessMode::Full)
            .raw_command_policy(RawCommandPolicy::Classified)
            .build();
            RedisTransactionEngine::new(
                invocation,
                DirectRedisTransactions::standalone(&url).expect("prepare ACL transaction adapter"),
            )
        }
    };

    let mut restricted_url = redis::parse_redis_url(&redis.url).expect("parse Redis test URL");
    restricted_url
        .set_username(&username)
        .expect("set restricted transaction username");
    restricted_url
        .set_password(Some(password))
        .expect("set restricted transaction password");
    let engine = engine_for(restricted_url.to_string()).await;
    let outcome = engine
        .invoke(
            RedisTransactionRequest::new()
                .watch(key.clone())
                .command(NativeRedisInvocation::new("GET").arg(key.clone()))
                .command(
                    NativeRedisInvocation::new("SET")
                        .arg(key.clone())
                        .arg("denied"),
                ),
        )
        .await
        .expect("ACL-limited transaction returns a structured outcome");
    match outcome {
        RedisTransactionOutcome::Rejected { failures } => {
            let failure = failures
                .iter()
                .find(|failure| failure.index() == Some(1))
                .expect("SET rejection carries its command index");
            assert_eq!(failure.code(), "NOPERM");
        }
        other => panic!("ACL denial must reject the transaction: {other:?}"),
    }
    let denied_exists: i64 = redis::cmd("EXISTS")
        .arg(&key)
        .query_async(&mut admin)
        .await
        .expect("verify denied transaction executed nothing");
    assert_eq!(denied_exists, 0);

    let mut watchless_url = redis::parse_redis_url(&redis.url).expect("parse Redis test URL");
    watchless_url
        .set_username(&watchless_username)
        .expect("set watchless transaction username");
    watchless_url
        .set_password(Some(password))
        .expect("set watchless transaction password");
    let watchless_engine = engine_for(watchless_url.to_string()).await;
    let watch_denied = watchless_engine
        .invoke(
            RedisTransactionRequest::new()
                .watch(key.clone())
                .command(NativeRedisInvocation::new("GET").arg(key.clone())),
        )
        .await
        .expect_err("WATCH permission failures must fail closed before MULTI");
    assert_eq!(watch_denied.code(), Some("TRANSACTION_WATCH_FAILED"));

    for user in [&username, &watchless_username] {
        redis::cmd("ACL")
            .arg("DELUSER")
            .arg(user)
            .query_async::<i64>(&mut admin)
            .await
            .expect("remove transaction ACL user");
    }
}

async fn invocation_router_client(
    url: &str,
    access: AccessMode,
    output_budget: Option<OutputBudget>,
) -> McpClient {
    let executor = DirectRedis::connect(url)
        .await
        .expect("connect invocation executor");
    let mut builder = RedisMcp::builder(executor)
        .access(access)
        .bundles([ToolBundle::Essentials, ToolBundle::Invocation])
        .raw_command_policy(RawCommandPolicy::Classified);
    if let Some(output_budget) = output_budget {
        builder = builder.output_budget(output_budget);
    }
    let router = builder.build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect invocation MCP client");
    client
        .initialize("redis-mcp-live-invocation-test", "0")
        .await
        .expect("initialize invocation MCP client");
    client
}

#[tokio::test]
async fn live_governed_argv_execution_matches_curated_tools() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let mut connection = redis::Client::open(redis.url.as_str())
        .expect("open argv verification client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect argv verification client");
    let key = test_key("argv:greeting");
    let missing = test_key("argv:missing");
    redis::cmd("SET")
        .arg(&key)
        .arg("hello")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed argv key");

    let read_client = invocation_router_client(&redis.url, AccessMode::ReadOnly, None).await;

    // `GET foo` agrees with `redis_get key=foo` on stored and missing keys.
    let curated = call_structured(&read_client, "redis_get", serde_json::json!({"key": key})).await;
    let argv = call_structured(
        &read_client,
        "redis_command_readonly",
        serde_json::json!({"command": "GET", "arguments": [{"value": key}]}),
    )
    .await;
    assert_eq!(curated["value"], "hello");
    assert_eq!(argv["value"]["value"], "hello");
    assert_eq!(argv["value"]["encoding"], "utf8");
    assert_eq!(argv["required_access"], "read_only");

    let curated_missing = call_structured(
        &read_client,
        "redis_get",
        serde_json::json!({"key": missing}),
    )
    .await;
    let argv_missing = call_structured(
        &read_client,
        "redis_command_readonly",
        serde_json::json!({"command": "GET", "arguments": [{"value": missing}]}),
    )
    .await;
    assert_eq!(curated_missing["exists"], false);
    assert_eq!(argv_missing["value"], serde_json::Value::Null);

    // Writes stay rejected at the read tier, before execution.
    let rejected = read_client
        .call_tool(
            "redis_command_readonly",
            serde_json::json!({
                "command": "SET",
                "arguments": [{"value": key}, {"value": "changed"}],
            }),
        )
        .await
        .expect("read-tier SET is a tool result");
    assert!(rejected.is_error, "{rejected:?}");
    let unchanged: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .expect("verify read tier executed nothing");
    assert_eq!(unchanged, "hello");

    // The write tier executes ordinary writes without unrestricted raw
    // commands and still rejects destructive forms.
    let write_client = invocation_router_client(&redis.url, AccessMode::ReadWrite, None).await;
    let binary = BASE64.encode([0xff, 0x00, 0x42]);
    let written = call_structured(
        &write_client,
        "redis_command_write",
        serde_json::json!({
            "command": "SET",
            "arguments": [
                {"value": key},
                {"value": binary, "encoding": "base64"},
            ],
        }),
    )
    .await;
    assert_eq!(written["value"], "OK");
    let round_trip = call_structured(
        &write_client,
        "redis_command_readonly",
        serde_json::json!({"command": "GET", "arguments": [{"value": key}]}),
    )
    .await;
    assert_eq!(round_trip["value"]["encoding"], "base64");
    assert_eq!(round_trip["value"]["value"], binary);

    let destructive = write_client
        .call_tool(
            "redis_command_write",
            serde_json::json!({"command": "DEL", "arguments": [{"value": key}]}),
        )
        .await
        .expect("write-tier DEL is a tool result");
    assert!(destructive.is_error, "{destructive:?}");

    // Output budgets bound argv results exactly like curated tools.
    let big = test_key("argv:big");
    redis::cmd("SET")
        .arg(&big)
        .arg("x".repeat(64 * 1024))
        .query_async::<()>(&mut connection)
        .await
        .expect("seed oversized argv value");
    let budget_client = invocation_router_client(
        &redis.url,
        AccessMode::ReadOnly,
        Some(OutputBudget::new(4_096, 1_000)),
    )
    .await;
    let overflow = budget_client
        .call_tool(
            "redis_command_readonly",
            serde_json::json!({"command": "GET", "arguments": [{"value": big}]}),
        )
        .await
        .expect("oversized argv result is a tool result");
    assert!(overflow.is_error, "{overflow:?}");
    assert_eq!(
        overflow.meta.as_ref().expect("argv output metadata")["io.redis.mcp/outputLimit"]["code"],
        "output_limit_exceeded"
    );

    // ACL denials classify identically through argv and curated paths.
    let username = format!("redis-mcp-argv-acl-{}", std::process::id());
    let password = "argv-acl-secret";
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg("~*")
        .arg("+get")
        .query_async::<()>(&mut connection)
        .await
        .expect("create argv ACL user");
    let mut restricted_url = redis::parse_redis_url(&redis.url).expect("parse Redis test URL");
    restricted_url
        .set_username(&username)
        .expect("set argv ACL username");
    restricted_url
        .set_password(Some(password))
        .expect("set argv ACL password");
    let restricted_client =
        invocation_router_client(restricted_url.as_str(), AccessMode::ReadWrite, None).await;
    let allowed = call_structured(
        &restricted_client,
        "redis_command_readonly",
        serde_json::json!({"command": "GET", "arguments": [{"value": key}]}),
    )
    .await;
    assert_eq!(allowed["value"]["value"], binary);
    let denied = restricted_client
        .call_tool(
            "redis_command_write",
            serde_json::json!({
                "command": "SET",
                "arguments": [{"value": key}, {"value": "denied"}],
            }),
        )
        .await
        .expect("ACL-denied argv write is a tool result");
    assert!(denied.is_error, "{denied:?}");
    let rendered = serde_json::to_string(&denied).expect("serialize ACL denial");
    assert!(rendered.contains("Authorization"), "{rendered}");
    assert!(!rendered.contains(password), "{denied:?}");
    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<i64>(&mut connection)
        .await
        .expect("remove argv ACL user");
}

async fn bulk_router_client(url: &str, access: AccessMode) -> McpClient {
    let executor = DirectRedis::connect(url)
        .await
        .expect("connect bulk executor");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover bulk capabilities");
    let router = RedisMcp::builder(executor)
        .access(access)
        .bundles([ToolBundle::Essentials, ToolBundle::Bulk])
        .capabilities(capabilities)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect bulk MCP client");
    client
        .initialize("redis-mcp-live-bulk-test", "0")
        .await
        .expect("initialize bulk MCP client");
    client
}

#[tokio::test]
async fn live_bulk_load_and_seed_are_bounded_deterministic_and_explicit() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let mut connection = redis::Client::open(redis.url.as_str())
        .expect("open bulk verification client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect bulk verification client");
    let client = bulk_router_client(&redis.url, AccessMode::ReadWrite).await;
    let prefix = test_key("bulk");

    // Every core record type loads, with expirations applied.
    let loaded = call_structured(
        &client,
        "redis_bulk_load",
        serde_json::json!({
            "records": [
                {
                    "key": {"value": format!("{prefix}:string")},
                    "value": {"string": {"value": {"value": "hello"}}},
                    "expire_ms": 60_000,
                },
                {
                    "key": {"value": format!("{prefix}:hash")},
                    "value": {"hash": {"fields": [
                        {"name": {"value": "name"}, "value": {"value": "ada"}},
                        {"name": {"value": "age"}, "value": {"value": "36"}},
                    ]}},
                },
                {
                    "key": {"value": format!("{prefix}:list")},
                    "value": {"list": {"elements": [
                        {"value": "one"}, {"value": "two"},
                    ]}},
                    "expire_ms": 60_000,
                },
                {
                    "key": {"value": format!("{prefix}:set")},
                    "value": {"set": {"members": [{"value": "alpha"}, {"value": "beta"}]}},
                },
                {
                    "key": {"value": format!("{prefix}:zset")},
                    "value": {"sorted_set": {"members": [
                        {"member": {"value": "first"}, "score": 1.5},
                        {"member": {"value": "second"}, "score": 2.5},
                    ]}},
                },
                {
                    "key": {"value": format!("{prefix}:binary")},
                    "value": {"string": {"value": {"value": BASE64.encode([0xff, 0x00, 0x01]), "encoding": "base64"}}},
                },
            ],
            "batch_size": 3,
            "concurrency": 2,
        }),
    )
    .await;
    assert_eq!(loaded["requested"], 6);
    assert_eq!(loaded["applied"], 6);
    assert_eq!(loaded["failed"], 0);
    assert_eq!(loaded["complete"], true);
    assert_eq!(loaded["batches"].as_array().expect("batches").len(), 2);
    let stored: String = redis::cmd("GET")
        .arg(format!("{prefix}:string"))
        .query_async(&mut connection)
        .await
        .expect("read bulk string");
    assert_eq!(stored, "hello");
    let string_ttl: i64 = redis::cmd("PTTL")
        .arg(format!("{prefix}:string"))
        .query_async(&mut connection)
        .await
        .expect("read bulk string TTL");
    assert!(string_ttl > 0, "string expiration rides SET PX");
    let list_ttl: i64 = redis::cmd("PTTL")
        .arg(format!("{prefix}:list"))
        .query_async(&mut connection)
        .await
        .expect("read bulk list TTL");
    assert!(list_ttl > 0, "list expiration follows as PEXPIRE");
    let age: String = redis::cmd("HGET")
        .arg(format!("{prefix}:hash"))
        .arg("age")
        .query_async(&mut connection)
        .await
        .expect("read bulk hash field");
    assert_eq!(age, "36");
    let score: f64 = redis::cmd("ZSCORE")
        .arg(format!("{prefix}:zset"))
        .arg("second")
        .query_async(&mut connection)
        .await
        .expect("read bulk zset score");
    assert_eq!(score, 2.5);
    let binary: Vec<u8> = redis::cmd("GET")
        .arg(format!("{prefix}:binary"))
        .query_async(&mut connection)
        .await
        .expect("read bulk binary value");
    assert_eq!(binary, vec![0xff, 0x00, 0x01]);

    // Dry runs validate and plan without touching Redis.
    let dry = call_structured(
        &client,
        "redis_bulk_load",
        serde_json::json!({
            "records": [{
                "key": {"value": format!("{prefix}:dry")},
                "value": {"string": {"value": {"value": "never"}}},
            }],
            "dry_run": true,
        }),
    )
    .await;
    assert_eq!(dry["dry_run"], true);
    assert_eq!(dry["attempted"], 0);
    assert_eq!(dry["total_commands"], 1);
    let dry_exists: i64 = redis::cmd("EXISTS")
        .arg(format!("{prefix}:dry"))
        .query_async(&mut connection)
        .await
        .expect("verify dry run wrote nothing");
    assert_eq!(dry_exists, 0);

    // Continue-on-error keeps identity for the failed record and applies the
    // rest; stop-on-error skips the remainder instead.
    let conflict = format!("{prefix}:conflict");
    redis::cmd("SET")
        .arg(&conflict)
        .arg("plain-string")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed conflicting string");
    let mixed = call_structured(
        &client,
        "redis_bulk_load",
        serde_json::json!({
            "records": [
                {
                    "key": {"value": format!("{prefix}:ok-1")},
                    "value": {"string": {"value": {"value": "fine"}}},
                },
                {
                    "key": {"value": conflict},
                    "value": {"hash": {"fields": [
                        {"name": {"value": "field"}, "value": {"value": "value"}},
                    ]}},
                },
                {
                    "key": {"value": format!("{prefix}:ok-2")},
                    "value": {"string": {"value": {"value": "fine"}}},
                },
            ],
            "batch_size": 1,
            "on_error": "continue",
        }),
    )
    .await;
    assert_eq!(mixed["applied"], 2);
    assert_eq!(mixed["failed"], 1);
    assert_eq!(mixed["complete"], true);
    let failure = &mixed["failures"][0];
    assert_eq!(failure["index"], 1);
    assert_eq!(failure["key"]["value"], conflict);
    assert_eq!(failure["partially_applied"], false);
    assert!(
        serde_json::to_string(failure)
            .expect("serialize bulk failure")
            .contains("WRONGTYPE"),
        "{failure:?}"
    );

    let stopped = call_structured(
        &client,
        "redis_bulk_load",
        serde_json::json!({
            "records": [
                {
                    "key": {"value": conflict},
                    "value": {"hash": {"fields": [
                        {"name": {"value": "field"}, "value": {"value": "value"}},
                    ]}},
                },
                {
                    "key": {"value": format!("{prefix}:never")},
                    "value": {"string": {"value": {"value": "never"}}},
                },
            ],
            "batch_size": 1,
            "on_error": "stop",
        }),
    )
    .await;
    assert_eq!(stopped["failed"], 1);
    assert_eq!(stopped["skipped"], 1);
    assert_eq!(stopped["complete"], false);
    let never_exists: i64 = redis::cmd("EXISTS")
        .arg(format!("{prefix}:never"))
        .query_async(&mut connection)
        .await
        .expect("verify stop-on-error skipped the rest");
    assert_eq!(never_exists, 0);

    // Deterministic seeding: identical requests generate identical datasets.
    let seed_input = serde_json::json!({
        "seed": 42,
        "count": 20,
        "key_prefix": format!("{prefix}:seed:"),
        "template": {"hash": {"fields": [
            {"name": "name", "value": {"token": {"length": 8}}},
            {"name": "tier", "value": {"choice": {"values": ["free", "pro"]}}},
            {"name": "id", "value": {"sequence": {"start": 100}}},
        ]}},
        "batch_size": 10,
    });
    let seeded = call_structured(&client, "redis_bulk_seed", seed_input.clone()).await;
    assert_eq!(seeded["applied"], 20);
    assert_eq!(seeded["sample_keys"][0], format!("{prefix}:seed:0"));
    let first_name: String = redis::cmd("HGET")
        .arg(format!("{prefix}:seed:7"))
        .arg("name")
        .query_async(&mut connection)
        .await
        .expect("read seeded name");
    let first_id: String = redis::cmd("HGET")
        .arg(format!("{prefix}:seed:7"))
        .arg("id")
        .query_async(&mut connection)
        .await
        .expect("read seeded id");
    assert_eq!(first_id, "107");
    let reseeded = call_structured(&client, "redis_bulk_seed", seed_input).await;
    assert_eq!(reseeded["applied"], 20);
    let second_name: String = redis::cmd("HGET")
        .arg(format!("{prefix}:seed:7"))
        .arg("name")
        .query_async(&mut connection)
        .await
        .expect("re-read seeded name");
    assert_eq!(
        first_name, second_name,
        "identical seed requests must regenerate identical values"
    );

    // Read-only routers never expose the bulk surface.
    let read_only = bulk_router_client(&redis.url, AccessMode::ReadOnly).await;
    let read_only_tools = read_only
        .list_tools()
        .await
        .expect("list read-only bulk tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(
        !read_only_tools
            .iter()
            .any(|name| name.starts_with("redis_bulk")),
        "bulk tools are write-gated"
    );

    // The suite shares one keyspace when REDIS_URL is set; remove this
    // test's keys so keyspace-sensitive diagnostics stay stable.
    let mut cleanup = redis::cmd("UNLINK");
    for suffix in [
        "string", "hash", "list", "set", "zset", "binary", "conflict", "ok-1", "ok-2", "never",
    ] {
        cleanup.arg(format!("{prefix}:{suffix}"));
    }
    for index in 0..20 {
        cleanup.arg(format!("{prefix}:seed:{index}"));
    }
    cleanup
        .query_async::<i64>(&mut connection)
        .await
        .expect("remove bulk test keys");
}

#[tokio::test]
async fn live_bulk_acl_denials_keep_record_identity_without_leaking_credentials() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let mut admin = redis::Client::open(redis.url.as_str())
        .expect("open bulk ACL admin client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect bulk ACL admin client");
    let username = format!("redis-mcp-bulk-acl-{}", std::process::id());
    let password = "bulk-acl-secret";
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg("~*")
        .arg("+set")
        .arg("+info")
        .arg("+hello")
        .query_async::<()>(&mut admin)
        .await
        .expect("create bulk ACL user");
    let mut restricted_url = redis::parse_redis_url(&redis.url).expect("parse Redis test URL");
    restricted_url
        .set_username(&username)
        .expect("set bulk ACL username");
    restricted_url
        .set_password(Some(password))
        .expect("set bulk ACL password");

    let executor = DirectRedis::connect(restricted_url.as_str())
        .await
        .expect("connect restricted bulk executor");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::ReadWrite)
        .bundles([ToolBundle::Bulk])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect restricted bulk MCP client");
    client
        .initialize("redis-mcp-live-bulk-acl-test", "0")
        .await
        .expect("initialize restricted bulk MCP client");

    let key = test_key("bulk:acl");
    let report = call_structured(
        &client,
        "redis_bulk_load",
        serde_json::json!({
            "records": [
                {
                    "key": {"value": format!("{key}:string")},
                    "value": {"string": {"value": {"value": "allowed"}}},
                },
                {
                    "key": {"value": format!("{key}:hash")},
                    "value": {"hash": {"fields": [
                        {"name": {"value": "field"}, "value": {"value": "denied"}},
                    ]}},
                },
            ],
            "on_error": "continue",
        }),
    )
    .await;
    assert_eq!(report["applied"], 1);
    assert_eq!(report["failed"], 1);
    let failure = &report["failures"][0];
    assert_eq!(failure["index"], 1);
    assert_eq!(failure["key"]["value"], format!("{key}:hash"));
    let rendered = serde_json::to_string(&report).expect("serialize bulk ACL report");
    assert!(rendered.contains("Authorization"), "{rendered}");
    assert!(!rendered.contains(password), "{report:?}");

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<i64>(&mut admin)
        .await
        .expect("remove bulk ACL user");
    redis::cmd("UNLINK")
        .arg(format!("{key}:string"))
        .arg(format!("{key}:hash"))
        .query_async::<i64>(&mut admin)
        .await
        .expect("remove bulk ACL test keys");
}

async fn blocking_router_client(url: &str) -> (McpClient, redis_mcp::RedisCapabilities) {
    let executor = DirectRedis::connect(url).await.expect("connect to Redis");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover Redis capabilities");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .capabilities(capabilities.clone())
        .blocking(DirectRedisBlocking::standalone(url).expect("blocking executor"))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect blocking MCP client");
    client
        .initialize("redis-mcp-live-blocking-test", "0")
        .await
        .expect("initialize blocking MCP client");
    (client, capabilities)
}

#[tokio::test]
async fn live_blocking_calls_are_finite_bounded_and_binary_safe() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let mut connection = redis::Client::open(redis.url.as_str())
        .expect("open blocking verification client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect blocking verification client");
    let (client, capabilities) = blocking_router_client(&redis.url).await;
    let prefix = test_key("blocking");
    let version = capabilities.redis_version().expect("discovered version");

    // An already-ready key answers immediately, in key priority order, with
    // binary-safe values.
    let binary_value = vec![0xff_u8, 0x00, 0x01];
    redis::cmd("RPUSH")
        .arg(format!("{prefix}:queue"))
        .arg(&binary_value)
        .query_async::<i64>(&mut connection)
        .await
        .expect("seed blocking list");
    let popped = call_structured(
        &client,
        "redis_blpop",
        serde_json::json!({
            "keys": [
                {"value": format!("{prefix}:empty")},
                {"value": format!("{prefix}:queue")},
            ],
            "timeout_ms": 2_000,
        }),
    )
    .await;
    assert_eq!(popped["timed_out"], false);
    assert_eq!(popped["popped"]["key"]["value"], format!("{prefix}:queue"));
    assert_eq!(popped["popped"]["element"]["encoding"], "base64");
    assert_eq!(
        popped["popped"]["element"]["value"],
        BASE64.encode(&binary_value)
    );

    // A server-side timeout is an explicit result, not an error.
    let started = Instant::now();
    let timed_out = call_structured(
        &client,
        "redis_brpop",
        serde_json::json!({
            "keys": [{"value": format!("{prefix}:empty")}],
            "timeout_ms": 750,
        }),
    )
    .await;
    assert_eq!(timed_out["timed_out"], true);
    assert!(timed_out["popped"].is_null());
    assert!(
        started.elapsed() >= Duration::from_millis(650),
        "blocking timeout returned before Redis' server deadline: {:?}",
        started.elapsed()
    );

    // The call genuinely blocks: an element pushed after the call starts is
    // still delivered.
    let mut pusher = connection.clone();
    let deferred_key = format!("{prefix}:deferred");
    let push_key = deferred_key.clone();
    let pushed_later = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        redis::cmd("LPUSH")
            .arg(&push_key)
            .arg("late-arrival")
            .query_async::<i64>(&mut pusher)
            .await
            .expect("push after blocking call started");
    });
    let awaited = call_structured(
        &client,
        "redis_blpop",
        serde_json::json!({
            "keys": [{"value": deferred_key}],
            "timeout_ms": 5_000,
        }),
    )
    .await;
    pushed_later.await.expect("deferred push completes");
    assert_eq!(awaited["timed_out"], false);
    assert_eq!(awaited["popped"]["element"]["value"], "late-arrival");

    // BLMOVE transfers one element and reports it.
    redis::cmd("RPUSH")
        .arg(format!("{prefix}:source"))
        .arg("moved")
        .query_async::<i64>(&mut connection)
        .await
        .expect("seed BLMOVE source");
    let moved = call_structured(
        &client,
        "redis_blmove",
        serde_json::json!({
            "source": {"value": format!("{prefix}:source")},
            "destination": {"value": format!("{prefix}:destination")},
            "from": "left",
            "to": "right",
            "timeout_ms": 2_000,
        }),
    )
    .await;
    assert_eq!(moved["timed_out"], false);
    assert_eq!(moved["element"]["value"], "moved");
    let destination_length: i64 = redis::cmd("LLEN")
        .arg(format!("{prefix}:destination"))
        .query_async(&mut connection)
        .await
        .expect("verify BLMOVE destination");
    assert_eq!(destination_length, 1);

    // Sorted-set pops preserve exact decimal score strings.
    redis::cmd("ZADD")
        .arg(format!("{prefix}:board"))
        .arg("1.5")
        .arg("low")
        .arg("2.25")
        .arg("high")
        .query_async::<i64>(&mut connection)
        .await
        .expect("seed blocking sorted set");
    let scored = call_structured(
        &client,
        "redis_bzpopmin",
        serde_json::json!({
            "keys": [{"value": format!("{prefix}:board")}],
            "timeout_ms": 2_000,
        }),
    )
    .await;
    assert_eq!(scored["popped"]["member"]["value"], "low");
    assert_eq!(scored["popped"]["score"], "1.5");

    // Redis 7.0 counted forms pop bounded batches from the first ready key.
    if version >= RedisVersion::new(7, 0, 0) {
        redis::cmd("RPUSH")
            .arg(format!("{prefix}:batch"))
            .arg("one")
            .arg("two")
            .arg("three")
            .query_async::<i64>(&mut connection)
            .await
            .expect("seed BLMPOP list");
        let batch = call_structured(
            &client,
            "redis_blmpop",
            serde_json::json!({
                "keys": [{"value": format!("{prefix}:batch")}],
                "end": "left",
                "count": 2,
                "timeout_ms": 2_000,
            }),
        )
        .await;
        assert_eq!(batch["popped"], 2);
        assert_eq!(batch["elements"][0]["value"], "one");
        assert_eq!(batch["elements"][1]["value"], "two");

        let scored_batch = call_structured(
            &client,
            "redis_bzmpop",
            serde_json::json!({
                "keys": [{"value": format!("{prefix}:board")}],
                "end": "max",
                "count": 5,
                "timeout_ms": 2_000,
            }),
        )
        .await;
        assert_eq!(scored_batch["popped"], 1);
        assert_eq!(scored_batch["members"][0]["member"]["value"], "high");
        assert_eq!(scored_batch["members"][0]["score"], "2.25");
    }

    // The Redis 8.10 multi-move form is version-gated in both directions.
    if version >= RedisVersion::new(8, 10, 0) {
        redis::cmd("RPUSH")
            .arg(format!("{prefix}:msource"))
            .arg("a")
            .arg("b")
            .query_async::<i64>(&mut connection)
            .await
            .expect("seed BLMOVEM source");
        let multi_moved = call_structured(
            &client,
            "redis_blmovem",
            serde_json::json!({
                "source": {"value": format!("{prefix}:msource")},
                "destination": {"value": format!("{prefix}:mdestination")},
                "from": "left",
                "to": "right",
                "timeout_ms": 2_000,
                "amount": {"type": "exactly", "count": 2, "ordering": "bulk"},
            }),
        )
        .await;
        assert_eq!(multi_moved["moved"], 2);
        assert_eq!(multi_moved["timed_out"], false);
    } else {
        let gated = client
            .call_tool(
                "redis_blmovem",
                serde_json::json!({
                    "source": {"value": format!("{prefix}:msource")},
                    "destination": {"value": format!("{prefix}:mdestination")},
                    "from": "left",
                    "to": "right",
                    "timeout_ms": 2_000,
                }),
            )
            .await
            .expect("capability gate is a tool result");
        assert!(gated.is_error, "{gated:?}");
    }

    // WAIT reports achieved acknowledgements without durability claims.
    let wait = call_structured(
        &client,
        "redis_wait",
        serde_json::json!({"replicas": 0, "timeout_ms": 200}),
    )
    .await;
    assert_eq!(wait["acknowledged_replicas"], 0);
    assert_eq!(wait["requirement_met"], true);

    // WAITAOF succeeds with zero requirements and surfaces the server's
    // explicit error when the AOF is disabled but a local count is requested.
    if version >= RedisVersion::new(7, 2, 0) {
        let wait_aof = call_structured(
            &client,
            "redis_waitaof",
            serde_json::json!({"local": 0, "replicas": 0, "timeout_ms": 200}),
        )
        .await;
        assert_eq!(wait_aof["acknowledged_local"], 0);
        assert_eq!(wait_aof["requirement_met"], true);
        let aof_disabled = client
            .call_tool(
                "redis_waitaof",
                serde_json::json!({"local": 1, "replicas": 0, "timeout_ms": 200}),
            )
            .await
            .expect("disabled AOF is a tool result");
        assert!(aof_disabled.is_error, "{aof_disabled:?}");
    }

    // Indefinite blocking is impossible: a zero timeout is rejected before
    // any connection is dialed.
    let zero_timeout = client
        .call_tool(
            "redis_blpop",
            serde_json::json!({
                "keys": [{"value": format!("{prefix}:empty")}],
                "timeout_ms": 0,
            }),
        )
        .await
        .expect("zero timeout is a tool result");
    assert!(zero_timeout.is_error, "{zero_timeout:?}");

    // Blocking tools require full access and are absent below it.
    let read_write = router_client(&redis.url, AccessMode::ReadWrite).await;
    let advertised = read_write
        .list_tools()
        .await
        .expect("list read-write tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(
        !advertised
            .iter()
            .any(|name| name.starts_with("redis_bl") || name.starts_with("redis_bz")),
        "blocking tools are full-access gated"
    );

    // The suite shares one keyspace when REDIS_URL is set; remove this
    // test's keys so keyspace-sensitive diagnostics stay stable.
    let mut cleanup = redis::cmd("UNLINK");
    for suffix in [
        "queue",
        "empty",
        "deferred",
        "source",
        "destination",
        "board",
        "batch",
        "msource",
        "mdestination",
    ] {
        cleanup.arg(format!("{prefix}:{suffix}"));
    }
    cleanup
        .query_async::<i64>(&mut connection)
        .await
        .expect("remove blocking test keys");
}

#[cfg(unix)]
#[tokio::test]
async fn live_monitor_sessions_are_redacted_bounded_and_closable() {
    // MONITOR observes every command on the server, so this test always uses
    // an isolated wrapper-managed instance instead of a shared REDIS_URL.
    let managed = match ManagedRedis::start().await {
        Ok(managed) => managed,
        Err(RedisServerError::BinaryNotFound { binary }) => {
            eprintln!("skipping MONITOR session test: {binary} is not on PATH");
            return;
        }
        Err(error) => panic!("start isolated MONITOR Redis: {error}"),
    };
    let url = managed.url();
    let executor = DirectRedis::connect(&url).await.expect("connect to Redis");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover Redis capabilities");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .capabilities(capabilities)
        .monitor_sessions(
            DirectRedisMonitorSessions::standalone(&url, MonitorSessionLimits::default())
                .expect("monitor session manager"),
        )
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect MONITOR MCP client");
    client
        .initialize("redis-mcp-live-monitor-test", "0")
        .await
        .expect("initialize MONITOR MCP client");
    let mut connection = redis::Client::open(url.as_str())
        .expect("open MONITOR traffic client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect MONITOR traffic client");

    // Redacted by default: command names and argument counts only.
    let session = call_structured(&client, "redis_monitor_start", serde_json::json!({})).await;
    let session_id = session["session_id"].as_str().expect("session id");
    assert_eq!(session["include_arguments"], false);

    // The default per-owner quota is one MONITOR session.
    let quota = client
        .call_tool("redis_monitor_start", serde_json::json!({}))
        .await
        .expect("quota rejection is a tool result");
    assert!(quota.is_error, "{quota:?}");

    redis::cmd("SET")
        .arg("monitor-observed-key")
        .arg("monitor-secret-value")
        .query_async::<String>(&mut connection)
        .await
        .expect("generate observed traffic");

    let mut observed = None;
    for _ in 0..10 {
        let page = call_structured(
            &client,
            "redis_monitor_read",
            serde_json::json!({"session_id": session_id, "wait_ms": 1_000}),
        )
        .await;
        let rendered = page.to_string();
        assert!(
            !rendered.contains("127.0.0.1"),
            "client addresses must be pseudonymized: {rendered}"
        );
        assert!(
            !rendered.contains("monitor-secret-value"),
            "argument values must be omitted by default: {rendered}"
        );
        if let Some(event) = page["events"]
            .as_array()
            .expect("monitor events")
            .iter()
            .find(|event| event["command"]["value"] == "SET")
        {
            assert_eq!(event["argument_count"], 2);
            assert!(event["arguments"].is_null());
            assert!(
                event["client"]
                    .as_str()
                    .expect("client pseudonym")
                    .starts_with("client-")
            );
            observed = Some(event.clone());
            break;
        }
    }
    assert!(observed.is_some(), "SET command must reach the session");

    // Closing releases the handle; further reads are owner-scoped not-found.
    let closed = call_structured(
        &client,
        "redis_monitor_close",
        serde_json::json!({"session_id": session_id}),
    )
    .await;
    assert_eq!(closed["closed"], true);
    let after_close = client
        .call_tool(
            "redis_monitor_read",
            serde_json::json!({"session_id": session_id}),
        )
        .await
        .expect("read after close is a tool result");
    assert!(after_close.is_error, "{after_close:?}");

    // Explicit opt-in captures binary-safe argument values.
    let capturing = call_structured(
        &client,
        "redis_monitor_start",
        serde_json::json!({"include_arguments": true}),
    )
    .await;
    let capturing_id = capturing["session_id"].as_str().expect("session id");
    redis::cmd("SET")
        .arg("monitor-observed-key")
        .arg("visible-value")
        .query_async::<String>(&mut connection)
        .await
        .expect("generate captured traffic");
    let mut captured = false;
    for _ in 0..10 {
        let page = call_structured(
            &client,
            "redis_monitor_read",
            serde_json::json!({"session_id": capturing_id, "wait_ms": 1_000}),
        )
        .await;
        if page["events"]
            .as_array()
            .expect("captured events")
            .iter()
            .any(|event| {
                event["command"]["value"] == "SET"
                    && event["arguments"][1]["value"] == "visible-value"
            })
        {
            captured = true;
            break;
        }
    }
    assert!(captured, "opted-in sessions must capture argument values");
    call_structured(
        &client,
        "redis_monitor_close",
        serde_json::json!({"session_id": capturing_id}),
    )
    .await;
}

#[cfg(unix)]
#[tokio::test]
async fn live_backup_lifecycle_is_guarded_and_confirmable() {
    // The backup state machine is server-global, so this test always uses an
    // isolated wrapper-managed instance instead of a shared REDIS_URL.
    let managed = match ManagedRedis::start().await {
        Ok(managed) => managed,
        Err(RedisServerError::BinaryNotFound { binary }) => {
            eprintln!("skipping backup lifecycle test: {binary} is not on PATH");
            return;
        }
        Err(error) => panic!("start isolated backup Redis: {error}"),
    };
    let url = managed.url();
    let executor = DirectRedis::connect(&url).await.expect("connect to Redis");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover Redis capabilities");
    let version = capabilities.redis_version().expect("discovered version");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .capabilities(capabilities)
        .bundle(ToolBundle::Admin)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect backup MCP client");
    client
        .initialize("redis-mcp-live-backup-test", "0")
        .await
        .expect("initialize backup MCP client");

    if version < RedisVersion::new(8, 10, 0) {
        let gated = client
            .call_tool("redis_backup_start", serde_json::json!({}))
            .await
            .expect("capability gate is a tool result");
        assert!(gated.is_error, "{gated:?}");
        return;
    }

    // Destructive transitions demand explicit confirmation before any
    // command is sent.
    let unconfirmed = client
        .call_tool("redis_backup_abort", serde_json::json!({}))
        .await
        .expect("missing confirmation is a tool result");
    assert!(unconfirmed.is_error, "{unconfirmed:?}");

    let started = client
        .call_tool("redis_backup_start", serde_json::json!({}))
        .await
        .expect("BACKUP START is a tool result");
    if started.is_error {
        // Backups depend on server directory configuration this harness does
        // not control; the guarded error path is still redacted and stable.
        eprintln!("skipping backup lifecycle transitions: BACKUP START unavailable: {started:?}");
        return;
    }
    let status = call_structured(&client, "redis_backup_status", serde_json::json!({})).await;
    assert!(status["state"].is_string(), "{status:?}");

    let sealed = call_structured(&client, "redis_backup_seal", serde_json::json!({})).await;
    assert_eq!(sealed["acknowledged"], true);

    let cleaned = call_structured(
        &client,
        "redis_backup_cleanup",
        serde_json::json!({"confirm": true}),
    )
    .await;
    assert_eq!(cleaned["acknowledged"], true);
}

#[tokio::test]
async fn live_blocking_deadline_exceeds_redis_rs_default_without_early_timeout() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let engine =
        redis_mcp::RedisBlockingEngine::new(DirectRedisBlocking::standalone(&redis.url).unwrap());
    let started = std::time::Instant::now();
    let reply = engine
        .pop_list(
            vec![test_key("blocking-over-500ms").into_bytes()],
            redis_mcp::RedisListEnd::Left,
            Duration::from_millis(850),
        )
        .await
        .unwrap();
    assert!(
        reply.is_none(),
        "an empty queue must end with the server timeout result"
    );
    assert!(started.elapsed() >= Duration::from_millis(800));
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn live_denied_multi_never_runs_writes_outside_a_transaction() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let mut admin = redis::Client::open(redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg("no-multi")
        .arg("reset")
        .arg("on")
        .arg(">test-secret")
        .arg("~*")
        .arg("+set")
        .arg("+exec")
        .query_async::<()>(&mut admin)
        .await
        .unwrap();
    let mut url = url::Url::parse(&redis.url).unwrap();
    url.set_username("no-multi").unwrap();
    url.set_password(Some("test-secret")).unwrap();
    let invocation =
        RedisInvocationEngine::builder(DirectRedis::connect(&redis.url).await.unwrap())
            .access(AccessMode::Full)
            .raw_command_policy(RawCommandPolicy::Classified)
            .build();
    let engine = RedisTransactionEngine::new(
        invocation,
        DirectRedisTransactions::standalone(url.as_str()).unwrap(),
    );
    let key = test_key("denied-multi");
    let result = engine
        .invoke(
            RedisTransactionRequest::new().command(
                NativeRedisInvocation::new("SET")
                    .arg(key.clone())
                    .arg("must-not-be-written"),
            ),
        )
        .await;
    assert!(result.is_err());
    let value: Option<String> = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(value, None);
}
