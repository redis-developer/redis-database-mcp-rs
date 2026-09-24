use std::{process::Stdio, time::Duration};

use reqwest::{Client as HttpClient, StatusCode, header};
use serde_json::{Value, json};
use tower_mcp::client::{HttpClientTransport, McpClient, StdioClientTransport};

#[cfg(unix)]
use redis_server_wrapper::{Error as RedisServerError, RedisServer, RedisServerHandle};

const TOKEN_A: &str = "live-http-agent-a-token";
const TOKEN_B: &str = "live-http-agent-b-token";
const FINAL_VERSION: &str = "2026-07-28";

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
                        "skipping live HTTP test: REDIS_URL is not set and {binary} is not on PATH"
                    );
                    None
                }
                Err(error) => panic!("start wrapper-managed Redis: {error}"),
            }
        }

        #[cfg(not(unix))]
        {
            eprintln!(
                "skipping live HTTP test: REDIS_URL is not set and self-hosting requires Unix"
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

struct HttpServer {
    child: tokio::process::Child,
    endpoint: String,
}

impl HttpServer {
    async fn start(redis_url: &str) -> Self {
        Self::start_with_session_ttl(redis_url, None).await
    }

    async fn start_with_session_ttl(redis_url: &str, session_ttl_ms: Option<u64>) -> Self {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve HTTP port");
        let address = probe.local_addr().expect("reserved address");
        drop(probe);

        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_redis-mcp-server"));
        command.args([
            "--url",
            redis_url,
            "--access",
            "read-write",
            "--http",
            &address.to_string(),
            "--http-max-body-bytes",
            "1024",
            "--http-max-sessions",
            "2",
        ]);
        if let Some(session_ttl_ms) = session_ttl_ms {
            command.args([
                "--http-session-ttl-ms",
                &session_ttl_ms.to_string(),
                "--http-drain-timeout-ms",
                "250",
            ]);
        }
        command
            .env(
                "REDIS_MCP_HTTP_BEARER_TOKENS",
                format!("{TOKEN_A},{TOKEN_B}"),
            )
            .env("RUST_LOG", "redis_mcp_server=warn")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command.spawn().expect("spawn HTTP server");

        let endpoint = format!("http://{address}/mcp");
        for _ in 0..100 {
            if let Some(status) = child.try_wait().expect("inspect HTTP server") {
                panic!("HTTP server exited during startup: {status}");
            }
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                return Self { child, endpoint };
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("HTTP server did not listen at {address}");
    }

    async fn shutdown(mut self) {
        #[cfg(unix)]
        {
            let pid = self.child.id().expect("HTTP server pid");
            let status = std::process::Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .expect("send SIGTERM to HTTP server");
            assert!(status.success(), "send SIGTERM to HTTP server: {status}");
        }
        #[cfg(not(unix))]
        self.child.start_kill().expect("stop HTTP server");

        let status = tokio::time::timeout(Duration::from_secs(15), self.child.wait())
            .await
            .expect("HTTP server shutdown timed out")
            .expect("wait for HTTP server");
        assert!(status.success(), "HTTP server shutdown failed: {status}");
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn test_key(suffix: &str) -> String {
    format!("redis-mcp-server:http:{}:{suffix}", std::process::id())
}

async fn connect_http(endpoint: &str, token: &str, name: &str) -> McpClient {
    let transport = HttpClientTransport::new(endpoint).bearer_token(token);
    let client = McpClient::connect(transport)
        .await
        .expect("connect HTTP MCP client");
    client
        .initialize(name, "0")
        .await
        .expect("initialize HTTP MCP client");
    client
}

#[tokio::test]
async fn http_matches_stdio_and_isolates_stateful_clients() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let server = HttpServer::start(&redis.url).await;
    let http_a = connect_http(&server.endpoint, TOKEN_A, "http-a").await;
    let http_b = connect_http(&server.endpoint, TOKEN_A, "http-b").await;
    let third_transport = HttpClientTransport::new(&server.endpoint).bearer_token(TOKEN_A);
    let third = McpClient::connect(third_transport)
        .await
        .expect("connect third HTTP client");
    third
        .initialize("http-over-limit", "0")
        .await
        .expect_err("third live HTTP session must exceed the configured limit");

    let stdio_transport = StdioClientTransport::spawn(
        env!("CARGO_BIN_EXE_redis-mcp-server"),
        &["--url", &redis.url, "--access", "read-write", "--stdio"],
    )
    .await
    .expect("spawn stdio server");
    let stdio = McpClient::connect(stdio_transport)
        .await
        .expect("connect stdio client");
    stdio
        .initialize("stdio-parity", "0")
        .await
        .expect("initialize stdio client");

    let http_tools = http_a.list_tools().await.expect("list HTTP tools");
    let stdio_tools = stdio.list_tools().await.expect("list stdio tools");
    assert_eq!(
        serde_json::to_value(&http_tools.tools).unwrap(),
        serde_json::to_value(&stdio_tools.tools).unwrap(),
        "HTTP and stdio must expose identical names, annotations, and schemas"
    );

    let channel = test_key("legacy-pubsub");
    let session_id = http_a
        .call_tool(
            "redis_subscribe",
            json!({"subscriptions": [{"value": channel}]}),
        )
        .await
        .expect("subscribe over HTTP")
        .structured_content
        .expect("structured subscription")["session_id"]
        .as_str()
        .expect("session id")
        .to_string();

    let foreign = http_b
        .call_tool(
            "redis_pubsub_read",
            json!({"session_id": session_id, "wait_ms": 1}),
        )
        .await
        .expect("foreign read returns a tool result");
    assert!(
        foreign.is_error,
        "foreign session handles must not be visible"
    );

    http_b
        .call_tool(
            "redis_publish",
            json!({"channel": {"value": channel}, "message": {"value": "isolated"}}),
        )
        .await
        .expect("publish over HTTP");
    let read_result = http_a
        .call_tool(
            "redis_pubsub_read",
            json!({"session_id": session_id, "wait_ms": 1000}),
        )
        .await
        .expect("owner reads its session");
    assert!(!read_result.is_error, "owner read failed: {read_result:?}");
    let read = read_result
        .structured_content
        .clone()
        .unwrap_or_else(|| panic!("structured Pub/Sub read missing from {read_result:?}"));
    assert_eq!(read["messages"][0]["payload"]["value"], "isolated");

    // Leave the subscription open: deleting the MCP session must drop its
    // owner guard and release the dedicated Redis Pub/Sub connection.
    http_a.shutdown().await.expect("shutdown first HTTP client");
    wait_for_zero_subscribers(&redis.url, &channel).await;
    http_b
        .shutdown()
        .await
        .expect("shutdown second HTTP client");
    stdio.shutdown().await.expect("shutdown stdio client");
    server.shutdown().await;
}

#[tokio::test]
async fn expired_http_session_releases_redis_resources_after_abrupt_disconnect() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let server = HttpServer::start_with_session_ttl(&redis.url, Some(100)).await;
    let client = connect_http(&server.endpoint, TOKEN_A, "abrupt-client").await;
    let channel = test_key("expired-pubsub");

    client
        .call_tool(
            "redis_subscribe",
            json!({"subscriptions": [{"value": channel}]}),
        )
        .await
        .expect("subscribe before abrupt disconnect");
    wait_for_subscriber_count(&redis.url, &channel, 1).await;

    // Dropping without client.shutdown() leaves the transport session for its
    // TTL reaper. The server must couple that expiry to Redis resource cleanup.
    drop(client);
    wait_for_zero_subscribers(&redis.url, &channel).await;
    server.shutdown().await;
}

#[tokio::test]
async fn authenticated_http_enforces_policy_and_supports_final_protocol() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let server = HttpServer::start(&redis.url).await;
    let client = HttpClient::new();

    let unauthorized = client
        .post(&server.endpoint)
        .json(&json!({"jsonrpc":"2.0", "id":1, "method":"server/discover"}))
        .send()
        .await
        .expect("unauthorized request");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let wrong_host = client
        .post(&server.endpoint)
        .bearer_auth(TOKEN_A)
        .header(header::HOST, "attacker.example")
        .json(&json!({"jsonrpc":"2.0", "id":2, "method":"server/discover"}))
        .send()
        .await
        .expect("wrong-host request");
    assert!(!wrong_host.status().is_success());

    let wrong_origin = client
        .post(&server.endpoint)
        .bearer_auth(TOKEN_A)
        .header(header::ORIGIN, "https://attacker.example")
        .json(&json!({"jsonrpc":"2.0", "id":3, "method":"server/discover"}))
        .send()
        .await
        .expect("wrong-origin request");
    assert!(!wrong_origin.status().is_success());

    let oversized = client
        .post(&server.endpoint)
        .bearer_auth(TOKEN_A)
        .header("content-type", "application/json")
        .body("x".repeat(2048))
        .send()
        .await
        .expect("oversized request");
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let discovery = client
        .post(&server.endpoint)
        .bearer_auth(TOKEN_A)
        .json(&json!({"jsonrpc":"2.0", "id":4, "method":"server/discover"}))
        .send()
        .await
        .expect("sessionless discovery");
    assert!(discovery.status().is_success());
    let discovery: Value = discovery.json().await.expect("discovery JSON");
    assert_eq!(
        discovery["result"]["supportedVersions"],
        json!(["2025-11-25", "2026-07-28"])
    );

    let legacy_init = client
        .post(&server.endpoint)
        .bearer_auth(TOKEN_A)
        .header(header::ACCEPT, "application/json, text/event-stream")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 40,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "legacy-owner", "version": "0"}
            }
        }))
        .send()
        .await
        .expect("initialize raw legacy session");
    assert!(legacy_init.status().is_success());
    let legacy_session_id = legacy_init
        .headers()
        .get("mcp-session-id")
        .expect("legacy session header")
        .to_str()
        .expect("legacy session header text")
        .to_string();
    let hijack = client
        .post(&server.endpoint)
        .bearer_auth(TOKEN_B)
        .header("mcp-session-id", &legacy_session_id)
        .json(&json!({"jsonrpc":"2.0", "method":"notifications/initialized"}))
        .send()
        .await
        .expect("attempt cross-principal legacy session access");
    assert_eq!(hijack.status(), StatusCode::FORBIDDEN);
    let initialized = client
        .post(&server.endpoint)
        .bearer_auth(TOKEN_A)
        .header("mcp-session-id", &legacy_session_id)
        .json(&json!({"jsonrpc":"2.0", "method":"notifications/initialized"}))
        .send()
        .await
        .expect("initialize owned legacy session");
    assert_eq!(initialized.status(), StatusCode::ACCEPTED);

    let listed = final_request(
        &client,
        &server.endpoint,
        TOKEN_A,
        "agent-a",
        5,
        "tools/list",
        json!({}),
    )
    .await;
    assert!(
        listed["result"]["tools"]
            .as_array()
            .is_some_and(|tools| !tools.is_empty())
    );

    let unknown = final_request(
        &client,
        &server.endpoint,
        TOKEN_A,
        "agent-a",
        6,
        "unknown/method",
        json!({}),
    )
    .await;
    assert_eq!(unknown["error"]["code"], -32601);

    let missing_task = final_request(
        &client,
        &server.endpoint,
        TOKEN_A,
        "agent-a",
        7,
        "tasks/get",
        json!({"taskId":"missing"}),
    )
    .await;
    assert!(missing_task.get("error").is_some());

    let channel = test_key("final-pubsub");
    let subscribed = final_tool(
        &client,
        &server.endpoint,
        TOKEN_A,
        "agent-a",
        8,
        "redis_subscribe",
        json!({"subscriptions": [{"value": channel}]}),
    )
    .await;
    let session_id = subscribed["result"]["structuredContent"]["session_id"]
        .as_str()
        .expect("final Pub/Sub session id")
        .to_string();

    // Client metadata is caller-controlled. A distinct configured credential,
    // even while claiming the same name, derives a distinct stable owner.
    let foreign = final_tool(
        &client,
        &server.endpoint,
        TOKEN_B,
        "agent-a",
        9,
        "redis_pubsub_read",
        json!({"session_id": session_id, "wait_ms": 1}),
    )
    .await;
    assert_eq!(foreign["result"]["isError"], true);

    final_tool(
        &client,
        &server.endpoint,
        TOKEN_A,
        "agent-a",
        10,
        "redis_publish",
        json!({"channel": {"value": channel}, "message": {"value": "final"}}),
    )
    .await;
    let read = final_tool(
        &client,
        &server.endpoint,
        TOKEN_A,
        "agent-a",
        11,
        "redis_pubsub_read",
        json!({"session_id": session_id, "wait_ms": 1000}),
    )
    .await;
    assert_eq!(
        read["result"]["structuredContent"]["messages"][0]["payload"]["value"],
        "final"
    );
    // Leave the final-protocol handle open. Graceful server shutdown must
    // close the Redis-side resource even without a session DELETE.
    server.shutdown().await;
    wait_for_zero_subscribers(&redis.url, &channel).await;
}

async fn final_tool(
    client: &HttpClient,
    endpoint: &str,
    token: &str,
    client_id: &str,
    id: u64,
    tool: &str,
    arguments: Value,
) -> Value {
    final_request(
        client,
        endpoint,
        token,
        client_id,
        id,
        "tools/call",
        json!({"name": tool, "arguments": arguments}),
    )
    .await
}

async fn final_request(
    client: &HttpClient,
    endpoint: &str,
    token: &str,
    client_id: &str,
    id: u64,
    method: &str,
    mut params: Value,
) -> Value {
    params.as_object_mut().expect("object params").insert(
        "_meta".to_string(),
        json!({
            "io.modelcontextprotocol/protocolVersion": FINAL_VERSION,
            "io.modelcontextprotocol/clientInfo": {"name": client_id, "version": "0"},
            "io.modelcontextprotocol/clientCapabilities": {}
        }),
    );
    let tool_name =
        (method == "tools/call").then(|| params["name"].as_str().expect("tool name").to_string());
    let mut request = client
        .post(endpoint)
        .bearer_auth(token)
        .header("mcp-protocol-version", FINAL_VERSION)
        .header("mcp-method", method)
        .header(header::ACCEPT, "application/json")
        .json(&json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}));
    if let Some(tool) = tool_name {
        request = request.header("mcp-name", tool);
    }
    let response = request.send().await.expect("final-protocol request");
    let status = response.status();
    let body: Value = response.json().await.expect("final-protocol JSON");
    assert!(
        status.is_success() || body.get("error").is_some(),
        "unexpected HTTP {status}: {body}"
    );
    body
}

async fn wait_for_zero_subscribers(redis_url: &str, channel: &str) {
    wait_for_subscriber_count(redis_url, channel, 0).await;
}

async fn wait_for_subscriber_count(redis_url: &str, channel: &str, expected: u64) {
    let client = redis::Client::open(redis_url).expect("open Redis observer");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect Redis observer");
    for _ in 0..100 {
        let counts: Vec<(String, u64)> = redis::cmd("PUBSUB")
            .arg("NUMSUB")
            .arg(channel)
            .query_async(&mut connection)
            .await
            .expect("inspect Pub/Sub subscribers");
        if counts.first().is_some_and(|(_, count)| *count == expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("Redis Pub/Sub subscriber count did not reach {expected}");
}
