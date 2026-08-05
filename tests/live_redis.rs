use redis_mcp::{AccessMode, DirectRedis, RedisMcp};
use tower_mcp::client::{ChannelTransport, McpClient, StdioClientTransport};

fn redis_url() -> Option<String> {
    std::env::var("REDIS_URL").ok()
}

fn test_key(suffix: &str) -> String {
    format!("redis-mcp:test:{}:{suffix}", std::process::id())
}

#[tokio::test]
async fn live_redis_round_trip_through_router() {
    let Some(url) = redis_url() else {
        eprintln!("skipping live Redis test: REDIS_URL is not set");
        return;
    };

    let executor = DirectRedis::connect(&url).await.expect("connect to Redis");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .raw_commands(true)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect MCP client");
    client
        .initialize("redis-mcp-live-test", "0")
        .await
        .expect("initialize MCP client");

    let key = test_key("router");
    let set = client
        .call_tool(
            "redis_set",
            serde_json::json!({"key": key, "value": "hello", "expires_in_seconds": 60}),
        )
        .await
        .expect("set key");
    assert_eq!(set.structured_content.as_ref().unwrap()["stored"], true);

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

#[tokio::test]
async fn live_redis_round_trip_through_stdio_server() {
    let Some(url) = redis_url() else {
        eprintln!("skipping stdio test: REDIS_URL is not set");
        return;
    };

    let binary = env!("CARGO_BIN_EXE_redis-mcp-server");
    let transport = StdioClientTransport::spawn(
        binary,
        &["--url", &url, "--access", "read-write", "--stdio"],
    )
    .await
    .expect("spawn redis-mcp-server");
    let client = McpClient::connect(transport)
        .await
        .expect("connect stdio MCP client");
    client
        .initialize("redis-mcp-stdio-test", "0")
        .await
        .expect("initialize stdio MCP client");

    let listed = client.list_tools().await.expect("list stdio tools");
    assert!(listed.tools.iter().any(|tool| tool.name == "redis_get"));
    assert!(listed.tools.iter().any(|tool| tool.name == "redis_set"));
    assert!(!listed.tools.iter().any(|tool| tool.name == "redis_del"));

    let key = test_key("stdio");
    client
        .call_tool(
            "redis_set",
            serde_json::json!({"key": key, "value": "over-stdio", "expires_in_seconds": 60}),
        )
        .await
        .expect("set key over stdio");
    let get = client
        .call_tool("redis_get", serde_json::json!({"key": key}))
        .await
        .expect("get key over stdio");
    assert_eq!(
        get.structured_content.as_ref().unwrap()["value"],
        "over-stdio"
    );

    let direct = DirectRedis::connect(&url)
        .await
        .expect("connect for cleanup");
    let cleanup = RedisMcp::builder(direct).access(AccessMode::Full).build();
    let cleanup_client = McpClient::connect(ChannelTransport::new(cleanup))
        .await
        .expect("connect cleanup client");
    cleanup_client
        .initialize("redis-mcp-cleanup", "0")
        .await
        .expect("initialize cleanup client");
    cleanup_client
        .call_tool("redis_del", serde_json::json!({"keys": [key]}))
        .await
        .expect("delete stdio test key");
}
