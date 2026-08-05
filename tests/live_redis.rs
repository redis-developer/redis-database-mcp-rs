use redis_mcp::{AccessMode, DirectRedis, RedisMcp, tool_names};
use tower_mcp::client::{ChannelTransport, McpClient, StdioClientTransport};

fn redis_url() -> Option<String> {
    std::env::var("REDIS_URL").ok()
}

fn test_key(suffix: &str) -> String {
    format!("redis-mcp:test:{}:{suffix}", std::process::id())
}

fn with_protocol(url: &str, protocol: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}protocol={protocol}")
}

async fn router_client(url: &str, access: AccessMode) -> McpClient {
    let executor = DirectRedis::connect(url).await.expect("connect to Redis");
    let router = RedisMcp::builder(executor)
        .access(access)
        .raw_commands(access == AccessMode::Full)
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

#[tokio::test]
async fn live_redis_round_trip_through_router() {
    let Some(url) = redis_url() else {
        eprintln!("skipping live Redis test: REDIS_URL is not set");
        return;
    };

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&url, protocol), AccessMode::Full).await;
        let key = test_key(&format!("router:{protocol}"));
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
}

#[tokio::test]
async fn live_curated_catalog_round_trip_in_resp2_and_resp3() {
    let Some(url) = redis_url() else {
        eprintln!("skipping curated live Redis test: REDIS_URL is not set");
        return;
    };

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
        assert_eq!(zrange["members"][0]["score"], 1.0);

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
async fn live_binary_values_remain_explicit_in_resp2_and_resp3() {
    let Some(url) = redis_url() else {
        eprintln!("skipping binary live Redis test: REDIS_URL is not set");
        return;
    };

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
        assert_eq!(zrange["members"][0]["score"], 1.0);

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
    assert_eq!(
        listed.tools.len(),
        tool_names(AccessMode::ReadWrite, false).len()
    );
    assert!(listed.tools.iter().any(|tool| tool.name == "redis_get"));
    assert!(listed.tools.iter().any(|tool| tool.name == "redis_set"));
    assert!(listed.tools.iter().any(|tool| tool.name == "redis_hget"));
    assert!(listed.tools.iter().any(|tool| tool.name == "redis_hset"));
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

    let hash_key = test_key("stdio-hash");
    client
        .call_tool(
            "redis_hset",
            serde_json::json!({"key": hash_key, "fields": {"name": "Ada"}}),
        )
        .await
        .expect("set hash over stdio");
    let hget = client
        .call_tool(
            "redis_hget",
            serde_json::json!({"key": hash_key, "field": "name"}),
        )
        .await
        .expect("get hash over stdio");
    assert_eq!(hget.structured_content.as_ref().unwrap()["value"], "Ada");

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
        .call_tool("redis_del", serde_json::json!({"keys": [key, hash_key]}))
        .await
        .expect("delete stdio test key");
}
