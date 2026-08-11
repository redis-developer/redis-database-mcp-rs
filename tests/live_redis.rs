use std::time::Duration;

use redis_mcp::{
    AccessMode, CapabilityStatus, DirectRedis, NativeRedisInvocation, OutputBudget,
    RawCommandPolicy, RedisDeployment, RedisInvocationEngine, RedisMcp, RedisModule, RedisValue,
    RedisVersion, ToolBundle, UnavailableToolPolicy, tool_names,
};
use tower_mcp::client::{ChannelTransport, McpClient, StdioClientTransport};

#[cfg(unix)]
use redis_mcp::RedisErrorKind;
#[cfg(unix)]
use redis_server_wrapper::{
    Direction, Error as RedisServerError, FaultProxy, RedisServer, RedisServerHandle,
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

    for protocol in ["resp2", "resp3"] {
        let client = router_client(&with_protocol(&url, protocol), AccessMode::Full).await;
        let prefix = test_key(&format!("sets:{protocol}"));
        let left = format!("{prefix}:{{same}}:left");
        let right = format!("{prefix}:{{same}}:right");
        let missing = format!("{prefix}:{{same}}:missing");
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
                serde_json::json!({"keys": [left, right, missing, wrong_type]}),
            )
            .await
            .expect("clean up set family");
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
    let field_expiration_tools = ["redis_hexpire", "redis_hpersist", "redis_httl"];
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
                "fields": {"expiring": "value", "persistent": "value"}
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
            serde_json::json!({"key": set, "members": oversized_members}),
        )
        .await
        .expect("oversized SMISMEMBER is a tool result");
    assert!(membership.is_error);
    assert!(
        serde_json::to_string(&membership)
            .expect("serialize oversized SMISMEMBER")
            .contains("configured output limit of 100 entries")
    );

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
        let page = client
            .call_tool(tool, serde_json::json!({"key": key, "start": 0, "stop": 9}))
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
    let Some(redis) = TestRedis::start().await else {
        return;
    };
    let url = redis.url;

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
            serde_json::json!({
                "key": key,
                "value": "over-stdio",
                "expiration": {"type": "seconds", "value": 60}
            }),
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

#[cfg(unix)]
#[tokio::test]
async fn live_acl_failures_are_classified_without_leaking_credentials() {
    let Some(redis) = TestRedis::start().await else {
        return;
    };
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
        .arg("+exists")
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

    restricted_url
        .set_password(Some(password))
        .expect("set restricted Redis password");
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

    let full_client = router_client(restricted_url.as_str(), AccessMode::Full).await;
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

    redis::cmd("DEL")
        .arg(&[&readable_hash, &readable_list, &readable_set])
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

    let fresh = router_client(proxy_url.as_str(), AccessMode::ReadOnly).await;
    let fresh_ping = fresh
        .call_tool("redis_ping", serde_json::json!({}))
        .await
        .expect("fresh DirectRedis PING");
    assert!(!fresh_ping.is_error);
}
