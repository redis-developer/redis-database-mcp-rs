use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use pretty_assertions::assert_eq;
use redis_mcp::{
    AccessMode, RawCommandPolicy, RedisCommand, RedisError, RedisExecutor, RedisMcp,
    RedisMcpBuildError, RedisValue, ToolBundle, tool_catalog, tool_names, tool_names_for,
};
use tower_mcp::client::{ChannelTransport, McpClient};

#[derive(Clone, Copy)]
struct StubRedis;

#[async_trait]
impl RedisExecutor for StubRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let value = match command.name() {
            "PING" => RedisValue::SimpleString("PONG".into()),
            "INFO" => RedisValue::BulkString(b"# Server\r\nredis_version:8.2.0\r\n".to_vec()),
            "DBSIZE" => RedisValue::Integer(2),
            "SCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"alpha".to_vec()),
                    RedisValue::BulkString(b"beta".to_vec()),
                ]),
            ]),
            "GET" => RedisValue::BulkString(b"hello".to_vec()),
            "EXISTS" => RedisValue::Integer(1),
            "MGET" => RedisValue::Array(vec![
                RedisValue::BulkString(b"hello".to_vec()),
                RedisValue::Nil,
            ]),
            "STRLEN" => RedisValue::Integer(5),
            "MEMORY" => RedisValue::Integer(64),
            "RANDOMKEY" => RedisValue::BulkString(b"alpha".to_vec()),
            "HGET" => RedisValue::BulkString(b"Ada".to_vec()),
            "HGETALL" => RedisValue::Map(vec![(
                RedisValue::BulkString(b"name".to_vec()),
                RedisValue::BulkString(b"Ada".to_vec()),
            )]),
            "LRANGE" => RedisValue::Array(vec![
                RedisValue::BulkString(b"second".to_vec()),
                RedisValue::BulkString(b"first".to_vec()),
            ]),
            "SMEMBERS" => RedisValue::Set(vec![
                RedisValue::BulkString(b"beta".to_vec()),
                RedisValue::BulkString(b"alpha".to_vec()),
            ]),
            "ZRANGE" => {
                if command
                    .arguments()
                    .iter()
                    .any(|argument| argument.eq_ignore_ascii_case(b"WITHSCORES"))
                {
                    RedisValue::Array(vec![
                        RedisValue::BulkString(b"alice".to_vec()),
                        RedisValue::BulkString(b"1.5".to_vec()),
                    ])
                } else {
                    RedisValue::Array(vec![RedisValue::BulkString(b"alice".to_vec())])
                }
            }
            "TYPE" => RedisValue::SimpleString("string".into()),
            "TTL" => RedisValue::Integer(-1),
            "SET" | "MSET" => RedisValue::Okay,
            "EXPIRE" | "PERSIST" => RedisValue::Integer(1),
            "INCR" => RedisValue::Integer(2),
            "APPEND" => RedisValue::Integer(5),
            "HSET" | "SADD" | "ZADD" => RedisValue::Integer(1),
            "LPUSH" => RedisValue::Integer(2),
            "DEL" | "UNLINK" => RedisValue::Integer(1),
            "ECHO" => RedisValue::BulkString(b"hello".to_vec()),
            _ => RedisValue::Nil,
        };
        Ok(value)
    }
}

async fn client(access: AccessMode, raw: bool) -> McpClient {
    let router = RedisMcp::builder(StubRedis)
        .access(access)
        .raw_commands(raw)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect in-process client");
    client
        .initialize("redis-mcp-contract-test", "0")
        .await
        .expect("initialize client");
    client
}

async fn client_for_bundles(
    access: AccessMode,
    bundles: impl IntoIterator<Item = ToolBundle>,
    raw_policy: RawCommandPolicy,
) -> McpClient {
    let router = RedisMcp::builder(StubRedis)
        .access(access)
        .bundles(bundles)
        .raw_command_policy(raw_policy)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect in-process client");
    client
        .initialize("redis-mcp-contract-test", "0")
        .await
        .expect("initialize client");
    client
}

fn structured_cases() -> Vec<(&'static str, serde_json::Value, &'static str)> {
    vec![
        ("redis_ping", serde_json::json!({}), "response"),
        ("redis_info", serde_json::json!({}), "properties"),
        ("redis_dbsize", serde_json::json!({}), "key_count"),
        (
            "redis_scan",
            serde_json::json!({"pattern": "*", "count": 10}),
            "keys",
        ),
        ("redis_get", serde_json::json!({"key": "greeting"}), "value"),
        (
            "redis_type",
            serde_json::json!({"key": "greeting"}),
            "key_type",
        ),
        (
            "redis_ttl",
            serde_json::json!({"key": "greeting"}),
            "ttl_seconds",
        ),
        (
            "redis_exists",
            serde_json::json!({"keys": ["greeting"]}),
            "existing",
        ),
        (
            "redis_mget",
            serde_json::json!({"keys": ["greeting", "missing"]}),
            "values",
        ),
        (
            "redis_strlen",
            serde_json::json!({"key": "greeting"}),
            "length_bytes",
        ),
        (
            "redis_memory_usage",
            serde_json::json!({"key": "greeting"}),
            "bytes",
        ),
        ("redis_randomkey", serde_json::json!({}), "key"),
        (
            "redis_hget",
            serde_json::json!({"key": "user:1", "field": "name"}),
            "value",
        ),
        (
            "redis_hgetall",
            serde_json::json!({"key": "user:1"}),
            "entries",
        ),
        (
            "redis_lrange",
            serde_json::json!({"key": "queue", "start": 0, "stop": -1}),
            "elements",
        ),
        (
            "redis_smembers",
            serde_json::json!({"key": "tags"}),
            "members",
        ),
        (
            "redis_zrange",
            serde_json::json!({"key": "leaders", "start": 0, "stop": -1, "withscores": true}),
            "members",
        ),
        (
            "redis_set",
            serde_json::json!({"key": "greeting", "value": "hello"}),
            "stored",
        ),
        (
            "redis_expire",
            serde_json::json!({"key": "greeting", "seconds": 60}),
            "applied",
        ),
        (
            "redis_persist",
            serde_json::json!({"key": "greeting"}),
            "applied",
        ),
        (
            "redis_mset",
            serde_json::json!({"entries": [{"key": "a", "value": "1"}, {"key": "b", "value": "2"}]}),
            "stored",
        ),
        ("redis_incr", serde_json::json!({"key": "counter"}), "value"),
        (
            "redis_append",
            serde_json::json!({"key": "greeting", "value": "!"}),
            "length_bytes",
        ),
        (
            "redis_hset",
            serde_json::json!({"key": "user:1", "fields": {"name": "Ada"}}),
            "fields_added",
        ),
        (
            "redis_lpush",
            serde_json::json!({"key": "queue", "elements": ["first", "second"]}),
            "length",
        ),
        (
            "redis_sadd",
            serde_json::json!({"key": "tags", "members": ["alpha", "beta"]}),
            "added",
        ),
        (
            "redis_zadd",
            serde_json::json!({"key": "leaders", "members": [{"score": 1.5, "member": "alice"}]}),
            "affected",
        ),
        (
            "redis_del",
            serde_json::json!({"keys": ["greeting"]}),
            "deleted",
        ),
        (
            "redis_unlink",
            serde_json::json!({"keys": ["temporary"]}),
            "unlinked",
        ),
        (
            "redis_command",
            serde_json::json!({"command": "ECHO", "arguments": ["hello"]}),
            "value",
        ),
    ]
}

#[tokio::test]
async fn access_modes_expose_exactly_the_expected_tools() {
    assert_eq!(tool_names(AccessMode::Full, false).len(), 29);
    assert_eq!(tool_names(AccessMode::Full, true).len(), 30);
    for (access, raw) in [
        (AccessMode::ReadOnly, false),
        (AccessMode::ReadWrite, false),
        (AccessMode::Full, false),
        (AccessMode::Full, true),
    ] {
        let client = client(access, raw).await;
        let listed = client.list_tools().await.expect("list tools");
        let actual = listed
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(actual, tool_names(access, raw));

        for tool in listed.tools {
            assert_eq!(tool.input_schema["type"], "object", "{}", tool.name);
            assert_eq!(
                tool.input_schema["additionalProperties"], false,
                "{}",
                tool.name
            );
            assert_eq!(
                tool.output_schema.as_ref().map(|schema| &schema["type"]),
                Some(&serde_json::json!("object")),
                "{}",
                tool.name
            );
            assert!(tool.annotations.is_some(), "{}", tool.name);
        }
    }
}

#[tokio::test]
async fn bundles_are_composable_and_raw_remains_a_separate_opt_in() {
    let diagnostics = client_for_bundles(
        AccessMode::Full,
        [ToolBundle::Diagnostics],
        RawCommandPolicy::Disabled,
    )
    .await;
    let listed = diagnostics.list_tools().await.expect("list diagnostics");
    assert_eq!(
        listed
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["redis_info"]
    );

    let essentials_and_raw = client_for_bundles(
        AccessMode::Full,
        [ToolBundle::Essentials],
        RawCommandPolicy::Classified,
    )
    .await;
    let actual = essentials_and_raw
        .list_tools()
        .await
        .expect("list essentials and raw")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        tool_names_for(AccessMode::Full, [ToolBundle::Essentials], true)
    );
    assert!(actual.iter().any(|name| name == "redis_command"));
    assert!(!actual.iter().any(|name| name == "redis_info"));
}

#[test]
fn invalid_builder_safety_configuration_is_rejected() {
    assert!(matches!(
        RedisMcp::builder(StubRedis).raw_commands(true).try_build(),
        Err(RedisMcpBuildError::RawCommandsRequireFullAccess)
    ));
    assert!(matches!(
        RedisMcp::builder(StubRedis)
            .command_timeout(Duration::ZERO)
            .try_build(),
        Err(RedisMcpBuildError::ZeroCommandTimeout)
    ));
}

#[tokio::test]
async fn tool_calls_return_structured_content() {
    let client = client(AccessMode::Full, true).await;
    for (name, arguments, expected_field) in structured_cases() {
        let result = client
            .call_tool(name, arguments)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(!result.is_error, "{name}");
        assert!(
            result
                .structured_content
                .as_ref()
                .is_some_and(|value| value.get(expected_field).is_some()),
            "{name}"
        );
    }
}

#[tokio::test]
async fn malformed_and_unbounded_inputs_fail_as_tool_results() {
    let client = client(AccessMode::Full, true).await;
    let cases = [
        ("redis_exists", serde_json::json!({"keys": []})),
        ("redis_mset", serde_json::json!({"entries": []})),
        (
            "redis_lpush",
            serde_json::json!({"key": "queue", "elements": []}),
        ),
        (
            "redis_sadd",
            serde_json::json!({"key": "tags", "members": []}),
        ),
        (
            "redis_zadd",
            serde_json::json!({
                "key": "leaders",
                "members": [{"score": 1.0, "member": "alice"}],
                "nx": true,
                "xx": true
            }),
        ),
        (
            "redis_expire",
            serde_json::json!({"key": "greeting", "seconds": 0}),
        ),
        (
            "redis_scan",
            serde_json::json!({"pattern": "*", "count": 0}),
        ),
        (
            "redis_get",
            serde_json::json!({"key": "greeting", "unknown": true}),
        ),
    ];

    for (name, arguments) in cases {
        let result = client
            .call_tool(name, arguments)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(result.is_error, "{name}");
    }
}

#[derive(Clone, Copy)]
struct BinaryRedis;

#[async_trait]
impl RedisExecutor for BinaryRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        Ok(match command.name() {
            "GET" | "HGET" => RedisValue::BulkString(vec![0xff, 0x00]),
            "MGET" => RedisValue::Array(vec![
                RedisValue::BulkString(vec![0xff, 0x00]),
                RedisValue::Nil,
            ]),
            "HGETALL" => RedisValue::Map(vec![(
                RedisValue::BulkString(vec![0xfe]),
                RedisValue::BulkString(vec![0xff]),
            )]),
            "LRANGE" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xff])]),
            "SMEMBERS" => RedisValue::Set(vec![RedisValue::BulkString(vec![0xff])]),
            "ZRANGE" => RedisValue::Array(vec![
                RedisValue::BulkString(vec![0xff]),
                RedisValue::BulkString(b"1.5".to_vec()),
            ]),
            _ => RedisValue::Nil,
        })
    }
}

#[tokio::test]
async fn binary_and_nil_values_are_explicit_across_curated_reads() {
    let router = RedisMcp::builder(BinaryRedis)
        .access(AccessMode::ReadOnly)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect binary client");
    client
        .initialize("redis-mcp-binary-test", "0")
        .await
        .expect("initialize binary client");

    let get = client
        .call_tool("redis_get", serde_json::json!({"key": "binary"}))
        .await
        .expect("binary get")
        .structured_content
        .expect("structured get");
    assert_eq!(get["encoding"], "base64");
    assert_eq!(get["value"], "/wA=");

    let mget = client
        .call_tool(
            "redis_mget",
            serde_json::json!({"keys": ["binary", "missing"]}),
        )
        .await
        .expect("binary mget")
        .structured_content
        .expect("structured mget");
    assert_eq!(mget["values"][0]["encoding"], "base64");
    assert_eq!(mget["values"][1]["exists"], false);
    assert_eq!(mget["values"][1]["value"], serde_json::Value::Null);

    for (name, arguments, path) in [
        (
            "redis_hget",
            serde_json::json!({"key": "hash", "field": "field"}),
            "/encoding",
        ),
        (
            "redis_hgetall",
            serde_json::json!({"key": "hash"}),
            "/entries/0/field_encoding",
        ),
        (
            "redis_lrange",
            serde_json::json!({"key": "list"}),
            "/elements/0/encoding",
        ),
        (
            "redis_smembers",
            serde_json::json!({"key": "set"}),
            "/members/0/encoding",
        ),
        (
            "redis_zrange",
            serde_json::json!({"key": "zset", "withscores": true}),
            "/members/0/encoding",
        ),
    ] {
        let structured = client
            .call_tool(name, arguments)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{name}: no structured content"));
        assert_eq!(structured.pointer(path), Some(&serde_json::json!("base64")));
    }
}

#[tokio::test]
async fn classified_raw_commands_fail_closed() {
    let client = client(AccessMode::Full, true).await;
    let result = client
        .call_tool(
            "redis_command",
            serde_json::json!({"command": "NEW.MODULE.COMMAND", "arguments": []}),
        )
        .await
        .expect("tool errors are returned as MCP results");
    assert!(result.is_error);
    assert!(
        serde_json::to_string(&result)
            .expect("serialize result")
            .contains("not classified")
    );
}

#[tokio::test]
async fn unrestricted_raw_policy_allows_unknown_names_but_keeps_hard_blocks() {
    let client = client_for_bundles(AccessMode::Full, [], RawCommandPolicy::Unrestricted).await;
    let unknown = client
        .call_tool(
            "redis_command",
            serde_json::json!({"command": "NEW.MODULE.COMMAND", "arguments": []}),
        )
        .await
        .expect("unknown command reaches unrestricted executor");
    assert!(!unknown.is_error);

    let blocked = client
        .call_tool(
            "redis_command",
            serde_json::json!({"command": "AUTH", "arguments": ["secret"]}),
        )
        .await
        .expect("hard block is represented as an MCP result");
    assert!(blocked.is_error);
    assert!(
        serde_json::to_string(&blocked)
            .expect("serialize blocked result")
            .contains("not supported")
    );
}

#[derive(Clone, Copy)]
struct SlowRedis;

#[async_trait]
impl RedisExecutor for SlowRedis {
    async fn execute(&self, _command: RedisCommand) -> Result<RedisValue, RedisError> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(RedisValue::SimpleString("PONG".into()))
    }
}

#[tokio::test]
async fn executor_futures_are_bounded_by_the_library_timeout() {
    let router = RedisMcp::builder(SlowRedis)
        .command_timeout(Duration::from_millis(5))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect timeout client");
    client
        .initialize("redis-mcp-timeout-test", "0")
        .await
        .expect("initialize timeout client");

    let result = client
        .call_tool("redis_ping", serde_json::json!({}))
        .await
        .expect("timeout is represented as a tool result");
    assert!(result.is_error);
    assert!(
        serde_json::to_string(&result)
            .expect("serialize result")
            .contains("timed out")
    );
}

#[tokio::test]
async fn curated_catalog_matches_checked_in_contract_snapshot() {
    let client = client(AccessMode::Full, true).await;
    let listed = client.list_tools().await.expect("list tools for snapshot");
    let mut contracts = listed
        .tools
        .into_iter()
        .map(|tool| {
            let metadata = tool_catalog()
                .iter()
                .find(|metadata| metadata.name == tool.name)
                .unwrap_or_else(|| panic!("missing catalog metadata for {}", tool.name));
            serde_json::json!({
                "name": metadata.name,
                "bundle": metadata.bundle.as_str(),
                "required_access": metadata.required_access.as_str(),
                "requires_raw_opt_in": metadata.requires_raw_opt_in,
                "protocol": tool,
            })
        })
        .collect::<Vec<_>>();
    contracts.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));

    let mut structured_results = BTreeMap::new();
    for (name, arguments, _) in structured_cases() {
        let result = client
            .call_tool(name, arguments)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(!result.is_error, "{name}");
        let mut structured = result
            .structured_content
            .unwrap_or_else(|| panic!("{name} returned no structured content"));
        if name == "redis_ping" {
            structured["latency_ms"] = serde_json::json!(0.0);
        }
        structured_results.insert(name, structured);
    }

    let actual = serde_json::to_string_pretty(&serde_json::json!({
        "catalog": contracts,
        "structured_results": structured_results,
    }))
    .expect("serialize contract snapshot");
    if std::env::var_os("REDIS_MCP_UPDATE_SNAPSHOTS").is_some() {
        std::fs::write(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/snapshots/curated_catalog.json"
            ),
            format!("{actual}\n"),
        )
        .expect("update contract snapshot");
        return;
    }
    if std::env::var_os("REDIS_MCP_PRINT_SNAPSHOT").is_some() {
        println!("{actual}");
        return;
    }
    assert_eq!(
        actual,
        include_str!("snapshots/curated_catalog.json").trim_end()
    );
}
