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
            "TYPE" => RedisValue::SimpleString("string".into()),
            "TTL" => RedisValue::Integer(-1),
            "SET" => RedisValue::Okay,
            "DEL" => RedisValue::Integer(1),
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

#[tokio::test]
async fn access_modes_expose_exactly_the_expected_tools() {
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
    let cases = [
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
            "redis_set",
            serde_json::json!({"key": "greeting", "value": "hello"}),
            "stored",
        ),
        (
            "redis_del",
            serde_json::json!({"keys": ["greeting"]}),
            "deleted",
        ),
        (
            "redis_command",
            serde_json::json!({"command": "ECHO", "arguments": ["hello"]}),
            "value",
        ),
    ];

    for (name, arguments, expected_field) in cases {
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

    let cases = [
        ("redis_ping", serde_json::json!({})),
        ("redis_info", serde_json::json!({})),
        ("redis_dbsize", serde_json::json!({})),
        (
            "redis_scan",
            serde_json::json!({"pattern": "*", "count": 10}),
        ),
        ("redis_get", serde_json::json!({"key": "greeting"})),
        ("redis_type", serde_json::json!({"key": "greeting"})),
        ("redis_ttl", serde_json::json!({"key": "greeting"})),
        (
            "redis_set",
            serde_json::json!({"key": "greeting", "value": "hello"}),
        ),
        ("redis_del", serde_json::json!({"keys": ["greeting"]})),
        (
            "redis_command",
            serde_json::json!({"command": "ECHO", "arguments": ["hello"]}),
        ),
    ];
    let mut structured_results = BTreeMap::new();
    for (name, arguments) in cases {
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
