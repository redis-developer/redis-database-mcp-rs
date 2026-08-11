use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use pretty_assertions::assert_eq;
use redis_mcp::{
    AccessMode, CapabilityStatus, OutputBudget, RawCommandPolicy, RedisCapabilities, RedisCommand,
    RedisDeployment, RedisError, RedisExecutor, RedisMcp, RedisMcpBuildError, RedisModule,
    RedisModuleCapability, RedisValue, RedisVersion, ToolBundle, UnavailableToolPolicy,
    tool_catalog, tool_names, tool_names_for, tool_names_for_capabilities,
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
            "HSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"7".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"name".to_vec()),
                    RedisValue::BulkString(b"Ada".to_vec()),
                ]),
            ]),
            "LRANGE" => RedisValue::Array(vec![
                RedisValue::BulkString(b"second".to_vec()),
                RedisValue::BulkString(b"first".to_vec()),
            ]),
            "SMEMBERS" => RedisValue::Set(vec![
                RedisValue::BulkString(b"beta".to_vec()),
                RedisValue::BulkString(b"alpha".to_vec()),
            ]),
            "SSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(vec![RedisValue::BulkString(b"alpha".to_vec())]),
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
            "ZSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"3".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"alice".to_vec()),
                    RedisValue::BulkString(b"1.5".to_vec()),
                ]),
            ]),
            "TYPE" => RedisValue::SimpleString("string".into()),
            "TTL" => RedisValue::Integer(-1),
            "SET" | "MSET" => RedisValue::Okay,
            "EXPIRE" | "PERSIST" => RedisValue::Integer(1),
            "INCR" => RedisValue::Integer(2),
            "APPEND" => RedisValue::Integer(5),
            "HSET" | "SADD" | "ZADD" => RedisValue::Integer(1),
            "LPUSH" => RedisValue::Integer(2),
            "DEL" | "UNLINK" => RedisValue::Integer(1),
            "JSON.GET" => RedisValue::BulkString(br#"[{"name":"Ada"}]"#.to_vec()),
            "JSON.TYPE" => RedisValue::Array(vec![RedisValue::BulkString(b"object".to_vec())]),
            "JSON.SET" => RedisValue::Okay,
            "JSON.DEL" => RedisValue::Integer(1),
            "FT._LIST" => RedisValue::Array(vec![RedisValue::BulkString(b"idx:docs".to_vec())]),
            "FT.INFO" => RedisValue::Array(vec![
                RedisValue::BulkString(b"index_name".to_vec()),
                RedisValue::BulkString(b"idx:docs".to_vec()),
                RedisValue::BulkString(b"num_docs".to_vec()),
                RedisValue::Integer(1),
            ]),
            "FT.SEARCH" => RedisValue::Array(vec![
                RedisValue::Integer(1),
                RedisValue::BulkString(b"doc:1".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"title".to_vec()),
                    RedisValue::BulkString(b"Redis guide".to_vec()),
                ]),
            ]),
            "FT.CREATE" | "FT.DROPINDEX" => RedisValue::Okay,
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

async fn client_with_budget(
    access: AccessMode,
    raw: bool,
    output_budget: OutputBudget,
) -> McpClient {
    let router = RedisMcp::builder(StubRedis)
        .access(access)
        .raw_commands(raw)
        .output_budget(output_budget)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect in-process budget client");
    client
        .initialize("redis-mcp-budget-test", "0")
        .await
        .expect("initialize budget client");
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

async fn full_catalog_client() -> McpClient {
    client_for_bundles(
        AccessMode::Full,
        ToolBundle::ALL.iter().copied(),
        RawCommandPolicy::Classified,
    )
    .await
}

async fn capability_client(
    capabilities: RedisCapabilities,
    policy: UnavailableToolPolicy,
) -> McpClient {
    let router = RedisMcp::builder(StubRedis)
        .access(AccessMode::Full)
        .bundles(ToolBundle::ALL.iter().copied())
        .capabilities(capabilities)
        .unavailable_tool_policy(policy)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect capability-aware client");
    client
        .initialize("redis-mcp-capability-test", "0")
        .await
        .expect("initialize capability-aware client");
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
            "redis_hscan",
            serde_json::json!({"key": "user:1", "count": 10}),
            "page",
        ),
        (
            "redis_lrange",
            serde_json::json!({"key": "queue", "start": 0, "stop": 1}),
            "elements",
        ),
        (
            "redis_smembers",
            serde_json::json!({"key": "tags"}),
            "members",
        ),
        (
            "redis_sscan",
            serde_json::json!({"key": "tags", "count": 10}),
            "page",
        ),
        (
            "redis_zrange",
            serde_json::json!({"key": "leaders", "start": 0, "stop": 0, "withscores": true}),
            "members",
        ),
        (
            "redis_zscan",
            serde_json::json!({"key": "leaders", "count": 10}),
            "page",
        ),
        (
            "redis_json_get",
            serde_json::json!({"key": "doc:1"}),
            "value",
        ),
        (
            "redis_json_type",
            serde_json::json!({"key": "doc:1"}),
            "types",
        ),
        ("redis_ft_list", serde_json::json!({}), "indexes"),
        (
            "redis_ft_info",
            serde_json::json!({"index": "idx:docs"}),
            "attributes",
        ),
        (
            "redis_ft_search",
            serde_json::json!({"index": "idx:docs", "query": "redis"}),
            "response",
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
            "redis_json_set",
            serde_json::json!({"key": "doc:1", "value": {"name": "Ada"}}),
            "stored",
        ),
        (
            "redis_ft_create",
            serde_json::json!({
                "index": "idx:docs",
                "on": "JSON",
                "prefixes": ["doc:"],
                "schema": [{"name": "$.name", "alias": "name", "field_type": "TEXT"}]
            }),
            "created",
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
            "redis_json_del",
            serde_json::json!({"key": "doc:1"}),
            "deleted",
        ),
        (
            "redis_ft_dropindex",
            serde_json::json!({"index": "idx:docs"}),
            "dropped",
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
    assert_eq!(tool_names(AccessMode::Full, false).len(), 32);
    assert_eq!(tool_names(AccessMode::Full, true).len(), 33);
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

    let json_read_only = client_for_bundles(
        AccessMode::ReadOnly,
        [ToolBundle::Json],
        RawCommandPolicy::Disabled,
    )
    .await
    .list_tools()
    .await
    .expect("list JSON read tools")
    .tools
    .into_iter()
    .map(|tool| tool.name)
    .collect::<Vec<_>>();
    assert_eq!(json_read_only, vec!["redis_json_get", "redis_json_type"]);

    let mut search_read_write = client_for_bundles(
        AccessMode::ReadWrite,
        [ToolBundle::Search],
        RawCommandPolicy::Disabled,
    )
    .await
    .list_tools()
    .await
    .expect("list Search read-write tools")
    .tools
    .into_iter()
    .map(|tool| tool.name)
    .collect::<Vec<_>>();
    search_read_write.sort();
    assert_eq!(
        search_read_write,
        vec![
            "redis_ft_create",
            "redis_ft_info",
            "redis_ft_list",
            "redis_ft_search",
        ]
    );
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
    assert!(matches!(
        RedisMcp::builder(StubRedis)
            .output_budget(OutputBudget::new(0, 1))
            .try_build(),
        Err(RedisMcpBuildError::ZeroOutputBytes)
    ));
    assert!(matches!(
        RedisMcp::builder(StubRedis)
            .output_budget(OutputBudget::new(1, 0))
            .try_build(),
        Err(RedisMcpBuildError::ZeroOutputEntries)
    ));
}

fn assert_output_limit(
    result: &tower_mcp::CallToolResult,
    dimension: &str,
    actual: usize,
    limit: usize,
) {
    assert!(result.is_error);
    let error = &result
        .meta
        .as_ref()
        .expect("structured output-limit metadata")["io.redis.mcp/outputLimit"];
    assert_eq!(error["code"], "output_limit_exceeded");
    assert_eq!(error["dimension"], dimension);
    assert_eq!(error["actual"], actual);
    assert_eq!(error["limit"], limit);
    assert_eq!(error["retryable"], true);
}

#[tokio::test]
async fn encoded_output_budget_accepts_exact_limit_and_rejects_one_byte_over() {
    let generous = client_with_budget(
        AccessMode::ReadOnly,
        false,
        OutputBudget::new(1_000_000, 1_000),
    )
    .await;
    let baseline = generous
        .call_tool("redis_get", serde_json::json!({"key": "greeting"}))
        .await
        .expect("baseline GET");
    let encoded_bytes = serde_json::to_vec(&baseline)
        .expect("serialize baseline GET")
        .len();

    let exact = client_with_budget(
        AccessMode::ReadOnly,
        false,
        OutputBudget::new(encoded_bytes, 1_000),
    )
    .await
    .call_tool("redis_get", serde_json::json!({"key": "greeting"}))
    .await
    .expect("exact-limit GET");
    assert!(!exact.is_error);

    let limited = client_with_budget(
        AccessMode::ReadOnly,
        false,
        OutputBudget::new(encoded_bytes - 1, 1_000),
    )
    .await
    .call_tool("redis_get", serde_json::json!({"key": "greeting"}))
    .await
    .expect("over-limit GET");
    assert_output_limit(&limited, "encoded_bytes", encoded_bytes, encoded_bytes - 1);
}

#[tokio::test]
async fn collection_budget_accepts_exact_limit_and_returns_retry_guidance() {
    let exact = client_with_budget(AccessMode::ReadOnly, false, OutputBudget::new(1_000_000, 2))
        .await
        .call_tool("redis_smembers", serde_json::json!({"key": "tags"}))
        .await
        .expect("exact-limit SMEMBERS");
    assert!(!exact.is_error);

    let limited = client_with_budget(AccessMode::ReadOnly, false, OutputBudget::new(1_000_000, 1))
        .await
        .call_tool("redis_smembers", serde_json::json!({"key": "tags"}))
        .await
        .expect("over-limit SMEMBERS");
    assert_output_limit(&limited, "collection_entries", 2, 1);
    assert!(
        limited.meta.as_ref().unwrap()["io.redis.mcp/outputLimit"]["guidance"]
            .as_str()
            .unwrap()
            .contains("redis_sscan")
    );
}

#[tokio::test]
async fn raw_commands_share_the_hard_encoded_response_budget() {
    let arguments = serde_json::json!({"command": "ECHO", "arguments": ["hello"]});
    let baseline = client_with_budget(AccessMode::Full, true, OutputBudget::new(1_000_000, 1_000))
        .await
        .call_tool("redis_command", arguments.clone())
        .await
        .expect("baseline raw command");
    let encoded_bytes = serde_json::to_vec(&baseline)
        .expect("serialize baseline raw result")
        .len();

    let limited = client_with_budget(
        AccessMode::Full,
        true,
        OutputBudget::new(encoded_bytes - 1, 1_000),
    )
    .await
    .call_tool("redis_command", arguments)
    .await
    .expect("over-limit raw command");
    assert_output_limit(&limited, "encoded_bytes", encoded_bytes, encoded_bytes - 1);
}

#[tokio::test]
async fn scan_and_range_outputs_expose_typed_continuations() {
    let client = client(AccessMode::ReadOnly, false).await;
    let scan = client
        .call_tool(
            "redis_hscan",
            serde_json::json!({"key": "user:1", "count": 10}),
        )
        .await
        .expect("HSCAN")
        .structured_content
        .expect("structured HSCAN");
    assert_eq!(scan["page"]["complete"], false);
    assert_eq!(scan["page"]["continuation"]["cursor"], 7);

    let range = client
        .call_tool(
            "redis_lrange",
            serde_json::json!({"key": "queue", "start": 0, "stop": 0}),
        )
        .await
        .expect("LRANGE")
        .structured_content
        .expect("structured LRANGE");
    assert_eq!(range["count"], 1);
    assert_eq!(range["page"]["complete"], false);
    assert_eq!(range["page"]["continuation"]["start"], 1);
}

#[tokio::test]
async fn tool_calls_return_structured_content() {
    let client = full_catalog_client().await;
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
    let client = full_catalog_client().await;
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
            "redis_lrange",
            serde_json::json!({"key": "queue", "start": 0, "stop": -1}),
        ),
        (
            "redis_zrange",
            serde_json::json!({"key": "leaders", "start": 0, "stop": 1000}),
        ),
        (
            "redis_get",
            serde_json::json!({"key": "greeting", "unknown": true}),
        ),
        (
            "redis_json_set",
            serde_json::json!({
                "key": "doc:1",
                "value": {},
                "nx": true,
                "xx": true
            }),
        ),
        (
            "redis_ft_search",
            serde_json::json!({"index": "idx", "query": "*", "limit_num": 101}),
        ),
        (
            "redis_ft_create",
            serde_json::json!({"index": "idx", "schema": []}),
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
async fn encoded_budget_is_measured_after_binary_base64_expansion() {
    let generous_router = RedisMcp::builder(BinaryRedis)
        .output_budget(OutputBudget::new(1_000_000, 1_000))
        .build();
    let generous = McpClient::connect(ChannelTransport::new(generous_router))
        .await
        .expect("connect generous binary client");
    generous
        .initialize("redis-mcp-binary-budget-test", "0")
        .await
        .expect("initialize generous binary client");
    let baseline = generous
        .call_tool("redis_get", serde_json::json!({"key": "binary"}))
        .await
        .expect("baseline binary GET");
    assert_eq!(
        baseline.structured_content.as_ref().unwrap()["value"],
        "/wA="
    );
    let encoded_bytes = serde_json::to_vec(&baseline)
        .expect("serialize binary GET")
        .len();

    let limited_router = RedisMcp::builder(BinaryRedis)
        .output_budget(OutputBudget::new(encoded_bytes - 1, 1_000))
        .build();
    let limited = McpClient::connect(ChannelTransport::new(limited_router))
        .await
        .expect("connect limited binary client");
    limited
        .initialize("redis-mcp-binary-budget-test", "0")
        .await
        .expect("initialize limited binary client");
    let result = limited
        .call_tool("redis_get", serde_json::json!({"key": "binary"}))
        .await
        .expect("over-limit binary GET");
    assert_output_limit(&result, "encoded_bytes", encoded_bytes, encoded_bytes - 1);
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
            .contains("SESSION_COMMAND_UNSUPPORTED")
    );
}

#[derive(Clone, Copy)]
struct MissingModulesRedis;

#[async_trait]
impl RedisExecutor for MissingModulesRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        assert_eq!(command.required_module(), Some(RedisModule::Json));
        Err(RedisError::new(
            redis_mcp::RedisErrorKind::Server,
            "ERR unknown command 'JSON.GET', with args beginning with: 'secret-key'",
        ))
    }
}

#[tokio::test]
async fn module_absence_is_stable_for_custom_executors() {
    let router = RedisMcp::builder(MissingModulesRedis)
        .bundles([ToolBundle::Json])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect missing-module client");
    client
        .initialize("redis-mcp-missing-module-test", "0")
        .await
        .expect("initialize missing-module client");

    let result = client
        .call_tool("redis_json_get", serde_json::json!({"key": "secret-key"}))
        .await
        .expect("module error is a tool result");
    let text = serde_json::to_string(&result).expect("serialize module error");
    assert!(result.is_error);
    assert!(text.contains("ModuleUnavailable"));
    assert!(text.contains("RedisJSON"));
    assert!(!text.contains("secret-key"));
}

#[tokio::test]
async fn unknown_capabilities_preserve_custom_executor_compatibility() {
    let client = capability_client(RedisCapabilities::unknown(), UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list unknown-capability tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(names.iter().any(|name| name == "redis_json_get"));
    assert!(names.iter().any(|name| name == "redis_memory_usage"));

    let result = client
        .call_tool("redis_ping", serde_json::json!({}))
        .await
        .expect("unknown capabilities allow execution");
    assert!(!result.is_error);
}

#[tokio::test]
async fn known_old_redis_can_hide_only_version_incompatible_tools() {
    let old = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(3, 2, 12));
    let client = capability_client(old.clone(), UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list old Redis tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "redis_memory_usage"));
    assert!(!names.iter().any(|name| name == "redis_unlink"));
    assert!(names.iter().any(|name| name == "redis_get"));

    let helper_names = tool_names_for_capabilities(
        AccessMode::Full,
        ToolBundle::ALL.iter().copied(),
        false,
        &old,
        UnavailableToolPolicy::Hide,
    );
    assert!(!helper_names.contains(&"redis_memory_usage"));

    let new = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(4, 0, 0));
    let new_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::Essentials],
        false,
        &new,
        UnavailableToolPolicy::Hide,
    );
    assert!(new_names.contains(&"redis_memory_usage"));
    assert!(new_names.contains(&"redis_unlink"));
}

#[tokio::test]
async fn known_missing_modules_commands_and_module_versions_filter_precisely() {
    let missing_modules = RedisCapabilities::unknown().with_module_inventory([]);
    let client = capability_client(missing_modules, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list missing-module tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name.starts_with("redis_json_")));
    assert!(!names.iter().any(|name| name.starts_with("redis_ft_")));

    let missing_get = RedisCapabilities::unknown().with_command_inventory(["MGET"]);
    let client = capability_client(missing_get, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list command-filtered tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "redis_get"));
    assert!(names.iter().any(|name| name == "redis_mget"));

    let old_search = RedisCapabilities::unknown().with_module(
        RedisModule::Search,
        RedisModuleCapability::available(Some(RedisVersion::new(1, 8, 0))),
    );
    let client = capability_client(old_search, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list old Search tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "redis_ft_list"));
    assert!(names.iter().any(|name| name == "redis_ft_search"));
}

#[tokio::test]
async fn known_cluster_mode_hides_tools_with_unimplemented_cluster_wide_semantics() {
    let cluster = RedisCapabilities::unknown().with_deployment(RedisDeployment::Cluster);
    let client = capability_client(cluster, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list cluster-compatible tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    for standalone_only in [
        "redis_info",
        "redis_dbsize",
        "redis_scan",
        "redis_randomkey",
    ] {
        assert!(!names.iter().any(|name| name == standalone_only));
    }
    assert!(names.iter().any(|name| name == "redis_get"));
    assert!(names.iter().any(|name| name == "redis_mget"));
}

#[tokio::test]
async fn advertised_capability_failures_have_stable_categories_and_codes() {
    let old = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(3, 2, 12));
    let client = capability_client(old, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool(
            "redis_memory_usage",
            serde_json::json!({"key": "never-sent"}),
        )
        .await
        .expect("version failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize version failure");
    assert!(result.is_error);
    assert!(text.contains("CapabilityUnavailable"));
    assert!(text.contains("REDIS_VERSION_UNAVAILABLE"));

    let missing_json = RedisCapabilities::unknown()
        .with_module(RedisModule::Json, RedisModuleCapability::unavailable());
    let client = capability_client(missing_json, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool("redis_json_get", serde_json::json!({"key": "never-sent"}))
        .await
        .expect("module failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize module failure");
    assert!(result.is_error);
    assert!(text.contains("ModuleUnavailable"));
    assert!(text.contains("MODULE_UNAVAILABLE"));

    let old_search = RedisCapabilities::unknown().with_module(
        RedisModule::Search,
        RedisModuleCapability::available(Some(RedisVersion::new(1, 8, 0))),
    );
    let client = capability_client(old_search, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool("redis_ft_list", serde_json::json!({}))
        .await
        .expect("module version failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize module version failure");
    assert!(result.is_error);
    assert!(text.contains("ModuleUnavailable"));
    assert!(text.contains("MODULE_VERSION_UNAVAILABLE"));

    let missing_command =
        RedisCapabilities::unknown().with_command("PING", CapabilityStatus::Unavailable);
    let client = capability_client(missing_command, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool("redis_ping", serde_json::json!({}))
        .await
        .expect("command failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize command failure");
    assert!(result.is_error);
    assert!(text.contains("CapabilityUnavailable"));
    assert!(text.contains("COMMAND_UNAVAILABLE"));

    let cluster = RedisCapabilities::unknown().with_deployment(RedisDeployment::Cluster);
    let client = capability_client(cluster, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool("redis_dbsize", serde_json::json!({}))
        .await
        .expect("deployment failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize deployment failure");
    assert!(result.is_error);
    assert!(text.contains("CapabilityUnavailable"));
    assert!(text.contains("DEPLOYMENT_UNAVAILABLE"));
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
    let client = full_catalog_client().await;
    let listed = client.list_tools().await.expect("list tools for snapshot");
    let mut contracts = listed
        .tools
        .into_iter()
        .map(|tool| {
            let metadata = tool_catalog()
                .iter()
                .find(|metadata| metadata.name == tool.name)
                .unwrap_or_else(|| panic!("missing catalog metadata for {}", tool.name));
            let requirements = metadata.capability_requirements();
            serde_json::json!({
                "name": metadata.name,
                "bundle": metadata.bundle.as_str(),
                "required_access": metadata.required_access.as_str(),
                "required_module": metadata.required_module().map(|module| module.as_str()),
                "capability_requirements": {
                    "minimum_redis_version": requirements.minimum_redis_version().map(|version| version.to_string()),
                    "required_module": requirements.required_module().map(|module| module.as_str()),
                    "minimum_module_version": requirements.minimum_module_version().map(|version| version.to_string()),
                    "required_commands": requirements.required_commands(),
                    "deployment": requirements.deployment().as_str(),
                },
                "requires_raw_opt_in": metadata.requires_raw_opt_in,
                "output_policy": metadata.output_policy().as_str(),
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
