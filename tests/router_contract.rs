use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

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
            "GET" | "GETEX" | "GETDEL" => RedisValue::BulkString(b"hello".to_vec()),
            "GETRANGE" => RedisValue::BulkString(b"ell".to_vec()),
            "DUMP" => RedisValue::BulkString(vec![0, 1, 2]),
            "OBJECT" => {
                if command
                    .arguments()
                    .first()
                    .is_some_and(|value| value == b"ENCODING")
                {
                    RedisValue::BulkString(b"embstr".to_vec())
                } else {
                    RedisValue::Integer(1)
                }
            }
            "EXISTS" => RedisValue::Integer(1),
            "MGET" => RedisValue::Array(vec![
                RedisValue::BulkString(b"hello".to_vec()),
                RedisValue::Nil,
            ]),
            "STRLEN" => RedisValue::Integer(5),
            "MEMORY" => RedisValue::Integer(64),
            "RANDOMKEY" => RedisValue::BulkString(b"alpha".to_vec()),
            "HGET" if command.tool_name() == "redis_vector_get_hash" => RedisValue::BulkString(
                [1.0_f32, 2.0_f32]
                    .into_iter()
                    .flat_map(f32::to_le_bytes)
                    .collect(),
            ),
            "HGET" => RedisValue::BulkString(b"Ada".to_vec()),
            "HGETALL" => RedisValue::Map(vec![(
                RedisValue::BulkString(b"name".to_vec()),
                RedisValue::BulkString(b"Ada".to_vec()),
            )]),
            "HEXISTS" => RedisValue::Integer(1),
            "HKEYS" => RedisValue::Array(vec![RedisValue::BulkString(b"name".to_vec())]),
            "HLEN" => RedisValue::Integer(1),
            "HMGET" => RedisValue::Array(vec![
                RedisValue::BulkString(b"Ada".to_vec()),
                RedisValue::Nil,
            ]),
            "HSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"7".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"name".to_vec()),
                    RedisValue::BulkString(b"Ada".to_vec()),
                ]),
            ]),
            "HSTRLEN" => RedisValue::Integer(3),
            "HTTL" => RedisValue::Array(vec![RedisValue::Integer(-1)]),
            "HVALS" => RedisValue::Array(vec![RedisValue::BulkString(b"Ada".to_vec())]),
            "LRANGE" => RedisValue::Array(vec![
                RedisValue::BulkString(b"second".to_vec()),
                RedisValue::BulkString(b"first".to_vec()),
            ]),
            "LINDEX" => RedisValue::BulkString(b"second".to_vec()),
            "LLEN" => RedisValue::Integer(2),
            "LPOS" => RedisValue::Array(vec![RedisValue::Integer(0)]),
            "SCARD" => RedisValue::Integer(2),
            "SDIFF" => RedisValue::Set(vec![RedisValue::BulkString(b"alpha".to_vec())]),
            "SINTER" => RedisValue::Set(vec![RedisValue::BulkString(b"beta".to_vec())]),
            "SISMEMBER" => RedisValue::Integer(1),
            "SMEMBERS" => RedisValue::Set(vec![
                RedisValue::BulkString(b"beta".to_vec()),
                RedisValue::BulkString(b"alpha".to_vec()),
            ]),
            "SMISMEMBER" => RedisValue::Array(vec![RedisValue::Integer(1), RedisValue::Integer(0)]),
            "SSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(vec![RedisValue::BulkString(b"alpha".to_vec())]),
            ]),
            "SUNION" => RedisValue::Set(vec![
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
            "ZSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"3".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"alice".to_vec()),
                    RedisValue::BulkString(b"1.5".to_vec()),
                ]),
            ]),
            "TYPE" => RedisValue::SimpleString("string".into()),
            "TTL" => RedisValue::Integer(-1),
            "SET" | "MSET" | "RENAME" | "RESTORE" | "LSET" | "LTRIM" => RedisValue::Okay,
            "EXPIRE" | "PERSIST" | "COPY" | "TOUCH" | "RENAMENX" => RedisValue::Integer(1),
            "INCR" | "DECR" | "DECRBY" | "INCRBY" => RedisValue::Integer(2),
            "INCRBYFLOAT" => RedisValue::BulkString(b"2.5".to_vec()),
            "SETRANGE" => RedisValue::Integer(5),
            "APPEND" => RedisValue::Integer(5),
            "HSET" | "SADD" | "SREM" | "ZADD" | "HINCRBY" | "HDEL" => RedisValue::Integer(1),
            "HINCRBYFLOAT" => RedisValue::BulkString(b"2.5".to_vec()),
            "HEXPIRE" | "HPERSIST" => RedisValue::Array(vec![RedisValue::Integer(1)]),
            "LPUSH" | "RPUSH" => RedisValue::Integer(2),
            "LPOP" | "RPOP" => RedisValue::Array(vec![RedisValue::BulkString(b"first".to_vec())]),
            "LMOVE" => RedisValue::BulkString(b"first".to_vec()),
            "LREM" => RedisValue::Integer(1),
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
            "FT.SEARCH"
                if matches!(
                    command.tool_name(),
                    "redis_ft_vector_search" | "redis_ft_hybrid_search"
                ) =>
            {
                RedisValue::Array(vec![
                    RedisValue::Integer(1),
                    RedisValue::BulkString(b"doc:1".to_vec()),
                    RedisValue::Array(vec![
                        RedisValue::BulkString(b"vector_distance".to_vec()),
                        RedisValue::BulkString(b"0.125".to_vec()),
                        RedisValue::BulkString(b"title".to_vec()),
                        RedisValue::BulkString(b"Redis guide".to_vec()),
                    ]),
                ])
            }
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

#[derive(Clone, Default)]
struct RecordingRedis {
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

#[derive(Clone)]
struct FixedRedis {
    response: RedisValue,
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

impl FixedRedis {
    fn new(response: RedisValue) -> Self {
        Self {
            response,
            commands: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl RedisExecutor for FixedRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        self.commands.lock().expect("fixed lock").push(command);
        Ok(self.response.clone())
    }
}

async fn fixed_client(executor: FixedRedis, capabilities: RedisCapabilities) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .capabilities(capabilities)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect fixed client");
    client
        .initialize("redis-mcp-fixed-test", "0")
        .await
        .expect("initialize fixed client");
    client
}

#[tokio::test]
async fn set_is_binary_safe_and_reports_prior_conditional_semantics() {
    let executor = FixedRedis::new(RedisValue::BulkString(vec![0xfd]));
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
    )
    .await;

    let result = client
        .call_tool(
            "redis_set",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "value": "/gE=",
                "value_encoding": "base64",
                "condition": "xx",
                "get": true,
                "expiration": {"type": "unix_milliseconds", "value": 123}
            }),
        )
        .await
        .expect("binary SET")
        .structured_content
        .expect("structured binary SET");
    assert_eq!(result["applied"], true);
    assert_eq!(result["previous_exists"], true);
    assert_eq!(result["previous_value"], "/Q==");
    assert_eq!(result["previous_value_encoding"], "base64");

    let commands = commands.lock().expect("recorded binary SET");
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].arguments()[0], [0xff, 0x00]);
    assert_eq!(commands[0].arguments()[1], [0xfe, 0x01]);
    assert_eq!(
        &commands[0].arguments()[2..],
        [
            b"XX".to_vec(),
            b"GET".to_vec(),
            b"PXAT".to_vec(),
            b"123".to_vec()
        ]
    );
}

#[tokio::test]
async fn set_distinguishes_applied_noop_and_nil_across_conditions() {
    for (response, condition, get, applied, previous_exists) in [
        (RedisValue::Okay, "nx", false, true, None),
        (RedisValue::Nil, "xx", false, false, None),
        (RedisValue::Nil, "nx", true, true, Some(false)),
        (
            RedisValue::BulkString(b"old".to_vec()),
            "nx",
            true,
            false,
            Some(true),
        ),
        (
            RedisValue::BulkString(b"old".to_vec()),
            "xx",
            true,
            true,
            Some(true),
        ),
        (RedisValue::Nil, "xx", true, false, Some(false)),
    ] {
        let client = fixed_client(
            FixedRedis::new(response),
            RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
        )
        .await;
        let result = client
            .call_tool(
                "redis_set",
                serde_json::json!({
                    "key": "condition",
                    "value": "new",
                    "condition": condition,
                    "get": get
                }),
            )
            .await
            .expect("conditional SET")
            .structured_content
            .expect("structured conditional SET");
        assert_eq!(result["applied"], applied, "{condition} get={get}");
        assert_eq!(
            result["previous_exists"],
            previous_exists.map_or(serde_json::Value::Null, serde_json::Value::Bool),
            "{condition} get={get}"
        );
    }
}

#[tokio::test]
async fn known_redis_six_rejects_set_nx_get_without_execution() {
    let executor = FixedRedis::new(RedisValue::Nil);
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 2, 0)),
    )
    .await;
    let result = client
        .call_tool(
            "redis_set",
            serde_json::json!({
                "key": "versioned",
                "value": "new",
                "condition": "nx",
                "get": true
            }),
        )
        .await
        .expect("version-gated SET result");
    assert!(result.is_error);
    assert!(
        serde_json::to_string(&result)
            .expect("serialize version error")
            .contains("Redis 7.0")
    );
    assert!(commands.lock().expect("version commands").is_empty());
}

#[tokio::test]
async fn side_effectful_value_returns_omit_oversized_payloads_but_report_outcomes() {
    let prior = vec![b'x'; 16];
    let client = fixed_client(
        FixedRedis::new(RedisValue::BulkString(prior.clone())),
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
    )
    .await;
    let set = client
        .call_tool(
            "redis_set",
            serde_json::json!({
                "key": "bounded-set",
                "value": "new",
                "condition": "xx",
                "get": true,
                "max_previous_bytes": 8
            }),
        )
        .await
        .expect("bounded SET")
        .structured_content
        .expect("structured bounded SET");
    assert_eq!(set["applied"], true);
    assert_eq!(set["previous_exists"], true);
    assert_eq!(set["previous_value_bytes"], 16);
    assert_eq!(set["previous_value_omitted"], true);
    assert_eq!(set["previous_value"], serde_json::Value::Null);

    for (tool, input) in [
        (
            "redis_getex",
            serde_json::json!({
                "key": "bounded-getex",
                "expiration": {"type": "seconds", "value": 60},
                "max_value_bytes": 8
            }),
        ),
        (
            "redis_getdel",
            serde_json::json!({"key": "bounded-getdel", "max_value_bytes": 8}),
        ),
    ] {
        let client = fixed_client(
            FixedRedis::new(RedisValue::BulkString(prior.clone())),
            RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
        )
        .await;
        let result = client
            .call_tool(tool, input)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(result["exists"], true, "{tool}");
        assert_eq!(result["value_bytes"], 16, "{tool}");
        assert_eq!(result["value_omitted"], true, "{tool}");
        assert_eq!(result["value"], serde_json::Value::Null, "{tool}");
    }
}

#[tokio::test]
async fn typed_expiration_variants_emit_exactly_one_redis_modifier() {
    for (expiration, expected) in [
        (
            serde_json::json!({"type": "seconds", "value": 10}),
            vec![b"EX".to_vec(), b"10".to_vec()],
        ),
        (
            serde_json::json!({"type": "milliseconds", "value": 20}),
            vec![b"PX".to_vec(), b"20".to_vec()],
        ),
        (
            serde_json::json!({"type": "unix_seconds", "value": 30}),
            vec![b"EXAT".to_vec(), b"30".to_vec()],
        ),
        (
            serde_json::json!({"type": "unix_milliseconds", "value": 40}),
            vec![b"PXAT".to_vec(), b"40".to_vec()],
        ),
        (
            serde_json::json!({"type": "keep_ttl"}),
            vec![b"KEEPTTL".to_vec()],
        ),
    ] {
        let executor = FixedRedis::new(RedisValue::Okay);
        let commands = executor.commands.clone();
        let client = fixed_client(
            executor,
            RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
        )
        .await;
        let result = client
            .call_tool(
                "redis_set",
                serde_json::json!({
                    "key": "expiration",
                    "value": "value",
                    "expiration": expiration
                }),
            )
            .await
            .expect("typed SET expiration");
        assert!(!result.is_error, "{result:?}");
        let commands = commands.lock().expect("SET expiration command");
        assert_eq!(&commands[0].arguments()[2..], expected);
    }

    for (expiration, expected) in [
        (
            serde_json::json!({"type": "seconds", "value": 10}),
            vec![b"EX".to_vec(), b"10".to_vec()],
        ),
        (
            serde_json::json!({"type": "milliseconds", "value": 20}),
            vec![b"PX".to_vec(), b"20".to_vec()],
        ),
        (
            serde_json::json!({"type": "unix_seconds", "value": 30}),
            vec![b"EXAT".to_vec(), b"30".to_vec()],
        ),
        (
            serde_json::json!({"type": "unix_milliseconds", "value": 40}),
            vec![b"PXAT".to_vec(), b"40".to_vec()],
        ),
        (
            serde_json::json!({"type": "persist"}),
            vec![b"PERSIST".to_vec()],
        ),
    ] {
        let executor = FixedRedis::new(RedisValue::Nil);
        let commands = executor.commands.clone();
        let client = fixed_client(
            executor,
            RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
        )
        .await;
        let result = client
            .call_tool(
                "redis_getex",
                serde_json::json!({"key": "expiration", "expiration": expiration}),
            )
            .await
            .expect("typed GETEX expiration");
        assert!(!result.is_error, "{result:?}");
        let commands = commands.lock().expect("GETEX expiration command");
        assert_eq!(&commands[0].arguments()[1..], expected);
    }
}

#[async_trait]
impl RedisExecutor for RecordingRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let response = match command.name() {
            "HSET" => RedisValue::Integer(1),
            "HMGET" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xfd]), RedisValue::Nil]),
            "HDEL" => RedisValue::Integer(1),
            "HEXPIRE" => RedisValue::Array(vec![RedisValue::Integer(1)]),
            "FT.SEARCH" => RedisValue::Array(vec![
                RedisValue::Integer(1),
                RedisValue::BulkString(b"doc:1".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"vector_distance".to_vec()),
                    RedisValue::BulkString(b"0".to_vec()),
                ]),
            ]),
            _ => RedisValue::Nil,
        };
        self.commands.lock().expect("recording lock").push(command);
        Ok(response)
    }
}

#[tokio::test]
async fn vector_values_remain_binary_safe_in_curated_commands() {
    let executor = RecordingRedis::default();
    let commands = executor.commands.clone();
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Search])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect recording client");
    client
        .initialize("redis-mcp-vector-recording-test", "0")
        .await
        .expect("initialize recording client");

    let vector = [1.0_f64, -2.5_f64];
    let mut expected = Vec::new();
    for value in vector {
        expected.extend_from_slice(&(value as f32).to_le_bytes());
    }
    let stored = client
        .call_tool(
            "redis_vector_set_hash",
            serde_json::json!({
                "key": "doc:1",
                "field": "embedding",
                "data_type": "FLOAT32",
                "vector": vector
            }),
        )
        .await
        .expect("store vector");
    assert!(!stored.is_error, "{stored:?}");

    let searched = client
        .call_tool(
            "redis_ft_vector_search",
            serde_json::json!({
                "index": "idx:docs",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": vector,
                "top_k": 1,
                "limit_num": 1
            }),
        )
        .await
        .expect("search vector");
    assert!(!searched.is_error, "{searched:?}");

    let commands = commands.lock().expect("recorded commands");
    let hset = commands
        .iter()
        .find(|command| command.tool_name() == "redis_vector_set_hash")
        .expect("recorded vector HSET");
    assert_eq!(hset.arguments()[2], expected);
    let search = commands
        .iter()
        .find(|command| command.tool_name() == "redis_ft_vector_search")
        .expect("recorded vector FT.SEARCH");
    let blob_position = search
        .arguments()
        .iter()
        .position(|argument| argument == b"BLOB")
        .expect("BLOB parameter");
    assert_eq!(search.arguments()[blob_position + 1], expected);
}

#[tokio::test]
async fn hash_multi_field_commands_preserve_binary_argv_and_request_order() {
    let executor = RecordingRedis::default();
    let commands = executor.commands.clone();
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::DataStructures])
        .capabilities(RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 4, 0)))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect hash recording client");
    client
        .initialize("redis-mcp-hash-recording-test", "0")
        .await
        .expect("initialize hash recording client");

    let set = client
        .call_tool(
            "redis_hset",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "entries": [{
                    "field": "/g==",
                    "field_encoding": "base64",
                    "value": "/Q==",
                    "value_encoding": "base64"
                }]
            }),
        )
        .await
        .expect("binary HSET");
    assert!(!set.is_error, "{set:?}");

    let get = client
        .call_tool(
            "redis_hmget",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "fields": [
                    {"field": "/g==", "field_encoding": "base64"},
                    "missing"
                ]
            }),
        )
        .await
        .expect("binary HMGET")
        .structured_content
        .expect("structured binary HMGET");
    assert_eq!(get["values"][0]["value"], "/Q==");
    assert_eq!(get["values"][0]["value_encoding"], "base64");
    assert_eq!(get["values"][1]["exists"], false);

    client
        .call_tool(
            "redis_hexpire",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "seconds": 60,
                "condition": "gt",
                "fields": [{"field": "/g==", "field_encoding": "base64"}]
            }),
        )
        .await
        .expect("binary HEXPIRE");
    client
        .call_tool(
            "redis_hdel",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "fields": [{"field": "/g==", "field_encoding": "base64"}]
            }),
        )
        .await
        .expect("binary HDEL");

    let commands = commands.lock().expect("recorded hash commands");
    let hset = commands
        .iter()
        .find(|command| command.tool_name() == "redis_hset")
        .expect("recorded HSET");
    assert_eq!(
        hset.arguments(),
        &[vec![0xff, 0x00], vec![0xfe], vec![0xfd]]
    );

    let hmget = commands
        .iter()
        .find(|command| command.tool_name() == "redis_hmget")
        .expect("recorded HMGET");
    assert_eq!(
        hmget.arguments(),
        &[vec![0xff, 0x00], vec![0xfe], b"missing".to_vec()]
    );

    let hexpire = commands
        .iter()
        .find(|command| command.tool_name() == "redis_hexpire")
        .expect("recorded HEXPIRE");
    assert_eq!(
        hexpire.arguments(),
        &[
            vec![0xff, 0x00],
            b"60".to_vec(),
            b"GT".to_vec(),
            b"FIELDS".to_vec(),
            b"1".to_vec(),
            vec![0xfe]
        ]
    );

    let hdel = commands
        .iter()
        .find(|command| command.tool_name() == "redis_hdel")
        .expect("recorded HDEL");
    assert_eq!(hdel.arguments(), &[vec![0xff, 0x00], vec![0xfe]]);
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
            "redis_hexists",
            serde_json::json!({"key": "user:1", "field": "name"}),
            "field_exists",
        ),
        (
            "redis_hkeys",
            serde_json::json!({"key": "user:1"}),
            "fields",
        ),
        ("redis_hlen", serde_json::json!({"key": "user:1"}), "length"),
        (
            "redis_hmget",
            serde_json::json!({"key": "user:1", "fields": ["name", "missing"]}),
            "values",
        ),
        (
            "redis_hscan",
            serde_json::json!({"key": "user:1", "count": 10}),
            "page",
        ),
        (
            "redis_hstrlen",
            serde_json::json!({"key": "user:1", "field": "name"}),
            "length_bytes",
        ),
        (
            "redis_httl",
            serde_json::json!({"key": "user:1", "fields": ["name"]}),
            "fields",
        ),
        (
            "redis_hvals",
            serde_json::json!({"key": "user:1"}),
            "values",
        ),
        (
            "redis_lindex",
            serde_json::json!({"key": "queue", "index": -1}),
            "value",
        ),
        ("redis_llen", serde_json::json!({"key": "queue"}), "length"),
        (
            "redis_lpos",
            serde_json::json!({"key": "queue", "value": "second", "count": 2}),
            "positions",
        ),
        (
            "redis_lrange",
            serde_json::json!({"key": "queue", "start": 0, "stop": 1}),
            "elements",
        ),
        (
            "redis_scard",
            serde_json::json!({"key": "tags"}),
            "cardinality",
        ),
        (
            "redis_sdiff",
            serde_json::json!({"keys": ["tags", "other"]}),
            "members",
        ),
        (
            "redis_sinter",
            serde_json::json!({"keys": ["tags", "other"]}),
            "members",
        ),
        (
            "redis_sismember",
            serde_json::json!({"key": "tags", "member": "alpha"}),
            "is_member",
        ),
        (
            "redis_smembers",
            serde_json::json!({"key": "tags"}),
            "members",
        ),
        (
            "redis_smismember",
            serde_json::json!({"key": "tags", "members": ["alpha", "missing"]}),
            "members",
        ),
        (
            "redis_sscan",
            serde_json::json!({"key": "tags", "count": 10}),
            "page",
        ),
        (
            "redis_sunion",
            serde_json::json!({"keys": ["tags", "other"]}),
            "members",
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
            "redis_vector_get_hash",
            serde_json::json!({"key": "doc:1", "field": "embedding", "data_type": "FLOAT32"}),
            "vector",
        ),
        (
            "redis_ft_vector_search",
            serde_json::json!({
                "index": "idx:docs",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0, 2.0],
                "return_fields": ["title"]
            }),
            "results",
        ),
        (
            "redis_ft_hybrid_search",
            serde_json::json!({
                "index": "idx:docs",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0, 2.0],
                "return_fields": ["title"],
                "filters": [{"type": "text", "field": "title", "value": "Redis"}]
            }),
            "results",
        ),
        (
            "redis_set",
            serde_json::json!({"key": "greeting", "value": "hello"}),
            "applied",
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
            "redis_getrange",
            serde_json::json!({"key": "greeting", "start": 1, "end": 3}),
            "value",
        ),
        (
            "redis_dump",
            serde_json::json!({"key": "greeting"}),
            "payload_base64",
        ),
        (
            "redis_object_inspect",
            serde_json::json!({"key": "greeting", "operation": "encoding"}),
            "encoding",
        ),
        (
            "redis_getex",
            serde_json::json!({"key": "greeting", "expiration": {"type": "seconds", "value": 60}}),
            "value",
        ),
        (
            "redis_setrange",
            serde_json::json!({"key": "greeting", "offset": 1, "value": "ell"}),
            "length_bytes",
        ),
        ("redis_decr", serde_json::json!({"key": "counter"}), "value"),
        (
            "redis_decrby",
            serde_json::json!({"key": "counter", "amount": 2}),
            "value",
        ),
        (
            "redis_incrby",
            serde_json::json!({"key": "counter", "amount": 2}),
            "value",
        ),
        (
            "redis_incrbyfloat",
            serde_json::json!({"key": "counter", "amount": 0.5}),
            "value",
        ),
        (
            "redis_copy",
            serde_json::json!({"source": "greeting", "destination": "greeting-copy"}),
            "copied",
        ),
        (
            "redis_touch",
            serde_json::json!({"keys": ["greeting"]}),
            "touched",
        ),
        (
            "redis_restore",
            serde_json::json!({"key": "restored", "payload_base64": "AA=="}),
            "restored",
        ),
        (
            "redis_hset",
            serde_json::json!({"key": "user:1", "fields": {"name": "Ada"}}),
            "fields_added",
        ),
        (
            "redis_hexpire",
            serde_json::json!({"key": "user:1", "seconds": 60, "fields": ["name"]}),
            "expirations_set",
        ),
        (
            "redis_hincrby",
            serde_json::json!({"key": "user:1", "field": "visits", "increment": 1}),
            "value",
        ),
        (
            "redis_hincrbyfloat",
            serde_json::json!({"key": "user:1", "field": "score", "increment": 0.5}),
            "value",
        ),
        (
            "redis_hpersist",
            serde_json::json!({"key": "user:1", "fields": ["name"]}),
            "expirations_removed",
        ),
        (
            "redis_lpush",
            serde_json::json!({"key": "queue", "elements": ["first", "second"]}),
            "length",
        ),
        (
            "redis_rpush",
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
            "redis_vector_set_hash",
            serde_json::json!({
                "key": "doc:1",
                "field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0, 2.0]
            }),
            "stored",
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
            "redis_hdel",
            serde_json::json!({"key": "user:1", "fields": ["name"]}),
            "deleted",
        ),
        (
            "redis_lpop",
            serde_json::json!({"key": "queue", "count": 1}),
            "elements",
        ),
        (
            "redis_lmove",
            serde_json::json!({"source": "queue", "destination": "archive", "from": "left", "to": "right"}),
            "moved",
        ),
        (
            "redis_lrem",
            serde_json::json!({"key": "queue", "count": 1, "value": "first"}),
            "removed",
        ),
        (
            "redis_lset",
            serde_json::json!({"key": "queue", "index": -1, "value": "last"}),
            "replaced",
        ),
        (
            "redis_ltrim",
            serde_json::json!({"key": "queue", "start": 0, "stop": 9}),
            "trimmed",
        ),
        (
            "redis_rpop",
            serde_json::json!({"key": "queue", "count": 1}),
            "elements",
        ),
        (
            "redis_srem",
            serde_json::json!({"key": "tags", "members": ["alpha"]}),
            "removed",
        ),
        (
            "redis_getdel",
            serde_json::json!({"key": "greeting"}),
            "value",
        ),
        (
            "redis_copy_replace",
            serde_json::json!({"source": "greeting", "destination": "greeting-copy"}),
            "copied",
        ),
        (
            "redis_rename",
            serde_json::json!({"source": "greeting", "destination": "renamed"}),
            "renamed",
        ),
        (
            "redis_renamenx",
            serde_json::json!({"source": "greeting", "destination": "renamed"}),
            "renamed",
        ),
        (
            "redis_restore_replace",
            serde_json::json!({"key": "restored", "payload_base64": "AA=="}),
            "restored",
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
    assert_eq!(tool_names(AccessMode::Full, false).len(), 78);
    assert_eq!(tool_names(AccessMode::Full, true).len(), 79);
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
async fn set_annotations_match_read_write_and_destructive_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated set tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in [
        "redis_scard",
        "redis_sdiff",
        "redis_sinter",
        "redis_sismember",
        "redis_smembers",
        "redis_smismember",
        "redis_sscan",
        "redis_sunion",
    ] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }

    let add = annotations("redis_sadd");
    assert!(!add.read_only_hint);
    assert!(!add.destructive_hint);
    assert!(add.idempotent_hint);

    let remove = annotations("redis_srem");
    assert!(!remove.read_only_hint);
    assert!(remove.destructive_hint);
    assert!(remove.idempotent_hint);
}

#[tokio::test]
async fn key_string_annotations_match_access_and_overwrite_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    let inspect = annotations("redis_object_inspect");
    assert!(inspect.read_only_hint);
    assert!(!inspect.destructive_hint);
    assert!(inspect.idempotent_hint);

    let copy = annotations("redis_copy");
    assert!(!copy.read_only_hint);
    assert!(!copy.destructive_hint);
    assert!(copy.idempotent_hint);

    for name in ["redis_set", "redis_expire", "redis_getex", "redis_touch"] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(!annotation.idempotent_hint, "{name}");
    }

    for name in [
        "redis_copy_replace",
        "redis_getdel",
        "redis_rename",
        "redis_renamenx",
        "redis_restore_replace",
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
    }
    assert!(!annotations("redis_restore_replace").idempotent_hint);
}

#[tokio::test]
async fn hash_annotations_match_read_write_and_destructive_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated hash tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in [
        "redis_hexists",
        "redis_hkeys",
        "redis_hlen",
        "redis_hmget",
        "redis_hstrlen",
        "redis_httl",
        "redis_hvals",
    ] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }

    for name in ["redis_hexpire", "redis_hincrby", "redis_hincrbyfloat"] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(!annotation.idempotent_hint, "{name}");
    }

    let persist = annotations("redis_hpersist");
    assert!(!persist.read_only_hint);
    assert!(!persist.destructive_hint);
    assert!(persist.idempotent_hint);

    let delete = annotations("redis_hdel");
    assert!(!delete.read_only_hint);
    assert!(delete.destructive_hint);
    assert!(delete.idempotent_hint);
}

#[tokio::test]
async fn list_annotations_match_read_write_and_destructive_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated list tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in ["redis_lindex", "redis_llen", "redis_lpos", "redis_lrange"] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }

    for name in ["redis_lpush", "redis_rpush"] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(!annotation.idempotent_hint, "{name}");
    }

    for name in [
        "redis_lpop",
        "redis_lmove",
        "redis_lrem",
        "redis_lset",
        "redis_ltrim",
        "redis_rpop",
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
    }
    assert!(annotations("redis_lset").idempotent_hint);
    assert!(!annotations("redis_ltrim").idempotent_hint);
    assert!(!annotations("redis_lmove").idempotent_hint);
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

    let search_read_only = client_for_bundles(
        AccessMode::ReadOnly,
        [ToolBundle::Search],
        RawCommandPolicy::Disabled,
    )
    .await
    .list_tools()
    .await
    .expect("list Search read tools")
    .tools
    .into_iter()
    .map(|tool| tool.name)
    .collect::<Vec<_>>();
    assert!(
        search_read_only
            .iter()
            .any(|name| name == "redis_ft_vector_search")
    );
    assert!(
        search_read_only
            .iter()
            .any(|name| name == "redis_ft_hybrid_search")
    );
    assert!(
        !search_read_only
            .iter()
            .any(|name| name == "redis_ft_create")
    );
    assert!(
        !search_read_only
            .iter()
            .any(|name| name == "redis_vector_set_hash")
    );

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
            "redis_ft_hybrid_search",
            "redis_ft_info",
            "redis_ft_list",
            "redis_ft_search",
            "redis_ft_vector_search",
            "redis_vector_get_hash",
            "redis_vector_set_hash",
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

    let algebra = client_with_budget(AccessMode::ReadOnly, false, OutputBudget::new(1_000_000, 1))
        .await
        .call_tool(
            "redis_sunion",
            serde_json::json!({"keys": ["tags", "other"]}),
        )
        .await
        .expect("over-limit SUNION");
    assert_output_limit(&algebra, "collection_entries", 2, 1);
    assert!(
        algebra.meta.as_ref().unwrap()["io.redis.mcp/outputLimit"]["guidance"]
            .as_str()
            .unwrap()
            .contains("SSCAN")
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
            "redis_rpush",
            serde_json::json!({"key": "queue", "elements": []}),
        ),
        (
            "redis_lpop",
            serde_json::json!({"key": "queue", "count": 0}),
        ),
        (
            "redis_lpos",
            serde_json::json!({"key": "queue", "value": "item", "rank": 0}),
        ),
        (
            "redis_hmget",
            serde_json::json!({"key": "hash", "fields": []}),
        ),
        (
            "redis_httl",
            serde_json::json!({"key": "hash", "fields": []}),
        ),
        ("redis_hset", serde_json::json!({"key": "hash"})),
        (
            "redis_hset",
            serde_json::json!({
                "key": "hash",
                "fields": {"field": "value"},
                "entries": [{"field": "other", "value": "value"}]
            }),
        ),
        (
            "redis_hset",
            serde_json::json!({
                "key": "hash",
                "entries": [
                    {"field": "field", "value": "one"},
                    {"field": "ZmllbGQ=", "field_encoding": "base64", "value": "two"}
                ]
            }),
        ),
        (
            "redis_hset",
            serde_json::json!({
                "key": "hash",
                "entries": [{"field": "not-base64", "field_encoding": "base64", "value": "value"}]
            }),
        ),
        (
            "redis_hexpire",
            serde_json::json!({"key": "hash", "seconds": 0, "fields": ["field"]}),
        ),
        (
            "redis_hexpire",
            serde_json::json!({"key": "hash", "seconds": 60, "fields": ["field", "field"]}),
        ),
        (
            "redis_hpersist",
            serde_json::json!({"key": "hash", "fields": []}),
        ),
        (
            "redis_hdel",
            serde_json::json!({"key": "hash", "fields": ["field", "field"]}),
        ),
        (
            "redis_sadd",
            serde_json::json!({"key": "tags", "members": []}),
        ),
        (
            "redis_smismember",
            serde_json::json!({"key": "tags", "members": []}),
        ),
        ("redis_sdiff", serde_json::json!({"keys": []})),
        (
            "redis_srem",
            serde_json::json!({"key": "tags", "members": []}),
        ),
        (
            "redis_sismember",
            serde_json::json!({
                "key": "tags",
                "member": "not-base64",
                "member_encoding": "base64"
            }),
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
        (
            "redis_vector_set_hash",
            serde_json::json!({
                "key": "doc:1",
                "field": "embedding",
                "data_type": "FLOAT32",
                "vector": []
            }),
        ),
        (
            "redis_ft_vector_search",
            serde_json::json!({
                "index": "idx",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0],
                "top_k": 101
            }),
        ),
        (
            "redis_ft_create",
            serde_json::json!({
                "index": "idx",
                "schema": [{"name": "embedding", "field_type": "VECTOR"}]
            }),
        ),
        (
            "redis_ft_hybrid_search",
            serde_json::json!({
                "index": "idx",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0],
                "top_k": 1,
                "filters": [{"type": "text", "field": "bad-field", "value": "x"}]
            }),
        ),
        (
            "redis_set",
            serde_json::json!({
                "key": "greeting",
                "value": "hello",
                "expiration": {"type": "seconds", "value": 0}
            }),
        ),
        (
            "redis_set",
            serde_json::json!({
                "key": "greeting",
                "value": "hello",
                "expiration": {"type": "seconds", "value": 10, "milliseconds": 20}
            }),
        ),
        (
            "redis_set",
            serde_json::json!({
                "key": "greeting",
                "value": "hello",
                "get": true,
                "max_previous_bytes": 0
            }),
        ),
        (
            "redis_set",
            serde_json::json!({
                "key": "not-base64",
                "key_encoding": "base64",
                "value": "hello"
            }),
        ),
        (
            "redis_getrange",
            serde_json::json!({"key": "greeting", "start": 10, "end": 9}),
        ),
        (
            "redis_getrange",
            serde_json::json!({"key": "greeting", "start": 0, "end": 65536}),
        ),
        (
            "redis_setrange",
            serde_json::json!({"key": "greeting", "offset": 16777216, "value": "x"}),
        ),
        (
            "redis_dump",
            serde_json::json!({"key": "greeting", "max_bytes": 0}),
        ),
        (
            "redis_dump",
            serde_json::json!({"key": "greeting", "max_bytes": 2}),
        ),
        (
            "redis_restore",
            serde_json::json!({"key": "restored", "payload_base64": "not-base64"}),
        ),
        (
            "redis_restore",
            serde_json::json!({
                "key": "restored",
                "payload_base64": "A".repeat(349529)
            }),
        ),
        (
            "redis_restore",
            serde_json::json!({
                "key": "restored",
                "payload_base64": "AA==",
                "idle_time_seconds": 0
            }),
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
            "HMGET" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xff]), RedisValue::Nil]),
            "HKEYS" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xfe])]),
            "HVALS" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xff])]),
            "EXISTS" => RedisValue::Integer(1),
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

#[derive(Clone, Default)]
struct ListContractRedis {
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

#[async_trait]
impl RedisExecutor for ListContractRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let key = command.arguments().first().map(Vec::as_slice);
        let response = match command.name() {
            "LPUSH" | "RPUSH" => RedisValue::Integer(2),
            "LLEN" if key == Some(b"missing".as_slice()) => RedisValue::Integer(0),
            "LLEN" => RedisValue::Integer(2),
            "LINDEX" if key == Some(b"missing".as_slice()) => RedisValue::Nil,
            "LINDEX" if key == Some(b"empty-value".as_slice()) => {
                RedisValue::BulkString(Vec::new())
            }
            "LINDEX" => RedisValue::BulkString(vec![0xff]),
            "LRANGE" if key == Some(b"missing".as_slice()) => RedisValue::Array(Vec::new()),
            "LRANGE" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xff])]),
            "LPOS" if key == Some(b"missing".as_slice()) => RedisValue::Array(Vec::new()),
            "LPOS" => RedisValue::Array(vec![RedisValue::Integer(1)]),
            "LPOP" | "RPOP" if key == Some(b"missing".as_slice()) => RedisValue::Nil,
            "LPOP" | "RPOP" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xff])]),
            "LMOVE" if key == Some(b"missing".as_slice()) => RedisValue::Nil,
            "LMOVE" => RedisValue::BulkString(vec![0xff]),
            "LREM" => RedisValue::Integer(1),
            "LSET" | "LTRIM" => RedisValue::Okay,
            "EXISTS" if key == Some(b"missing".as_slice()) => RedisValue::Integer(0),
            "EXISTS" => RedisValue::Integer(1),
            _ => RedisValue::Nil,
        };
        self.commands
            .lock()
            .expect("list contract lock")
            .push(command);
        Ok(response)
    }
}

async fn list_contract_client(executor: ListContractRedis) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::DataStructures])
        .capabilities(RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 2, 0)))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect list contract client");
    client
        .initialize("redis-mcp-list-contract-test", "0")
        .await
        .expect("initialize list contract client");
    client
}

#[tokio::test]
async fn list_commands_preserve_binary_argv_and_native_ordering_options() {
    let executor = ListContractRedis::default();
    let commands = executor.commands.clone();
    let client = list_contract_client(executor).await;

    for tool in ["redis_lpush", "redis_rpush"] {
        let result = client
            .call_tool(
                tool,
                serde_json::json!({
                    "key": "/wA=",
                    "key_encoding": "base64",
                    "elements": [
                        {"value": "/g==", "value_encoding": "base64"},
                        "tail"
                    ]
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(!result.is_error, "{tool}: {result:?}");
    }
    client
        .call_tool(
            "redis_lindex",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64", "index": -1}),
        )
        .await
        .expect("binary LINDEX");
    client
        .call_tool(
            "redis_lpos",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "value": "/g==",
                "value_encoding": "base64",
                "rank": -1,
                "count": 2,
                "max_len": 10
            }),
        )
        .await
        .expect("binary LPOS");
    client
        .call_tool(
            "redis_lpop",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64", "count": 2}),
        )
        .await
        .expect("binary LPOP");
    client
        .call_tool(
            "redis_lrem",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "count": -1,
                "value": "/g==",
                "value_encoding": "base64"
            }),
        )
        .await
        .expect("binary LREM");
    client
        .call_tool(
            "redis_lset",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "index": -1,
                "value": "/g==",
                "value_encoding": "base64"
            }),
        )
        .await
        .expect("binary LSET");
    client
        .call_tool(
            "redis_ltrim",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64", "start": -2, "stop": -1}),
        )
        .await
        .expect("binary LTRIM");
    client
        .call_tool(
            "redis_lmove",
            serde_json::json!({
                "source": "/wA=",
                "source_encoding": "base64",
                "destination": "/Q==",
                "destination_encoding": "base64",
                "from": "right",
                "to": "left"
            }),
        )
        .await
        .expect("binary LMOVE");
    client
        .call_tool(
            "redis_rpop",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64", "count": 2}),
        )
        .await
        .expect("binary RPOP");

    let commands = commands.lock().expect("recorded list commands");
    let arguments = |tool: &str| {
        commands
            .iter()
            .find(|command| command.tool_name() == tool)
            .unwrap_or_else(|| panic!("missing {tool}"))
            .arguments()
    };
    for tool in ["redis_lpush", "redis_rpush"] {
        assert_eq!(
            arguments(tool),
            &[vec![0xff, 0x00], vec![0xfe], b"tail".to_vec()]
        );
    }
    assert_eq!(
        arguments("redis_lindex"),
        &[vec![0xff, 0x00], b"-1".to_vec()]
    );
    assert_eq!(
        arguments("redis_lpos"),
        &[
            vec![0xff, 0x00],
            vec![0xfe],
            b"RANK".to_vec(),
            b"-1".to_vec(),
            b"COUNT".to_vec(),
            b"2".to_vec(),
            b"MAXLEN".to_vec(),
            b"10".to_vec(),
        ]
    );
    assert_eq!(arguments("redis_lpop"), &[vec![0xff, 0x00], b"2".to_vec()]);
    assert_eq!(
        arguments("redis_lrem"),
        &[vec![0xff, 0x00], b"-1".to_vec(), vec![0xfe]]
    );
    assert_eq!(
        arguments("redis_lset"),
        &[vec![0xff, 0x00], b"-1".to_vec(), vec![0xfe]]
    );
    assert_eq!(
        arguments("redis_ltrim"),
        &[vec![0xff, 0x00], b"-2".to_vec(), b"-1".to_vec()]
    );
    assert_eq!(
        arguments("redis_lmove"),
        &[
            vec![0xff, 0x00],
            vec![0xfd],
            b"RIGHT".to_vec(),
            b"LEFT".to_vec(),
        ]
    );
    assert_eq!(arguments("redis_rpop"), &[vec![0xff, 0x00], b"2".to_vec()]);
}

#[tokio::test]
async fn list_reads_and_pops_distinguish_binary_empty_and_missing_results() {
    let client = list_contract_client(ListContractRedis::default()).await;

    let binary = client
        .call_tool(
            "redis_lindex",
            serde_json::json!({"key": "present", "index": -1}),
        )
        .await
        .expect("binary LINDEX")
        .structured_content
        .expect("structured binary LINDEX");
    assert_eq!(binary["list_exists"], true);
    assert_eq!(binary["element_exists"], true);
    assert_eq!(binary["value"], "/w==");
    assert_eq!(binary["encoding"], "base64");

    let empty = client
        .call_tool(
            "redis_lindex",
            serde_json::json!({"key": "empty-value", "index": 0}),
        )
        .await
        .expect("empty-value LINDEX")
        .structured_content
        .expect("structured empty-value LINDEX");
    assert_eq!(empty["element_exists"], true);
    assert_eq!(empty["value"], "");
    assert_eq!(empty["encoding"], "utf8");

    let missing = client
        .call_tool(
            "redis_lindex",
            serde_json::json!({"key": "missing", "index": 0}),
        )
        .await
        .expect("missing LINDEX")
        .structured_content
        .expect("structured missing LINDEX");
    assert_eq!(missing["list_exists"], false);
    assert_eq!(missing["element_exists"], false);
    assert_eq!(missing["value"], serde_json::Value::Null);

    let range = client
        .call_tool("redis_lrange", serde_json::json!({"key": "missing"}))
        .await
        .expect("missing LRANGE")
        .structured_content
        .expect("structured missing LRANGE");
    assert_eq!(range["exists"], false);
    assert_eq!(range["elements"], serde_json::json!([]));

    let positions = client
        .call_tool(
            "redis_lpos",
            serde_json::json!({"key": "missing", "value": "needle"}),
        )
        .await
        .expect("missing LPOS")
        .structured_content
        .expect("structured missing LPOS");
    assert_eq!(positions["exists"], false);
    assert_eq!(positions["positions"], serde_json::json!([]));

    for tool in ["redis_lpop", "redis_rpop"] {
        let popped = client
            .call_tool(tool, serde_json::json!({"key": "missing", "count": 2}))
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: no structured content"));
        assert_eq!(popped["found"], false, "{tool}");
        assert_eq!(popped["popped"], 0, "{tool}");
        assert_eq!(popped["elements"], serde_json::json!([]), "{tool}");
    }

    let moved = client
        .call_tool(
            "redis_lmove",
            serde_json::json!({
                "source": "missing",
                "destination": "archive",
                "from": "left",
                "to": "right"
            }),
        )
        .await
        .expect("missing LMOVE")
        .structured_content
        .expect("structured missing LMOVE");
    assert_eq!(moved["moved"], false);
    assert_eq!(moved["value"], serde_json::Value::Null);
}

#[derive(Clone, Default)]
struct SetContractRedis {
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

#[async_trait]
impl RedisExecutor for SetContractRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let key = command.arguments().first().map(Vec::as_slice);
        let response = match command.name() {
            "SCARD" if key == Some(b"missing".as_slice()) => RedisValue::Integer(0),
            "SCARD" => RedisValue::Integer(2),
            "SISMEMBER" => RedisValue::Integer(
                (key != Some(b"missing".as_slice())
                    && command.arguments().get(1).map(Vec::as_slice) != Some(b"missing".as_slice()))
                    as i64,
            ),
            "SMISMEMBER" => RedisValue::Array(
                command
                    .arguments()
                    .iter()
                    .skip(1)
                    .map(|member| {
                        RedisValue::Integer(
                            (key != Some(b"missing".as_slice()) && member.as_slice() != b"missing")
                                as i64,
                        )
                    })
                    .collect(),
            ),
            "SMEMBERS" if key == Some(b"missing".as_slice()) => RedisValue::Set(Vec::new()),
            "SMEMBERS" => RedisValue::Set(vec![
                RedisValue::BulkString(b"zeta".to_vec()),
                RedisValue::BulkString(vec![0xff]),
                RedisValue::BulkString(b"alpha".to_vec()),
            ]),
            "SSCAN" if key == Some(b"missing".as_slice()) => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(Vec::new()),
            ]),
            "SSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"zeta".to_vec()),
                    RedisValue::BulkString(b"alpha".to_vec()),
                ]),
            ]),
            "SDIFF" | "SINTER" | "SUNION" if key == Some(b"missing".as_slice()) => {
                RedisValue::Set(Vec::new())
            }
            "SDIFF" | "SINTER" | "SUNION" => RedisValue::Set(vec![
                RedisValue::BulkString(b"zeta".to_vec()),
                RedisValue::BulkString(vec![0xff]),
                RedisValue::BulkString(b"alpha".to_vec()),
            ]),
            "SADD" | "SREM" => RedisValue::Integer(1),
            "EXISTS" if key == Some(b"missing".as_slice()) => RedisValue::Integer(0),
            "EXISTS" => RedisValue::Integer(1),
            _ => RedisValue::Nil,
        };
        self.commands
            .lock()
            .expect("set contract lock")
            .push(command);
        Ok(response)
    }
}

async fn set_contract_client(executor: SetContractRedis) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::DataStructures])
        .capabilities(RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 2, 0)))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect set contract client");
    client
        .initialize("redis-mcp-set-contract-test", "0")
        .await
        .expect("initialize set contract client");
    client
}

#[tokio::test]
async fn set_commands_preserve_binary_argv_and_ordered_membership_contracts() {
    let executor = SetContractRedis::default();
    let commands = executor.commands.clone();
    let client = set_contract_client(executor).await;

    for tool in ["redis_sadd", "redis_srem"] {
        let result = client
            .call_tool(
                tool,
                serde_json::json!({
                    "key": "/wA=",
                    "key_encoding": "base64",
                    "members": [
                        {"member": "/g==", "member_encoding": "base64"},
                        "tail"
                    ]
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(!result.is_error, "{tool}: {result:?}");
    }
    client
        .call_tool(
            "redis_scard",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64"}),
        )
        .await
        .expect("binary SCARD");
    client
        .call_tool(
            "redis_sismember",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "member": "/g==",
                "member_encoding": "base64"
            }),
        )
        .await
        .expect("binary SISMEMBER");
    let multiple = client
        .call_tool(
            "redis_smismember",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "members": [
                    {"member": "/g==", "member_encoding": "base64"},
                    "missing"
                ]
            }),
        )
        .await
        .expect("binary SMISMEMBER")
        .structured_content
        .expect("structured SMISMEMBER");
    assert_eq!(multiple["members"][0]["member"], "/g==");
    assert_eq!(multiple["members"][0]["member_encoding"], "base64");
    assert_eq!(multiple["members"][0]["is_member"], true);
    assert_eq!(multiple["members"][1]["member"], "missing");
    assert_eq!(multiple["members"][1]["is_member"], false);

    client
        .call_tool(
            "redis_smembers",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64"}),
        )
        .await
        .expect("binary SMEMBERS");
    client
        .call_tool(
            "redis_sscan",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "cursor": 5,
                "pattern": "a*",
                "count": 2
            }),
        )
        .await
        .expect("binary SSCAN");
    for tool in ["redis_sdiff", "redis_sinter", "redis_sunion"] {
        let result = client
            .call_tool(
                tool,
                serde_json::json!({
                    "keys": [
                        {"key": "/wA=", "key_encoding": "base64"},
                        {"key": "/Q==", "key_encoding": "base64"}
                    ]
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(result["ordering"], "byte_sorted", "{tool}");
        assert_eq!(result["members"][0]["value"], "alpha", "{tool}");
        assert_eq!(result["members"][2]["encoding"], "base64", "{tool}");
    }

    let commands = commands.lock().expect("recorded set commands");
    let arguments = |tool: &str| {
        commands
            .iter()
            .find(|command| command.tool_name() == tool)
            .unwrap_or_else(|| panic!("missing {tool}"))
            .arguments()
    };
    for tool in ["redis_sadd", "redis_srem"] {
        assert_eq!(
            arguments(tool),
            &[vec![0xff, 0x00], vec![0xfe], b"tail".to_vec()]
        );
    }
    assert_eq!(arguments("redis_scard"), &[vec![0xff, 0x00]]);
    assert_eq!(
        arguments("redis_sismember"),
        &[vec![0xff, 0x00], vec![0xfe]]
    );
    assert_eq!(
        arguments("redis_smismember"),
        &[vec![0xff, 0x00], vec![0xfe], b"missing".to_vec()]
    );
    assert_eq!(arguments("redis_smembers"), &[vec![0xff, 0x00]]);
    assert_eq!(
        arguments("redis_sscan"),
        &[
            vec![0xff, 0x00],
            b"5".to_vec(),
            b"MATCH".to_vec(),
            b"a*".to_vec(),
            b"COUNT".to_vec(),
            b"2".to_vec(),
        ]
    );
    for tool in ["redis_sdiff", "redis_sinter", "redis_sunion"] {
        assert_eq!(arguments(tool), &[vec![0xff, 0x00], vec![0xfd]]);
    }
}

#[tokio::test]
async fn set_reads_distinguish_missing_sets_and_empty_algebra_results() {
    let client = set_contract_client(SetContractRedis::default()).await;

    let cardinality = client
        .call_tool("redis_scard", serde_json::json!({"key": "missing"}))
        .await
        .expect("missing SCARD")
        .structured_content
        .expect("structured missing SCARD");
    assert_eq!(cardinality["exists"], false);
    assert_eq!(cardinality["cardinality"], 0);

    let one = client
        .call_tool(
            "redis_sismember",
            serde_json::json!({"key": "missing", "member": "missing"}),
        )
        .await
        .expect("missing SISMEMBER")
        .structured_content
        .expect("structured missing SISMEMBER");
    assert_eq!(one["set_exists"], false);
    assert_eq!(one["is_member"], false);

    let multiple = client
        .call_tool(
            "redis_smismember",
            serde_json::json!({"key": "missing", "members": ["missing", "missing"]}),
        )
        .await
        .expect("missing SMISMEMBER")
        .structured_content
        .expect("structured missing SMISMEMBER");
    assert_eq!(multiple["set_exists"], false);
    assert_eq!(multiple["count"], 2);
    assert!(
        multiple["members"]
            .as_array()
            .expect("membership array")
            .iter()
            .all(|member| member["is_member"] == false)
    );

    for tool in ["redis_smembers", "redis_sscan"] {
        let result = client
            .call_tool(tool, serde_json::json!({"key": "missing"}))
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(result["exists"], false, "{tool}");
        assert_eq!(result["members"], serde_json::json!([]), "{tool}");
    }

    for tool in ["redis_sdiff", "redis_sinter", "redis_sunion"] {
        let result = client
            .call_tool(tool, serde_json::json!({"keys": ["missing"]}))
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(result["count"], 0, "{tool}");
        assert_eq!(result["members"], serde_json::json!([]), "{tool}");
    }
}

#[derive(Clone, Copy)]
struct HashEdgeRedis;

#[async_trait]
impl RedisExecutor for HashEdgeRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let key = command.arguments().first().map(Vec::as_slice);
        Ok(match command.name() {
            "HGET"
                if key == Some(b"empty".as_slice())
                    && command.arguments().get(1).map(Vec::as_slice)
                        == Some(b"present".as_slice()) =>
            {
                RedisValue::BulkString(Vec::new())
            }
            "HGET" => RedisValue::Nil,
            "HMGET" if key == Some(b"empty".as_slice()) => {
                RedisValue::Array(vec![RedisValue::BulkString(Vec::new()), RedisValue::Nil])
            }
            "HMGET" => RedisValue::Array(vec![RedisValue::Nil, RedisValue::Nil]),
            "HSTRLEN" => RedisValue::Integer(0),
            "HEXISTS"
                if key == Some(b"empty".as_slice())
                    && command.arguments().get(1).map(Vec::as_slice)
                        == Some(b"present".as_slice()) =>
            {
                RedisValue::Integer(1)
            }
            "HEXISTS" => RedisValue::Integer(0),
            "EXISTS" if key == Some(b"missing-hash".as_slice()) => RedisValue::Integer(0),
            "EXISTS" => RedisValue::Integer(1),
            _ => RedisValue::Nil,
        })
    }
}

#[tokio::test]
async fn hash_reads_distinguish_empty_missing_field_and_missing_hash() {
    let router = RedisMcp::builder(HashEdgeRedis)
        .access(AccessMode::ReadOnly)
        .bundles([ToolBundle::DataStructures])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect hash edge client");
    client
        .initialize("redis-mcp-hash-edge-test", "0")
        .await
        .expect("initialize hash edge client");

    let empty = client
        .call_tool(
            "redis_hget",
            serde_json::json!({"key": "empty", "field": "present"}),
        )
        .await
        .expect("empty HGET")
        .structured_content
        .expect("structured empty HGET");
    assert_eq!(empty["hash_exists"], true);
    assert_eq!(empty["field_exists"], true);
    assert_eq!(empty["value"], "");
    assert_eq!(empty["encoding"], "utf8");

    let missing_field = client
        .call_tool(
            "redis_hget",
            serde_json::json!({"key": "empty", "field": "missing"}),
        )
        .await
        .expect("missing field HGET")
        .structured_content
        .expect("structured missing field HGET");
    assert_eq!(missing_field["hash_exists"], true);
    assert_eq!(missing_field["field_exists"], false);
    assert_eq!(missing_field["value"], serde_json::Value::Null);

    let missing_hash = client
        .call_tool(
            "redis_hget",
            serde_json::json!({"key": "missing-hash", "field": "missing"}),
        )
        .await
        .expect("missing hash HGET")
        .structured_content
        .expect("structured missing hash HGET");
    assert_eq!(missing_hash["hash_exists"], false);
    assert_eq!(missing_hash["field_exists"], false);

    let multiple = client
        .call_tool(
            "redis_hmget",
            serde_json::json!({"key": "empty", "fields": ["present", "missing"]}),
        )
        .await
        .expect("edge HMGET")
        .structured_content
        .expect("structured edge HMGET");
    assert_eq!(multiple["hash_exists"], true);
    assert_eq!(multiple["values"][0]["exists"], true);
    assert_eq!(multiple["values"][0]["value"], "");
    assert_eq!(multiple["values"][1]["exists"], false);

    let length = client
        .call_tool(
            "redis_hstrlen",
            serde_json::json!({"key": "empty", "field": "present"}),
        )
        .await
        .expect("empty HSTRLEN")
        .structured_content
        .expect("structured empty HSTRLEN");
    assert_eq!(length["length_bytes"], 0);
    assert_eq!(length["field_exists"], true);
    assert_eq!(length["hash_exists"], true);
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
            "redis_hmget",
            serde_json::json!({"key": "hash", "fields": ["field", "missing"]}),
            "/values/0/value_encoding",
        ),
        (
            "redis_hkeys",
            serde_json::json!({"key": "hash"}),
            "/fields/0/encoding",
        ),
        (
            "redis_hvals",
            serde_json::json!({"key": "hash"}),
            "/values/0/encoding",
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
    assert!(!names.iter().any(|name| name == "redis_object_inspect"));
    assert!(!names.iter().any(|name| name == "redis_unlink"));
    assert!(!names.iter().any(|name| name == "redis_copy"));
    assert!(!names.iter().any(|name| name == "redis_getdel"));
    assert!(!names.iter().any(|name| name == "redis_restore"));
    assert!(names.iter().any(|name| name == "redis_get"));
    assert!(names.iter().any(|name| name == "redis_dump"));
    assert!(names.iter().any(|name| name == "redis_touch"));
    assert!(names.iter().any(|name| name == "redis_lindex"));
    for name in ["redis_lpos", "redis_lpop", "redis_lmove", "redis_rpop"] {
        assert!(!names.iter().any(|candidate| candidate == name), "{name}");
    }
    assert!(!names.iter().any(|name| name == "redis_smismember"));

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
    assert!(new_names.contains(&"redis_object_inspect"));
    assert!(new_names.contains(&"redis_unlink"));
    assert!(!new_names.contains(&"redis_restore"));

    let supported = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 2, 0));
    let redis_six = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 0, 0));
    let redis_six_list_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::DataStructures],
        false,
        &redis_six,
        UnavailableToolPolicy::Hide,
    );
    assert!(redis_six_list_names.contains(&"redis_lpos"));
    for name in ["redis_lpop", "redis_lmove", "redis_rpop"] {
        assert!(!redis_six_list_names.contains(&name), "{name}");
    }
    assert!(!redis_six_list_names.contains(&"redis_smismember"));
    let supported_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::Essentials],
        false,
        &supported,
        UnavailableToolPolicy::Hide,
    );
    for name in [
        "redis_copy",
        "redis_getdel",
        "redis_getex",
        "redis_restore",
        "redis_restore_replace",
    ] {
        assert!(supported_names.contains(&name), "{name}");
    }
    let supported_list_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::DataStructures],
        false,
        &supported,
        UnavailableToolPolicy::Hide,
    );
    for name in [
        "redis_lpos",
        "redis_lpop",
        "redis_lmove",
        "redis_rpop",
        "redis_smismember",
    ] {
        assert!(supported_list_names.contains(&name), "{name}");
    }

    let pre_field_expiration =
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 2, 0));
    let pre_field_expiration_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::DataStructures],
        false,
        &pre_field_expiration,
        UnavailableToolPolicy::Hide,
    );
    for name in ["redis_hexpire", "redis_hpersist", "redis_httl"] {
        assert!(!pre_field_expiration_names.contains(&name), "{name}");
    }
    assert!(pre_field_expiration_names.contains(&"redis_hstrlen"));

    let field_expiration =
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 4, 0));
    let field_expiration_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::DataStructures],
        false,
        &field_expiration,
        UnavailableToolPolicy::Hide,
    );
    for name in ["redis_hexpire", "redis_hpersist", "redis_httl"] {
        assert!(field_expiration_names.contains(&name), "{name}");
    }
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
    assert!(!names.iter().any(|name| name.starts_with("redis_vector_")));

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
    assert!(!names.iter().any(|name| name == "redis_ft_vector_search"));
    assert!(!names.iter().any(|name| name == "redis_ft_hybrid_search"));
    assert!(!names.iter().any(|name| name == "redis_vector_get_hash"));
    assert!(!names.iter().any(|name| name == "redis_vector_set_hash"));

    let pre_vector_search = RedisCapabilities::unknown().with_module(
        RedisModule::Search,
        RedisModuleCapability::available(Some(RedisVersion::new(2, 2, 0))),
    );
    let client = capability_client(pre_vector_search, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list pre-vector Search tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(names.iter().any(|name| name == "redis_ft_list"));
    assert!(names.iter().any(|name| name == "redis_ft_search"));
    assert!(!names.iter().any(|name| name == "redis_ft_vector_search"));
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
    assert!(names.iter().any(|name| name == "redis_ft_vector_search"));
    assert!(names.iter().any(|name| name == "redis_ft_hybrid_search"));
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

    let pre_field_expiration =
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 2, 0));
    let client = capability_client(pre_field_expiration, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool(
            "redis_hexpire",
            serde_json::json!({"key": "never-sent", "seconds": 60, "fields": ["field"]}),
        )
        .await
        .expect("hash field expiration version failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize hash version error");
    assert!(result.is_error);
    assert!(text.contains("CapabilityUnavailable"));
    assert!(text.contains("REDIS_VERSION_UNAVAILABLE"));
    assert!(!text.contains("never-sent"));

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

    let pre_vector_search = RedisCapabilities::unknown().with_module(
        RedisModule::Search,
        RedisModuleCapability::available(Some(RedisVersion::new(2, 2, 0))),
    );
    let client = capability_client(pre_vector_search, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool(
            "redis_ft_vector_search",
            serde_json::json!({
                "index": "idx",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0],
                "top_k": 1,
                "limit_num": 1
            }),
        )
        .await
        .expect("vector module version failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize vector version failure");
    assert!(result.is_error);
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
