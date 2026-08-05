use async_trait::async_trait;
use pretty_assertions::assert_eq;
use redis_mcp::{AccessMode, RedisExecutor, RedisMcp, tool_names};
use tower_mcp::client::{ChannelTransport, McpClient};

#[derive(Clone, Copy)]
struct StubRedis;

#[async_trait]
impl RedisExecutor for StubRedis {
    async fn execute(&self, command: redis::Cmd) -> redis::RedisResult<redis::Value> {
        let packed = String::from_utf8_lossy(&command.get_packed_command()).into_owned();
        let value = if packed.contains("\r\nPING\r\n") {
            redis::Value::SimpleString("PONG".into())
        } else if packed.contains("\r\nINFO\r\n") {
            redis::Value::BulkString(b"# Server\r\nredis_version:8.2.0\r\n".to_vec())
        } else if packed.contains("\r\nDBSIZE\r\n") {
            redis::Value::Int(2)
        } else if packed.contains("\r\nSCAN\r\n") {
            redis::Value::Array(vec![
                redis::Value::BulkString(b"0".to_vec()),
                redis::Value::Array(vec![
                    redis::Value::BulkString(b"alpha".to_vec()),
                    redis::Value::BulkString(b"beta".to_vec()),
                ]),
            ])
        } else if packed.contains("\r\nGET\r\n") {
            redis::Value::BulkString(b"hello".to_vec())
        } else if packed.contains("\r\nTYPE\r\n") {
            redis::Value::SimpleString("string".into())
        } else if packed.contains("\r\nTTL\r\n") {
            redis::Value::Int(-1)
        } else if packed.contains("\r\nSET\r\n") {
            redis::Value::Okay
        } else if packed.contains("\r\nDEL\r\n") {
            redis::Value::Int(1)
        } else if packed.contains("\r\nECHO\r\n") {
            redis::Value::BulkString(b"hello".to_vec())
        } else {
            redis::Value::Nil
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
