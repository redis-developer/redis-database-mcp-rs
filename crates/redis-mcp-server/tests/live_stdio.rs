use redis_mcp::{AccessMode, tool_names};
use tower_mcp::client::{McpClient, StdioClientTransport};

#[cfg(unix)]
use redis_server_wrapper::{Error as RedisServerError, RedisServer, RedisServerHandle};

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
                        "skipping live stdio test: REDIS_URL is not set and {binary} is not on PATH"
                    );
                    None
                }
                Err(error) => panic!("start wrapper-managed Redis: {error}"),
            }
        }

        #[cfg(not(unix))]
        {
            eprintln!(
                "skipping live stdio test: REDIS_URL is not set and self-hosting requires Unix"
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
    format!("redis-mcp-server:test:{}:{suffix}", std::process::id())
}

#[tokio::test]
async fn standalone_round_trip_through_stdio_server() {
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
        tool_names(AccessMode::ReadWrite, false).len() + 6
    );
    for required in [
        "redis_get",
        "redis_set",
        "redis_hget",
        "redis_hset",
        "redis_subscribe",
    ] {
        assert!(listed.tools.iter().any(|tool| tool.name == required));
    }
    assert!(!listed.tools.iter().any(|tool| tool.name == "redis_del"));

    let channel = test_key("pubsub");
    let session_id = client
        .call_tool(
            "redis_subscribe",
            serde_json::json!({"subscriptions": [{"value": channel}]}),
        )
        .await
        .expect("subscribe over stdio")
        .structured_content
        .expect("structured stdio subscription")["session_id"]
        .as_str()
        .expect("stdio session id")
        .to_string();
    client
        .call_tool(
            "redis_publish",
            serde_json::json!({
                "channel": {"value": channel},
                "message": {"value": "over-stdio"}
            }),
        )
        .await
        .expect("publish over stdio");
    let message = client
        .call_tool(
            "redis_pubsub_read",
            serde_json::json!({"session_id": session_id, "wait_ms": 1000}),
        )
        .await
        .expect("read Pub/Sub over stdio")
        .structured_content
        .expect("structured stdio Pub/Sub read");
    assert_eq!(message["messages"][0]["payload"]["value"], "over-stdio");
    client
        .call_tool(
            "redis_pubsub_close",
            serde_json::json!({"session_id": session_id}),
        )
        .await
        .expect("close Pub/Sub session over stdio");

    let key = test_key("string");
    let hash_key = test_key("hash");
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
        .expect("get key over stdio")
        .structured_content
        .expect("structured GET");
    assert_eq!(get["value"], "over-stdio");

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
        .expect("get hash over stdio")
        .structured_content
        .expect("structured HGET");
    assert_eq!(hget["value"], "Ada");

    let redis_client = redis::Client::open(url).expect("open Redis for cleanup");
    let mut connection = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("connect Redis for cleanup");
    redis::cmd("DEL")
        .arg(key)
        .arg(hash_key)
        .query_async::<()>(&mut connection)
        .await
        .expect("clean up stdio test keys");
}
