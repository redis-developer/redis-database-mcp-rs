#![cfg(unix)]

use std::{io, net::TcpListener};

use redis_server_wrapper::{Error as RedisServerError, RedisCluster, RedisClusterHandle};
use tower_mcp::client::{McpClient, StdioClientTransport};

struct TestCluster {
    seed_urls: Vec<String>,
    _managed: Option<ManagedCluster>,
}

impl TestCluster {
    async fn start() -> Option<Self> {
        if let Ok(seed_urls) = std::env::var("REDIS_CLUSTER_URLS") {
            let seed_urls = seed_urls
                .split(',')
                .map(str::trim)
                .filter(|url| !url.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            assert!(
                !seed_urls.is_empty(),
                "REDIS_CLUSTER_URLS must not be empty"
            );
            return Some(Self {
                seed_urls,
                _managed: None,
            });
        }

        match ManagedCluster::start().await {
            Ok(managed) => Some(Self {
                seed_urls: managed.seed_urls(),
                _managed: Some(managed),
            }),
            Err(RedisServerError::BinaryNotFound { binary, .. }) => {
                eprintln!(
                    "skipping live Cluster stdio test: REDIS_CLUSTER_URLS is not set and {binary} is not on PATH"
                );
                None
            }
            Err(error) => panic!("start wrapper-managed Redis Cluster: {error}"),
        }
    }
}

struct ManagedCluster {
    cluster: RedisClusterHandle,
    _directory: tempfile::TempDir,
}

impl ManagedCluster {
    async fn start() -> Result<Self, RedisServerError> {
        let directory = tempfile::tempdir().expect("create Redis Cluster directory");
        let base_port = available_cluster_base_port(3)
            .expect("find available Redis Cluster client and bus port ranges");
        let cluster = RedisCluster::builder()
            .masters(3)
            .replicas_per_master(0)
            .base_port(base_port)
            .bind("127.0.0.1")
            .dir(directory.path())
            .start()
            .await?;
        Ok(Self {
            cluster,
            _directory: directory,
        })
    }

    fn seed_urls(&self) -> Vec<String> {
        self.cluster
            .node_addrs()
            .into_iter()
            .map(|address| format!("redis://{address}/"))
            .collect()
    }
}

fn available_cluster_base_port(nodes: u16) -> io::Result<u16> {
    const FIRST_CANDIDATE: u32 = 20_000;
    const LAST_CANDIDATE: u32 = 50_000;
    let node_span = u32::from(nodes);
    let candidate_count = (LAST_CANDIDATE - FIRST_CANDIDATE) / node_span;
    let start = std::process::id() % candidate_count;

    for index in 0..candidate_count {
        let candidate = FIRST_CANDIDATE + ((start + index) % candidate_count) * node_span;
        let base = u16::try_from(candidate).expect("cluster port candidate fits u16");
        let Ok(first) = TcpListener::bind(("127.0.0.1", base)) else {
            continue;
        };
        let Some(highest_client) = base.checked_add(nodes - 1) else {
            continue;
        };
        let Some(highest_bus) = highest_client.checked_add(10_000) else {
            continue;
        };

        let mut reservations = vec![first];
        let mut complete = true;
        for port in ((base + 1)..=highest_client).chain((base + 10_000)..=highest_bus) {
            match TcpListener::bind(("127.0.0.1", port)) {
                Ok(listener) => reservations.push(listener),
                Err(_) => {
                    complete = false;
                    break;
                }
            }
        }
        if complete {
            return Ok(base);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AddrNotAvailable,
        "could not reserve Redis Cluster client and bus port ranges",
    ))
}

#[tokio::test]
async fn cluster_round_trip_through_stdio_server() {
    let Some(cluster) = TestCluster::start().await else {
        return;
    };

    let binary = env!("CARGO_BIN_EXE_redis-mcp-server");
    let mut arguments = vec![
        "--access".to_string(),
        "read-write".to_string(),
        "--stdio".to_string(),
    ];
    for seed_url in &cluster.seed_urls {
        arguments.push("--cluster-url".to_string());
        arguments.push(seed_url.clone());
    }
    let argument_refs = arguments.iter().map(String::as_str).collect::<Vec<_>>();
    let transport = StdioClientTransport::spawn(binary, &argument_refs)
        .await
        .expect("spawn cluster-aware redis-mcp-server");
    let client = McpClient::connect(transport)
        .await
        .expect("connect Cluster stdio client");
    client
        .initialize("redis-mcp-cluster-stdio-test", "0")
        .await
        .expect("initialize Cluster stdio client");

    let channel = format!("redis-mcp-server:cluster:{}:pubsub", std::process::id());
    let session_id = client
        .call_tool(
            "redis_subscribe",
            serde_json::json!({"subscriptions": [{"value": channel}]}),
        )
        .await
        .expect("subscribe over Cluster stdio")
        .structured_content
        .expect("structured Cluster subscription")["session_id"]
        .as_str()
        .expect("Cluster session id")
        .to_string();
    client
        .call_tool(
            "redis_publish",
            serde_json::json!({
                "channel": {"value": channel},
                "message": {"value": "cluster-stdio"}
            }),
        )
        .await
        .expect("publish over Cluster stdio");
    let messages = client
        .call_tool(
            "redis_pubsub_read",
            serde_json::json!({"session_id": session_id, "wait_ms": 1000}),
        )
        .await
        .expect("read Pub/Sub over Cluster stdio")
        .structured_content
        .expect("structured Cluster Pub/Sub read");
    assert_eq!(messages["messages"][0]["payload"]["value"], "cluster-stdio");
    client
        .call_tool(
            "redis_pubsub_close",
            serde_json::json!({"session_id": session_id}),
        )
        .await
        .expect("close Cluster session");

    let key = format!("redis-mcp-server:{{stdio}}:{}", std::process::id());
    let set = client
        .call_tool(
            "redis_set",
            serde_json::json!({
                "key": key,
                "value": "over-cluster-stdio",
                "expiration": {"type": "seconds", "value": 60}
            }),
        )
        .await
        .expect("set Cluster key over stdio");
    assert!(!set.is_error, "{set:?}");
    let get = client
        .call_tool("redis_get", serde_json::json!({"key": key}))
        .await
        .expect("get Cluster key over stdio")
        .structured_content
        .expect("structured Cluster GET");
    assert_eq!(get["value"], "over-cluster-stdio");
}
