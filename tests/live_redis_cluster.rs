#![cfg(unix)]

use std::{collections::BTreeMap, io, net::TcpListener};

use redis_mcp::{
    AccessMode, DirectRedis, DirectRedisCluster, RawCommandPolicy, RedisExecutor, RedisMcp,
};
use redis_server_wrapper::{Error as RedisServerError, RedisCluster, RedisClusterHandle};
use tower_mcp::client::{ChannelTransport, McpClient, StdioClientTransport};

struct TestCluster {
    seed_urls: Vec<String>,
    _managed: Option<ManagedCluster>,
}

impl TestCluster {
    async fn start() -> Option<Self> {
        if let Ok(seed_urls) = std::env::var("REDIS_CLUSTER_URLS") {
            let seed_urls = parse_seed_urls(&seed_urls);
            assert!(
                !seed_urls.is_empty(),
                "REDIS_CLUSTER_URLS must contain at least one URL"
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
                    "skipping live Redis Cluster test: REDIS_CLUSTER_URLS is not set and {binary} is not on PATH"
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
        let directory = tempfile::tempdir().expect("create Redis Cluster test directory");
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

fn parse_seed_urls(seed_urls: &str) -> Vec<String> {
    seed_urls
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_string)
        .collect()
}

// redis-server-wrapper 0.5 can allocate standalone ports, but cluster fixtures
// still require a caller-selected client range plus the derived bus range.
// Keep the unavoidable check/start race isolated here until upstream issue
// joshrotenberg/redis-server-wrapper#166 provides range-aware auto allocation.
fn available_cluster_base_port(nodes: u16) -> io::Result<u16> {
    for _ in 0..256 {
        let first = TcpListener::bind(("127.0.0.1", 0))?;
        let base = first.local_addr()?.port();
        let Some(highest_client) = base.checked_add(nodes - 1) else {
            continue;
        };
        let Some(highest_bus) = highest_client.checked_add(10_000) else {
            continue;
        };

        let mut reservations = vec![first];
        let client_ports = (base + 1)..=highest_client;
        let bus_ports = (base + 10_000)..=highest_bus;
        let mut complete = true;
        for port in client_ports.chain(bus_ports) {
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

fn with_protocol(url: &str, protocol: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}protocol={protocol}")
}

async fn router_client(executor: impl RedisExecutor) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .raw_command_policy(RawCommandPolicy::Classified)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect cluster MCP client");
    client
        .initialize("redis-mcp-cluster-test", "0")
        .await
        .expect("initialize cluster MCP client");
    client
}

#[derive(Debug)]
struct SlotOwner {
    is_seed: bool,
    ranges: Vec<(u16, u16)>,
}

fn parse_slot_owners(cluster_nodes: &str) -> Vec<SlotOwner> {
    cluster_nodes
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 9
                || !fields[2].split(',').any(|flag| flag == "master")
                || fields[2]
                    .split(',')
                    .any(|flag| matches!(flag, "fail" | "fail?" | "handshake"))
            {
                return None;
            }
            let ranges = fields[8..]
                .iter()
                .filter(|slot| !slot.starts_with('['))
                .filter_map(|slot| {
                    let (start, end) = slot.split_once('-').unwrap_or((slot, slot));
                    Some((start.parse().ok()?, end.parse().ok()?))
                })
                .collect::<Vec<_>>();
            (!ranges.is_empty()).then(|| SlotOwner {
                is_seed: fields[2].split(',').any(|flag| flag == "myself"),
                ranges,
            })
        })
        .collect()
}

fn owner_for_slot(owners: &[SlotOwner], slot: u16) -> Option<usize> {
    owners.iter().position(|owner| {
        owner
            .ranges
            .iter()
            .any(|(start, end)| (*start..=*end).contains(&slot))
    })
}

async fn keys_on_three_masters(seed_url: &str, protocol: &str) -> (Vec<String>, String) {
    let client = redis::Client::open(seed_url).expect("open cluster seed client");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect directly to cluster seed");
    let cluster_nodes: String = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(&mut connection)
        .await
        .expect("read Redis Cluster topology");
    let owners = parse_slot_owners(&cluster_nodes);
    assert!(
        owners.len() >= 3,
        "cluster test requires at least three healthy masters: {cluster_nodes}"
    );
    let seed_owner = owners
        .iter()
        .position(|owner| owner.is_seed)
        .expect("seed node appears in CLUSTER NODES");

    let mut keys_by_owner = BTreeMap::new();
    for candidate in 0..10_000_u32 {
        let key = format!(
            "redis-mcp:cluster:{}:{protocol}:{{candidate-{candidate}}}",
            std::process::id()
        );
        let slot: u16 = redis::cmd("CLUSTER")
            .arg("KEYSLOT")
            .arg(&key)
            .query_async(&mut connection)
            .await
            .expect("calculate Redis Cluster key slot");
        if let Some(owner) = owner_for_slot(&owners, slot) {
            keys_by_owner.entry(owner).or_insert(key);
        }
        if keys_by_owner.len() == owners.len() {
            break;
        }
    }
    assert_eq!(
        keys_by_owner.len(),
        owners.len(),
        "find one test key for every cluster master"
    );
    let remote_key = keys_by_owner
        .iter()
        .find(|(owner, _)| **owner != seed_owner)
        .map(|(_, key)| key.clone())
        .expect("find a key not owned by the seed node");
    (keys_by_owner.into_values().collect(), remote_key)
}

#[tokio::test]
async fn cluster_routes_curated_and_raw_tools_across_three_masters() {
    let Some(cluster) = TestCluster::start().await else {
        return;
    };

    for protocol in ["resp2", "resp3"] {
        let seed_urls = cluster
            .seed_urls
            .iter()
            .map(|url| with_protocol(url, protocol))
            .collect::<Vec<_>>();
        let (keys, remote_key) = keys_on_three_masters(&seed_urls[0], protocol).await;

        let direct = router_client(
            DirectRedis::connect(&seed_urls[0])
                .await
                .expect("connect standalone adapter to cluster seed"),
        )
        .await;
        let moved = direct
            .call_tool(
                "redis_set",
                serde_json::json!({"key": remote_key, "value": "single-node"}),
            )
            .await
            .expect("MOVED is represented as a tool result");
        assert!(
            moved.is_error,
            "single-node adapter unexpectedly routed MOVED"
        );
        let moved = serde_json::to_string(&moved).expect("serialize MOVED result");
        assert!(moved.contains("MOVED"), "{moved}");

        let routed = router_client(
            DirectRedisCluster::connect(&seed_urls)
                .await
                .expect("connect cluster-aware adapter"),
        )
        .await;
        let entries = keys
            .iter()
            .enumerate()
            .map(|(index, key)| serde_json::json!({"key": key, "value": format!("value-{index}")}))
            .collect::<Vec<_>>();
        let mset = routed
            .call_tool("redis_mset", serde_json::json!({"entries": entries}))
            .await
            .expect("multi-slot MSET")
            .structured_content
            .expect("structured MSET result");
        assert_eq!(mset["stored"], keys.len());

        let mget = routed
            .call_tool("redis_mget", serde_json::json!({"keys": keys}))
            .await
            .expect("multi-slot MGET")
            .structured_content
            .expect("structured MGET result");
        for (index, value) in mget["values"]
            .as_array()
            .expect("MGET values array")
            .iter()
            .enumerate()
        {
            assert_eq!(value["value"], format!("value-{index}"));
        }

        let cross_slot = routed
            .call_tool(
                "redis_command",
                serde_json::json!({"command": "RENAME", "arguments": [&keys[0], &keys[1]]}),
            )
            .await
            .expect("CROSSSLOT is represented as a tool result");
        assert!(cross_slot.is_error);
        let cross_slot =
            serde_json::to_string(&cross_slot).expect("serialize CROSSSLOT tool result");
        assert!(cross_slot.contains("[InvalidRequest]"), "{cross_slot}");
        assert!(cross_slot.contains("CROSSSLOT"), "{cross_slot}");

        let deleted = routed
            .call_tool("redis_del", serde_json::json!({"keys": keys}))
            .await
            .expect("multi-slot DEL")
            .structured_content
            .expect("structured DEL result");
        assert_eq!(deleted["deleted"], keys.len());
    }

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
        .expect("connect cluster stdio client");
    client
        .initialize("redis-mcp-cluster-stdio-test", "0")
        .await
        .expect("initialize cluster stdio client");
    let (_, remote_key) = keys_on_three_masters(&cluster.seed_urls[0], "stdio").await;
    let set = client
        .call_tool(
            "redis_set",
            serde_json::json!({
                "key": remote_key,
                "value": "over-cluster-stdio",
                "expires_in_seconds": 60
            }),
        )
        .await
        .expect("set remote key over cluster stdio");
    assert!(!set.is_error, "{set:?}");
    let get = client
        .call_tool("redis_get", serde_json::json!({"key": remote_key}))
        .await
        .expect("get remote key over cluster stdio");
    assert_eq!(
        get.structured_content.as_ref().unwrap()["value"],
        "over-cluster-stdio"
    );
}
