#![cfg(unix)]

use std::{collections::BTreeMap, io, net::TcpListener};

use redis_mcp::{
    AccessMode, CapabilityStatus, DirectRedis, DirectRedisCluster, RawCommandPolicy,
    RedisDeployment, RedisExecutor, RedisMcp,
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
        let remote_hash = format!("{remote_key}:hash");
        let remote_set = format!("{remote_key}:set");

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

        let cluster_executor = DirectRedisCluster::connect(&seed_urls)
            .await
            .expect("connect cluster-aware adapter");
        let capabilities = cluster_executor
            .discover_capabilities()
            .await
            .expect("discover cluster capabilities");
        assert!(capabilities.redis_version().is_some());
        assert_eq!(capabilities.deployment(), RedisDeployment::Cluster);
        assert_eq!(capabilities.command("GET"), CapabilityStatus::Available);
        let routed = router_client(cluster_executor).await;
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

        let hash_set = routed
            .call_tool(
                "redis_hset",
                serde_json::json!({
                    "key": remote_hash,
                    "fields": {"name": "Ada", "visits": "1"}
                }),
            )
            .await
            .expect("remote-slot HSET")
            .structured_content
            .expect("structured remote-slot HSET");
        assert_eq!(hash_set["fields_added"], 2);
        let hash_values = routed
            .call_tool(
                "redis_hmget",
                serde_json::json!({"key": remote_hash, "fields": ["visits", "name", "missing"]}),
            )
            .await
            .expect("remote-slot HMGET")
            .structured_content
            .expect("structured remote-slot HMGET");
        assert_eq!(hash_values["values"][0]["value"], "1");
        assert_eq!(hash_values["values"][1]["value"], "Ada");
        assert_eq!(hash_values["values"][2]["exists"], false);
        let hash_incremented = routed
            .call_tool(
                "redis_hincrby",
                serde_json::json!({"key": remote_hash, "field": "visits", "increment": 2}),
            )
            .await
            .expect("remote-slot HINCRBY")
            .structured_content
            .expect("structured remote-slot HINCRBY");
        assert_eq!(hash_incremented["value"], 3);
        let hash_deleted = routed
            .call_tool(
                "redis_hdel",
                serde_json::json!({"key": remote_hash, "fields": ["name", "visits"]}),
            )
            .await
            .expect("remote-slot HDEL")
            .structured_content
            .expect("structured remote-slot HDEL");
        assert_eq!(hash_deleted["deleted"], 2);

        routed
            .call_tool(
                "redis_sadd",
                serde_json::json!({"key": remote_set, "members": ["alpha", "beta"]}),
            )
            .await
            .expect("remote-slot SADD");
        let remote_membership = routed
            .call_tool(
                "redis_smismember",
                serde_json::json!({"key": remote_set, "members": ["beta", "missing"]}),
            )
            .await
            .expect("remote-slot SMISMEMBER")
            .structured_content
            .expect("structured remote-slot SMISMEMBER");
        assert_eq!(remote_membership["members"][0]["is_member"], true);
        assert_eq!(remote_membership["members"][1]["is_member"], false);
        let remote_removed = routed
            .call_tool(
                "redis_srem",
                serde_json::json!({"key": remote_set, "members": ["beta"]}),
            )
            .await
            .expect("remote-slot SREM")
            .structured_content
            .expect("structured remote-slot SREM");
        assert_eq!(remote_removed["removed"], 1);

        let same_source = format!(
            "redis-mcp:test:{}:{{issue18-{protocol}}}:source",
            std::process::id()
        );
        let same_copy = format!(
            "redis-mcp:test:{}:{{issue18-{protocol}}}:copy",
            std::process::id()
        );
        let same_renamed = format!(
            "redis-mcp:test:{}:{{issue18-{protocol}}}:renamed",
            std::process::id()
        );
        let same_list_source = format!(
            "redis-mcp:test:{}:{{issue20-{protocol}}}:list-source",
            std::process::id()
        );
        let same_list_destination = format!(
            "redis-mcp:test:{}:{{issue20-{protocol}}}:list-destination",
            std::process::id()
        );
        let cross_list_source = format!("{}:list-source", keys[0]);
        let cross_list_destination = format!("{}:list-destination", keys[1]);
        let same_set_left = format!(
            "redis-mcp:test:{}:{{issue21-{protocol}}}:set-left",
            std::process::id()
        );
        let same_set_right = format!(
            "redis-mcp:test:{}:{{issue21-{protocol}}}:set-right",
            std::process::id()
        );
        let cross_set_left = format!("{}:set-left", keys[0]);
        let cross_set_right = format!("{}:set-right", keys[1]);
        routed
            .call_tool(
                "redis_set",
                serde_json::json!({"key": same_source, "value": "same-slot"}),
            )
            .await
            .expect("same-slot SET");
        let copied = routed
            .call_tool(
                "redis_copy",
                serde_json::json!({"source": same_source, "destination": same_copy}),
            )
            .await
            .expect("same-slot COPY")
            .structured_content
            .expect("structured same-slot COPY");
        assert_eq!(copied["copied"], true);
        let renamed = routed
            .call_tool(
                "redis_rename",
                serde_json::json!({"source": same_copy, "destination": same_renamed}),
            )
            .await
            .expect("same-slot RENAME")
            .structured_content
            .expect("structured same-slot RENAME");
        assert_eq!(renamed["renamed"], true);

        routed
            .call_tool(
                "redis_rpush",
                serde_json::json!({"key": same_list_source, "elements": ["same-slot"]}),
            )
            .await
            .expect("same-slot RPUSH");
        let list_moved = routed
            .call_tool(
                "redis_lmove",
                serde_json::json!({
                    "source": same_list_source,
                    "destination": same_list_destination,
                    "from": "left",
                    "to": "right"
                }),
            )
            .await
            .expect("same-slot LMOVE")
            .structured_content
            .expect("structured same-slot LMOVE");
        assert_eq!(list_moved["moved"], true);
        assert_eq!(list_moved["value"], "same-slot");

        routed
            .call_tool(
                "redis_rpush",
                serde_json::json!({"key": cross_list_source, "elements": ["cross-slot"]}),
            )
            .await
            .expect("cross-slot source RPUSH");
        let cross_list_move = routed
            .call_tool(
                "redis_lmove",
                serde_json::json!({
                    "source": cross_list_source,
                    "destination": cross_list_destination,
                    "from": "left",
                    "to": "right"
                }),
            )
            .await
            .expect("cross-slot LMOVE is represented as a tool result");
        assert!(cross_list_move.is_error);
        let cross_list_move =
            serde_json::to_string(&cross_list_move).expect("serialize LMOVE CROSSSLOT");
        assert!(cross_list_move.contains("CROSSSLOT"), "{cross_list_move}");

        routed
            .call_tool(
                "redis_sadd",
                serde_json::json!({"key": same_set_left, "members": ["alpha", "beta"]}),
            )
            .await
            .expect("same-slot left SADD");
        routed
            .call_tool(
                "redis_sadd",
                serde_json::json!({"key": same_set_right, "members": ["beta", "gamma"]}),
            )
            .await
            .expect("same-slot right SADD");
        for (tool, expected_count) in [("redis_sdiff", 1), ("redis_sinter", 1), ("redis_sunion", 3)]
        {
            let result = routed
                .call_tool(
                    tool,
                    serde_json::json!({"keys": [same_set_left, same_set_right]}),
                )
                .await
                .unwrap_or_else(|error| panic!("same-slot {tool}: {error}"))
                .structured_content
                .unwrap_or_else(|| panic!("same-slot {tool}: structured result"));
            assert_eq!(result["count"], expected_count, "{tool}");
        }

        routed
            .call_tool(
                "redis_sadd",
                serde_json::json!({"key": cross_set_left, "members": ["alpha"]}),
            )
            .await
            .expect("cross-slot left SADD");
        routed
            .call_tool(
                "redis_sadd",
                serde_json::json!({"key": cross_set_right, "members": ["beta"]}),
            )
            .await
            .expect("cross-slot right SADD");
        for tool in ["redis_sdiff", "redis_sinter", "redis_sunion"] {
            let cross_slot = routed
                .call_tool(
                    tool,
                    serde_json::json!({"keys": [cross_set_left, cross_set_right]}),
                )
                .await
                .unwrap_or_else(|error| panic!("cross-slot {tool}: {error}"));
            assert!(cross_slot.is_error, "{tool}: {cross_slot:?}");
            let cross_slot = serde_json::to_string(&cross_slot)
                .unwrap_or_else(|error| panic!("serialize {tool} CROSSSLOT: {error}"));
            assert!(cross_slot.contains("CROSSSLOT"), "{tool}: {cross_slot}");
        }

        for tool in ["redis_copy", "redis_rename", "redis_renamenx"] {
            let cross_slot = routed
                .call_tool(
                    tool,
                    serde_json::json!({"source": &keys[0], "destination": &keys[1]}),
                )
                .await
                .unwrap_or_else(|error| panic!("{tool}: {error}"));
            assert!(cross_slot.is_error, "{tool}: {cross_slot:?}");
            let cross_slot = serde_json::to_string(&cross_slot)
                .unwrap_or_else(|error| panic!("serialize {tool} CROSSSLOT: {error}"));
            assert!(cross_slot.contains("CROSSSLOT"), "{tool}: {cross_slot}");
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
        let same_slot_deleted = routed
            .call_tool(
                "redis_del",
                serde_json::json!({
                    "keys": [
                        same_source,
                        same_renamed,
                        same_list_source,
                        same_list_destination,
                        cross_list_source,
                        cross_list_destination,
                        remote_set,
                        same_set_left,
                        same_set_right,
                        cross_set_left,
                        cross_set_right
                    ]
                }),
            )
            .await
            .expect("delete same-slot keys")
            .structured_content
            .expect("structured same-slot DEL");
        assert_eq!(same_slot_deleted["deleted"], 9);
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
                "expiration": {"type": "seconds", "value": 60}
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
