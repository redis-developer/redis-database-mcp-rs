#![cfg(unix)]

use std::{collections::BTreeMap, io, net::TcpListener, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use redis_mcp::{
    AccessMode, CapabilityStatus, DirectRedis, DirectRedisCluster, DirectRedisPubSubSessionManager,
    PubSubSessionLimits, PubSubSessionManager, RawCommandPolicy, RedisDeployment, RedisExecutor,
    RedisMcp, RedisModule, RedisVersion, ToolBundle,
};
use redis_server_wrapper::{Error as RedisServerError, RedisCluster, RedisClusterHandle};
use tower_mcp::client::{ChannelTransport, McpClient};

static CLUSTER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct TestCluster {
    seed_urls: Vec<String>,
    _managed: Option<ManagedCluster>,
}

#[tokio::test]
async fn set_and_sorted_set_store_tools_route_only_same_slot_keys_in_cluster() {
    let _cluster_guard = CLUSTER_TEST_LOCK.lock().await;
    let Some(cluster) = TestCluster::start().await else {
        return;
    };
    let discovery = DirectRedisCluster::connect(&cluster.seed_urls)
        .await
        .expect("connect cluster algebra capability discovery adapter");
    let version = discovery
        .discover_capabilities()
        .await
        .expect("discover cluster algebra capabilities")
        .redis_version()
        .expect("Redis Cluster reports its version");

    for protocol in ["resp2", "resp3"] {
        let urls = cluster
            .seed_urls
            .iter()
            .map(|url| with_protocol(url, protocol))
            .collect::<Vec<_>>();
        let client = router_client(
            DirectRedisCluster::connect(&urls)
                .await
                .expect("connect cluster algebra protocol adapter"),
        )
        .await;
        let same = format!(
            "redis-mcp:algebra:{{same-{}}}:{protocol}",
            std::process::id()
        );
        let other = format!(
            "redis-mcp:algebra:{{other-{}}}:{protocol}",
            std::process::id()
        );
        let set_left = format!("{same}:set-left");
        let set_right = format!("{same}:set-right");
        let cross_set = format!("{other}:set");
        let zset_left = format!("{same}:zset-left");
        let zset_right = format!("{same}:zset-right");
        let cross_zset = format!("{other}:zset");

        for (key, members) in [
            (&set_left, serde_json::json!(["alpha", "beta"])),
            (&set_right, serde_json::json!(["beta", "gamma"])),
            (&cross_set, serde_json::json!(["delta"])),
        ] {
            client
                .call_tool(
                    "redis_sadd",
                    serde_json::json!({"key": key, "members": members}),
                )
                .await
                .expect("seed cluster algebra set");
        }
        for (key, members) in [
            (
                &zset_left,
                serde_json::json!([
                    {"score": 1, "member": "alpha"},
                    {"score": 2, "member": "beta"}
                ]),
            ),
            (
                &zset_right,
                serde_json::json!([
                    {"score": 3, "member": "beta"},
                    {"score": 4, "member": "gamma"}
                ]),
            ),
            (
                &cross_zset,
                serde_json::json!([{"score": 5, "member": "delta"}]),
            ),
        ] {
            client
                .call_tool(
                    "redis_zadd",
                    serde_json::json!({"key": key, "members": members}),
                )
                .await
                .expect("seed cluster algebra sorted set");
        }

        if version >= RedisVersion::new(8, 10, 0) {
            for (tool, expected) in [("redis_sdiffcard", 1), ("redis_sunioncard", 3)] {
                let result = client
                    .call_tool(
                        tool,
                        serde_json::json!({"keys": [set_left, set_right], "limit": 10}),
                    )
                    .await
                    .unwrap_or_else(|error| panic!("same-slot {tool}: {error}"))
                    .structured_content
                    .unwrap_or_else(|| panic!("same-slot {tool}: structured result"));
                assert_eq!(result["cardinality"], expected, "{tool}");
            }
        }
        if version >= RedisVersion::new(7, 0, 0) {
            let result = client
                .call_tool(
                    "redis_zintercard",
                    serde_json::json!({"keys": [zset_left, zset_right], "limit": 10}),
                )
                .await
                .expect("same-slot ZINTERCARD")
                .structured_content
                .expect("structured same-slot ZINTERCARD");
            assert_eq!(result["cardinality"], 1);
        }

        for (tool, suffix, expected) in [
            ("redis_sdiffstore", "set-difference", 1),
            ("redis_sinterstore", "set-intersection", 1),
            ("redis_sunionstore", "set-union", 3),
        ] {
            let destination = format!("{same}:{suffix}");
            let result = client
                .call_tool(
                    tool,
                    serde_json::json!({
                        "destination": destination,
                        "keys": [set_left, set_right]
                    }),
                )
                .await
                .unwrap_or_else(|error| panic!("same-slot {tool}: {error}"))
                .structured_content
                .unwrap_or_else(|| panic!("same-slot {tool}: structured result"));
            assert_eq!(result["destination_cardinality"], expected, "{tool}");
        }

        let zdiff = client
            .call_tool(
                "redis_zdiffstore",
                serde_json::json!({
                    "destination": format!("{same}:zset-difference"),
                    "keys": [zset_left, zset_right]
                }),
            )
            .await
            .expect("same-slot ZDIFFSTORE")
            .structured_content
            .expect("structured same-slot ZDIFFSTORE");
        assert_eq!(zdiff["destination_cardinality"], 1);

        for (tool, suffix, expected) in [
            ("redis_zinterstore", "zset-intersection", 1),
            ("redis_zunionstore", "zset-union", 3),
        ] {
            let result = client
                .call_tool(
                    tool,
                    serde_json::json!({
                        "destination": format!("{same}:{suffix}"),
                        "sources": [
                            {"key": zset_left, "weight": 2},
                            {"key": zset_right, "weight": 1}
                        ],
                        "aggregate": "max"
                    }),
                )
                .await
                .unwrap_or_else(|error| panic!("same-slot {tool}: {error}"))
                .structured_content
                .unwrap_or_else(|| panic!("same-slot {tool}: structured result"));
            assert_eq!(result["destination_cardinality"], expected, "{tool}");
        }
        let range = client
            .call_tool(
                "redis_zrangestore",
                serde_json::json!({
                    "destination": format!("{same}:zset-range"),
                    "source": zset_left,
                    "range": {"kind": "rank", "start": 0, "stop": 1}
                }),
            )
            .await
            .expect("same-slot ZRANGESTORE")
            .structured_content
            .expect("structured same-slot ZRANGESTORE");
        assert_eq!(range["destination_cardinality"], 2);

        let mut cross_slot_calls = vec![
            (
                "redis_sdiffstore",
                serde_json::json!({
                    "destination": format!("{same}:cross-set-difference"),
                    "keys": [set_left, cross_set]
                }),
            ),
            (
                "redis_sinterstore",
                serde_json::json!({
                    "destination": format!("{same}:cross-set-intersection"),
                    "keys": [set_left, cross_set]
                }),
            ),
            (
                "redis_sunionstore",
                serde_json::json!({
                    "destination": format!("{same}:cross-set-union"),
                    "keys": [set_left, cross_set]
                }),
            ),
            (
                "redis_zdiffstore",
                serde_json::json!({
                    "destination": format!("{same}:cross-zset-difference"),
                    "keys": [zset_left, cross_zset]
                }),
            ),
            (
                "redis_zinterstore",
                serde_json::json!({
                    "destination": format!("{same}:cross-zset-intersection"),
                    "sources": [zset_left, cross_zset]
                }),
            ),
            (
                "redis_zunionstore",
                serde_json::json!({
                    "destination": format!("{same}:cross-zset-union"),
                    "sources": [zset_left, cross_zset]
                }),
            ),
            (
                "redis_zrangestore",
                serde_json::json!({
                    "destination": format!("{same}:cross-zset-range"),
                    "source": cross_zset,
                    "range": {"kind": "rank", "start": 0, "stop": 0}
                }),
            ),
        ];
        if version >= RedisVersion::new(8, 10, 0) {
            cross_slot_calls.extend([
                (
                    "redis_sdiffcard",
                    serde_json::json!({"keys": [set_left, cross_set]}),
                ),
                (
                    "redis_sunioncard",
                    serde_json::json!({"keys": [set_left, cross_set]}),
                ),
            ]);
        }
        if version >= RedisVersion::new(7, 0, 0) {
            cross_slot_calls.push((
                "redis_zintercard",
                serde_json::json!({"keys": [zset_left, cross_zset]}),
            ));
        }
        for (tool, arguments) in cross_slot_calls {
            let result = client
                .call_tool(tool, arguments)
                .await
                .unwrap_or_else(|error| panic!("{tool} cross-slot result: {error}"));
            assert!(result.is_error, "{tool}: {result:?}");
            let result = serde_json::to_string(&result)
                .unwrap_or_else(|error| panic!("serialize {tool} CROSSSLOT: {error}"));
            assert!(result.contains("CROSSSLOT"), "{tool}: {result}");
        }
    }
}

#[tokio::test]
async fn bitmap_geo_and_hll_multi_key_tools_enforce_same_slot_cluster_contracts() {
    let _cluster_guard = CLUSTER_TEST_LOCK.lock().await;
    let Some(cluster) = TestCluster::start().await else {
        return;
    };
    let executor = DirectRedisCluster::connect(&cluster.seed_urls)
        .await
        .expect("connect cluster-aware specialized data adapter");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover specialized data Cluster capabilities");
    assert_eq!(capabilities.deployment(), RedisDeployment::Cluster);
    let client = router_client(executor).await;

    let prefix = format!("redis-mcp:specialized:{{same-{}}}", std::process::id());
    let other = format!("redis-mcp:specialized:{{other-{}}}", std::process::id());
    let bitmap_left = format!("{prefix}:bitmap-left");
    let bitmap_right = format!("{prefix}:bitmap-right");
    let bitmap_result = format!("{prefix}:bitmap-result");
    let geo = format!("{prefix}:geo");
    let geo_store = format!("{prefix}:geo-store");
    let hll_left = format!("{prefix}:hll-left");
    let hll_right = format!("{prefix}:hll-right");
    let hll_merged = format!("{prefix}:hll-merged");
    let cross_bitmap = format!("{other}:bitmap-result");
    let cross_geo = format!("{other}:geo-store");
    let cross_hll = format!("{other}:hll");

    for key in [&bitmap_left, &bitmap_right] {
        client
            .call_tool(
                "redis_setbit",
                serde_json::json!({"key": key, "offset": 1, "value": true}),
            )
            .await
            .expect("same-slot SETBIT");
    }
    let bitop = client
        .call_tool(
            "redis_bitop",
            serde_json::json!({
                "destination": bitmap_result, "operation": "and",
                "sources": [bitmap_left, bitmap_right]
            }),
        )
        .await
        .expect("same-slot BITOP")
        .structured_content
        .expect("structured same-slot BITOP");
    assert_eq!(bitop["result_length_bytes"], 1);
    let routed_bit = client
        .call_tool(
            "redis_getbit",
            serde_json::json!({"key": bitmap_result, "offset": 1}),
        )
        .await
        .expect("route GETBIT by destination")
        .structured_content
        .expect("structured routed GETBIT");
    assert_eq!(routed_bit["set"], true);

    client
        .call_tool(
            "redis_geoadd",
            serde_json::json!({
                "key": geo,
                "members": [
                    {"member": "one", "longitude": 0, "latitude": 0},
                    {"member": "two", "longitude": 0.01, "latitude": 0.01}
                ]
            }),
        )
        .await
        .expect("same-slot GEOADD");
    let geo_stored = client
        .call_tool(
            "redis_geosearchstore",
            serde_json::json!({
                "destination": geo_store, "source": geo,
                "center": {"kind": "member", "member": "one"},
                "shape": {"kind": "radius", "radius": 10, "unit": "kilometers"},
                "count": 10
            }),
        )
        .await
        .expect("same-slot GEOSEARCHSTORE")
        .structured_content
        .expect("structured same-slot GEOSEARCHSTORE");
    assert_eq!(geo_stored["stored"], 2);

    for (key, elements) in [
        (&hll_left, serde_json::json!(["one", "two"])),
        (&hll_right, serde_json::json!(["two", "three"])),
    ] {
        client
            .call_tool(
                "redis_pfadd",
                serde_json::json!({"key": key, "elements": elements}),
            )
            .await
            .expect("same-slot PFADD");
    }
    let estimate = client
        .call_tool(
            "redis_pfcount",
            serde_json::json!({"keys": [hll_left, hll_right]}),
        )
        .await
        .expect("same-slot PFCOUNT")
        .structured_content
        .expect("structured same-slot PFCOUNT");
    assert_eq!(estimate["estimated_cardinality"], "3");
    client
        .call_tool(
            "redis_pfmerge",
            serde_json::json!({"destination": hll_merged, "sources": [hll_left, hll_right]}),
        )
        .await
        .expect("same-slot PFMERGE");

    for (tool, arguments) in [
        (
            "redis_bitop",
            serde_json::json!({
                "destination": cross_bitmap, "operation": "or", "sources": [bitmap_left]
            }),
        ),
        (
            "redis_geosearchstore",
            serde_json::json!({
                "destination": cross_geo, "source": geo,
                "center": {"kind": "member", "member": "one"},
                "shape": {"kind": "radius", "radius": 10, "unit": "kilometers"},
                "count": 10
            }),
        ),
        (
            "redis_pfcount",
            serde_json::json!({"keys": [hll_left, cross_hll]}),
        ),
        (
            "redis_pfmerge",
            serde_json::json!({"destination": cross_hll, "sources": [hll_left]}),
        ),
    ] {
        let result = client
            .call_tool(tool, arguments)
            .await
            .unwrap_or_else(|error| panic!("{tool} cross-slot result: {error}"));
        assert!(result.is_error, "{tool}: {result:?}");
        let result = serde_json::to_string(&result)
            .unwrap_or_else(|error| panic!("serialize {tool} CROSSSLOT: {error}"));
        assert!(result.contains("CROSSSLOT"), "{tool}: {result}");
    }
}

#[tokio::test]
async fn redis_eight_modern_tools_route_and_enforce_atomic_slots_in_cluster() {
    let _cluster_guard = CLUSTER_TEST_LOCK.lock().await;
    let Some(cluster) = TestCluster::start().await else {
        return;
    };
    let discovery = DirectRedisCluster::connect(&cluster.seed_urls)
        .await
        .expect("connect cluster-aware Redis 8 discovery adapter");
    let capabilities = discovery
        .discover_capabilities()
        .await
        .expect("discover Redis 8 Cluster capabilities");
    let version = capabilities
        .redis_version()
        .expect("Redis Cluster reports its version");
    if version < RedisVersion::new(8, 0, 0) {
        return;
    }

    for protocol in ["resp2", "resp3"] {
        let urls = cluster
            .seed_urls
            .iter()
            .map(|url| with_protocol(url, protocol))
            .collect::<Vec<_>>();
        let executor = DirectRedisCluster::connect(&urls)
            .await
            .expect("connect Redis 8 Cluster protocol adapter");
        let client = router_client(executor).await;
        let same = format!(
            "redis-mcp:modern:{{same-{}}}:{protocol}",
            std::process::id()
        );
        let other = format!(
            "redis-mcp:modern:{{other-{}}}:{protocol}",
            std::process::id()
        );
        let vector = format!("{same}:vectors");
        let array = format!("{same}:array");
        let source = format!("{same}:source");
        let destination = format!("{same}:destination");
        let cross_destination = format!("{other}:destination");
        let mset_one = format!("{same}:mset-one");
        let mset_two = format!("{same}:mset-two");
        let cross_mset = format!("{other}:mset");
        let stream = format!("{same}:stream");

        let added = client
            .call_tool(
                "redis_vadd",
                serde_json::json!({
                    "key": vector,
                    "vector": {"type": "values", "values": [1, 0]},
                    "element": {"value": "alpha"}
                }),
            )
            .await
            .expect("Cluster VADD")
            .structured_content
            .expect("structured Cluster VADD");
        assert_eq!(added["changed"], true, "{protocol}");
        let cardinality = client
            .call_tool("redis_vcard", serde_json::json!({"key": vector}))
            .await
            .expect("routed Cluster VCARD")
            .structured_content
            .expect("structured Cluster VCARD");
        assert_eq!(cardinality["cardinality"], 1, "{protocol}");

        if version >= RedisVersion::new(8, 4, 0) {
            let same_slot = client
                .call_tool(
                    "redis_msetex",
                    serde_json::json!({
                        "entries": [
                            {"key": {"value": mset_one}, "value": {"value": "one"}},
                            {"key": {"value": mset_two}, "value": {"value": "two"}}
                        ],
                        "expiration": {"type": "seconds", "value": 60}
                    }),
                )
                .await
                .expect("same-slot Cluster MSETEX")
                .structured_content
                .expect("structured same-slot Cluster MSETEX");
            assert_eq!(same_slot["applied"], true, "{protocol}");

            let cross_slot = client
                .call_tool(
                    "redis_msetex",
                    serde_json::json!({
                        "entries": [
                            {"key": {"value": mset_one}, "value": {"value": "one"}},
                            {"key": {"value": cross_mset}, "value": {"value": "other"}}
                        ]
                    }),
                )
                .await
                .expect("cross-slot MSETEX is represented as a tool result");
            assert!(cross_slot.is_error, "{protocol}: {cross_slot:?}");
            assert!(
                serde_json::to_string(&cross_slot)
                    .expect("serialize MSETEX CROSSSLOT")
                    .contains("CROSSSLOT"),
                "{protocol}: {cross_slot:?}"
            );
        }

        if version >= RedisVersion::new(8, 8, 0) {
            let set = client
                .call_tool(
                    "redis_arset",
                    serde_json::json!({
                        "key": array,
                        "index": 0,
                        "values": [{"value": "one"}, {"value": "two"}]
                    }),
                )
                .await
                .expect("routed Cluster ARSET")
                .structured_content
                .expect("structured Cluster ARSET");
            assert_eq!(set["new_slots"], 2, "{protocol}");
            let scan = client
                .call_tool(
                    "redis_arscan",
                    serde_json::json!({"key": array, "start": 0, "end": 10, "limit": 10}),
                )
                .await
                .expect("routed Cluster ARSCAN")
                .structured_content
                .expect("structured Cluster ARSCAN");
            assert_eq!(scan["count"], 2, "{protocol}");
        }

        client
            .call_tool(
                "redis_xadd",
                serde_json::json!({
                    "key": stream,
                    "id": {"type": "explicit", "id": {"milliseconds": 1, "sequence": 0}},
                    "fields": [{"field": "value", "value": "cluster"}]
                }),
            )
            .await
            .expect("routed Cluster XADD");
        if version >= RedisVersion::new(8, 2, 0) {
            let deleted = client
                .call_tool(
                    "redis_xdelex",
                    serde_json::json!({
                        "key": stream,
                        "reference_policy": "delete_references",
                        "ids": [{"milliseconds": 1, "sequence": 0}]
                    }),
                )
                .await
                .expect("routed Cluster XDELEX")
                .structured_content
                .expect("structured Cluster XDELEX");
            assert_eq!(
                deleted["results"].as_array().unwrap().len(),
                1,
                "{protocol}"
            );
        }

        if version >= RedisVersion::new(8, 10, 0) {
            client
                .call_tool(
                    "redis_rpush",
                    serde_json::json!({
                        "key": source,
                        "elements": [{"value": "one"}, {"value": "two"}]
                    }),
                )
                .await
                .expect("seed same-slot LMOVEM source");
            let moved = client
                .call_tool(
                    "redis_lmovem",
                    serde_json::json!({
                        "source": {"value": source},
                        "destination": {"value": destination},
                        "from": "right",
                        "to": "left",
                        "amount": {"type": "up_to", "count": 2, "ordering": "bulk"}
                    }),
                )
                .await
                .expect("same-slot Cluster LMOVEM")
                .structured_content
                .expect("structured same-slot Cluster LMOVEM");
            assert_eq!(moved["count"], 2, "{protocol}");

            let cross_slot = client
                .call_tool(
                    "redis_lmovem",
                    serde_json::json!({
                        "source": {"value": source},
                        "destination": {"value": cross_destination},
                        "from": "left",
                        "to": "right"
                    }),
                )
                .await
                .expect("cross-slot LMOVEM is represented as a tool result");
            assert!(cross_slot.is_error, "{protocol}: {cross_slot:?}");
            assert!(
                serde_json::to_string(&cross_slot)
                    .expect("serialize LMOVEM CROSSSLOT")
                    .contains("CROSSSLOT"),
                "{protocol}: {cross_slot:?}"
            );
        }

        client
            .call_tool(
                "redis_del",
                serde_json::json!({
                    "keys": [
                        vector, array, source, destination, cross_destination,
                        mset_one, mset_two, cross_mset, stream
                    ]
                }),
            )
            .await
            .expect("clean modern Cluster keys");
    }
}

#[tokio::test]
async fn bounded_sort_routes_and_enforces_cluster_key_contracts() {
    let _cluster_guard = CLUSTER_TEST_LOCK.lock().await;
    let Some(cluster) = TestCluster::start().await else {
        return;
    };
    let discovery = DirectRedisCluster::connect(&cluster.seed_urls)
        .await
        .expect("connect Cluster SORT discovery adapter");
    let capabilities = discovery
        .discover_capabilities()
        .await
        .expect("discover Cluster SORT capabilities");
    let version = capabilities
        .redis_version()
        .expect("Redis Cluster reports its version");
    if version < RedisVersion::new(7, 0, 0) {
        return;
    }

    for protocol in ["resp2", "resp3"] {
        let urls = cluster
            .seed_urls
            .iter()
            .map(|url| with_protocol(url, protocol))
            .collect::<Vec<_>>();
        let executor = DirectRedisCluster::connect(&urls)
            .await
            .expect("connect Cluster SORT protocol adapter");
        let client = router_client(executor).await;
        let source = format!(
            "redis-mcp:sort:{{same-{}}}:{protocol}:source",
            std::process::id()
        );
        let destination = format!(
            "redis-mcp:sort:{{same-{}}}:{protocol}:destination",
            std::process::id()
        );
        let cross_destination = format!(
            "redis-mcp:sort:{{other-{}}}:{protocol}:destination",
            std::process::id()
        );

        client
            .call_tool(
                "redis_lpush",
                serde_json::json!({"key": source, "elements": ["3", "1", "2"]}),
            )
            .await
            .expect("seed Cluster SORT source");
        let sorted = client
            .call_tool(
                "redis_sort",
                serde_json::json!({
                    "key": source,
                    "by": "nosort",
                    "get": ["#"],
                    "count": 3
                }),
            )
            .await
            .expect("same-slot local-pattern SORT_RO")
            .structured_content
            .expect("structured same-slot SORT_RO");
        assert_eq!(sorted["returned"], 3, "{protocol}");

        let stored = client
            .call_tool(
                "redis_sort_store",
                serde_json::json!({
                    "key": source,
                    "destination": destination,
                    "count": 3
                }),
            )
            .await
            .expect("same-slot SORT STORE")
            .structured_content
            .expect("structured same-slot SORT STORE");
        assert_eq!(stored["stored"], 3, "{protocol}");

        let cross_slot = client
            .call_tool(
                "redis_sort_store",
                serde_json::json!({
                    "key": source,
                    "destination": cross_destination,
                    "count": 3
                }),
            )
            .await
            .expect("cross-slot SORT STORE result");
        assert!(cross_slot.is_error, "{protocol}: {cross_slot:?}");
        assert!(
            serde_json::to_string(&cross_slot)
                .expect("serialize SORT STORE CROSSSLOT")
                .contains("CROSSSLOT"),
            "{protocol}: {cross_slot:?}"
        );

        let external = client
            .call_tool(
                "redis_sort",
                serde_json::json!({"key": source, "get": ["object:*->name"], "count": 3}),
            )
            .await
            .expect("Cluster external GET result");
        assert!(external.is_error, "{protocol}: {external:?}");
        assert!(
            serde_json::to_string(&external)
                .expect("serialize Cluster external GET rejection")
                .contains("CLUSTER_SORT_EXTERNAL_KEYS_UNSUPPORTED"),
            "{protocol}: {external:?}"
        );

        client
            .call_tool(
                "redis_del",
                serde_json::json!({"keys": [source, destination]}),
            )
            .await
            .expect("clean Cluster SORT keys");
    }
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

struct TestJsonCluster {
    seed_urls: Vec<String>,
    _managed: Option<ManagedCluster>,
}

impl TestJsonCluster {
    async fn start() -> Option<Self> {
        if let Ok(seed_urls) = std::env::var("REDIS_STACK_CLUSTER_URLS") {
            let seed_urls = parse_seed_urls(&seed_urls);
            assert!(
                !seed_urls.is_empty(),
                "REDIS_STACK_CLUSTER_URLS must contain at least one URL"
            );
            return Some(Self {
                seed_urls,
                _managed: None,
            });
        }

        let server_bin = redis_server_wrapper::stack::detect_server_bin();
        let module_args = redis_server_wrapper::stack::detect_stack_modules(&server_bin);
        let json_module = module_args
            .windows(2)
            .find(|pair| pair[0] == "--loadmodule" && pair[1].ends_with("rejson.so"))
            .map(|pair| pair[1].clone());
        let Some(_json_module) = json_module else {
            eprintln!(
                "skipping live RedisJSON Cluster test: REDIS_STACK_CLUSTER_URLS is not set and no local rejson.so was detected"
            );
            return None;
        };
        let search_module = module_args
            .windows(2)
            .find(|pair| pair[0] == "--loadmodule" && pair[1].ends_with("redisearch.so"))
            .map(|pair| pair[1].clone());
        let Some(_search_module) = search_module else {
            eprintln!(
                "skipping live Redis Stack Cluster test: REDIS_STACK_CLUSTER_URLS is not set and no local redisearch.so was detected"
            );
            return None;
        };

        let directory = tempfile::tempdir().expect("create RedisJSON Cluster test directory");
        let base_port = available_cluster_base_port(3)
            .expect("find available RedisJSON Cluster client and bus port ranges");
        let cluster = match RedisCluster::builder()
            .masters(3)
            .replicas_per_master(0)
            .base_port(base_port)
            .bind("127.0.0.1")
            .dir(directory.path())
            .redis_server_bin(server_bin)
            .start()
            .await
        {
            Ok(cluster) => cluster,
            Err(RedisServerError::BinaryNotFound { binary, .. }) => {
                eprintln!(
                    "skipping live RedisJSON Cluster test: REDIS_STACK_CLUSTER_URLS is not set and {binary} is not on PATH"
                );
                return None;
            }
            Err(error) => panic!("start wrapper-managed RedisJSON Cluster: {error}"),
        };
        cluster
            .require_module_on_all_nodes("ReJSON")
            .await
            .expect("RedisJSON is loaded on every cluster node");
        cluster
            .require_module_on_all_nodes("search")
            .await
            .expect("Redis Query Engine is loaded on every cluster node");
        let managed = ManagedCluster {
            cluster,
            _directory: directory,
        };
        Some(Self {
            seed_urls: managed.seed_urls(),
            _managed: Some(managed),
        })
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

async fn cluster_key_slot(connection: &mut redis::aio::MultiplexedConnection, key: &str) -> u16 {
    redis::cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg(key)
        .query_async(connection)
        .await
        .unwrap_or_else(|error| panic!("calculate Redis Cluster slot for {key}: {error}"))
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

async fn pubsub_session_router_client(
    executor: impl RedisExecutor,
    manager: DirectRedisPubSubSessionManager,
) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::ReadWrite)
        .pubsub_sessions(manager)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect Cluster Pub/Sub session MCP client");
    client
        .initialize("redis-mcp-cluster-pubsub-session-test", "0")
        .await
        .expect("initialize Cluster Pub/Sub session MCP client");
    client
}

async fn json_router_client(executor: impl RedisExecutor) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Json])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect RedisJSON Cluster MCP client");
    client
        .initialize("redis-mcp-json-cluster-test", "0")
        .await
        .expect("initialize RedisJSON Cluster MCP client");
    client
}

async fn search_router_client(executor: impl RedisExecutor) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::DataStructures, ToolBundle::Search])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect Search Cluster MCP client");
    client
        .initialize("redis-mcp-search-cluster-test", "0")
        .await
        .expect("initialize Search Cluster MCP client");
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

#[tokio::test]
async fn pubsub_sessions_deliver_global_and_sharded_messages_in_cluster() {
    let _cluster_guard = CLUSTER_TEST_LOCK.lock().await;
    let Some(cluster) = TestCluster::start().await else {
        return;
    };
    let executor = DirectRedisCluster::connect(&cluster.seed_urls)
        .await
        .expect("connect cluster-aware adapter for Pub/Sub sessions");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover Cluster Pub/Sub capabilities");
    let manager = DirectRedisPubSubSessionManager::cluster(
        &cluster.seed_urls,
        PubSubSessionLimits::default()
            .with_max_read_duration(Duration::from_secs(2))
            .with_operation_timeout(Duration::from_secs(5)),
    )
    .expect("create Cluster Pub/Sub session manager");
    let client = pubsub_session_router_client(executor, manager.clone()).await;

    let global_channel = format!("redis-mcp:cluster-session:{}:global", std::process::id());
    let global_session = client
        .call_tool(
            "redis_subscribe",
            serde_json::json!({"subscriptions": [{"value": global_channel}]}),
        )
        .await
        .expect("open global Cluster Pub/Sub session")
        .structured_content
        .expect("structured global Cluster session")["session_id"]
        .as_str()
        .expect("global Cluster session id")
        .to_string();
    client
        .call_tool(
            "redis_publish",
            serde_json::json!({"channel": {"value": global_channel}, "message": {"value": "global"}}),
        )
        .await
        .expect("publish global Cluster message");
    let global_read = client
        .call_tool(
            "redis_pubsub_read",
            serde_json::json!({"session_id": global_session, "wait_ms": 1000}),
        )
        .await
        .expect("read global Cluster message")
        .structured_content
        .expect("structured global Cluster read");
    assert_eq!(global_read["messages"][0]["payload"]["value"], "global");
    client
        .call_tool(
            "redis_pubsub_close",
            serde_json::json!({"session_id": global_session}),
        )
        .await
        .expect("close global Cluster session");

    if capabilities
        .redis_version()
        .is_some_and(|version| version >= RedisVersion::new(7, 0, 0))
    {
        let shard_channel = format!("redis-mcp:cluster-session:{}:{{shard}}", std::process::id());
        let shard_session = client
            .call_tool(
                "redis_ssubscribe",
                serde_json::json!({"subscriptions": [{"value": shard_channel}]}),
            )
            .await
            .expect("open sharded Cluster Pub/Sub session")
            .structured_content
            .expect("structured sharded Cluster session")["session_id"]
            .as_str()
            .expect("sharded Cluster session id")
            .to_string();
        client
            .call_tool(
                "redis_spublish",
                serde_json::json!({"channel": {"value": shard_channel}, "message": {"value": "sharded"}}),
            )
            .await
            .expect("publish sharded Cluster message");
        let shard_read = client
            .call_tool(
                "redis_pubsub_read",
                serde_json::json!({"session_id": shard_session, "wait_ms": 1000}),
            )
            .await
            .expect("read sharded Cluster message")
            .structured_content
            .expect("structured sharded Cluster read");
        assert_eq!(shard_read["messages"][0]["kind"], "sharded");
        assert_eq!(shard_read["messages"][0]["payload"]["value"], "sharded");
        client
            .call_tool(
                "redis_pubsub_close",
                serde_json::json!({"session_id": shard_session}),
            )
            .await
            .expect("close sharded Cluster session");
    }

    manager.shutdown().await;
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
async fn diagnostics_fan_out_with_bounded_redacted_cluster_summaries() {
    let _cluster_guard = CLUSTER_TEST_LOCK.lock().await;
    let Some(cluster) = TestCluster::start().await else {
        return;
    };
    let executor = DirectRedisCluster::connect(&cluster.seed_urls)
        .await
        .expect("connect cluster-aware diagnostics adapter");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover Cluster diagnostic capabilities");
    assert_eq!(capabilities.deployment(), RedisDeployment::Cluster);
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .capabilities(capabilities)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect Cluster diagnostics MCP client");
    client
        .initialize("redis-mcp-cluster-diagnostics-test", "0")
        .await
        .expect("initialize Cluster diagnostics MCP client");

    let inputs = serde_json::json!({"max_cluster_nodes": 8});
    let cluster_info = client
        .call_tool("redis_cluster_info", inputs.clone())
        .await
        .expect("CLUSTER INFO fan-out")
        .structured_content
        .expect("structured CLUSTER INFO fan-out");
    assert_eq!(cluster_info["healthy_nodes"], 3);
    assert_eq!(cluster_info["cluster"]["nodes_queried"], 3);
    assert_eq!(cluster_info["cluster"]["nodes_succeeded"], 3);
    assert_eq!(cluster_info["cluster"]["complete"], true);
    assert_eq!(cluster_info["cluster"]["node_addresses_redacted"], true);
    assert_eq!(cluster_info["nodes"][0]["node"], "node-1");

    let health = client
        .call_tool("redis_health_check", inputs.clone())
        .await
        .expect("Cluster health fan-out")
        .structured_content
        .expect("structured Cluster health fan-out");
    assert_eq!(health["status"], "ok");
    assert_eq!(health["nodes"].as_array().map(Vec::len), Some(3));
    assert_eq!(health["cluster"]["complete"], true);

    let clients = client
        .call_tool(
            "redis_client_list",
            serde_json::json!({"max_results": 20, "max_cluster_nodes": 8}),
        )
        .await
        .expect("Cluster CLIENT LIST fan-out")
        .structured_content
        .expect("structured Cluster CLIENT LIST fan-out");
    assert_eq!(clients["cluster"]["nodes_queried"], 3);
    assert_eq!(clients["cluster"]["node_addresses_redacted"], true);
    assert!(
        clients["clients"]
            .as_array()
            .is_some_and(|clients| !clients.is_empty())
    );
    assert_eq!(clients["clients"][0]["address"], serde_json::Value::Null);

    for (tool, field) in [
        ("redis_connection_summary", "nodes"),
        ("redis_keyspace_summary", "nodes"),
        ("redis_memory_stats", "nodes"),
        ("redis_memory_summary", "nodes"),
        ("redis_module_list", "modules"),
        ("redis_acl_whoami", "identities"),
    ] {
        let result = client
            .call_tool(tool, inputs.clone())
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(!result.is_error, "{tool}: {result:?}");
        let structured = result
            .structured_content
            .unwrap_or_else(|| panic!("{tool} structured output"));
        assert!(structured[field].is_array(), "{tool}: {structured}");
        assert_eq!(structured["cluster"]["nodes_queried"], 3, "{tool}");
        let serialized = serde_json::to_string(&structured)
            .unwrap_or_else(|error| panic!("serialize {tool}: {error}"));
        for seed in &cluster.seed_urls {
            let address = seed
                .strip_prefix("redis://")
                .and_then(|value| value.strip_suffix('/'))
                .unwrap_or(seed);
            assert!(
                !serialized.contains(address),
                "{tool} leaked {address}: {serialized}"
            );
        }
    }

    let slowlog = client
        .call_tool(
            "redis_slowlog",
            serde_json::json!({"limit": 2, "max_cluster_nodes": 8}),
        )
        .await
        .expect("Cluster SLOWLOG fan-out")
        .structured_content
        .expect("structured Cluster SLOWLOG fan-out");
    assert_eq!(slowlog["cluster"]["nodes_queried"], 3);
    assert!(slowlog["entries"].is_array());

    let latency = client
        .call_tool(
            "redis_latency_history",
            serde_json::json!({"event": "command", "limit": 2, "max_cluster_nodes": 8}),
        )
        .await
        .expect("Cluster LATENCY HISTORY fan-out")
        .structured_content
        .expect("structured Cluster LATENCY HISTORY fan-out");
    assert_eq!(latency["cluster"]["nodes_queried"], 3);
    assert!(latency["samples"].is_array());

    let hotkeys = client
        .call_tool(
            "redis_hotkeys",
            serde_json::json!({"count": 1, "max_keys": 1, "top": 1}),
        )
        .await
        .expect("Cluster hotkey capability result");
    assert!(hotkeys.is_error);
    assert!(
        serde_json::to_string(&hotkeys)
            .expect("serialize Cluster hotkey error")
            .contains("[CapabilityUnavailable]")
    );
}

#[tokio::test]
async fn redis_json_mget_enforces_the_native_cluster_same_slot_contract() {
    let _cluster_guard = CLUSTER_TEST_LOCK.lock().await;
    let Some(cluster) = TestJsonCluster::start().await else {
        return;
    };
    let executor = DirectRedisCluster::connect(&cluster.seed_urls)
        .await
        .expect("connect RedisJSON cluster-aware adapter");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover RedisJSON Cluster capabilities");
    assert_eq!(capabilities.deployment(), RedisDeployment::Cluster);
    assert_eq!(
        capabilities.module(RedisModule::Json).status(),
        CapabilityStatus::Available
    );
    let client = json_router_client(executor).await;

    let same_slot_keys = ["redis-mcp:json:{same}:one", "redis-mcp:json:{same}:two"];
    let cross_slot_key = "redis-mcp:json:{other}:three";
    let seed = redis::Client::open(cluster.seed_urls[0].as_str())
        .expect("open RedisJSON Cluster seed client");
    let mut connection = seed
        .get_multiplexed_async_connection()
        .await
        .expect("connect to RedisJSON Cluster seed");
    let first_slot = cluster_key_slot(&mut connection, same_slot_keys[0]).await;
    let second_slot = cluster_key_slot(&mut connection, same_slot_keys[1]).await;
    let cross_slot = cluster_key_slot(&mut connection, cross_slot_key).await;
    assert_eq!(
        first_slot, second_slot,
        "matching hash tags must share a slot"
    );
    assert_ne!(
        first_slot, cross_slot,
        "distinct test tags must not collide"
    );

    for (index, key) in same_slot_keys
        .iter()
        .copied()
        .chain(std::iter::once(cross_slot_key))
        .enumerate()
    {
        let result = client
            .call_tool(
                "redis_json_set",
                serde_json::json!({"key": key, "value": {"index": index}}),
            )
            .await
            .expect("route JSON.SET by key");
        assert!(!result.is_error, "JSON.SET {key}: {result:?}");
    }

    let same_slot = client
        .call_tool(
            "redis_json_mget",
            serde_json::json!({"keys": same_slot_keys, "path": "$.index"}),
        )
        .await
        .expect("same-slot JSON.MGET result")
        .structured_content
        .expect("structured same-slot JSON.MGET result");
    assert_eq!(same_slot["values"][0]["value"], serde_json::json!([0]));
    assert_eq!(same_slot["values"][1]["value"], serde_json::json!([1]));

    let cross_slot = client
        .call_tool(
            "redis_json_mget",
            serde_json::json!({"keys": [same_slot_keys[0], cross_slot_key]}),
        )
        .await
        .expect("cross-slot JSON.MGET is represented as a tool result");
    assert!(cross_slot.is_error);
    let cross_slot =
        serde_json::to_string(&cross_slot).expect("serialize JSON.MGET CROSSSLOT result");
    assert!(cross_slot.contains("[InvalidRequest]"), "{cross_slot}");
    assert!(cross_slot.contains("CROSSSLOT"), "{cross_slot}");
}

#[tokio::test]
async fn search_routes_same_slot_index_documents_and_aggregates_in_cluster() {
    let _cluster_guard = CLUSTER_TEST_LOCK.lock().await;
    let Some(cluster) = TestJsonCluster::start().await else {
        return;
    };
    let executor = DirectRedisCluster::connect(&cluster.seed_urls)
        .await
        .expect("connect Search cluster-aware adapter");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover Search Cluster capabilities");
    assert_eq!(capabilities.deployment(), RedisDeployment::Cluster);
    assert_eq!(
        capabilities.module(RedisModule::Search).status(),
        CapabilityStatus::Available
    );
    let client = search_router_client(executor).await;

    let suffix = std::process::id();
    let index = format!("redis-mcp:{{search-{suffix}}}:index");
    let prefix = format!("redis-mcp:{{search-{suffix}}}:doc:");
    for number in 0..4 {
        let set = client
            .call_tool(
                "redis_hset",
                serde_json::json!({
                    "key": format!("{prefix}{number}"),
                    "fields": {
                        "title": format!("Redis cluster search {number}"),
                        "category": format!("category-{number}")
                    }
                }),
            )
            .await
            .expect("route Cluster HSET");
        assert!(!set.is_error, "{set:?}");
    }

    let created = client
        .call_tool(
            "redis_ft_create",
            serde_json::json!({
                "index": index,
                "on": "HASH",
                "prefixes": [prefix],
                "schema": [
                    {"name": "title", "field_type": "TEXT"},
                    {"name": "category", "field_type": "TAG"}
                ]
            }),
        )
        .await
        .expect("route Cluster FT.CREATE");
    assert!(!created.is_error, "{created:?}");

    let mut searched = None;
    for _ in 0..20 {
        let result = client
            .call_tool(
                "redis_ft_search",
                serde_json::json!({"index": index, "query": "redis", "limit_num": 1}),
            )
            .await
            .expect("route Cluster FT.SEARCH");
        if !result.is_error
            && result
                .structured_content
                .as_ref()
                .and_then(|output| output["total"].as_u64())
                == Some(4)
        {
            searched = result.structured_content;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let searched = searched.expect("same-slot Cluster document becomes searchable");
    assert!(
        searched["documents"][0]["id"]
            .as_str()
            .is_some_and(|id| id.starts_with(&prefix))
    );

    let aggregate = client
        .call_tool(
            "redis_ft_aggregate",
            serde_json::json!({
                "index": index,
                "query": "*",
                "stages": [{
                    "type": "group_by",
                    "properties": ["@category"],
                    "reducers": [{"function": "count", "alias": "count"}]
                }],
                "limit_num": 10,
                "cursor": {"count": 1, "max_idle_ms": 5000}
            }),
        )
        .await
        .expect("route Cluster FT.AGGREGATE");
    assert!(!aggregate.is_error, "{aggregate:?}");
    let aggregate = aggregate
        .structured_content
        .expect("structured Cluster aggregate");
    assert_eq!(aggregate["count"], 1);
    let cursor_id = aggregate["cursor_id"]
        .as_u64()
        .filter(|cursor| *cursor != 0)
        .expect("Cluster aggregate cursor continuation");
    let continued = client
        .call_tool(
            "redis_ft_cursor_read",
            serde_json::json!({"index": index, "cursor_id": cursor_id, "count": 1}),
        )
        .await
        .expect("route Cluster FT.CURSOR READ");
    assert!(!continued.is_error, "{continued:?}");
    let cursor_id = continued
        .structured_content
        .expect("structured Cluster cursor page")["cursor_id"]
        .as_u64()
        .filter(|cursor| *cursor != 0)
        .expect("Cluster cursor has more rows");
    let deleted = client
        .call_tool(
            "redis_ft_cursor_del",
            serde_json::json!({"index": index, "cursor_id": cursor_id}),
        )
        .await
        .expect("route Cluster FT.CURSOR DEL");
    assert!(!deleted.is_error, "{deleted:?}");

    let alias = format!("redis-mcp:{{search-{suffix}}}:alias");
    let added = client
        .call_tool(
            "redis_ft_aliasadd",
            serde_json::json!({"alias": alias, "index": index}),
        )
        .await
        .expect("route same-slot Cluster FT.ALIASADD");
    assert!(!added.is_error, "{added:?}");
    let cross_slot = client
        .call_tool(
            "redis_ft_aliasupdate",
            serde_json::json!({
                "alias": format!("redis-mcp:{{other-{suffix}}}:alias"),
                "index": index
            }),
        )
        .await
        .expect("cross-slot Cluster alias result");
    assert!(cross_slot.is_error);
    assert!(format!("{cross_slot:?}").contains("CROSSSLOT"));
    let deleted = client
        .call_tool("redis_ft_aliasdel", serde_json::json!({"alias": alias}))
        .await
        .expect("route Cluster FT.ALIASDEL");
    assert!(!deleted.is_error, "{deleted:?}");

    let dropped = client
        .call_tool(
            "redis_ft_dropindex",
            serde_json::json!({"index": index, "delete_docs": true}),
        )
        .await
        .expect("route Cluster FT.DROPINDEX");
    assert!(!dropped.is_error, "{dropped:?}");
}

#[tokio::test]
async fn pubsub_cluster_aggregation_is_bounded_deduplicated_and_slot_routed() {
    let _cluster_guard = CLUSTER_TEST_LOCK.lock().await;
    let Some(cluster) = TestCluster::start().await else {
        return;
    };

    let mut channel = format!("redis-mcp:cluster:pubsub:{{{}}}", std::process::id()).into_bytes();
    channel.extend([0xff, 0x00]);
    let channel_base64 = BASE64.encode(&channel);
    let mut subscribers = Vec::new();
    for seed_url in &cluster.seed_urls {
        let client = redis::Client::open(seed_url.as_str()).expect("open cluster Pub/Sub node");
        let mut subscriber = client
            .get_async_pubsub()
            .await
            .expect("connect cluster Pub/Sub node");
        subscriber
            .subscribe(channel.clone())
            .await
            .expect("subscribe on cluster node");
        subscribers.push(subscriber);
    }

    let executor = DirectRedisCluster::connect(&cluster.seed_urls)
        .await
        .expect("connect cluster Pub/Sub adapter");
    let capabilities = executor
        .discover_capabilities()
        .await
        .expect("discover cluster Pub/Sub capabilities");
    let redis_version = capabilities.redis_version();
    let router = RedisMcp::builder(executor)
        .access(AccessMode::ReadWrite)
        .capabilities(capabilities)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect cluster Pub/Sub MCP client");
    client
        .initialize("redis-mcp-cluster-pubsub-test", "0")
        .await
        .expect("initialize cluster Pub/Sub MCP client");

    let channels = client
        .call_tool(
            "redis_pubsub_channels",
            serde_json::json!({
                "pattern": {"value": channel_base64, "encoding": "base64"},
                "limit": 4,
                "max_cluster_nodes": cluster.seed_urls.len()
            }),
        )
        .await
        .expect("cluster PUBSUB CHANNELS")
        .structured_content
        .expect("structured cluster PUBSUB CHANNELS");
    assert_eq!(channels["count"], 1);
    assert_eq!(channels["channels"][0]["value"], channel_base64);
    assert_eq!(channels["channels"][0]["encoding"], "base64");
    assert_eq!(
        channels["cluster"]["nodes_queried"],
        cluster.seed_urls.len()
    );
    assert_eq!(channels["cluster"]["complete"], true);

    let counts = client
        .call_tool(
            "redis_pubsub_numsub",
            serde_json::json!({
                "channels": [{"value": channel_base64, "encoding": "base64"}],
                "max_cluster_nodes": cluster.seed_urls.len()
            }),
        )
        .await
        .expect("cluster PUBSUB NUMSUB")
        .structured_content
        .expect("structured cluster PUBSUB NUMSUB");
    assert_eq!(counts["counts"][0]["subscribers"], cluster.seed_urls.len());

    let published = client
        .call_tool(
            "redis_publish",
            serde_json::json!({
                "channel": {"value": channel_base64, "encoding": "base64"},
                "message": {"value": "/gE=", "encoding": "base64"}
            }),
        )
        .await
        .expect("cluster PUBLISH")
        .structured_content
        .expect("structured cluster PUBLISH");
    assert_eq!(published["receivers"], 1);
    assert_eq!(published["receiver_count_scope"], "executing_node");

    let bounded = client
        .call_tool(
            "redis_pubsub_channels",
            serde_json::json!({"limit": 4, "max_cluster_nodes": 2}),
        )
        .await
        .expect("cluster node limit tool result");
    assert!(bounded.is_error);
    assert!(
        serde_json::to_string(&bounded)
            .expect("serialize cluster node limit")
            .contains("CLUSTER_NODE_LIMIT_EXCEEDED")
    );

    if redis_version.is_some_and(|version| version >= RedisVersion::new(7, 0, 0)) {
        let shard_channel = format!("redis-mcp:cluster:shard:{{{}}}", std::process::id());
        let published = client
            .call_tool(
                "redis_spublish",
                serde_json::json!({
                    "channel": {"value": shard_channel},
                    "message": {"value": "hello"}
                }),
            )
            .await
            .expect("cluster SPUBLISH")
            .structured_content
            .expect("structured cluster SPUBLISH");
        assert_eq!(published["receivers"], 0);
        let counts = client
            .call_tool(
                "redis_pubsub_shardnumsub",
                serde_json::json!({
                    "channels": [{"value": shard_channel}],
                    "max_cluster_nodes": cluster.seed_urls.len()
                }),
            )
            .await
            .expect("cluster PUBSUB SHARDNUMSUB")
            .structured_content
            .expect("structured cluster PUBSUB SHARDNUMSUB");
        assert_eq!(counts["counts"][0]["subscribers"], 0);
        assert_eq!(counts["cluster"]["nodes_queried"], cluster.seed_urls.len());
    }

    drop(subscribers);
}

#[tokio::test]
async fn cluster_routes_curated_and_raw_tools_across_three_masters() {
    let _cluster_guard = CLUSTER_TEST_LOCK.lock().await;
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
        let remote_zset = format!("{remote_key}:zset");
        let remote_stream = format!("{remote_key}:stream");

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

        routed
            .call_tool(
                "redis_zadd",
                serde_json::json!({
                    "key": remote_zset,
                    "members": [
                        {"score": "1.25", "member": "alice"},
                        {"score": 2, "member": "bob"}
                    ]
                }),
            )
            .await
            .expect("remote-slot ZADD");
        let remote_scores = routed
            .call_tool(
                "redis_zmscore",
                serde_json::json!({"key": remote_zset, "members": ["bob", "missing"]}),
            )
            .await
            .expect("remote-slot ZMSCORE")
            .structured_content
            .expect("structured remote-slot ZMSCORE");
        assert_eq!(remote_scores["members"][0]["score"], "2");
        assert_eq!(remote_scores["members"][1]["member_exists"], false);
        let remote_range = routed
            .call_tool(
                "redis_zrange",
                serde_json::json!({
                    "key": remote_zset,
                    "range": {
                        "kind": "score",
                        "min": {"kind": "negative_infinity"},
                        "max": {"kind": "positive_infinity"},
                        "limit": 10
                    },
                    "withscores": true
                }),
            )
            .await
            .expect("remote-slot score ZRANGE")
            .structured_content
            .expect("structured remote-slot score ZRANGE");
        assert_eq!(remote_range["count"], 2);
        let remote_increment = routed
            .call_tool(
                "redis_zincrby",
                serde_json::json!({"key": remote_zset, "member": "alice", "increment": "0.25"}),
            )
            .await
            .expect("remote-slot ZINCRBY")
            .structured_content
            .expect("structured remote-slot ZINCRBY");
        assert_eq!(remote_increment["score"], "1.5");
        let remote_popped = routed
            .call_tool(
                "redis_zpopmax",
                serde_json::json!({"key": remote_zset, "count": 1}),
            )
            .await
            .expect("remote-slot ZPOPMAX")
            .structured_content
            .expect("structured remote-slot ZPOPMAX");
        assert_eq!(remote_popped["members"][0]["member"], "bob");
        let remote_zrem = routed
            .call_tool(
                "redis_zrem",
                serde_json::json!({"key": remote_zset, "members": ["alice"]}),
            )
            .await
            .expect("remote-slot ZREM")
            .structured_content
            .expect("structured remote-slot ZREM");
        assert_eq!(remote_zrem["removed"], 1);

        routed
            .call_tool(
                "redis_xadd",
                serde_json::json!({
                    "key": remote_stream,
                    "fields": [{"field": "event", "value": "remote"}]
                }),
            )
            .await
            .expect("remote-slot XADD");
        let remote_entries = routed
            .call_tool(
                "redis_xrange",
                serde_json::json!({"key": remote_stream, "count": 1}),
            )
            .await
            .expect("remote-slot XRANGE")
            .structured_content
            .expect("structured remote-slot XRANGE");
        assert_eq!(remote_entries["entries"][0]["fields"][0]["value"], "remote");

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
        let same_stream_left = format!(
            "redis-mcp:test:{}:{{issue23-{protocol}}}:stream-left",
            std::process::id()
        );
        let same_stream_right = format!(
            "redis-mcp:test:{}:{{issue23-{protocol}}}:stream-right",
            std::process::id()
        );
        let cross_stream_left = format!("{}:stream-left", keys[0]);
        let cross_stream_right = format!("{}:stream-right", keys[1]);
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

        for stream in [
            &same_stream_left,
            &same_stream_right,
            &cross_stream_left,
            &cross_stream_right,
        ] {
            routed
                .call_tool(
                    "redis_xadd",
                    serde_json::json!({
                        "key": stream,
                        "fields": [{"field": "event", "value": "cluster"}]
                    }),
                )
                .await
                .expect("seed cluster stream");
        }
        let same_slot_stream_read = routed
            .call_tool(
                "redis_xread",
                serde_json::json!({
                    "streams": [
                        {"key": same_stream_left, "offset": {"type": "explicit", "id": {"milliseconds": 0, "sequence": 0}}},
                        {"key": same_stream_right, "offset": {"type": "explicit", "id": {"milliseconds": 0, "sequence": 0}}}
                    ],
                    "count": 1
                }),
            )
            .await
            .expect("same-slot multi-stream XREAD")
            .structured_content
            .expect("structured same-slot multi-stream XREAD");
        assert_eq!(same_slot_stream_read["count"], 2);
        assert_eq!(
            same_slot_stream_read["streams"].as_array().unwrap().len(),
            2
        );

        let cross_slot_stream_read = routed
            .call_tool(
                "redis_xread",
                serde_json::json!({
                    "streams": [
                        {"key": cross_stream_left, "offset": {"type": "explicit", "id": {"milliseconds": 0, "sequence": 0}}},
                        {"key": cross_stream_right, "offset": {"type": "explicit", "id": {"milliseconds": 0, "sequence": 0}}}
                    ],
                    "count": 1
                }),
            )
            .await
            .expect("cross-slot XREAD is represented as a tool result");
        assert!(cross_slot_stream_read.is_error);
        let cross_slot_stream_read =
            serde_json::to_string(&cross_slot_stream_read).expect("serialize XREAD CROSSSLOT");
        assert!(
            cross_slot_stream_read.contains("CROSSSLOT"),
            "{cross_slot_stream_read}"
        );

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
        let stream_deleted = routed
            .call_tool(
                "redis_del",
                serde_json::json!({
                    "keys": [
                        remote_stream,
                        same_stream_left,
                        same_stream_right,
                        cross_stream_left,
                        cross_stream_right
                    ]
                }),
            )
            .await
            .expect("delete cluster stream keys")
            .structured_content
            .expect("structured stream-key DEL");
        assert_eq!(stream_deleted["deleted"], 5);
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
                        remote_zset,
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
}
