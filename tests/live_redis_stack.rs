use std::time::Duration;

use redis_mcp::{AccessMode, CapabilityStatus, DirectRedis, RedisMcp, RedisModule, ToolBundle};
use tower_mcp::{
    CallToolResult,
    client::{ChannelTransport, McpClient},
};

#[cfg(unix)]
use redis_server_wrapper::{Error as RedisServerError, RedisServer, RedisServerHandle};

struct TestRedisStack {
    url: String,
    #[cfg(unix)]
    _managed: Option<ManagedRedisStack>,
}

impl TestRedisStack {
    async fn start() -> Option<Self> {
        if let Ok(url) = std::env::var("REDIS_STACK_URL") {
            return Some(Self {
                url,
                #[cfg(unix)]
                _managed: None,
            });
        }

        #[cfg(unix)]
        {
            match ManagedRedisStack::start().await {
                Ok(managed) => Some(Self {
                    url: managed.url(),
                    _managed: Some(managed),
                }),
                Err(RedisServerError::BinaryNotFound { binary }) => {
                    eprintln!(
                        "skipping live Redis Stack test: REDIS_STACK_URL is not set and {binary} is not on PATH"
                    );
                    None
                }
                Err(RedisServerError::ModuleNotLoaded { .. }) => {
                    eprintln!(
                        "skipping live Redis Stack test: REDIS_STACK_URL is not set and the local server does not provide both RedisJSON and Search"
                    );
                    None
                }
                Err(error) => panic!("start wrapper-managed Redis Stack: {error}"),
            }
        }

        #[cfg(not(unix))]
        {
            eprintln!(
                "skipping live Redis Stack test: REDIS_STACK_URL is not set and self-hosting requires Unix"
            );
            None
        }
    }
}

#[cfg(unix)]
struct ManagedRedisStack {
    _server: RedisServerHandle,
    _directory: tempfile::TempDir,
    port: u16,
}

#[cfg(unix)]
impl ManagedRedisStack {
    async fn start() -> Result<Self, RedisServerError> {
        let directory = tempfile::tempdir().expect("create Redis Stack test directory");
        let server = RedisServer::new()
            .auto_port()
            .bind("127.0.0.1")
            .dir(directory.path())
            .start()
            .await?;
        server.require_module("ReJSON").await?;
        server.require_module("search").await?;
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

async fn stack_client(url: &str) -> McpClient {
    let executor = DirectRedis::connect(url)
        .await
        .expect("connect to Redis Stack");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([
            ToolBundle::DataStructures,
            ToolBundle::Json,
            ToolBundle::Search,
        ])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect Stack MCP client");
    client
        .initialize("redis-mcp-stack-test", "0")
        .await
        .expect("initialize Stack MCP client");
    client
}

#[tokio::test]
async fn direct_adapter_discovers_stack_modules_and_versions() {
    let Some(redis) = TestRedisStack::start().await else {
        return;
    };
    let executor = DirectRedis::connect(&redis.url)
        .await
        .expect("connect for Stack capability discovery");
    let capabilities = executor
        .discover_capabilities_with_timeout(Duration::from_secs(2))
        .await
        .expect("discover Stack capabilities");

    for module in [RedisModule::Json, RedisModule::Search] {
        let capability = capabilities.module(module);
        assert_eq!(capability.status(), CapabilityStatus::Available);
        assert!(capability.version().is_some(), "{module} version");
    }
    assert_eq!(
        capabilities.command("JSON.GET"),
        CapabilityStatus::Available
    );
    assert_eq!(
        capabilities.command("FT.SEARCH"),
        CapabilityStatus::Available
    );
}

async fn call(client: &McpClient, tool: &str, input: serde_json::Value) -> CallToolResult {
    let result = client
        .call_tool(tool, input)
        .await
        .unwrap_or_else(|error| panic!("{tool}: {error}"));
    assert!(!result.is_error, "{tool}: {result:?}");
    result
}

fn structured(result: CallToolResult) -> serde_json::Value {
    result
        .structured_content
        .expect("successful tool result has structured content")
}

#[tokio::test]
async fn redis_json_and_search_lifecycle_through_router() {
    let Some(redis) = TestRedisStack::start().await else {
        return;
    };
    let client = stack_client(&redis.url).await;
    let suffix = std::process::id();
    let key = format!("redis-mcp:stack:{suffix}:doc:1");
    let prefix = format!("redis-mcp:stack:{suffix}:doc:");
    let index = format!("redis-mcp-stack-{suffix}");

    let set = structured(
        call(
            &client,
            "redis_json_set",
            serde_json::json!({
                "key": key,
                "value": {"name": "Ada Lovelace", "score": 42}
            }),
        )
        .await,
    );
    assert_eq!(set["stored"], true);

    let get = structured(call(&client, "redis_json_get", serde_json::json!({"key": key})).await);
    assert_eq!(get["exists"], true);
    assert!(get["value"].to_string().contains("Ada Lovelace"));

    let value_type =
        structured(call(&client, "redis_json_type", serde_json::json!({"key": key})).await);
    assert_eq!(value_type["types"][0], "object");

    let created = structured(
        call(
            &client,
            "redis_ft_create",
            serde_json::json!({
                "index": index,
                "on": "JSON",
                "prefixes": [prefix],
                "schema": [
                    {"name": "$.name", "alias": "name", "field_type": "TEXT"},
                    {"name": "$.score", "alias": "score", "field_type": "NUMERIC", "sortable": true}
                ]
            }),
        )
        .await,
    );
    assert_eq!(created["created"], true);

    let indexes = structured(call(&client, "redis_ft_list", serde_json::json!({})).await);
    assert!(
        indexes["indexes"]
            .as_array()
            .is_some_and(|indexes| indexes.iter().any(|value| value == &index))
    );

    let info = structured(
        call(
            &client,
            "redis_ft_info",
            serde_json::json!({"index": index}),
        )
        .await,
    );
    assert_eq!(info["index"], index);
    assert!(info["attributes"].is_object());

    let mut extra_keys = Vec::new();
    for number in 2..=12 {
        let extra_key = format!("{prefix}{number}");
        call(
            &client,
            "redis_json_set",
            serde_json::json!({
                "key": extra_key,
                "value": {"name": format!("Ada Lovelace {number}"), "score": number}
            }),
        )
        .await;
        extra_keys.push(extra_key);
    }

    let mut found = None;
    for _ in 0..20 {
        let search = structured(
            call(
                &client,
                "redis_ft_search",
                serde_json::json!({
                    "index": index,
                    "query": "@name:Ada",
                    "limit_num": 10
                }),
            )
            .await,
        );
        if search["total"].as_u64().is_some_and(|total| total > 0) {
            found = Some(search);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let search = found.expect("new JSON document becomes searchable");
    assert!(search["response"].to_string().contains(&key));

    let mut first_page = None;
    for _ in 0..40 {
        let search = structured(
            call(
                &client,
                "redis_ft_search",
                serde_json::json!({
                    "index": index,
                    "query": "@name:Ada",
                    "limit_offset": 0,
                    "limit_num": 5
                }),
            )
            .await,
        );
        if search["total"].as_u64().is_some_and(|total| total >= 12) {
            first_page = Some(search);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let first_page = first_page.expect("all JSON documents become searchable");
    assert_eq!(first_page["page"]["returned"], 5);
    assert_eq!(first_page["page"]["complete"], false);
    assert_eq!(first_page["page"]["continuation"]["offset"], 5);

    let second_page = structured(
        call(
            &client,
            "redis_ft_search",
            serde_json::json!({
                "index": index,
                "query": "@name:Ada",
                "limit_offset": first_page["page"]["continuation"]["offset"],
                "limit_num": 5
            }),
        )
        .await,
    );
    assert_eq!(second_page["limit_offset"], 5);
    assert_eq!(second_page["page"]["returned"], 5);

    let dropped = structured(
        call(
            &client,
            "redis_ft_dropindex",
            serde_json::json!({"index": index}),
        )
        .await,
    );
    assert_eq!(dropped["dropped"], true);
    assert_eq!(dropped["documents_deleted"], false);

    for document_key in std::iter::once(key).chain(extra_keys) {
        let deleted = structured(
            call(
                &client,
                "redis_json_del",
                serde_json::json!({"key": document_key}),
            )
            .await,
        );
        assert_eq!(deleted["deleted"], 1);
    }
}

#[tokio::test]
async fn vector_and_hybrid_search_cover_hash_and_json_models() {
    let Some(redis) = TestRedisStack::start().await else {
        return;
    };
    let client = stack_client(&redis.url).await;
    let suffix = std::process::id();

    let hash_prefix = format!("redis-mcp:vector:{suffix}:hash:");
    let hash_index = format!("redis-mcp-vector-hash-{suffix}");
    let hash_documents = [
        (
            "1",
            "Redis vector guide",
            "database",
            "10",
            "-122.4,37.8",
            [1.0, 0.0, 0.0],
        ),
        (
            "2",
            "Rust search guide",
            "language",
            "20",
            "-73.9,40.7",
            [0.0, 1.0, 0.0],
        ),
        (
            "3",
            "Caching patterns",
            "database",
            "30",
            "-0.1,51.5",
            [0.0, 0.0, 1.0],
        ),
    ];
    for (id, title, category, price, location, vector) in hash_documents {
        let key = format!("{hash_prefix}{id}");
        call(
            &client,
            "redis_hset",
            serde_json::json!({
                "key": key,
                "fields": {
                    "title": title,
                    "category": category,
                    "price": price,
                    "location": location
                }
            }),
        )
        .await;
        call(
            &client,
            "redis_vector_set_hash",
            serde_json::json!({
                "key": key,
                "field": "embedding",
                "data_type": "FLOAT32",
                "vector": vector
            }),
        )
        .await;
    }

    let read_vector = structured(
        call(
            &client,
            "redis_vector_get_hash",
            serde_json::json!({
                "key": format!("{hash_prefix}1"),
                "field": "embedding",
                "data_type": "FLOAT32"
            }),
        )
        .await,
    );
    assert_eq!(read_vector["vector"], serde_json::json!([1.0, 0.0, 0.0]));
    assert_eq!(read_vector["bytes"], 12);

    let created = structured(
        call(
            &client,
            "redis_ft_create",
            serde_json::json!({
                "index": hash_index,
                "on": "HASH",
                "prefixes": [hash_prefix],
                "schema": [
                    {
                        "name": "embedding",
                        "field_type": "VECTOR",
                        "vector": {
                            "algorithm": "FLAT",
                            "data_type": "FLOAT32",
                            "dimensions": 3,
                            "distance_metric": "L2",
                            "initial_capacity": 10,
                            "block_size": 10
                        }
                    },
                    {"name": "title", "field_type": "TEXT"},
                    {"name": "category", "field_type": "TAG"},
                    {"name": "price", "field_type": "NUMERIC"},
                    {"name": "location", "field_type": "GEO"}
                ]
            }),
        )
        .await,
    );
    assert_eq!(created["vector_fields"], 1);

    let mut vector_result = None;
    for _ in 0..40 {
        let result = structured(
            call(
                &client,
                "redis_ft_vector_search",
                serde_json::json!({
                    "index": hash_index,
                    "vector_field": "embedding",
                    "data_type": "FLOAT32",
                    "vector": [1.0, 0.0, 0.0],
                    "top_k": 3,
                    "limit_num": 2,
                    "return_fields": ["title", "category", "price"]
                }),
            )
            .await,
        );
        if result["total"].as_u64().is_some_and(|total| total >= 3) {
            vector_result = Some(result);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let vector_result = vector_result.expect("HASH vectors become searchable");
    assert_eq!(vector_result["results"][0]["id"], format!("{hash_prefix}1"));
    assert_eq!(vector_result["results"][0]["distance"], 0.0);
    assert_eq!(vector_result["page"]["continuation"]["offset"], 2);

    let hybrid = structured(
        call(
            &client,
            "redis_ft_hybrid_search",
            serde_json::json!({
                "index": hash_index,
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0, 0.0, 0.0],
                "top_k": 3,
                "limit_num": 3,
                "return_fields": ["title", "category", "price"],
                "hybrid_policy": "batches",
                "batch_size": 2,
                "filters": [
                    {"type": "text", "field": "title", "value": "Redis"},
                    {"type": "tag", "field": "category", "values": ["database"]},
                    {"type": "numeric", "field": "price", "minimum": 5, "maximum": 15},
                    {
                        "type": "geo",
                        "field": "location",
                        "longitude": -122.4,
                        "latitude": 37.8,
                        "radius": 5,
                        "unit": "km"
                    }
                ]
            }),
        )
        .await,
    );
    assert_eq!(hybrid["total"], 1);
    assert_eq!(hybrid["results"][0]["id"], format!("{hash_prefix}1"));

    let json_prefix = format!("redis-mcp:vector:{suffix}:json:");
    let json_index = format!("redis-mcp-vector-json-{suffix}");
    for (id, name, vector) in [
        ("1", "alpha", [1.0, 0.0, 0.0]),
        ("2", "beta", [0.0, 1.0, 0.0]),
    ] {
        call(
            &client,
            "redis_json_set",
            serde_json::json!({
                "key": format!("{json_prefix}{id}"),
                "value": {"name": name, "embedding": vector}
            }),
        )
        .await;
    }
    call(
        &client,
        "redis_ft_create",
        serde_json::json!({
            "index": json_index,
            "on": "JSON",
            "prefixes": [json_prefix],
            "schema": [
                {
                    "name": "$.embedding",
                    "alias": "embedding",
                    "field_type": "VECTOR",
                    "vector": {
                        "algorithm": "HNSW",
                        "data_type": "FLOAT64",
                        "dimensions": 3,
                        "distance_metric": "COSINE",
                        "m": 8,
                        "ef_construction": 40,
                        "ef_runtime": 20
                    }
                },
                {"name": "$.name", "alias": "name", "field_type": "TAG"}
            ]
        }),
    )
    .await;

    let mut json_result = None;
    for _ in 0..40 {
        let result = structured(
            call(
                &client,
                "redis_ft_vector_search",
                serde_json::json!({
                    "index": json_index,
                    "vector_field": "embedding",
                    "data_type": "FLOAT64",
                    "vector": [1.0, 0.0, 0.0],
                    "top_k": 2,
                    "limit_num": 2,
                    "return_fields": ["name"],
                    "ef_runtime": 20
                }),
            )
            .await,
        );
        if result["total"].as_u64().is_some_and(|total| total >= 2) {
            json_result = Some(result);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let json_result = json_result.expect("JSON vectors become searchable");
    assert_eq!(json_result["results"][0]["id"], format!("{json_prefix}1"));

    for index in [hash_index, json_index] {
        let dropped = structured(
            call(
                &client,
                "redis_ft_dropindex",
                serde_json::json!({"index": index, "delete_docs": true}),
            )
            .await,
        );
        assert_eq!(dropped["dropped"], true);
    }
}
