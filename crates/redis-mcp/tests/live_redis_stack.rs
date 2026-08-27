use std::time::Duration;

use redis_mcp::{
    AccessMode, CapabilityStatus, DirectRedis, OutputBudget, RedisCapabilities, RedisMcp,
    RedisModule, RedisVersion, ToolBundle,
};
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

fn with_protocol(url: &str, protocol: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}protocol={protocol}")
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
            ToolBundle::Essentials,
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

async fn stack_client_with_budget(url: &str, output_budget: OutputBudget) -> McpClient {
    let executor = DirectRedis::connect(url)
        .await
        .expect("connect to Redis Stack");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Json])
        .output_budget(output_budget)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect budget Stack MCP client");
    client
        .initialize("redis-mcp-stack-budget-test", "0")
        .await
        .expect("initialize budget Stack MCP client");
    client
}

async fn stack_search_client_with_budget(url: &str, output_budget: OutputBudget) -> McpClient {
    let executor = DirectRedis::connect(url)
        .await
        .expect("connect to Redis Stack");
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Search])
        .output_budget(output_budget)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect budget Search MCP client");
    client
        .initialize("redis-mcp-search-budget-test", "0")
        .await
        .expect("initialize budget Search MCP client");
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
async fn complete_redis_json_family_is_structured_bounded_and_semantic() {
    let Some(redis) = TestRedisStack::start().await else {
        return;
    };
    let client = stack_client(&redis.url).await;
    let suffix = std::process::id();
    let key = format!("redis-mcp:stack:{suffix}:json-family");
    let missing = format!("{key}:missing");

    let set = structured(
        call(
            &client,
            "redis_json_set",
            serde_json::json!({
                "key": key,
                "value": {
                    "name": "Ada",
                    "score": 41.5,
                    "enabled": true,
                    "items": [1, 2, 3],
                    "legacy_items": [[1, 2]],
                    "details": {"language": "Rust", "year": 2015},
                    "obsolete": true,
                    "binary_string": "nul:\u{0000}:end"
                }
            }),
        )
        .await,
    );
    assert_eq!(set["stored"], true);

    let conditional_noop = structured(
        call(
            &client,
            "redis_json_set",
            serde_json::json!({"key": key, "value": {"replaced": true}, "nx": true}),
        )
        .await,
    );
    assert_eq!(conditional_noop["stored"], false);

    let missing_path = structured(
        call(
            &client,
            "redis_json_get",
            serde_json::json!({"key": key, "path": "$.missing"}),
        )
        .await,
    );
    assert_eq!(missing_path["key_existed_before"], true);
    assert_eq!(missing_path["exists"], false);
    assert_eq!(missing_path["match_count"], 0);
    assert_eq!(missing_path["value"], serde_json::json!([]));

    let missing_key = structured(
        call(
            &client,
            "redis_json_get",
            serde_json::json!({"key": missing}),
        )
        .await,
    );
    assert_eq!(missing_key["key_existed_before"], false);
    assert_eq!(missing_key["exists"], false);
    assert_eq!(missing_key["value"], serde_json::Value::Null);

    let values = structured(
        call(
            &client,
            "redis_json_mget",
            serde_json::json!({"keys": [key, missing], "path": "$.name"}),
        )
        .await,
    );
    assert_eq!(values["values"][0]["value"], serde_json::json!(["Ada"]));
    assert_eq!(values["values"][0]["match_count"], 1);
    assert_eq!(values["values"][1]["key_existed_before"], false);
    assert_eq!(values["values"][1]["match_count"], 0);

    let legacy_length = structured(
        call(
            &client,
            "redis_json_strlen",
            serde_json::json!({"key": key, "path": ".name"}),
        )
        .await,
    );
    assert_eq!(legacy_length["path_mode"], "legacy");
    assert_eq!(legacy_length["types"], serde_json::json!(["string"]));
    assert_eq!(legacy_length["values"], serde_json::json!([3]));

    let wrong_type = structured(
        call(
            &client,
            "redis_json_strlen",
            serde_json::json!({"key": key, "path": "$.score"}),
        )
        .await,
    );
    assert_eq!(wrong_type["types"], serde_json::json!(["number"]));
    assert_eq!(wrong_type["values"], serde_json::json!([null]));

    let object_keys = structured(
        call(
            &client,
            "redis_json_objkeys",
            serde_json::json!({"key": key, "path": "$.details"}),
        )
        .await,
    );
    let object_keys = object_keys["keys"][0].as_array().expect("object keys");
    assert!(object_keys.contains(&serde_json::json!("language")));
    assert!(object_keys.contains(&serde_json::json!("year")));

    let object_length = structured(
        call(
            &client,
            "redis_json_objlen",
            serde_json::json!({"key": key, "path": "$.details"}),
        )
        .await,
    );
    assert_eq!(object_length["values"], serde_json::json!([2]));

    let array_length = structured(
        call(
            &client,
            "redis_json_arrlen",
            serde_json::json!({"key": key, "path": "$.items"}),
        )
        .await,
    );
    assert_eq!(array_length["values"], serde_json::json!([3]));

    let incremented = structured(
        call(
            &client,
            "redis_json_numincrby",
            serde_json::json!({"key": key, "path": "$.score", "value": 0.5}),
        )
        .await,
    );
    assert_eq!(incremented["values"], serde_json::json!([42.0]));

    let toggled = structured(
        call(
            &client,
            "redis_json_toggle",
            serde_json::json!({"key": key, "path": "$.enabled"}),
        )
        .await,
    );
    assert_eq!(toggled["values"], serde_json::json!([0]));

    let appended = structured(
        call(
            &client,
            "redis_json_arrappend",
            serde_json::json!({
                "key": key,
                "path": "$.items",
                "values": [4, {"kind": "tail"}]
            }),
        )
        .await,
    );
    assert_eq!(appended["values"], serde_json::json!([5]));

    let inserted = structured(
        call(
            &client,
            "redis_json_arrinsert",
            serde_json::json!({"key": key, "path": "$.items", "index": 0, "values": [0]}),
        )
        .await,
    );
    assert_eq!(inserted["values"], serde_json::json!([6]));

    let popped = structured(
        call(
            &client,
            "redis_json_arrpop",
            serde_json::json!({"key": key, "path": "$.items"}),
        )
        .await,
    );
    assert_eq!(popped["popped"], 1);
    assert_eq!(popped["values"][0], serde_json::json!({"kind": "tail"}));
    assert_eq!(popped["values_omitted"], false);

    let legacy_array_value = structured(
        call(
            &client,
            "redis_json_arrpop",
            serde_json::json!({"key": key, "path": ".legacy_items"}),
        )
        .await,
    );
    assert_eq!(legacy_array_value["path_mode"], "legacy");
    assert_eq!(legacy_array_value["popped"], 1);
    assert_eq!(legacy_array_value["values"], serde_json::json!([[1, 2]]));

    let trimmed = structured(
        call(
            &client,
            "redis_json_arrtrim",
            serde_json::json!({"key": key, "path": "$.items", "start": 0, "stop": 2}),
        )
        .await,
    );
    assert_eq!(trimmed["values"], serde_json::json!([3]));

    let merged = structured(
        call(
            &client,
            "redis_json_merge",
            serde_json::json!({
                "key": key,
                "value": {"obsolete": null, "new_field": {"ready": true}}
            }),
        )
        .await,
    );
    assert_eq!(merged["merged"], true);

    let cleared = structured(
        call(
            &client,
            "redis_json_clear",
            serde_json::json!({"key": key, "path": "$.details"}),
        )
        .await,
    );
    assert_eq!(cleared["cleared"], 1);

    let document =
        structured(call(&client, "redis_json_get", serde_json::json!({"key": key})).await);
    assert_eq!(document["value"][0]["score"], 42.0);
    assert_eq!(document["value"][0]["enabled"], false);
    assert_eq!(document["value"][0]["items"], serde_json::json!([0, 1, 2]));
    assert_eq!(document["value"][0]["details"], serde_json::json!({}));
    assert_eq!(document["value"][0]["new_field"]["ready"], true);
    assert!(document["value"][0].get("obsolete").is_none());
    assert_eq!(document["value"][0]["binary_string"], "nul:\u{0}:end");

    let malformed = client
        .call_tool(
            "redis_json_strlen",
            serde_json::json!({"key": key, "path": "$["}),
        )
        .await
        .expect("malformed JSONPath tool result");
    assert!(malformed.is_error);

    let budget_client =
        stack_client_with_budget(&redis.url, OutputBudget::new(256 * 1024, 1)).await;
    let oversized = budget_client
        .call_tool(
            "redis_json_objkeys",
            serde_json::json!({"key": key, "path": "$"}),
        )
        .await
        .expect("budgeted JSON.OBJKEYS result");
    assert!(oversized.is_error);

    let popped_large = structured(
        call(
            &client,
            "redis_json_arrappend",
            serde_json::json!({
                "key": key,
                "path": "$.items",
                "values": ["a value too large for the requested return budget"]
            }),
        )
        .await,
    );
    assert_eq!(popped_large["values"], serde_json::json!([4]));
    let omitted = structured(
        call(
            &client,
            "redis_json_arrpop",
            serde_json::json!({
                "key": key,
                "path": "$.items",
                "max_returned_bytes": 1
            }),
        )
        .await,
    );
    assert_eq!(omitted["popped"], 1);
    assert_eq!(omitted["values_omitted"], true);
    assert_eq!(omitted["values"], serde_json::Value::Null);

    let deleted =
        structured(call(&client, "redis_json_del", serde_json::json!({"key": key})).await);
    assert_eq!(deleted["deleted"], 1);
}

#[tokio::test]
async fn redis_json_acl_key_patterns_are_enforced_without_leaking_credentials() {
    let Some(redis) = TestRedisStack::start().await else {
        return;
    };
    let suffix = std::process::id();
    let username = format!("redis-mcp-json-{suffix}");
    let password = "mcp-json-secret";
    let allowed_prefix = format!("redis-mcp:stack:{suffix}:acl:allowed:");
    let allowed_key = format!("{allowed_prefix}doc");
    let denied_key = format!("redis-mcp:stack:{suffix}:acl:denied:doc");

    let admin = redis::Client::open(redis.url.as_str()).expect("open Stack admin client");
    let mut connection = admin
        .get_multiplexed_async_connection()
        .await
        .expect("connect Stack admin client");
    for key in [&allowed_key, &denied_key] {
        redis::cmd("JSON.SET")
            .arg(key)
            .arg("$")
            .arg(r#"{"name":"Ada"}"#)
            .query_async::<()>(&mut connection)
            .await
            .expect("seed ACL JSON document");
    }
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("on")
        .arg(format!(">{password}"))
        .arg("resetkeys")
        .arg(format!("~{allowed_prefix}*"))
        .arg("-@all")
        .arg("+json.get")
        .arg("+json.strlen")
        .arg("+json.type")
        .arg("+exists")
        .query_async::<()>(&mut connection)
        .await
        .expect("create restricted RedisJSON user");

    let authenticated_url =
        redis
            .url
            .replacen("redis://", &format!("redis://{username}:{password}@"), 1);
    let client = stack_client(&authenticated_url).await;
    let allowed = client
        .call_tool(
            "redis_json_strlen",
            serde_json::json!({"key": allowed_key, "path": "$.name"}),
        )
        .await
        .expect("allowed RedisJSON read");
    assert!(!allowed.is_error, "{allowed:?}");

    let denied = client
        .call_tool("redis_json_get", serde_json::json!({"key": denied_key}))
        .await
        .expect("denied RedisJSON read result");
    assert!(denied.is_error);
    assert!(!format!("{denied:?}").contains(password));

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<()>(&mut connection)
        .await
        .expect("delete restricted RedisJSON user");
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
async fn complete_search_family_is_structured_bounded_and_stateful() {
    let Some(redis) = TestRedisStack::start().await else {
        return;
    };
    let client = stack_client(&redis.url).await;
    let suffix = std::process::id();
    let prefix = format!("redis-mcp:search:{suffix}:doc:");
    let first_index = format!("redis-mcp-search-{suffix}-v1");
    let second_index = format!("redis-mcp-search-{suffix}-v2");
    let alias = format!("redis-mcp-search-{suffix}-current");
    let dictionary = format!("redis-mcp-search-{suffix}-dictionary");

    for number in 0..12 {
        call(
            &client,
            "redis_hset",
            serde_json::json!({
                "key": format!("{prefix}{number}"),
                "fields": {
                    "title": format!("Redis search guide {number}"),
                    "category": format!("category-{}", number % 4),
                    "score": number.to_string()
                }
            }),
        )
        .await;
    }
    call(
        &client,
        "redis_hset",
        serde_json::json!({
            "key": format!("{prefix}11"),
            "entries": [{
                "field": "blob",
                "value": "/wA=",
                "value_encoding": "base64"
            }]
        }),
    )
    .await;

    let schema = serde_json::json!([
        {"name": "title", "field_type": "TEXT"},
        {"name": "category", "field_type": "TAG", "sortable": true}
    ]);
    call(
        &client,
        "redis_ft_create",
        serde_json::json!({
            "index": first_index,
            "on": "HASH",
            "prefixes": [prefix],
            "schema": schema
        }),
    )
    .await;
    let altered = structured(
        call(
            &client,
            "redis_ft_alter",
            serde_json::json!({
                "index": first_index,
                "field": {"name": "score", "field_type": "NUMERIC", "sortable": true}
            }),
        )
        .await,
    );
    assert_eq!(altered["added"], true);

    let mut searched = None;
    for _ in 0..40 {
        let result = structured(
            call(
                &client,
                "redis_ft_search",
                serde_json::json!({
                    "index": first_index,
                    "query": "@title:$term",
                    "params": [{"name": "term", "value": "redis"}],
                    "dialect": 2,
                    "return_fields": ["title", "category", "score"],
                    "sortby": "score",
                    "sortby_order": "DESC",
                    "withscores": true,
                    "limit_num": 4
                }),
            )
            .await,
        );
        if result["total"].as_u64().is_some_and(|total| total == 12) {
            searched = Some(result);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let searched = searched.expect("all HASH documents become searchable");
    assert_eq!(searched["count"], 4);
    assert!(searched["documents"][0]["score"].as_f64().is_some());
    assert!(
        searched["documents"][0]["fields"]
            .to_string()
            .contains("score")
    );
    assert_eq!(searched["page"]["continuation"]["offset"], 4);

    let binary = structured(
        call(
            &client,
            "redis_ft_search",
            serde_json::json!({
                "index": first_index,
                "query": "@score:[11 11]",
                "return_fields": ["blob"],
                "limit_num": 1
            }),
        )
        .await,
    );
    assert_eq!(
        binary["documents"][0]["fields"][0]["value"]["encoding"],
        "base64"
    );
    assert_eq!(
        binary["documents"][0]["fields"][0]["value"]["value"],
        "/wA="
    );

    let aggregate = structured(
        call(
            &client,
            "redis_ft_aggregate",
            serde_json::json!({
                "index": first_index,
                "query": "*",
                "stages": [
                    {
                        "type": "group_by",
                        "properties": ["@category"],
                        "reducers": [{"function": "count", "alias": "count"}]
                    },
                    {
                        "type": "sort_by",
                        "fields": [{"property": "@count", "order": "desc"}]
                    }
                ],
                "limit_num": 10,
                "cursor": {"count": 1, "max_idle_ms": 5000}
            }),
        )
        .await,
    );
    assert!(aggregate["total"].as_u64().is_some_and(|total| total >= 1));
    assert!(aggregate["rows"].is_array());
    let cursor_id = aggregate["cursor_id"]
        .as_u64()
        .filter(|cursor| *cursor != 0)
        .expect("WITHCURSOR returns a continuation for four groups");
    let continued = structured(
        call(
            &client,
            "redis_ft_cursor_read",
            serde_json::json!({
                "index": first_index,
                "cursor_id": cursor_id,
                "count": 1
            }),
        )
        .await,
    );
    let cursor_id = continued["cursor_id"]
        .as_u64()
        .filter(|cursor| *cursor != 0)
        .expect("cursor remains after a second one-row page");
    let deleted = structured(
        call(
            &client,
            "redis_ft_cursor_del",
            serde_json::json!({"index": first_index, "cursor_id": cursor_id}),
        )
        .await,
    );
    assert_eq!(deleted["deleted"], true);

    let explained = structured(
        call(
            &client,
            "redis_ft_explain",
            serde_json::json!({"index": first_index, "query": "redis", "dialect": 2}),
        )
        .await,
    );
    assert!(
        explained["plan"]
            .as_str()
            .is_some_and(|plan| !plan.is_empty())
    );

    let profile = structured(
        call(
            &client,
            "redis_ft_profile",
            serde_json::json!({
                "index": first_index,
                "command": "search",
                "query": "redis",
                "limit_num": 2
            }),
        )
        .await,
    );
    assert!(profile["results"].is_array());
    assert!(profile["profile"].is_array());

    let tags = structured(
        call(
            &client,
            "redis_ft_tagvals",
            serde_json::json!({"index": first_index, "field": "category"}),
        )
        .await,
    );
    assert_eq!(tags["count"], 4);
    assert_eq!(tags["deprecated"], true);
    let budget_client =
        stack_search_client_with_budget(&redis.url, OutputBudget::new(256 * 1024, 1)).await;
    let bounded = budget_client
        .call_tool(
            "redis_ft_tagvals",
            serde_json::json!({"index": first_index, "field": "category"}),
        )
        .await
        .expect("bounded FT.TAGVALS result");
    assert!(bounded.is_error);
    assert!(format!("{bounded:?}").contains("output_limit_exceeded"));

    let synonyms = structured(
        call(
            &client,
            "redis_ft_synupdate",
            serde_json::json!({
                "index": first_index,
                "group_id": "speed",
                "terms": ["fast", "quick"]
            }),
        )
        .await,
    );
    assert_eq!(synonyms["updated"], true);
    let synonyms = structured(
        call(
            &client,
            "redis_ft_syndump",
            serde_json::json!({"index": first_index}),
        )
        .await,
    );
    assert!(synonyms["entries"].to_string().contains("speed"));

    let added = structured(
        call(
            &client,
            "redis_ft_dictadd",
            serde_json::json!({"dict": dictionary, "terms": ["redis", "valkey"]}),
        )
        .await,
    );
    assert_eq!(added["changed"], 2);
    let terms = structured(
        call(
            &client,
            "redis_ft_dictdump",
            serde_json::json!({"dict": dictionary}),
        )
        .await,
    );
    assert_eq!(terms["count"], 2);
    let removed = structured(
        call(
            &client,
            "redis_ft_dictdel",
            serde_json::json!({"dict": dictionary, "terms": ["redis", "valkey"]}),
        )
        .await,
    );
    assert_eq!(removed["changed"], 2);

    call(
        &client,
        "redis_ft_aliasadd",
        serde_json::json!({"alias": alias, "index": first_index}),
    )
    .await;
    let alias_search = structured(
        call(
            &client,
            "redis_ft_search",
            serde_json::json!({"index": alias, "query": "redis", "nocontent": true}),
        )
        .await,
    );
    assert_eq!(alias_search["total"], 12);

    call(
        &client,
        "redis_ft_create",
        serde_json::json!({
            "index": second_index,
            "on": "HASH",
            "prefixes": [prefix],
            "schema": [
                {"name": "title", "field_type": "TEXT"},
                {"name": "category", "field_type": "TAG"},
                {"name": "score", "field_type": "NUMERIC"}
            ]
        }),
    )
    .await;
    call(
        &client,
        "redis_ft_aliasupdate",
        serde_json::json!({"alias": alias, "index": second_index}),
    )
    .await;
    call(
        &client,
        "redis_ft_aliasdel",
        serde_json::json!({"alias": alias}),
    )
    .await;

    let missing_index = client
        .call_tool(
            "redis_ft_search",
            serde_json::json!({"index": format!("{first_index}-missing"), "query": "*"}),
        )
        .await
        .expect("missing Search index result");
    let malformed_query = client
        .call_tool(
            "redis_ft_search",
            serde_json::json!({"index": first_index, "query": "@title:["}),
        )
        .await
        .expect("malformed Search query result");
    assert!(missing_index.is_error);
    assert!(malformed_query.is_error);
    assert_ne!(format!("{missing_index:?}"), format!("{malformed_query:?}"));

    for index in [first_index, second_index] {
        call(
            &client,
            "redis_ft_dropindex",
            serde_json::json!({"index": index}),
        )
        .await;
    }
    for number in 0..12 {
        call(
            &client,
            "redis_del",
            serde_json::json!({"keys": [format!("{prefix}{number}")]}),
        )
        .await;
    }
}

#[tokio::test]
async fn search_acl_commands_are_enforced_without_leaking_credentials() {
    let Some(redis) = TestRedisStack::start().await else {
        return;
    };
    let suffix = std::process::id();
    let username = format!("redis-mcp-search-{suffix}");
    let password = "mcp-search-secret";
    let key = format!("redis-mcp:search:{suffix}:acl:doc");
    let prefix = format!("redis-mcp:search:{suffix}:acl:");
    let index = format!("redis-mcp-search-{suffix}-acl");

    let admin = redis::Client::open(redis.url.as_str()).expect("open Stack admin client");
    let mut connection = admin
        .get_multiplexed_async_connection()
        .await
        .expect("connect Stack admin client");
    redis::cmd("HSET")
        .arg(&key)
        .arg("title")
        .arg("Redis ACL guide")
        .query_async::<()>(&mut connection)
        .await
        .expect("seed Search ACL document");
    redis::cmd("FT.CREATE")
        .arg(&index)
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg(1)
        .arg(&prefix)
        .arg("SCHEMA")
        .arg("title")
        .arg("TEXT")
        .query_async::<()>(&mut connection)
        .await
        .expect("create Search ACL index");
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("on")
        .arg(format!(">{password}"))
        .arg("resetkeys")
        .arg("~*")
        .arg("-@all")
        .arg("+ft.search")
        .query_async::<()>(&mut connection)
        .await
        .expect("create restricted Search user");

    let authenticated_url =
        redis
            .url
            .replacen("redis://", &format!("redis://{username}:{password}@"), 1);
    let client = stack_client(&authenticated_url).await;
    let mut allowed = None;
    for _ in 0..20 {
        let result = client
            .call_tool(
                "redis_ft_search",
                serde_json::json!({"index": index, "query": "redis", "nocontent": true}),
            )
            .await
            .expect("allowed FT.SEARCH result");
        if !result.is_error
            && result
                .structured_content
                .as_ref()
                .and_then(|output| output["total"].as_u64())
                == Some(1)
        {
            allowed = Some(result);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(allowed.is_some(), "FT.SEARCH is allowed by ACL");

    let denied = client
        .call_tool("redis_ft_info", serde_json::json!({"index": index}))
        .await
        .expect("denied FT.INFO result");
    assert!(denied.is_error);
    assert!(!format!("{denied:?}").contains(password));

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<()>(&mut connection)
        .await
        .expect("delete restricted Search user");
    redis::cmd("FT.DROPINDEX")
        .arg(&index)
        .query_async::<()>(&mut connection)
        .await
        .expect("drop Search ACL index");
    redis::cmd("DEL")
        .arg(&key)
        .query_async::<()>(&mut connection)
        .await
        .expect("delete Search ACL document");
}

#[tokio::test]
async fn search_structured_replies_work_with_resp3() {
    let Some(redis) = TestRedisStack::start().await else {
        return;
    };
    let client = stack_client(&with_protocol(&redis.url, "3")).await;
    let suffix = std::process::id();
    let key = format!("redis-mcp:search:{suffix}:resp3:doc");
    let second_key = format!("redis-mcp:search:{suffix}:resp3:other");
    let prefix = format!("redis-mcp:search:{suffix}:resp3:");
    let index = format!("redis-mcp-search-{suffix}-resp3");
    call(
        &client,
        "redis_hset",
        serde_json::json!({
            "key": key,
            "fields": {"title": "Redis RESP3 search", "category": "database"}
        }),
    )
    .await;
    call(
        &client,
        "redis_vector_set_hash",
        serde_json::json!({
            "key": key, "field": "embedding", "data_type": "FLOAT32", "vector": [1.0, 0.0]
        }),
    )
    .await;
    call(
        &client,
        "redis_hset",
        serde_json::json!({
            "key": second_key,
            "fields": {"title": "Other cache", "category": "cache"}
        }),
    )
    .await;
    call(
        &client,
        "redis_vector_set_hash",
        serde_json::json!({
            "key": second_key, "field": "embedding", "data_type": "FLOAT32", "vector": [0.0, 1.0]
        }),
    )
    .await;
    call(
        &client,
        "redis_ft_create",
        serde_json::json!({
            "index": index,
            "prefixes": [prefix],
            "schema": [
                {"name": "title", "field_type": "TEXT"},
                {"name": "category", "field_type": "TAG"},
                {
                    "name": "embedding",
                    "field_type": "VECTOR",
                    "vector": {
                        "algorithm": "FLAT", "data_type": "FLOAT32", "dimensions": 2,
                        "distance_metric": "L2"
                    }
                }
            ]
        }),
    )
    .await;

    let mut search = None;
    let mut last_search_error = None;
    for _ in 0..20 {
        let result = client
            .call_tool(
                "redis_ft_search",
                serde_json::json!({"index": index, "query": "redis", "limit_num": 1}),
            )
            .await
            .expect("RESP3 FT.SEARCH result");
        if !result.is_error {
            let structured = result.structured_content.clone();
            if structured
                .as_ref()
                .and_then(|output| output["total"].as_u64())
                == Some(1)
            {
                search = structured;
                break;
            }
        }
        last_search_error = Some(result);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let search = search.unwrap_or_else(|| {
        panic!("structured RESP3 FT.SEARCH result; last error: {last_search_error:?}")
    });
    assert_eq!(search["total"], 1);
    assert_eq!(search["documents"][0]["id"], key);

    let vector = structured(
        call(
            &client,
            "redis_ft_vector_search",
            serde_json::json!({
                "index": index,
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0, 0.0],
                "top_k": 2,
                "limit_num": 2,
                "return_fields": ["title"]
            }),
        )
        .await,
    );
    assert_eq!(vector["results"][0]["id"], key);

    let aggregate = structured(
        call(
            &client,
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
                "cursor": {"count": 1}
            }),
        )
        .await,
    );
    assert_eq!(aggregate["count"], 1);
    let cursor_id = aggregate["cursor_id"].as_u64().expect("RESP3 cursor id");
    assert!(cursor_id > 0);
    let cursor_page = structured(
        call(
            &client,
            "redis_ft_cursor_read",
            serde_json::json!({"index": index, "cursor_id": cursor_id, "count": 1}),
        )
        .await,
    );
    assert_eq!(cursor_page["count"], 1);

    let profile = structured(
        call(
            &client,
            "redis_ft_profile",
            serde_json::json!({
                "index": index, "command": "search", "query": "redis", "limit_num": 1
            }),
        )
        .await,
    );
    assert!(profile["profile"].is_array() || profile["profile"].is_object());

    call(
        &client,
        "redis_ft_dropindex",
        serde_json::json!({"index": index, "delete_docs": true}),
    )
    .await;
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

async fn stack_timeseries_client(
    url: &str,
    access: AccessMode,
    output_budget: Option<OutputBudget>,
    capabilities: Option<RedisCapabilities>,
) -> Option<McpClient> {
    let executor = DirectRedis::connect(url)
        .await
        .expect("connect timeseries executor");
    let capabilities = match capabilities {
        Some(capabilities) => capabilities,
        None => {
            let discovered = executor
                .discover_capabilities()
                .await
                .expect("discover timeseries capabilities");
            if discovered.module(RedisModule::TimeSeries).status() != CapabilityStatus::Available {
                eprintln!("skipping timeseries test: the target does not provide RedisTimeSeries");
                return None;
            }
            discovered
        }
    };
    let mut builder = RedisMcp::builder(executor)
        .access(access)
        .bundles([ToolBundle::TimeSeries])
        .capabilities(capabilities);
    if let Some(output_budget) = output_budget {
        builder = builder.output_budget(output_budget);
    }
    let router = builder.build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect timeseries MCP client");
    client
        .initialize("redis-mcp-live-timeseries-test", "0")
        .await
        .expect("initialize timeseries MCP client");
    Some(client)
}

#[tokio::test]
async fn complete_timeseries_family_is_exact_bounded_and_version_gated() {
    let Some(stack) = TestRedisStack::start().await else {
        return;
    };

    // The pinned inventory must match what the live module actually
    // advertises, so drift in either direction fails loudly.
    let mut inventory_connection = redis::Client::open(stack.url.as_str())
        .expect("open inventory client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect inventory client");
    let advertised: Vec<String> = redis::cmd("COMMAND")
        .arg("LIST")
        .query_async::<Vec<String>>(&mut inventory_connection)
        .await
        .expect("list commands")
        .into_iter()
        .filter(|name| name.to_ascii_uppercase().starts_with("TS."))
        .map(|name| name.to_ascii_uppercase())
        .collect();
    if !advertised.is_empty() {
        let mut advertised = advertised;
        advertised.sort_unstable();
        let pinned: serde_json::Value = serde_json::from_str(include_str!(
            "fixtures/redis-timeseries-commands-1.12.6.json"
        ))
        .expect("pinned timeseries inventory");
        let pinned = pinned["commands"]
            .as_array()
            .expect("pinned command array")
            .iter()
            .map(|command| command["name"].as_str().expect("command name").to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            advertised, pinned,
            "live TS command surface drifted from the pinned inventory"
        );
    }

    for protocol in ["resp2", "resp3"] {
        let url = with_protocol(&stack.url, protocol);
        let Some(client) = stack_timeseries_client(&url, AccessMode::Full, None, None).await else {
            return;
        };
        let prefix = format!("redis-mcp:ts:{protocol}:{}", std::process::id());
        let sensor = format!("s{protocol}{}", std::process::id());
        let temperature = format!("{prefix}:temperature");
        let humidity = format!("{prefix}:humidity");
        let compacted = format!("{prefix}:temperature:hourly");

        // Creation, metadata, and labels.
        let created = structured(
            call(
                &client,
                "redis_ts_create",
                serde_json::json!({
                    "key": temperature,
                    "options": {
                        "retention_ms": 86_400_000_u64,
                        "duplicate_policy": "BLOCK",
                        "labels": [
                            {"name": "sensor", "value": sensor},
                            {"name": "kind", "value": "temperature"},
                        ],
                    },
                }),
            )
            .await,
        );
        assert_eq!(created["created"], true);
        let created_humidity = structured(
            call(
                &client,
                "redis_ts_create",
                serde_json::json!({
                    "key": humidity,
                    "options": {
                        "labels": [
                            {"name": "sensor", "value": sensor},
                            {"name": "kind", "value": "humidity"},
                        ],
                    },
                }),
            )
            .await,
        );
        assert_eq!(created_humidity["created"], true);

        let info = structured(
            call(
                &client,
                "redis_ts_info",
                serde_json::json!({"key": temperature}),
            )
            .await,
        );
        assert_eq!(info["attributes"]["retentionTime"], 86_400_000_u64);

        // Exact timestamps: explicit writes echo the exact integer back.
        let base = 1_700_000_000_000_i64;
        let added = structured(
            call(
                &client,
                "redis_ts_add",
                serde_json::json!({
                    "key": temperature,
                    "timestamp": base.to_string(),
                    "value": 21.5,
                }),
            )
            .await,
        );
        assert_eq!(added["timestamp"], base);
        for offset in 1..10_i64 {
            let added = structured(
                call(
                    &client,
                    "redis_ts_add",
                    serde_json::json!({
                        "key": temperature,
                        "timestamp": (base + offset * 1_000).to_string(),
                        "value": 21.5 + offset as f64,
                    }),
                )
                .await,
            );
            assert_eq!(added["timestamp"], base + offset * 1_000);
        }

        // Bulk writes keep per-sample failures aligned while others apply.
        let madd = structured(
            call(
                &client,
                "redis_ts_madd",
                serde_json::json!({
                    "samples": [
                        {"key": humidity, "timestamp": base.to_string(), "value": 40.0},
                        {"key": temperature, "timestamp": base.to_string(), "value": 99.0},
                        {"key": humidity, "timestamp": (base + 1_000).to_string(), "value": 41.5},
                    ],
                }),
            )
            .await,
        );
        assert_eq!(madd["requested"], 3);
        assert_eq!(madd["accepted"], 2);
        assert_eq!(madd["results"][0]["timestamp"], base);
        assert!(
            madd["results"][1]["error"].is_string(),
            "duplicate BLOCK policy must fail in-band: {madd:?}"
        );
        assert_eq!(madd["results"][2]["timestamp"], base + 1_000);

        // Counter adjustments report the affected timestamp.
        let counter = format!("{prefix}:counter");
        let incremented = structured(
            call(
                &client,
                "redis_ts_incrby",
                serde_json::json!({
                    "key": counter,
                    "value": 5.0,
                    "timestamp": base.to_string(),
                }),
            )
            .await,
        );
        assert_eq!(incremented["timestamp"], base);
        let decremented = structured(
            call(
                &client,
                "redis_ts_decrby",
                serde_json::json!({
                    "key": counter,
                    "value": 2.0,
                    "timestamp": (base + 1_000).to_string(),
                }),
            )
            .await,
        );
        assert_eq!(decremented["timestamp"], base + 1_000);
        let counter_latest =
            structured(call(&client, "redis_ts_get", serde_json::json!({"key": counter})).await);
        assert_eq!(counter_latest["sample"]["value"], 3.0);

        // Bounded pages with exact range continuation.
        let first_page = structured(
            call(
                &client,
                "redis_ts_range",
                serde_json::json!({"key": temperature, "count": 4}),
            )
            .await,
        );
        assert_eq!(first_page["samples"].as_array().expect("samples").len(), 4);
        assert_eq!(first_page["page"]["complete"], false);
        assert_eq!(
            first_page["page"]["continuation"]["start"],
            base + 3 * 1_000 + 1
        );
        let second_page = structured(
            call(
                &client,
                "redis_ts_range",
                serde_json::json!({
                    "key": temperature,
                    "from_timestamp": (base + 3 * 1_000 + 1).to_string(),
                    "count": 100,
                }),
            )
            .await,
        );
        assert_eq!(second_page["page"]["complete"], true);
        assert_eq!(second_page["samples"].as_array().expect("samples").len(), 6);
        assert_eq!(second_page["samples"][0]["timestamp"], base + 4_000);
        assert_eq!(second_page["samples"][0]["value"], 25.5);

        let descending = structured(
            call(
                &client,
                "redis_ts_revrange",
                serde_json::json!({"key": temperature, "count": 3}),
            )
            .await,
        );
        assert_eq!(descending["samples"][0]["timestamp"], base + 9_000);
        assert_eq!(
            descending["page"]["continuation"]["start"],
            base + 7 * 1_000 - 1
        );

        // Server-side aggregation and value filters.
        let buckets = structured(
            call(
                &client,
                "redis_ts_range",
                serde_json::json!({
                    "key": temperature,
                    "aggregation": {"aggregation": "avg", "bucket_duration_ms": 5_000},
                }),
            )
            .await,
        );
        assert!(
            buckets["samples"].as_array().expect("buckets").len() >= 2,
            "{buckets:?}"
        );
        let filtered = structured(
            call(
                &client,
                "redis_ts_range",
                serde_json::json!({
                    "key": temperature,
                    "filter_by_value": {"minimum": 30.0, "maximum": 100.0},
                }),
            )
            .await,
        );
        assert_eq!(filtered["samples"].as_array().expect("samples").len(), 1);
        assert_eq!(filtered["samples"][0]["value"], 30.5);

        // Multi-series queries with labels, filters, and grouping.
        let mrange = structured(
            call(
                &client,
                "redis_ts_mrange",
                serde_json::json!({
                    "filters": [format!("sensor={sensor}"), format!("kind=temperature")],
                    "with_labels": true,
                }),
            )
            .await,
        );
        assert_eq!(mrange["series_count"], 1);
        assert_eq!(mrange["series"][0]["key"], temperature);
        assert!(
            mrange["series"][0]["labels"]
                .as_array()
                .expect("labels")
                .iter()
                .any(|label| label["name"] == "kind" && label["value"] == "temperature"),
            "{mrange:?}"
        );
        let grouped = structured(
            call(
                &client,
                "redis_ts_mrevrange",
                serde_json::json!({
                    "filters": [format!("sensor={sensor}")],
                    "count": 1,
                    "group_by": {"label": "sensor", "reducer": "max"},
                }),
            )
            .await,
        );
        assert_eq!(grouped["series_count"], 1, "{grouped:?}");

        let mget = structured(
            call(
                &client,
                "redis_ts_mget",
                serde_json::json!({"filters": [format!("sensor={sensor}")], "with_labels": true}),
            )
            .await,
        );
        assert_eq!(mget["series_count"], 2, "{mget:?}");
        let queried = structured(
            call(
                &client,
                "redis_ts_queryindex",
                serde_json::json!({"filters": [format!("sensor={sensor}")]}),
            )
            .await,
        );
        assert_eq!(queried["key_count"], 2, "{queried:?}");

        // Compaction rules appear in metadata and are removable.
        let rule_destination = structured(
            call(
                &client,
                "redis_ts_create",
                serde_json::json!({"key": compacted}),
            )
            .await,
        );
        assert_eq!(rule_destination["created"], true);
        let rule = structured(
            call(
                &client,
                "redis_ts_createrule",
                serde_json::json!({
                    "source_key": temperature,
                    "destination_key": compacted,
                    "aggregation": "avg",
                    "bucket_duration_ms": 3_600_000_u64,
                }),
            )
            .await,
        );
        assert_eq!(rule["applied"], true);
        let info_with_rule = structured(
            call(
                &client,
                "redis_ts_info",
                serde_json::json!({"key": temperature}),
            )
            .await,
        );
        assert!(
            serde_json::to_string(&info_with_rule["attributes"]["rules"])
                .expect("serialize rules")
                .contains(&compacted),
            "{info_with_rule:?}"
        );
        let removed_rule = structured(
            call(
                &client,
                "redis_ts_deleterule",
                serde_json::json!({
                    "source_key": temperature,
                    "destination_key": compacted,
                }),
            )
            .await,
        );
        assert_eq!(removed_rule["applied"], true);

        // Alteration replaces labels and can trim by retention.
        let altered = structured(
            call(
                &client,
                "redis_ts_alter",
                serde_json::json!({
                    "key": temperature,
                    "retention_ms": 3_600_000_u64,
                    "labels": [{"name": "sensor", "value": sensor}],
                }),
            )
            .await,
        );
        assert_eq!(altered["altered"], true);
        let info_after_alter = structured(
            call(
                &client,
                "redis_ts_info",
                serde_json::json!({"key": temperature}),
            )
            .await,
        );
        assert_eq!(
            info_after_alter["attributes"]["retentionTime"],
            3_600_000_u64
        );
        assert!(
            !serde_json::to_string(&info_after_alter["attributes"]["labels"])
                .expect("serialize labels")
                .contains("temperature"),
            "label replacement must drop the old label set: {info_after_alter:?}"
        );

        // Destructive sample deletion reports the removed count.
        let deleted = structured(
            call(
                &client,
                "redis_ts_del",
                serde_json::json!({
                    "key": temperature,
                    "from_timestamp": base.to_string(),
                    "to_timestamp": (base + 4_000).to_string(),
                }),
            )
            .await,
        );
        assert_eq!(deleted["deleted_samples"], 5);

        // Wrong-type failures surface as stable tool errors.
        let plain = format!("{prefix}:plain");
        redis::cmd("SET")
            .arg(&plain)
            .arg("not-a-series")
            .query_async::<()>(&mut inventory_connection)
            .await
            .expect("seed plain string");
        let wrong_type = client
            .call_tool("redis_ts_get", serde_json::json!({"key": plain}))
            .await
            .expect("wrong-type TS.GET is a tool result");
        assert!(wrong_type.is_error, "{wrong_type:?}");

        // Oversized pages fail with the stable output-limit contract.
        let Some(budget_client) = stack_timeseries_client(
            &url,
            AccessMode::ReadOnly,
            Some(OutputBudget::new(600, 1_000)),
            None,
        )
        .await
        else {
            return;
        };
        let overflow = budget_client
            .call_tool(
                "redis_ts_range",
                serde_json::json!({"key": humidity, "count": 100}),
            )
            .await
            .expect("oversized TS.RANGE is a tool result");
        assert!(overflow.is_error, "{overflow:?}");
        assert_eq!(
            overflow.meta.as_ref().expect("timeseries output metadata")["io.redis.mcp/outputLimit"]
                ["code"],
            "output_limit_exceeded"
        );

        // Known-old module versions fail closed before execution.
        let old_capabilities = RedisCapabilities::unknown()
            .with_module_inventory([(RedisModule::TimeSeries, Some(RedisVersion::new(1, 4, 0)))]);
        let Some(old_client) =
            stack_timeseries_client(&url, AccessMode::Full, None, Some(old_capabilities)).await
        else {
            return;
        };
        let gated_latest = old_client
            .call_tool(
                "redis_ts_get",
                serde_json::json!({"key": temperature, "latest": true}),
            )
            .await
            .expect("gated LATEST is a tool result");
        assert!(gated_latest.is_error, "{gated_latest:?}");
        assert!(
            serde_json::to_string(&gated_latest)
                .expect("serialize gated LATEST")
                .contains("RedisTimeSeries 1.8"),
            "{gated_latest:?}"
        );
        let gated_del = old_client
            .call_tool(
                "redis_ts_del",
                serde_json::json!({
                    "key": temperature,
                    "from_timestamp": "-",
                    "to_timestamp": "+",
                }),
            )
            .await
            .expect("gated TS.DEL is a tool result");
        assert!(gated_del.is_error, "{gated_del:?}");
    }
}

#[tokio::test]
async fn timeseries_acl_denials_are_classified_without_leaking_credentials() {
    let Some(stack) = TestRedisStack::start().await else {
        return;
    };
    let mut admin = redis::Client::open(stack.url.as_str())
        .expect("open timeseries ACL admin client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect timeseries ACL admin client");
    let probe = DirectRedis::connect(&stack.url)
        .await
        .expect("connect timeseries ACL probe");
    if probe
        .discover_capabilities()
        .await
        .expect("discover timeseries ACL capabilities")
        .module(RedisModule::TimeSeries)
        .status()
        != CapabilityStatus::Available
    {
        eprintln!("skipping timeseries ACL test: the target does not provide RedisTimeSeries");
        return;
    }

    let key = format!("redis-mcp:ts:acl:{}", std::process::id());
    redis::cmd("TS.ADD")
        .arg(&key)
        .arg("1700000000000")
        .arg("1.5")
        .query_async::<i64>(&mut admin)
        .await
        .expect("seed ACL time series");

    let username = format!("redis-mcp-ts-acl-{}", std::process::id());
    let password = "ts-acl-secret";
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg("~*")
        .arg("+ts.get")
        .query_async::<()>(&mut admin)
        .await
        .expect("create timeseries ACL user");

    let capabilities = probe
        .discover_capabilities()
        .await
        .expect("discover capabilities for the restricted client");
    let mut restricted_url = redis::parse_redis_url(&stack.url).expect("parse stack URL");
    restricted_url
        .set_username(&username)
        .expect("set timeseries ACL username");
    restricted_url
        .set_password(Some(password))
        .expect("set timeseries ACL password");
    let Some(client) = stack_timeseries_client(
        restricted_url.as_str(),
        AccessMode::Full,
        None,
        Some(capabilities),
    )
    .await
    else {
        return;
    };

    let allowed = structured(call(&client, "redis_ts_get", serde_json::json!({"key": key})).await);
    assert_eq!(allowed["sample"]["timestamp"], 1_700_000_000_000_i64);

    let denied = client
        .call_tool(
            "redis_ts_add",
            serde_json::json!({"key": key, "value": 2.0}),
        )
        .await
        .expect("denied TS.ADD is a tool result");
    assert!(denied.is_error, "{denied:?}");
    let rendered = serde_json::to_string(&denied).expect("serialize denied TS.ADD");
    assert!(!rendered.contains(password), "{denied:?}");

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<i64>(&mut admin)
        .await
        .expect("remove timeseries ACL user");
}
