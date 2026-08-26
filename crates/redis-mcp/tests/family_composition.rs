use async_trait::async_trait;
#[cfg(all(feature = "strings", not(feature = "hashes")))]
use redis_mcp::RedisMcpBuildError;
#[cfg(not(feature = "hashes"))]
use redis_mcp::tool_catalog;
use redis_mcp::{
    AccessMode, RedisCommand, RedisError, RedisErrorKind, RedisExecutor, RedisMcp, RedisValue,
    ToolFamily, tool_names_for_families,
};
use schemars::JsonSchema;
use serde::Deserialize;
use tower_mcp::{
    CallToolResult, McpRouter, ToolBuilder,
    client::{ChannelTransport, McpClient},
};

#[derive(Clone, Copy)]
struct StubRedis;

#[async_trait]
impl RedisExecutor for StubRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        Err(RedisError::new(
            RedisErrorKind::Other,
            format!("{} is not executed by this catalog test", command.name()),
        ))
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HostStatusInput {}

async fn listed_names(router: McpRouter) -> Vec<String> {
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect family-composition client");
    client
        .initialize("redis-mcp-family-test", "0")
        .await
        .expect("initialize family-composition client");
    client
        .list_tools()
        .await
        .expect("list composed tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect()
}

#[cfg(feature = "strings")]
#[tokio::test]
async fn selected_family_merges_with_a_host_router() {
    let redis = RedisMcp::builder(StubRedis)
        .access(AccessMode::Full)
        .family(ToolFamily::Strings)
        .build();
    let host = McpRouter::new()
        .server_info("composed-host", "1")
        .tool(
            ToolBuilder::new("host_status")
                .description("Return host status.")
                .handler(|_input: HostStatusInput| async { Ok(CallToolResult::text("ready")) })
                .build(),
        )
        .merge(redis);

    let names = listed_names(host).await;
    assert!(names.iter().any(|name| name == "host_status"));
    assert!(names.iter().any(|name| name == "redis_get"));
    assert!(names.iter().any(|name| name == "redis_digest"));
    assert!(!names.iter().any(|name| name == "redis_scan"));
    assert!(!names.iter().any(|name| name == "redis_hget"));
    #[cfg(not(feature = "hashes"))]
    assert!(
        tool_catalog()
            .iter()
            .all(|metadata| metadata.name != "redis_hget")
    );

    let expected = tool_names_for_families(AccessMode::Full, [ToolFamily::Strings]);
    let redis_names = names
        .iter()
        .filter_map(|name| name.starts_with("redis_").then_some(name.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(redis_names, expected);
}

#[cfg(feature = "full")]
#[tokio::test]
async fn all_families_match_the_family_catalog_without_cross_cutting_tools() {
    let router = RedisMcp::builder(StubRedis)
        .access(AccessMode::Full)
        .families(ToolFamily::ALL.iter().copied())
        .build();
    let names = listed_names(router).await;
    assert_eq!(
        names,
        tool_names_for_families(AccessMode::Full, ToolFamily::ALL.iter().copied())
    );
    assert!(!names.iter().any(|name| name == "redis_info"));
    assert!(!names.iter().any(|name| name == "redis_command"));
}

#[cfg(all(feature = "strings", not(feature = "hashes")))]
#[test]
fn selecting_a_family_omitted_at_compile_time_fails_closed() {
    let result = RedisMcp::builder(StubRedis)
        .family(ToolFamily::Hashes)
        .try_build();
    assert!(matches!(
        result,
        Err(RedisMcpBuildError::FamilyNotCompiled(ToolFamily::Hashes))
    ));
}
