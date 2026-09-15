//! End-to-end documentation passthrough against the real pinned redis/docs
//! revision.
//!
//! Gated on `REDIS_MCP_DOCS_LIVE_TEST` because it performs the server's only
//! outbound HTTPS egress, and on a Redis target like every other live test.

#![cfg(feature = "docs")]

use tower_mcp::client::{McpClient, StdioClientTransport};

#[cfg(unix)]
use redis_server_wrapper::{Error as RedisServerError, RedisServer};

#[tokio::test]
async fn documentation_round_trips_through_the_stdio_server() {
    if std::env::var("REDIS_MCP_DOCS_LIVE_TEST").is_err() {
        eprintln!("skipping docs live test: REDIS_MCP_DOCS_LIVE_TEST is not set");
        return;
    }
    #[cfg(unix)]
    let mut managed = None;
    let url = if let Ok(url) = std::env::var("REDIS_URL") {
        url
    } else {
        #[cfg(unix)]
        {
            let directory = tempfile::tempdir().expect("create docs live-test Redis directory");
            match RedisServer::new()
                .auto_port()
                .bind("127.0.0.1")
                .dir(directory.path())
                .no_stack_modules()
                .start()
                .await
            {
                Ok(server) => {
                    let url = format!("redis://127.0.0.1:{}/", server.port());
                    managed = Some((server, directory));
                    url
                }
                Err(RedisServerError::BinaryNotFound { binary }) => {
                    eprintln!(
                        "skipping docs live test: REDIS_URL is not set and {binary} is not on PATH"
                    );
                    return;
                }
                Err(error) => panic!("start wrapper-managed Redis for docs test: {error}"),
            }
        }
        #[cfg(not(unix))]
        {
            eprintln!(
                "skipping docs live test: REDIS_URL is not set and self-hosting requires Unix"
            );
            return;
        }
    };

    let binary = env!("CARGO_BIN_EXE_redis-mcp-server");
    let transport =
        StdioClientTransport::spawn(binary, &["--url", &url, "--enable-docs", "--stdio"])
            .await
            .expect("spawn redis-mcp-server with docs enabled");
    let client = McpClient::connect(transport)
        .await
        .expect("connect stdio MCP client");
    client
        .initialize("redis-mcp-docs-live-test", "0")
        .await
        .expect("initialize stdio MCP client");

    let templates = client
        .list_resource_templates()
        .await
        .expect("list resource templates");
    assert!(
        templates
            .resource_templates
            .iter()
            .any(|template| template.uri_template == redis_mcp::REDIS_DOCS_URI_TEMPLATE),
        "{templates:?}"
    );

    let page = client
        .read_resource("redis-mcp://docs/commands/get")
        .await
        .expect("read official GET documentation");
    let content = serde_json::to_value(page.contents.first().expect("doc content"))
        .expect("serialize doc content");
    let text = content["text"].as_str().expect("doc text");
    assert!(text.starts_with("# GET"), "{text}");
    assert!(
        text.to_ascii_lowercase().contains("value of"),
        "official GET page should describe returning the value: {text}"
    );
    assert!(text.contains("CC BY-NC-SA 4.0"));
    assert!(text.contains(redis_mcp::DEFAULT_REDIS_DOCS_PIN));

    let unknown = client
        .read_resource("redis-mcp://docs/commands/not-a-command")
        .await
        .expect_err("unknown commands fail closed");
    assert!(unknown.to_string().contains("UNKNOWN_DOC_COMMAND"));

    let bounded_transport = StdioClientTransport::spawn(
        binary,
        &[
            "--url",
            &url,
            "--enable-docs",
            "--docs-max-bytes",
            "128",
            "--stdio",
        ],
    )
    .await
    .expect("spawn redis-mcp-server with a small docs byte ceiling");
    let bounded_client = McpClient::connect(bounded_transport)
        .await
        .expect("connect bounded docs MCP client");
    bounded_client
        .initialize("redis-mcp-docs-bounds-test", "0")
        .await
        .expect("initialize bounded docs MCP client");
    let oversized = bounded_client
        .read_resource("redis-mcp://docs/commands/get")
        .await
        .expect_err("the concrete HTTP fetcher enforces the byte ceiling");
    assert!(oversized.to_string().contains("DOC_TOO_LARGE"));

    #[cfg(unix)]
    drop(managed);
}
