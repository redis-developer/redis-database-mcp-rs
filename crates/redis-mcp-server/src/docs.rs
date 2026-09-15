//! HTTP fetcher for the library's passthrough documentation surface.
//!
//! Fetches `content/commands/{slug}.md` from the pinned redis/docs revision
//! on raw.githubusercontent.com. This is the server's only outbound HTTP
//! egress and exists behind both the `docs` Cargo feature and the explicit
//! `[docs] enabled` runtime opt-in; the library re-validates slugs, bounds,
//! and attribution on every page it serves.

use async_trait::async_trait;
use redis_mcp::{RedisDocsFetcher, RedisError, RedisErrorKind};

pub(crate) struct HttpDocsFetcher {
    client: reqwest::Client,
}

impl HttpDocsFetcher {
    pub(crate) fn new() -> Result<Self, tower_mcp::BoxError> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("redis-mcp-server/", env!("CARGO_PKG_VERSION")))
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("cannot build the documentation HTTP client: {error}"))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl RedisDocsFetcher for HttpDocsFetcher {
    async fn fetch_command_doc(
        &self,
        pin: &str,
        slug: &str,
        max_bytes: usize,
    ) -> Result<String, RedisError> {
        let url = format!(
            "https://raw.githubusercontent.com/redis/docs/{pin}/content/commands/{slug}.md"
        );
        let mut response = self.client.get(&url).send().await.map_err(|error| {
            // reqwest errors can embed the URL; report the category only.
            let category = if error.is_timeout() {
                "timed out"
            } else if error.is_connect() {
                "connection failed"
            } else {
                "request failed"
            };
            RedisError::new(
                RedisErrorKind::Connection,
                format!("documentation fetch for {slug} {category}"),
            )
            .with_code("DOC_FETCH_FAILED")
        })?;
        match response.status() {
            status if status.is_success() => {}
            status if status.as_u16() == 404 => {
                return Err(RedisError::new(
                    RedisErrorKind::InvalidRequest,
                    format!("no documentation page for {slug} exists at the pinned revision"),
                )
                .with_code("DOC_NOT_FOUND"));
            }
            status => {
                return Err(RedisError::new(
                    RedisErrorKind::Server,
                    format!("documentation fetch for {slug} returned HTTP {status}"),
                )
                .with_code("DOC_FETCH_FAILED"));
            }
        }
        if let Some(length) = response.content_length()
            && length > max_bytes as u64
        {
            return Err(RedisError::new(
                RedisErrorKind::OutputLimit,
                format!(
                    "documentation for {slug} advertises {length} bytes; the ceiling is {max_bytes}"
                ),
            )
            .with_code("DOC_TOO_LARGE"));
        }
        let mut body = Vec::with_capacity(
            response
                .content_length()
                .unwrap_or_default()
                .min(max_bytes as u64) as usize,
        );
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            RedisError::new(
                RedisErrorKind::Connection,
                format!("documentation body for {slug} could not be read"),
            )
            .with_code("DOC_FETCH_FAILED")
        })? {
            let body_bytes = body.len().checked_add(chunk.len()).ok_or_else(|| {
                RedisError::new(
                    RedisErrorKind::OutputLimit,
                    format!("documentation for {slug} exceeds the {max_bytes}-byte ceiling"),
                )
                .with_code("DOC_TOO_LARGE")
            })?;
            if body_bytes > max_bytes {
                return Err(RedisError::new(
                    RedisErrorKind::OutputLimit,
                    format!("documentation for {slug} exceeds the {max_bytes}-byte ceiling"),
                )
                .with_code("DOC_TOO_LARGE"));
            }
            body.extend_from_slice(&chunk);
        }
        String::from_utf8(body).map_err(|_| {
            RedisError::new(
                RedisErrorKind::InvalidResponse,
                format!("documentation for {slug} is not valid UTF-8"),
            )
            .with_code("DOC_FETCH_FAILED")
        })
    }
}
