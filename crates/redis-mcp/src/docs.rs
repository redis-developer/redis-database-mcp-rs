//! Official Redis command documentation as passthrough resources.
//!
//! The redis/docs content is licensed CC BY-NC-SA 4.0, so it is conveyed at
//! read time from a pinned commit instead of being compiled into this crate.
//! The library owns the contract — URI template, pinned-inventory
//! validation, bounds, caching, redaction, and attribution — while network
//! egress lives behind the host-supplied [`RedisDocsFetcher`] boundary,
//! keeping the library core free of external fetching exactly like every
//! other side-effect boundary. Reads are enabled only when a host configures
//! a fetcher, because they introduce egress the base surface never performs.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use tower_mcp::{
    CompleteResult, McpRouter,
    protocol::{CompleteParams, CompletionReference, ReadResourceResult},
    resource::ResourceTemplateBuilder,
};

use crate::{
    executor::{RedisError, RedisErrorKind},
    raw::native_command_inventory,
};

/// The redis/docs commit whose `content/commands` tree this library version
/// was written against.
pub const DEFAULT_REDIS_DOCS_PIN: &str = "f8693349287b0efbef3c865b6f6a2aceca88594d";
/// Default ceiling for one fetched documentation page.
pub const DEFAULT_MAX_DOC_BYTES: usize = 64 * 1024;
/// Default number of fetched pages kept in the bounded in-memory cache.
pub const DEFAULT_DOC_CACHE_ENTRIES: usize = 128;
/// Default ceiling for one documentation fetch.
pub const DEFAULT_DOC_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// The documentation resource URI template.
pub const REDIS_DOCS_URI_TEMPLATE: &str = "redis-mcp://docs/commands/{command}";

const MAX_COMPLETION_RESULTS: usize = 50;

/// Bounds and pinning for documentation reads.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisDocsOptions {
    pin: String,
    max_doc_bytes: usize,
    cache_entries: usize,
    fetch_timeout: Duration,
}

impl Default for RedisDocsOptions {
    fn default() -> Self {
        Self {
            pin: DEFAULT_REDIS_DOCS_PIN.to_string(),
            max_doc_bytes: DEFAULT_MAX_DOC_BYTES,
            cache_entries: DEFAULT_DOC_CACHE_ENTRIES,
            fetch_timeout: DEFAULT_DOC_FETCH_TIMEOUT,
        }
    }
}

impl RedisDocsOptions {
    /// The pinned redis/docs commit documentation is served from.
    pub fn pin(&self) -> &str {
        &self.pin
    }

    /// Ceiling for one fetched documentation page.
    pub const fn max_doc_bytes(&self) -> usize {
        self.max_doc_bytes
    }

    /// Number of fetched pages kept in the bounded cache.
    pub const fn cache_entries(&self) -> usize {
        self.cache_entries
    }

    /// Ceiling for one documentation fetch.
    pub const fn fetch_timeout(&self) -> Duration {
        self.fetch_timeout
    }

    /// Replace the pinned redis/docs commit.
    pub fn with_pin(mut self, pin: impl Into<String>) -> Self {
        self.pin = pin.into();
        self
    }

    /// Replace the per-page byte ceiling.
    pub const fn with_max_doc_bytes(mut self, value: usize) -> Self {
        self.max_doc_bytes = value;
        self
    }

    /// Replace the cache capacity.
    pub const fn with_cache_entries(mut self, value: usize) -> Self {
        self.cache_entries = value;
        self
    }

    /// Replace the fetch timeout.
    pub const fn with_fetch_timeout(mut self, value: Duration) -> Self {
        self.fetch_timeout = value;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), RedisError> {
        if self.pin.is_empty()
            || !self
                .pin
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '.')
            || self.max_doc_bytes == 0
            || self.cache_entries == 0
            || self.fetch_timeout.is_zero()
        {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "documentation options require a well-formed pin and non-zero bounds",
            )
            .with_code("INVALID_DOCS_OPTIONS"));
        }
        Ok(())
    }
}

/// Fetches one pinned documentation page for the library.
///
/// Implementations perform the only network egress in this surface: given
/// the pinned revision and a validated command slug, return the raw markdown
/// of `content/commands/{slug}.md` from redis/docs at that revision.
/// Implementations should enforce `max_bytes` at the transport where
/// possible; the library re-checks it either way. Errors must not embed
/// response bodies.
#[async_trait]
pub trait RedisDocsFetcher: Send + Sync + 'static {
    async fn fetch_command_doc(
        &self,
        pin: &str,
        slug: &str,
        max_bytes: usize,
    ) -> Result<String, RedisError>;
}

#[async_trait]
impl<T> RedisDocsFetcher for Arc<T>
where
    T: RedisDocsFetcher + ?Sized,
{
    async fn fetch_command_doc(
        &self,
        pin: &str,
        slug: &str,
        max_bytes: usize,
    ) -> Result<String, RedisError> {
        self.as_ref().fetch_command_doc(pin, slug, max_bytes).await
    }
}

struct DocsRuntime {
    fetcher: Arc<dyn RedisDocsFetcher>,
    options: RedisDocsOptions,
    /// Lowercased slugs of the classified native command inventory: the
    /// exact commands the invocation surface can execute, and therefore the
    /// exact pages this surface serves. Unknown slugs fail closed without a
    /// fetch.
    slugs: Vec<String>,
    cache: Mutex<DocCache>,
}

impl fmt::Debug for DocsRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocsRuntime")
            .field("options", &self.options)
            .field("slugs", &self.slugs.len())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct DocCache {
    pages: HashMap<String, String>,
    order: VecDeque<String>,
}

impl DocCache {
    fn get(&self, slug: &str) -> Option<String> {
        self.pages.get(slug).cloned()
    }

    fn insert(&mut self, slug: String, page: String, capacity: usize) {
        if self.pages.contains_key(&slug) {
            return;
        }
        while self.pages.len() >= capacity {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.pages.remove(&evicted);
        }
        self.order.push_back(slug.clone());
        self.pages.insert(slug, page);
    }
}

/// Register the documentation template and completion on the router.
pub(crate) fn add_docs(
    router: McpRouter,
    fetcher: Arc<dyn RedisDocsFetcher>,
    options: RedisDocsOptions,
) -> McpRouter {
    let slugs = native_command_inventory()
        .into_iter()
        .map(|entry| entry.name.to_ascii_lowercase().replace(' ', "-"))
        .collect::<Vec<_>>();
    let runtime = Arc::new(DocsRuntime {
        fetcher,
        options,
        slugs,
        cache: Mutex::new(DocCache::default()),
    });
    let template_runtime = runtime.clone();
    let template = ResourceTemplateBuilder::new(REDIS_DOCS_URI_TEMPLATE)
        .name("redis-command-docs")
        .title("Official Redis command documentation")
        .description(
            "Official Redis documentation for one classified command, conveyed at read time from the pinned redis/docs revision (network egress to the configured fetcher). Content is © Redis Ltd., CC BY-NC-SA 4.0; unknown commands fail without fetching.",
        )
        .mime_type("text/markdown")
        .handler(move |uri: String, variables: HashMap<String, String>| {
            let runtime = template_runtime.clone();
            async move {
                let slug = variables.get("command").cloned().unwrap_or_default();
                let page = read_command_doc(&runtime, &slug).await.map_err(|error| {
                    tower_mcp::Error::tool(format!("{error} [{:?}]", error.kind()))
                })?;
                Ok(ReadResourceResult::text(uri, page))
            }
        });
    router
        .resource_template(template)
        .completion_handler(move |params: CompleteParams| {
            let runtime = runtime.clone();
            async move { Ok(complete(&runtime, &params)) }
        })
}

async fn read_command_doc(runtime: &DocsRuntime, slug: &str) -> Result<String, RedisError> {
    runtime.options.validate()?;
    if slug.is_empty()
        || slug.len() > 64
        || !slug.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
    {
        return Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            "command documentation slugs are lowercase command names, for example `get` or `zadd`",
        )
        .with_code("INVALID_DOC_SLUG"));
    }
    if !runtime.slugs.iter().any(|known| known == slug) {
        return Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            format!(
                "{slug} is not a classified Redis command in this library's inventory; no fetch was attempted"
            ),
        )
        .with_code("UNKNOWN_DOC_COMMAND"));
    }
    if let Ok(cache) = runtime.cache.lock()
        && let Some(page) = cache.get(slug)
    {
        return Ok(page);
    }
    let fetched = tokio::time::timeout(
        runtime.options.fetch_timeout,
        runtime.fetcher.fetch_command_doc(
            &runtime.options.pin,
            slug,
            runtime.options.max_doc_bytes,
        ),
    )
    .await
    .map_err(|_| {
        RedisError::new(
            RedisErrorKind::Timeout,
            format!(
                "fetching documentation for {slug} exceeded the {}ms limit",
                runtime.options.fetch_timeout.as_millis()
            ),
        )
        .with_code("DOC_FETCH_TIMEOUT")
    })??;
    if fetched.len() > runtime.options.max_doc_bytes {
        return Err(RedisError::new(
            RedisErrorKind::OutputLimit,
            format!(
                "documentation for {slug} is {} bytes; the configured ceiling is {}",
                fetched.len(),
                runtime.options.max_doc_bytes
            ),
        )
        .with_code("DOC_TOO_LARGE"));
    }
    let page = render_page(&runtime.options, slug, &fetched);
    if let Ok(mut cache) = runtime.cache.lock() {
        cache.insert(
            slug.to_string(),
            page.clone(),
            runtime.options.cache_entries,
        );
    }
    Ok(page)
}

/// Strip Hugo front matter into a small typed header and append attribution.
fn render_page(options: &RedisDocsOptions, slug: &str, raw: &str) -> String {
    let (front, body) = split_front_matter(raw);
    let mut page = String::new();
    let title = front
        .as_deref()
        .and_then(|front| front_matter_value(front, "title"))
        .unwrap_or_else(|| slug.to_ascii_uppercase().replace('-', " "));
    page.push_str(&format!("# {title}\n\n"));
    if let Some(front) = front.as_deref() {
        let mut facts = Vec::new();
        if let Some(since) = front_matter_value(front, "since") {
            facts.push(format!("since Redis {since}"));
        }
        if let Some(complexity) = front_matter_value(front, "complexity") {
            facts.push(format!("complexity {complexity}"));
        }
        if !facts.is_empty() {
            page.push_str(&format!("*{}.*\n\n", facts.join("; ")));
        }
    }
    page.push_str(body.trim_start());
    let short_pin = &options.pin[..options.pin.len().min(12)];
    page.push_str(&format!(
        "\n\n---\nSource: https://github.com/redis/docs/blob/{}/content/commands/{slug}.md (pin {short_pin}). \
         © Redis Ltd., licensed CC BY-NC-SA 4.0; conveyed unmodified apart from this header and footer.\n",
        options.pin
    ));
    page
}

fn split_front_matter(raw: &str) -> (Option<String>, &str) {
    let Some(rest) = raw.strip_prefix("---\n") else {
        return (None, raw);
    };
    let Some(end) = rest.find("\n---\n") else {
        return (None, raw);
    };
    (
        Some(rest[..end].to_string()),
        &rest[end + "\n---\n".len()..],
    )
}

fn front_matter_value(front: &str, key: &str) -> Option<String> {
    front.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.trim() != key {
            return None;
        }
        let value = value.trim().trim_matches('"').trim_matches('\'').trim();
        (!value.is_empty()).then(|| value.to_string())
    })
}

fn complete(runtime: &DocsRuntime, params: &CompleteParams) -> CompleteResult {
    let CompletionReference::Resource { uri } = &params.reference else {
        return CompleteResult::new(Vec::new());
    };
    if uri != REDIS_DOCS_URI_TEMPLATE || params.argument.name != "command" {
        return CompleteResult::new(Vec::new());
    }
    let prefix = params.argument.value.to_ascii_lowercase();
    let matches = runtime
        .slugs
        .iter()
        .filter(|slug| slug.starts_with(&prefix))
        .take(MAX_COMPLETION_RESULTS)
        .cloned()
        .collect::<Vec<_>>();
    CompleteResult::new(matches)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tower_mcp::protocol::CompletionArgument;

    use super::*;

    struct FakeFetcher {
        fetches: AtomicUsize,
        reply: String,
    }

    impl FakeFetcher {
        fn new(reply: &str) -> Arc<Self> {
            Arc::new(Self {
                fetches: AtomicUsize::new(0),
                reply: reply.to_string(),
            })
        }
    }

    #[async_trait]
    impl RedisDocsFetcher for FakeFetcher {
        async fn fetch_command_doc(
            &self,
            _pin: &str,
            _slug: &str,
            _max_bytes: usize,
        ) -> Result<String, RedisError> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(self.reply.clone())
        }
    }

    struct HangingFetcher;

    #[async_trait]
    impl RedisDocsFetcher for HangingFetcher {
        async fn fetch_command_doc(
            &self,
            _pin: &str,
            _slug: &str,
            _max_bytes: usize,
        ) -> Result<String, RedisError> {
            std::future::pending().await
        }
    }

    fn runtime(fetcher: Arc<dyn RedisDocsFetcher>, options: RedisDocsOptions) -> DocsRuntime {
        DocsRuntime {
            fetcher,
            options,
            slugs: native_command_inventory()
                .into_iter()
                .map(|entry| entry.name.to_ascii_lowercase().replace(' ', "-"))
                .collect(),
            cache: Mutex::new(DocCache::default()),
        }
    }

    const DOC: &str =
        "---\ntitle: GET\nsince: 1.0.0\ncomplexity: O(1)\n---\nReturns the value of a key.\n";

    #[tokio::test]
    async fn known_commands_render_with_header_and_attribution() {
        let fetcher = FakeFetcher::new(DOC);
        let runtime = runtime(fetcher.clone(), RedisDocsOptions::default());
        let page = read_command_doc(&runtime, "get").await.expect("GET doc");
        assert!(page.starts_with("# GET\n"), "{page}");
        assert!(
            page.contains("since Redis 1.0.0; complexity O(1)"),
            "{page}"
        );
        assert!(page.contains("Returns the value of a key."));
        assert!(page.contains("content/commands/get.md"));
        assert!(page.contains("CC BY-NC-SA 4.0"));
        assert!(page.contains(DEFAULT_REDIS_DOCS_PIN));
    }

    #[tokio::test]
    async fn unknown_and_malformed_slugs_fail_closed_without_fetching() {
        let fetcher = FakeFetcher::new(DOC);
        let runtime = runtime(fetcher.clone(), RedisDocsOptions::default());
        let unknown = read_command_doc(&runtime, "made-up-command")
            .await
            .expect_err("unknown command");
        assert_eq!(unknown.code(), Some("UNKNOWN_DOC_COMMAND"));
        let malformed = read_command_doc(&runtime, "../etc/passwd")
            .await
            .expect_err("malformed slug");
        assert_eq!(malformed.code(), Some("INVALID_DOC_SLUG"));
        let shouting = read_command_doc(&runtime, "GET")
            .await
            .expect_err("uppercase slug");
        assert_eq!(shouting.code(), Some("INVALID_DOC_SLUG"));
        assert_eq!(fetcher.fetches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn pages_are_cached_and_the_cache_is_bounded() {
        let fetcher = FakeFetcher::new(DOC);
        let runtime = runtime(
            fetcher.clone(),
            RedisDocsOptions::default().with_cache_entries(1),
        );
        read_command_doc(&runtime, "get").await.expect("first GET");
        read_command_doc(&runtime, "get").await.expect("cached GET");
        assert_eq!(fetcher.fetches.load(Ordering::SeqCst), 1);
        read_command_doc(&runtime, "set")
            .await
            .expect("SET evicts GET");
        read_command_doc(&runtime, "get")
            .await
            .expect("GET refetches");
        assert_eq!(fetcher.fetches.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn oversized_pages_and_slow_fetchers_fail_with_stable_codes() {
        let oversized = FakeFetcher::new(&"x".repeat(64));
        let runtime = runtime(
            oversized,
            RedisDocsOptions::default().with_max_doc_bytes(16),
        );
        let error = read_command_doc(&runtime, "get")
            .await
            .expect_err("oversized page");
        assert_eq!(error.code(), Some("DOC_TOO_LARGE"));

        let runtime = runtime_with_timeout();
        let error = read_command_doc(&runtime, "get")
            .await
            .expect_err("hanging fetch");
        assert_eq!(error.code(), Some("DOC_FETCH_TIMEOUT"));
    }

    fn runtime_with_timeout() -> DocsRuntime {
        runtime(
            Arc::new(HangingFetcher),
            RedisDocsOptions::default().with_fetch_timeout(Duration::from_millis(50)),
        )
    }

    #[tokio::test]
    async fn pages_without_front_matter_render_verbatim_with_attribution() {
        let fetcher = FakeFetcher::new("Just a body.\n");
        let runtime = runtime(fetcher, RedisDocsOptions::default());
        let page = read_command_doc(&runtime, "ping").await.expect("PING doc");
        assert!(page.starts_with("# PING\n"), "{page}");
        assert!(page.contains("Just a body."));
        assert!(page.contains("CC BY-NC-SA 4.0"));
    }

    #[test]
    fn completion_matches_inventory_prefixes_only_for_the_docs_template() {
        let fetcher = FakeFetcher::new(DOC);
        let runtime = runtime(fetcher, RedisDocsOptions::default());
        let complete_for = |uri: &str, value: &str| {
            complete(
                &runtime,
                &CompleteParams {
                    reference: CompletionReference::Resource {
                        uri: uri.to_string(),
                    },
                    argument: CompletionArgument {
                        name: "command".to_string(),
                        value: value.to_string(),
                    },
                    context: None,
                    meta: None,
                },
            )
        };
        let zset = complete_for(REDIS_DOCS_URI_TEMPLATE, "zadd");
        assert!(zset.completion.values.contains(&"zadd".to_string()));
        let bounded = complete_for(REDIS_DOCS_URI_TEMPLATE, "");
        assert!(bounded.completion.values.len() <= MAX_COMPLETION_RESULTS);
        let foreign = complete_for("redis-mcp://guidance/{slug}", "za");
        assert!(foreign.completion.values.is_empty());
    }

    #[test]
    fn options_validate_pin_and_bounds() {
        assert!(RedisDocsOptions::default().validate().is_ok());
        let error = RedisDocsOptions::default()
            .with_pin("../evil")
            .validate()
            .expect_err("path-like pins are rejected");
        assert_eq!(error.code(), Some("INVALID_DOCS_OPTIONS"));
        let error = RedisDocsOptions::default()
            .with_max_doc_bytes(0)
            .validate()
            .expect_err("zero byte ceiling");
        assert_eq!(error.code(), Some("INVALID_DOCS_OPTIONS"));
    }
}
