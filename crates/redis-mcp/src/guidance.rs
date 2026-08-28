//! Curated Redis expertise exposed as MCP prompts and resources.
//!
//! Tools answer "what can I run"; this module answers "what should I do".
//! Static, versioned guides ship compiled into the library so every consumer
//! of the router gets the same reviewed content, live resources describe the
//! actual configured surface, and prompts turn common operational questions
//! into guided workflows that reference the catalog's exact tool names.
//! Nothing here fetches external content, and every document is bounded.

use std::collections::HashMap;
use std::sync::Arc;

use tower_mcp::{
    McpRouter,
    prompt::{Prompt, PromptBuilder},
    protocol::{Content, GetPromptResult, PromptMessage, PromptRole},
    resource::ResourceBuilder,
};

use crate::capabilities::{RedisCapabilities, RedisDeployment};
use crate::catalog::{RedisModule, tool_catalog};

/// Maximum bytes for one compiled-in guidance document.
///
/// Documents are static and reviewed, so the bound is a regression guard:
/// a guide that outgrows it should be split, not shipped.
pub const MAX_GUIDANCE_DOC_BYTES: usize = 16 * 1024;

/// One compiled-in guidance document.
#[derive(Debug, Clone, Copy)]
pub struct GuidanceDoc {
    /// Stable URI path segment under `redis-mcp://guidance/`.
    pub slug: &'static str,
    /// Human-readable title.
    pub title: &'static str,
    /// One-line summary shown in `resources/list`.
    pub description: &'static str,
    /// Markdown content, compiled into the library.
    pub content: &'static str,
}

impl GuidanceDoc {
    /// The document's resource URI.
    pub fn uri(&self) -> String {
        format!("redis-mcp://guidance/{}", self.slug)
    }
}

/// Every compiled-in guidance document, in presentation order.
pub const GUIDANCE_DOCS: &[GuidanceDoc] = &[
    GuidanceDoc {
        slug: "data-modeling",
        title: "Redis data modeling",
        description: "Choose keys and structures for a workload: key design, structure selection, bounded reads, and empirical verification.",
        content: include_str!("../guidance/data-modeling.md"),
    },
    GuidanceDoc {
        slug: "memory-tuning",
        title: "Redis memory tuning",
        description: "Measure memory pressure, interpret fragmentation and eviction, choose an eviction policy, and shrink the dataset.",
        content: include_str!("../guidance/memory-tuning.md"),
    },
    GuidanceDoc {
        slug: "latency-diagnosis",
        title: "Redis latency diagnosis",
        description: "A working order for latency incidents: slowlog and latency events, cause matching, bounded MONITOR, and structural fixes.",
        content: include_str!("../guidance/latency-diagnosis.md"),
    },
    GuidanceDoc {
        slug: "search-index-design",
        title: "Redis Query Engine index design",
        description: "Design Search indexes from the queries: field types, HASH versus JSON, vector fields, querying, and index operations.",
        content: include_str!("../guidance/search-index-design.md"),
    },
    GuidanceDoc {
        slug: "cluster-key-design",
        title: "Redis Cluster key design",
        description: "Hash tags, slots, what same-slot enables, anti-patterns, and how cluster changes the meaning of keyspace operations.",
        content: include_str!("../guidance/cluster-key-design.md"),
    },
    GuidanceDoc {
        slug: "expiration-strategies",
        title: "Redis expiration strategies",
        description: "TTL mechanics, hash-field expiration, cache/session/lock patterns, and the pitfalls checklist.",
        content: include_str!("../guidance/expiration-strategies.md"),
    },
    GuidanceDoc {
        slug: "pipeline-vs-transaction",
        title: "Batching, transactions, and scripts",
        description: "Multi-key commands, bulk workflows, MULTI/EXEC, and scripts: decision rules, costs, and durability boundaries.",
        content: include_str!("../guidance/pipeline-vs-transaction.md"),
    },
];

/// Register guidance prompts and resources on the router.
///
/// The capability snapshot renders into the live `redis-mcp://capabilities`
/// resource so clients can read what the target supports without a tool
/// call.
pub(crate) fn add_guidance(
    mut router: McpRouter,
    capabilities: &Arc<RedisCapabilities>,
) -> McpRouter {
    router = router.resource(index_resource());
    for doc in GUIDANCE_DOCS {
        router = router.resource(
            ResourceBuilder::new(doc.uri())
                .name(format!("guidance-{}", doc.slug))
                .title(doc.title)
                .description(doc.description)
                .mime_type("text/markdown")
                .text(doc.content),
        );
    }
    router = router.resource(catalog_resource());
    router = router.resource(capabilities_resource(capabilities));
    for prompt in guidance_prompts() {
        router = router.prompt(prompt);
    }
    router
}

fn index_resource() -> tower_mcp::resource::Resource {
    let mut index = String::from(
        "# Redis guidance\n\nCurated, versioned Redis operational guides compiled into this server.\n\n",
    );
    for doc in GUIDANCE_DOCS {
        index.push_str(&format!(
            "- `{}` — {}: {}\n",
            doc.uri(),
            doc.title,
            doc.description
        ));
    }
    index.push_str(
        "\nLive descriptions of this server:\n\
         - `redis-mcp://catalog` — every advertised tool with bundle, access tier, and capability requirements\n\
         - `redis-mcp://capabilities` — the Redis version, deployment, and module snapshot this router was built with\n",
    );
    ResourceBuilder::new("redis-mcp://guidance")
        .name("guidance-index")
        .title("Redis guidance index")
        .description("Index of the compiled-in Redis guides and live server-description resources.")
        .mime_type("text/markdown")
        .text(index)
}

fn catalog_resource() -> tower_mcp::resource::Resource {
    let tools = tool_catalog()
        .iter()
        .map(|metadata| {
            let requirements = metadata.capability_requirements();
            serde_json::json!({
                "name": metadata.name,
                "bundle": metadata.bundle.as_str(),
                "required_access": metadata.required_access.as_str(),
                "requires_raw_opt_in": metadata.requires_raw_opt_in,
                "minimum_redis_version": requirements
                    .minimum_redis_version()
                    .map(|version| version.to_string()),
                "required_module": requirements.required_module().map(RedisModule::as_str),
                "deployment": requirements.deployment().as_str(),
            })
        })
        .collect::<Vec<_>>();
    ResourceBuilder::new("redis-mcp://catalog")
        .name("tool-catalog")
        .title("Redis MCP tool catalog")
        .description("Every curated tool this library version knows, with bundle, access tier, raw opt-in, and capability requirements. Runtime selection and access mode decide which subset a given router advertises.")
        .json(serde_json::json!({ "tools": tools }))
}

fn capabilities_resource(capabilities: &Arc<RedisCapabilities>) -> tower_mcp::resource::Resource {
    let modules = [
        RedisModule::Json,
        RedisModule::Search,
        RedisModule::TimeSeries,
    ]
    .iter()
    .map(|module| {
        serde_json::json!({
            "module": module.as_str(),
            "version": capabilities
                .module(*module)
                .version()
                .map(|version| version.to_string()),
        })
    })
    .collect::<Vec<_>>();
    let snapshot = serde_json::json!({
        "redis_version": capabilities
            .redis_version()
            .map(|version| version.to_string()),
        "deployment": match capabilities.deployment() {
            RedisDeployment::Standalone => "standalone",
            RedisDeployment::Cluster => "cluster",
            _ => "unknown",
        },
        "modules": modules,
        "note": "Snapshot taken when this router was built; null fields were not discovered.",
    });
    ResourceBuilder::new("redis-mcp://capabilities")
        .name("target-capabilities")
        .title("Redis target capability snapshot")
        .description("The Redis version, deployment topology, and module versions this router was configured with. Tools preflight against this snapshot.")
        .json(snapshot)
}

fn user_prompt(description: &str, text: String) -> GetPromptResult {
    GetPromptResult {
        description: Some(description.to_string()),
        messages: vec![PromptMessage {
            role: PromptRole::User,
            content: Content::text(text),
            meta: None,
        }],
        meta: None,
    }
}

fn argument<'a>(arguments: &'a HashMap<String, String>, name: &str, fallback: &'a str) -> &'a str {
    arguments
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(fallback)
}

pub(crate) fn guidance_prompts() -> Vec<Prompt> {
    vec![
        diagnose_latency_prompt(),
        review_memory_prompt(),
        plan_data_model_prompt(),
        design_search_index_prompt(),
        plan_bulk_load_prompt(),
    ]
}

fn diagnose_latency_prompt() -> Prompt {
    PromptBuilder::new("redis_diagnose_latency")
        .title("Diagnose Redis latency")
        .description(
            "Guided latency investigation: establish a baseline, read the server's own evidence, match findings to causes, and propose bounded fixes.",
        )
        .optional_arg(
            "symptom",
            "What was observed (for example: p99 spikes every 5 minutes, one endpoint slow).",
        )
        .handler(|arguments: HashMap<String, String>| async move {
            let symptom = argument(&arguments, "symptom", "unspecified latency complaints");
            Ok(user_prompt(
                "Guided Redis latency investigation",
                format!(
                    "Diagnose Redis latency. Reported symptom: {symptom}.\n\n\
                     Work in this order and report evidence at each step:\n\
                     1. Baseline: call redis_ping for measured round-trip latency and redis_health_check for role, persistence, and load. If ping is fast, say so — the problem may be outside Redis.\n\
                     2. Server evidence: call redis_slowlog for slow command shapes, redis_latency_history and redis_latency_overview for spike events, and redis_info for ops/sec, connected_clients, and blocked_clients.\n\
                     3. Match findings to causes using the playbook in the resource redis-mcp://guidance/latency-diagnosis (read it): O(N) commands, fork/aof events, expiration bursts, hot keys (redis_hotkeys), swapping (redis_memory_summary fragmentation below 1.0).\n\
                     4. Only if still unexplained, observe live traffic with a bounded MONITOR session: redis_monitor_start, a few redis_monitor_read pages, then always redis_monitor_close.\n\
                     5. Recommend fixes tied to the evidence, preferring bounded reads (redis_scan, redis_hscan), batching (redis_mget, redis_transaction), and TTL jitter over configuration changes; any redis_config_set change must state its service impact.\n\n\
                     Report: baseline numbers, the strongest evidence found, the diagnosed cause (or the classes ruled out), and concrete next actions."
                ),
            ))
        })
        .build()
}

fn review_memory_prompt() -> Prompt {
    PromptBuilder::new("redis_review_memory")
        .title("Review Redis memory")
        .description(
            "Guided memory review: usage, fragmentation, eviction posture, biggest costs, and dataset-reduction options.",
        )
        .optional_arg(
            "focus",
            "Optional area to prioritize (for example: fragmentation, eviction policy, one keyspace prefix).",
        )
        .handler(|arguments: HashMap<String, String>| async move {
            let focus = argument(&arguments, "focus", "a full review");
            Ok(user_prompt(
                "Guided Redis memory review",
                format!(
                    "Review this Redis target's memory. Requested focus: {focus}.\n\n\
                     1. Measure: redis_memory_summary (used, peak, fragmentation, evictions), redis_memory_stats (dataset versus overhead, client buffers, replication), redis_keyspace_summary (keys and TTL coverage per database).\n\
                     2. Interpret using redis-mcp://guidance/memory-tuning (read it): headroom against maxmemory, fragmentation around 1.0-1.5 is healthy, below 1.0 means swapping, climbing evicted_keys means undersized memory or wrong policy.\n\
                     3. Sample the expensive keys: redis_hotkeys for traffic, redis_key_summary and redis_memory_usage for the biggest suspects, redis_object_inspect for encodings that fell out of compact representations.\n\
                     4. Check the eviction posture with redis_config_get (maxmemory, maxmemory-policy) and state whether the policy matches how the data is used (cache versus state, TTL coverage from step 1).\n\
                     5. Recommend: TTLs with jitter where missing (redis_expire, redis_hexpire on 7.4+), structure changes that restore compact encodings, redis_unlink for bulk deletion, and only then configuration changes — redis_config_set requires explicit confirmation and a stated service impact.\n\n\
                     Report the numbers, the two or three largest cost drivers, and prioritized actions."
                ),
            ))
        })
        .build()
}

fn plan_data_model_prompt() -> Prompt {
    PromptBuilder::new("redis_plan_data_model")
        .title("Plan a Redis data model")
        .description(
            "Design keys and structures for a described workload, then verify the design empirically with seeded data.",
        )
        .required_arg(
            "workload",
            "The data and access patterns to model (entities, cardinalities, reads, writes, retention).",
        )
        .handler(|arguments: HashMap<String, String>| async move {
            let workload = argument(&arguments, "workload", "an unspecified workload — ask for entities, access patterns, and retention before designing");
            Ok(user_prompt(
                "Guided Redis data-model design",
                format!(
                    "Design a Redis data model for this workload: {workload}\n\n\
                     Method:\n\
                     1. Read redis-mcp://guidance/data-modeling and, if the target is clustered (check redis-mcp://capabilities), redis-mcp://guidance/cluster-key-design.\n\
                     2. Propose keys and structures: key scheme per entity, structure choice with the reason (hash versus JSON, list versus stream, sorted-set score semantics), TTL policy per key class (redis-mcp://guidance/expiration-strategies), and hash tags for any keys that must interact on Cluster.\n\
                     3. State the access paths as tool calls (for example redis_hset + redis_hmget, redis_zadd + redis_zrange with explicit bounds) and show that every read is bounded — cursor pages or explicit ranges, never whole-structure reads that grow with the data.\n\
                     4. Verify empirically: seed a deterministic sample with redis_bulk_seed, measure with redis_memory_usage and redis_object_inspect (encodings), and exercise the read paths.\n\
                     5. Call out growth risks: which structure grows unbounded without redis_ltrim/redis_xtrim, which multi-key operations require same-slot keys, and what changes if cardinality grows 100x.\n\n\
                     Deliver: the key scheme, structure table with rationale, the tool-level access paths, and the verification results or plan."
                ),
            ))
        })
        .build()
}

fn design_search_index_prompt() -> Prompt {
    PromptBuilder::new("redis_design_search_index")
        .title("Design a Redis Search index")
        .description(
            "Design a Query Engine index from the queries it must serve: field types, document shape, vector parameters, and validation.",
        )
        .required_arg(
            "queries",
            "The queries the index must serve, in plain language or query syntax.",
        )
        .optional_arg(
            "data_description",
            "What the documents look like (hash or JSON, fields, cardinalities, vector dimensions).",
        )
        .handler(|arguments: HashMap<String, String>| async move {
            let queries = argument(&arguments, "queries", "unspecified queries — ask what must be searchable before designing");
            let data = argument(&arguments, "data_description", "undescribed documents — inspect a sample with redis_scan and redis_hgetall or redis_json_get first");
            Ok(user_prompt(
                "Guided Search index design",
                format!(
                    "Design a Redis Query Engine index.\nQueries to serve: {queries}\nDocuments: {data}\n\n\
                     Method:\n\
                     1. Confirm the Search capability from redis-mcp://capabilities and read redis-mcp://guidance/search-index-design.\n\
                     2. Derive the field list strictly from the queries: tag for exact categorical matches, text only where humans search words, numeric for ranges and sorting, geo for radius, vector (flat versus hnsw with explicit dimension and metric) for similarity — with the reasoning per field.\n\
                     3. Choose document shape (on: hash versus on: json) and a key prefix so only collection keys are indexed.\n\
                     4. Write the redis_ft_create call, create the index, and verify with redis_ft_info (indexing progress, doc counts, field shapes).\n\
                     5. Validate each target query with redis_ft_search or redis_ft_aggregate using explicit limits and return_fields; inspect plans with redis_ft_explain and timing with redis_ft_profile where relevance or cost is unclear.\n\
                     6. Recommend operating practice: query through an alias (redis_ft_aliasadd), evolve additively with redis_ft_alter, rebuild-and-swap for breaking changes.\n\n\
                     Deliver: the schema with per-field rationale, the create call, and validation results for every query."
                ),
            ))
        })
        .build()
}

fn plan_bulk_load_prompt() -> Prompt {
    PromptBuilder::new("redis_plan_bulk_load")
        .title("Plan a bounded Redis bulk load")
        .description(
            "Plan and execute a bounded dataset load: record mapping, dry-run validation, batching, error policy, and verification.",
        )
        .required_arg(
            "dataset",
            "What is being loaded (record shape, approximate count, target structures, expirations).",
        )
        .handler(|arguments: HashMap<String, String>| async move {
            let dataset = argument(&arguments, "dataset", "an unspecified dataset — ask for record shape, count, and target structures first");
            Ok(user_prompt(
                "Guided bounded bulk load",
                format!(
                    "Plan a bounded bulk load into Redis for: {dataset}\n\n\
                     Method:\n\
                     1. Read redis-mcp://guidance/pipeline-vs-transaction for what bulk loading does and does not guarantee (bounded batches, per-record identity, no atomicity claims), and redis-mcp://guidance/data-modeling for the target structures. On Cluster targets, records route per key — check redis-mcp://capabilities.\n\
                     2. Map records to redis_bulk_load structured records (string, hash, list, set, sorted_set, JSON, vector) with expirations where the model calls for them; for synthetic or test data prefer deterministic redis_bulk_seed with an explicit seed.\n\
                     3. Validate first: run redis_bulk_load with dry_run true and confirm the plan (record count, command count) with zero writes.\n\
                     4. Load with explicit bounds — batch_size, concurrency, and an error policy chosen deliberately: stop-on-error for all-or-mostly loads, continue-on-error when partial progress is acceptable and failures will be reconciled from the report's per-record identities.\n\
                     5. Verify: spot-check records (redis_get, redis_hmget, redis_zscore), compare counts with redis_dbsize or redis_scan pages, and check memory cost with redis_memory_usage; clean up any staging keys with redis_unlink.\n\n\
                     Report: the mapping, the dry-run plan, the applied/failed/skipped counts from each load call, and verification results. Never echo full record payloads back into the conversation."
                ),
            ))
        })
        .build()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn referenced_tool_names(text: &str) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        let bytes = text.as_bytes();
        let mut index = 0;
        while let Some(offset) = text[index..].find("redis_") {
            let start = index + offset;
            let mut end = start;
            while end < bytes.len()
                && (bytes[end].is_ascii_lowercase()
                    || bytes[end].is_ascii_digit()
                    || bytes[end] == b'_')
            {
                end += 1;
            }
            // Skip the crate name in prose like "redis_mcp" if it ever
            // appears; only catalog-shaped names are validated.
            names.insert(text[start..end].to_string());
            index = end;
        }
        names
    }

    #[test]
    fn every_referenced_tool_name_exists_in_the_catalog() {
        let catalog = tool_catalog()
            .iter()
            .map(|tool| tool.name)
            .collect::<BTreeSet<_>>();
        let mut sources = GUIDANCE_DOCS
            .iter()
            .map(|doc| (doc.uri(), doc.content.to_string()))
            .collect::<Vec<_>>();
        for prompt in guidance_prompts() {
            let name = prompt.definition().name.clone();
            let rendered = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("test runtime")
                .block_on(prompt.get(HashMap::new()))
                .expect("prompt renders without arguments");
            let text = rendered
                .messages
                .iter()
                .filter_map(|message| match &message.content {
                    Content::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            sources.push((format!("prompt:{name}"), text));
        }
        for (source, text) in sources {
            for name in referenced_tool_names(&text) {
                assert!(
                    catalog.contains(name.as_str()),
                    "{source} references unknown tool {name}"
                );
            }
        }
    }

    #[test]
    fn guidance_docs_are_bounded_unique_and_self_described() {
        let mut slugs = BTreeSet::new();
        for doc in GUIDANCE_DOCS {
            assert!(
                doc.content.len() <= MAX_GUIDANCE_DOC_BYTES,
                "{} is {} bytes; split it instead of growing past {MAX_GUIDANCE_DOC_BYTES}",
                doc.slug,
                doc.content.len()
            );
            assert!(slugs.insert(doc.slug), "duplicate slug {}", doc.slug);
            assert!(!doc.title.is_empty() && !doc.description.is_empty());
            assert!(
                doc.content.starts_with("# "),
                "{} must start with a title heading",
                doc.slug
            );
        }
    }

    #[test]
    fn cross_references_between_guides_resolve() {
        let uris = GUIDANCE_DOCS
            .iter()
            .map(|doc| doc.uri())
            .collect::<BTreeSet<_>>();
        for doc in GUIDANCE_DOCS {
            let mut index = 0;
            while let Some(offset) = doc.content[index..].find("redis-mcp://guidance/") {
                let start = index + offset;
                let end = doc.content[start..]
                    .find(|character: char| {
                        !character.is_ascii_alphanumeric()
                            && character != '-'
                            && character != '/'
                            && character != ':'
                    })
                    .map_or(doc.content.len(), |position| start + position);
                let uri = &doc.content[start..end];
                assert!(
                    uris.contains(uri),
                    "{} references missing guide {uri}",
                    doc.slug
                );
                index = end;
            }
        }
    }
}
