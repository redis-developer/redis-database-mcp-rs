//! Durable, cluster-safe agent handoff contracts.

use std::{collections::HashMap, fmt, sync::Arc};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tower_mcp::{
    McpRouter,
    prompt::PromptBuilder,
    protocol::{Content, GetPromptResult, PromptMessage, PromptRole},
    resource::ResourceBuilder,
};

/// Stable resource URI for the durable handoff guide.
pub const COORDINATION_GUIDE_URI: &str = "redis-mcp://guidance/agent-handoffs";

/// Default number of independent capability inbox shards.
pub const DEFAULT_COORDINATION_SHARDS: u16 = 16;
/// Default maximum encoded payload size accepted by a publish or completion.
pub const DEFAULT_COORDINATION_MAX_PAYLOAD_BYTES: usize = 256 * 1024;
/// Default maximum encoded metadata size accepted by a publish.
pub const DEFAULT_COORDINATION_MAX_METADATA_BYTES: usize = 16 * 1024;

const MAX_NAMESPACE_BYTES: usize = 48;
const MAX_CAPABILITY_BYTES: usize = 64;

/// Bounds and key namespace for the durable handoff router.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinationConfig {
    namespace: String,
    shards: u16,
    max_payload_bytes: usize,
    max_metadata_bytes: usize,
}

impl Default for CoordinationConfig {
    fn default() -> Self {
        Self {
            namespace: "redis-mcp".to_string(),
            shards: DEFAULT_COORDINATION_SHARDS,
            max_payload_bytes: DEFAULT_COORDINATION_MAX_PAYLOAD_BYTES,
            max_metadata_bytes: DEFAULT_COORDINATION_MAX_METADATA_BYTES,
        }
    }
}

impl CoordinationConfig {
    /// Replace the safe, colon-free key namespace.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Replace the number of capability inbox shards.
    pub fn with_shards(mut self, shards: u16) -> Self {
        self.shards = shards;
        self
    }

    /// Replace the maximum encoded payload size.
    pub fn with_max_payload_bytes(mut self, bytes: usize) -> Self {
        self.max_payload_bytes = bytes;
        self
    }

    /// Replace the maximum encoded metadata size.
    pub fn with_max_metadata_bytes(mut self, bytes: usize) -> Self {
        self.max_metadata_bytes = bytes;
        self
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub const fn shards(&self) -> u16 {
        self.shards
    }

    pub const fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }

    pub const fn max_metadata_bytes(&self) -> usize {
        self.max_metadata_bytes
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_token("namespace", &self.namespace, MAX_NAMESPACE_BYTES)?;
        if self.shards == 0 || self.max_payload_bytes == 0 || self.max_metadata_bytes == 0 {
            return Err("coordination shard and byte limits must be greater than zero".into());
        }
        Ok(())
    }

    pub(crate) fn shard_for(&self, idempotency_key: &str) -> u16 {
        let digest = Sha256::digest(idempotency_key.as_bytes());
        u16::from_be_bytes([digest[0], digest[1]]) % self.shards
    }

    pub(crate) fn keys(&self, handle: &ParsedHandle) -> CoordinationKeys {
        let tag = format!("{}:{}:{}", self.namespace, handle.capability, handle.shard);
        let prefix = format!("rmcp:{{{tag}}}");
        CoordinationKeys {
            inbox: format!("{prefix}:inbox"),
            state: format!("{prefix}:handoff:{}", handle.id),
            events: format!("{prefix}:handoff:{}:events", handle.id),
            idempotency: format!(
                "{prefix}:idem:{}:{}",
                handle.principal_digest, handle.idempotency_digest
            ),
        }
    }
}

/// A stable authenticated identity used for durable ownership checks.
///
/// Debug output is deliberately redacted. Hosts should derive this value from
/// an authenticated principal rather than an HTTP or MCP session identifier.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CoordinationPrincipal(Arc<str>);

impl CoordinationPrincipal {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 {
            return Err("coordination principal must contain 1..=512 bytes".into());
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn random() -> Self {
        Self(format!("local:{:032x}", rand::random::<u128>()).into())
    }

    pub(crate) fn digest(&self) -> String {
        digest_hex(self.0.as_bytes())
    }
}

impl fmt::Debug for CoordinationPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CoordinationPrincipal([REDACTED])")
    }
}

/// A bounded, explicitly tagged handoff or completion payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CoordinationPayload {
    Json {
        value: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_ref: Option<String>,
    },
    Text {
        value: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_ref: Option<String>,
    },
    Binary {
        /// Standard padded base64.
        value: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_ref: Option<String>,
    },
}

impl CoordinationPayload {
    pub(crate) fn validate(&self, max_bytes: usize) -> Result<(), String> {
        if let Self::Binary { value, .. } = self {
            BASE64
                .decode(value)
                .map_err(|_| "binary payload value must be standard padded base64")?;
        }
        let (content_type, schema_ref) = match self {
            Self::Json { schema_ref, .. } => (None, schema_ref.as_deref()),
            Self::Text {
                content_type,
                schema_ref,
                ..
            }
            | Self::Binary {
                content_type,
                schema_ref,
                ..
            } => (content_type.as_deref(), schema_ref.as_deref()),
        };
        for (name, value) in [("content_type", content_type), ("schema_ref", schema_ref)] {
            if value.is_some_and(|value| value.is_empty() || value.len() > 256) {
                return Err(format!("{name} must contain 1..=256 bytes when present"));
            }
        }
        let bytes = serde_json::to_vec(self).map_err(|error| error.to_string())?;
        if bytes.len() > max_bytes {
            return Err(format!(
                "encoded payload is {} bytes; maximum is {max_bytes}",
                bytes.len()
            ));
        }
        Ok(())
    }
}

/// Durable lifecycle state for one handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CoordinationStatus {
    Published,
    Claimed,
    AwaitingApproval,
    Completed,
}

impl CoordinationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::Claimed => "claimed",
            Self::AwaitingApproval => "awaiting_approval",
            Self::Completed => "completed",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "published" => Ok(Self::Published),
            "claimed" => Ok(Self::Claimed),
            "awaiting_approval" => Ok(Self::AwaitingApproval),
            "completed" => Ok(Self::Completed),
            _ => Err(format!("unknown coordination status {value}")),
        }
    }
}

/// Opaque durable handoff locator returned by publish and claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct CoordinationHandle(String);

impl CoordinationHandle {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn from_stored(value: String) -> Self {
        Self(value)
    }

    pub(crate) fn create(
        capability: &str,
        shard: u16,
        principal: &CoordinationPrincipal,
        idempotency_key: &str,
    ) -> Result<Self, String> {
        validate_capability(capability)?;
        let id = format!("{:032x}", rand::random::<u128>());
        Ok(Self(format!(
            "v1.{capability}.{shard}.{id}.{}.{}",
            principal.digest(),
            digest_hex(idempotency_key.as_bytes())
        )))
    }

    pub(crate) fn parse(&self, max_shards: u16) -> Result<ParsedHandle, String> {
        let parts = self.0.split('.').collect::<Vec<_>>();
        if parts.len() != 6 || parts[0] != "v1" {
            return Err("invalid coordination handle".into());
        }
        validate_capability(parts[1])?;
        let shard = parts[2]
            .parse::<u16>()
            .map_err(|_| "invalid coordination handle shard")?;
        if shard >= max_shards
            || parts[3].len() != 32
            || parts[4].len() != 64
            || parts[5].len() != 64
            || !parts[3..]
                .iter()
                .all(|part| part.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err("invalid coordination handle".into());
        }
        Ok(ParsedHandle {
            capability: parts[1].to_string(),
            shard,
            id: parts[3].to_string(),
            principal_digest: parts[4].to_string(),
            idempotency_digest: parts[5].to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedHandle {
    pub(crate) capability: String,
    pub(crate) shard: u16,
    pub(crate) id: String,
    pub(crate) principal_digest: String,
    pub(crate) idempotency_digest: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CoordinationKeys {
    pub(crate) inbox: String,
    pub(crate) state: String,
    pub(crate) events: String,
    pub(crate) idempotency: String,
}

pub(crate) fn validate_capability(capability: &str) -> Result<(), String> {
    validate_token("capability", capability, MAX_CAPABILITY_BYTES)
}

fn validate_token(name: &str, value: &str, max: usize) -> Result<(), String> {
    if value.is_empty()
        || value.len() > max
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!(
            "{name} must contain 1..={max} ASCII letters, digits, '-' or '_'"
        ));
    }
    Ok(())
}

pub(crate) fn digest_hex(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn add_resources(router: McpRouter) -> McpRouter {
    router.resource(
        ResourceBuilder::new(COORDINATION_GUIDE_URI)
            .name("guidance-agent-handoffs")
            .title("Durable agent handoffs with Redis Streams")
            .description("Publish, claim, complete, inspect, and recover bounded agent handoffs with cluster-safe keys and durable identity.")
            .mime_type("text/markdown")
            .text(include_str!("../guidance/agent-handoffs.md")),
    )
    .prompt(
        PromptBuilder::new("redis_coordinate_handoff")
            .title("Coordinate durable agent work")
            .description("Guide a producer, worker, approver, or recovery operator through the durable handoff lifecycle.")
            .optional_arg(
                "role",
                "One of producer, worker, approver, or recovery; omit to explain the full lifecycle.",
            )
            .handler(|arguments: HashMap<String, String>| async move {
                let role = arguments.get("role").map(String::as_str).unwrap_or("full lifecycle");
                Ok(GetPromptResult {
                    description: Some("Durable Redis agent handoff workflow".to_string()),
                    messages: vec![PromptMessage {
                        role: PromptRole::User,
                        content: Content::text(format!(
                            "Act as the {role} in a durable Redis agent handoff. First read {COORDINATION_GUIDE_URI}. Use only the bounded coordination tools. Producers call redis_handoff_publish with a stable idempotency key and retain its handle/resource_uri. Workers poll explicit shards with redis_handoff_claim. Before a risky action, the current claimant calls redis_handoff_request_approval with a stable idempotency key and a non-sensitive question; accept, decline, and cancel remain durable and do not complete the handoff. Workers perform idempotent external effects, then call redis_handoff_complete with a stable completion idempotency key. Read redis_handoff_status or the canonical resource for progress. Recovery operators use redis_handoff_recover only after a justified idle threshold, then inspect the correlated timeline. State clearly that delivery is at least once and do not claim exactly-once processing."
                        )),
                        meta: None,
                    }],
                    meta: None,
                })
            })
            .build(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_for_a_handoff_share_one_cluster_hash_tag() {
        let config = CoordinationConfig::default();
        let principal = CoordinationPrincipal::new("agent-a").unwrap();
        let handle = CoordinationHandle::create("incident", 3, &principal, "publish-1").unwrap();
        let parsed = handle.parse(config.shards()).unwrap();
        let keys = config.keys(&parsed);
        for key in [&keys.state, &keys.events, &keys.idempotency] {
            assert_eq!(
                redis_tower_cluster::slot_for_key(keys.inbox.as_bytes()),
                redis_tower_cluster::slot_for_key(key.as_bytes())
            );
        }
    }

    #[test]
    fn principal_debug_output_is_redacted() {
        let principal = CoordinationPrincipal::new("super-secret-token").unwrap();
        assert_eq!(
            format!("{principal:?}"),
            "CoordinationPrincipal([REDACTED])"
        );
    }

    #[test]
    fn shard_selection_has_a_stable_known_vector() {
        let config = CoordinationConfig::default();
        assert_eq!(config.shard_for("incident-42-v1"), 11);
        assert_eq!(config.clone().with_shards(4).shard_for("incident-42-v1"), 3);
    }

    #[test]
    fn approval_waiting_status_round_trips() {
        assert_eq!(
            CoordinationStatus::parse(CoordinationStatus::AwaitingApproval.as_str()).unwrap(),
            CoordinationStatus::AwaitingApproval
        );
    }

    #[test]
    fn payload_validation_enforces_encoding_and_encoded_size() {
        let invalid_binary = CoordinationPayload::Binary {
            value: "not base64".to_string(),
            content_type: None,
            schema_ref: None,
        };
        assert!(invalid_binary.validate(1_024).is_err());

        let oversized = CoordinationPayload::Text {
            value: "x".repeat(128),
            content_type: Some("text/plain".to_string()),
            schema_ref: None,
        };
        assert!(oversized.validate(64).is_err());
    }
}
