//! Stable catalog metadata and host-selectable tool bundles.

use std::fmt;

use crate::{AccessMode, RedisVersion};

/// Dominant strategy a tool uses to keep successful MCP output bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ToolOutputPolicy {
    /// The result has a fixed or input-bounded number of fields. The global
    /// encoded-byte ceiling still applies.
    IntrinsicallyBounded,
    /// The result is variable-sized and relies on the global entry and byte
    /// ceilings, with an error that tells callers how to narrow the request.
    BudgetGuarded,
    /// Redis cursor pagination exposes an explicit continuation cursor.
    CursorPaginated,
    /// A bounded rank/index range exposes an explicit continuation start.
    RangePaginated,
    /// Offset pagination exposes an explicit continuation offset.
    OffsetPaginated,
}

impl ToolOutputPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IntrinsicallyBounded => "intrinsically_bounded",
            Self::BudgetGuarded => "budget_guarded",
            Self::CursorPaginated => "cursor_paginated",
            Self::RangePaginated => "range_paginated",
            Self::OffsetPaginated => "offset_paginated",
        }
    }
}

/// Optional Redis capability required by a tool or command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum RedisModule {
    /// RedisJSON commands such as `JSON.GET` and `JSON.SET`.
    Json,
    /// Redis Query Engine commands such as `FT.SEARCH` and `FT.CREATE`.
    Search,
}

impl RedisModule {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Json => "redis_json",
            Self::Search => "search",
        }
    }

    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Json => "RedisJSON",
            Self::Search => "Redis Query Engine",
        }
    }
}

impl fmt::Display for RedisModule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.display_name())
    }
}

/// Redis capabilities that must be present for a catalog tool to work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolCapabilityRequirements {
    minimum_redis_version: Option<RedisVersion>,
    required_module: Option<RedisModule>,
    minimum_module_version: Option<RedisVersion>,
    required_commands: &'static [&'static str],
    deployment: ToolDeploymentRequirement,
}

impl ToolCapabilityRequirements {
    pub const fn minimum_redis_version(self) -> Option<RedisVersion> {
        self.minimum_redis_version
    }

    pub const fn required_module(self) -> Option<RedisModule> {
        self.required_module
    }

    pub const fn minimum_module_version(self) -> Option<RedisVersion> {
        self.minimum_module_version
    }

    pub const fn required_commands(self) -> &'static [&'static str] {
        self.required_commands
    }

    pub const fn deployment(self) -> ToolDeploymentRequirement {
        self.deployment
    }
}

/// Target deployment modes on which a tool has correct library semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ToolDeploymentRequirement {
    #[default]
    Any,
    Standalone,
    Cluster,
}

impl ToolDeploymentRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::Standalone => "standalone",
            Self::Cluster => "cluster",
        }
    }
}

/// Coherent groups of Redis tools that hosts can compose deliberately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ToolBundle {
    /// Broadly useful connection, key, and data access tools.
    Essentials,
    /// Native Redis collection and data-structure operations.
    DataStructures,
    /// RedisJSON document operations.
    Json,
    /// Redis Query Engine and search operations.
    Search,
    /// Operational inspection and troubleshooting tools.
    Diagnostics,
    /// Server configuration and administrative operations.
    Admin,
    /// Deliberately bounded bulk workflows.
    Bulk,
    /// Generic Redis command execution, gated by a separate policy.
    Raw,
}

impl ToolBundle {
    /// Bundles enabled by the curated standalone default.
    pub const DEFAULTS: &'static [Self] =
        &[Self::Essentials, Self::DataStructures, Self::Diagnostics];

    /// Every bundle understood by this library version.
    pub const ALL: &'static [Self] = &[
        Self::Essentials,
        Self::DataStructures,
        Self::Json,
        Self::Search,
        Self::Diagnostics,
        Self::Admin,
        Self::Bulk,
        Self::Raw,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Essentials => "essentials",
            Self::DataStructures => "data_structures",
            Self::Json => "json",
            Self::Search => "search",
            Self::Diagnostics => "diagnostics",
            Self::Admin => "admin",
            Self::Bulk => "bulk",
            Self::Raw => "raw",
        }
    }
}

impl fmt::Display for ToolBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable classification metadata for one MCP tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ToolMetadata {
    pub name: &'static str,
    pub bundle: ToolBundle,
    pub required_access: AccessMode,
    /// Whether a separate raw-command policy must enable this tool.
    pub requires_raw_opt_in: bool,
}

impl ToolMetadata {
    /// Optional Redis capability required by this tool.
    pub const fn required_module(self) -> Option<RedisModule> {
        match self.bundle {
            ToolBundle::Json => Some(RedisModule::Json),
            ToolBundle::Search => Some(RedisModule::Search),
            _ => None,
        }
    }

    /// Version, module, and command requirements used for discovery-aware
    /// catalog filtering and stable preflight errors.
    pub fn capability_requirements(self) -> ToolCapabilityRequirements {
        let minimum_redis_version = match self.name {
            "redis_dump" => Some(RedisVersion::new(2, 6, 0)),
            "redis_touch" | "redis_hstrlen" => Some(RedisVersion::new(3, 2, 0)),
            "redis_memory_usage" | "redis_object_inspect" | "redis_unlink" => {
                Some(RedisVersion::new(4, 0, 0))
            }
            "redis_restore" | "redis_restore_replace" | "redis_zpopmax" | "redis_zpopmin" => {
                Some(RedisVersion::new(5, 0, 0))
            }
            "redis_lpos" => Some(RedisVersion::new(6, 0, 0)),
            "redis_copy" | "redis_copy_replace" | "redis_getdel" | "redis_getex"
            | "redis_lmove" | "redis_lpop" | "redis_rpop" | "redis_smismember" | "redis_zadd"
            | "redis_zmscore" | "redis_zrange" => Some(RedisVersion::new(6, 2, 0)),
            "redis_hexpire" | "redis_hpersist" | "redis_httl" => Some(RedisVersion::new(7, 4, 0)),
            _ => None,
        };
        let minimum_module_version = match self.name {
            "redis_ft_list" => Some(RedisVersion::new(2, 0, 0)),
            "redis_vector_get_hash"
            | "redis_vector_set_hash"
            | "redis_ft_vector_search"
            | "redis_ft_hybrid_search" => Some(RedisVersion::new(2, 4, 0)),
            _ => None,
        };
        let required_commands = match self.name {
            "redis_ping" => &["PING"] as &'static [&'static str],
            "redis_info" => &["INFO"],
            "redis_dbsize" => &["DBSIZE"],
            "redis_scan" => &["SCAN"],
            "redis_get" => &["GET"],
            "redis_type" => &["TYPE"],
            "redis_ttl" => &["TTL"],
            "redis_exists" => &["EXISTS"],
            "redis_mget" => &["MGET"],
            "redis_strlen" => &["STRLEN"],
            "redis_memory_usage" => &["MEMORY"],
            "redis_randomkey" => &["RANDOMKEY"],
            "redis_getrange" => &["GETRANGE"],
            "redis_dump" => &["DUMP"],
            "redis_object_inspect" => &["OBJECT"],
            "redis_hget" => &["HGET", "EXISTS"],
            "redis_hgetall" => &["HGETALL"],
            "redis_hexists" => &["HEXISTS", "EXISTS"],
            "redis_hkeys" => &["HKEYS"],
            "redis_hlen" => &["HLEN"],
            "redis_hmget" => &["HMGET", "EXISTS"],
            "redis_hscan" => &["HSCAN"],
            "redis_hstrlen" => &["HSTRLEN", "HEXISTS", "EXISTS"],
            "redis_httl" => &["HTTL", "EXISTS"],
            "redis_hvals" => &["HVALS"],
            "redis_lindex" => &["LINDEX", "EXISTS"],
            "redis_llen" => &["LLEN"],
            "redis_lpos" => &["LPOS", "EXISTS"],
            "redis_lrange" => &["LRANGE", "EXISTS"],
            "redis_scard" => &["SCARD"],
            "redis_sdiff" => &["SDIFF"],
            "redis_sinter" => &["SINTER"],
            "redis_sismember" => &["SISMEMBER", "EXISTS"],
            "redis_smembers" => &["SMEMBERS"],
            "redis_smismember" => &["SMISMEMBER", "EXISTS"],
            "redis_sscan" => &["SSCAN", "EXISTS"],
            "redis_sunion" => &["SUNION"],
            "redis_zcard" => &["ZCARD"],
            "redis_zcount" => &["ZCOUNT", "EXISTS"],
            "redis_zmscore" => &["ZMSCORE", "EXISTS"],
            "redis_zrange" => &["ZRANGE", "EXISTS"],
            "redis_zrank" => &["ZRANK", "EXISTS"],
            "redis_zrevrank" => &["ZREVRANK", "EXISTS"],
            "redis_zscan" => &["ZSCAN", "EXISTS"],
            "redis_zscore" => &["ZSCORE", "EXISTS"],
            "redis_json_get" => &["JSON.GET"],
            "redis_json_type" => &["JSON.TYPE"],
            "redis_ft_list" => &["FT._LIST"],
            "redis_ft_info" => &["FT.INFO"],
            "redis_ft_search" => &["FT.SEARCH"],
            "redis_vector_get_hash" => &["HGET"],
            "redis_ft_vector_search" | "redis_ft_hybrid_search" => &["FT.SEARCH"],
            "redis_set" => &["SET"],
            "redis_expire" => &["EXPIRE"],
            "redis_persist" => &["PERSIST"],
            "redis_mset" => &["MSET"],
            "redis_incr" => &["INCR"],
            "redis_append" => &["APPEND"],
            "redis_getex" => &["GETEX"],
            "redis_setrange" => &["SETRANGE"],
            "redis_decr" => &["DECR"],
            "redis_decrby" => &["DECRBY"],
            "redis_incrby" => &["INCRBY"],
            "redis_incrbyfloat" => &["INCRBYFLOAT"],
            "redis_copy" | "redis_copy_replace" => &["COPY"],
            "redis_touch" => &["TOUCH"],
            "redis_restore" | "redis_restore_replace" => &["RESTORE"],
            "redis_hset" => &["HSET"],
            "redis_hexpire" => &["HEXPIRE"],
            "redis_hincrby" => &["HINCRBY"],
            "redis_hincrbyfloat" => &["HINCRBYFLOAT"],
            "redis_hpersist" => &["HPERSIST"],
            "redis_lpush" => &["LPUSH"],
            "redis_rpush" => &["RPUSH"],
            "redis_sadd" => &["SADD"],
            "redis_zadd" => &["ZADD"],
            "redis_zincrby" => &["ZINCRBY"],
            "redis_json_set" => &["JSON.SET"],
            "redis_ft_create" => &["FT.CREATE"],
            "redis_vector_set_hash" => &["HSET"],
            "redis_del" => &["DEL"],
            "redis_hdel" => &["HDEL"],
            "redis_lpop" => &["LPOP"],
            "redis_lmove" => &["LMOVE"],
            "redis_lrem" => &["LREM"],
            "redis_lset" => &["LSET"],
            "redis_ltrim" => &["LTRIM", "EXISTS"],
            "redis_rpop" => &["RPOP"],
            "redis_srem" => &["SREM"],
            "redis_zpopmax" => &["ZPOPMAX"],
            "redis_zpopmin" => &["ZPOPMIN"],
            "redis_zrem" => &["ZREM"],
            "redis_zremrangebyscore" => &["ZREMRANGEBYSCORE"],
            "redis_unlink" => &["UNLINK"],
            "redis_getdel" => &["GETDEL"],
            "redis_rename" => &["RENAME"],
            "redis_renamenx" => &["RENAMENX"],
            "redis_json_del" => &["JSON.DEL"],
            "redis_ft_dropindex" => &["FT.DROPINDEX"],
            "redis_command" => &[],
            _ => &[],
        };
        let deployment = match self.name {
            // redis-rs routes these no-key or cursor commands to one node, or
            // returns a fan-out shape the tool does not aggregate. Advertising
            // database-wide semantics on Cluster would therefore mislead.
            "redis_info" | "redis_dbsize" | "redis_scan" | "redis_randomkey" => {
                ToolDeploymentRequirement::Standalone
            }
            _ => ToolDeploymentRequirement::Any,
        };
        ToolCapabilityRequirements {
            minimum_redis_version,
            required_module: self.required_module(),
            minimum_module_version,
            required_commands,
            deployment,
        }
    }

    /// Dominant output-bounding strategy for this tool.
    pub fn output_policy(self) -> ToolOutputPolicy {
        match self.name {
            "redis_scan" | "redis_hscan" | "redis_sscan" | "redis_zscan" => {
                ToolOutputPolicy::CursorPaginated
            }
            "redis_lrange" | "redis_zrange" => ToolOutputPolicy::RangePaginated,
            "redis_ft_search" | "redis_ft_vector_search" | "redis_ft_hybrid_search" => {
                ToolOutputPolicy::OffsetPaginated
            }
            "redis_info"
            | "redis_get"
            | "redis_getdel"
            | "redis_getex"
            | "redis_getrange"
            | "redis_dump"
            | "redis_set"
            | "redis_mget"
            | "redis_randomkey"
            | "redis_hget"
            | "redis_hgetall"
            | "redis_hkeys"
            | "redis_hmget"
            | "redis_hvals"
            | "redis_sdiff"
            | "redis_sinter"
            | "redis_smembers"
            | "redis_smismember"
            | "redis_sunion"
            | "redis_zmscore"
            | "redis_json_get"
            | "redis_json_type"
            | "redis_ft_list"
            | "redis_ft_info"
            | "redis_vector_get_hash"
            | "redis_command" => ToolOutputPolicy::BudgetGuarded,
            _ => ToolOutputPolicy::IntrinsicallyBounded,
        }
    }
}

pub(crate) const CATALOG: &[ToolMetadata] = &[
    ToolMetadata {
        name: "redis_ping",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_info",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_dbsize",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_scan",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_get",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_type",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ttl",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_exists",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_mget",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_strlen",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_memory_usage",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_randomkey",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_getrange",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_dump",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_object_inspect",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hget",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hgetall",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hexists",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hkeys",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hlen",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hmget",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hscan",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hstrlen",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_httl",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hvals",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_lindex",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_llen",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_lpos",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_lrange",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_scard",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_sdiff",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_sinter",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_sismember",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_smembers",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_smismember",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_sscan",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_sunion",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zcard",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zcount",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zmscore",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zrange",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zrank",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zrevrank",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zscan",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zscore",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_get",
        bundle: ToolBundle::Json,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_type",
        bundle: ToolBundle::Json,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_list",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_info",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_search",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vector_get_hash",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_vector_search",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_hybrid_search",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_set",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_expire",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_persist",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_mset",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_incr",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_append",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_getex",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_setrange",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_decr",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_decrby",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_incrby",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_incrbyfloat",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_copy",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_touch",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_restore",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hset",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hexpire",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hincrby",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hincrbyfloat",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hpersist",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_lpush",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_rpush",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_sadd",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zadd",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zincrby",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_set",
        bundle: ToolBundle::Json,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_create",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vector_set_hash",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_del",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_unlink",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hdel",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_lpop",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_lmove",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_lrem",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_lset",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ltrim",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_rpop",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_srem",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zpopmax",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zpopmin",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zrem",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zremrangebyscore",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_getdel",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_copy_replace",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_rename",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_renamenx",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_restore_replace",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_del",
        bundle: ToolBundle::Json,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_dropindex",
        bundle: ToolBundle::Search,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_command",
        bundle: ToolBundle::Raw,
        required_access: AccessMode::Full,
        requires_raw_opt_in: true,
    },
];

/// Metadata for every tool implemented by this library version.
pub fn tool_catalog() -> &'static [ToolMetadata] {
    CATALOG
}

pub(crate) fn selected_tool_names(
    access: AccessMode,
    bundles: &[ToolBundle],
    raw_enabled: bool,
) -> Vec<&'static str> {
    let mut names = CATALOG
        .iter()
        .filter(|tool| access.permits(tool.required_access))
        .filter(|tool| {
            if tool.requires_raw_opt_in {
                raw_enabled
            } else {
                bundles.contains(&tool.bundle)
            }
        })
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_names_are_unique() {
        let mut names = CATALOG.iter().map(|tool| tool.name).collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), CATALOG.len());
    }

    #[test]
    fn module_bundles_are_explicit_and_not_defaulted() {
        assert!(!ToolBundle::DEFAULTS.contains(&ToolBundle::Json));
        assert!(!ToolBundle::DEFAULTS.contains(&ToolBundle::Search));
        for tool in CATALOG {
            match tool.bundle {
                ToolBundle::Json => assert_eq!(tool.required_module(), Some(RedisModule::Json)),
                ToolBundle::Search => {
                    assert_eq!(tool.required_module(), Some(RedisModule::Search));
                }
                _ => assert_eq!(tool.required_module(), None),
            }
        }
    }
}
