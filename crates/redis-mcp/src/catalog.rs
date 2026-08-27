//! Stable catalog metadata and host-selectable tool bundles.

use std::{fmt, sync::OnceLock};

use crate::{AccessMode, RedisVersion, ToolFamily};

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
    /// RedisTimeSeries commands such as `TS.ADD` and `TS.RANGE`.
    TimeSeries,
}

impl RedisModule {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Json => "redis_json",
            Self::Search => "search",
            Self::TimeSeries => "timeseries",
        }
    }

    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Json => "RedisJSON",
            Self::Search => "Redis Query Engine",
            Self::TimeSeries => "RedisTimeSeries",
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
    /// RedisTimeSeries sample, metadata, and query operations.
    TimeSeries,
    /// Operational inspection and troubleshooting tools.
    Diagnostics,
    /// Stateful, owner-isolated Redis session operations.
    Sessions,
    /// Bounded atomic MULTI/EXEC transactions on dedicated connections.
    Transactions,
    /// Bounded Lua scripting and Redis Functions operations.
    Scripting,
    /// Server configuration and administrative operations.
    Admin,
    /// Deliberately bounded bulk workflows.
    Bulk,
    /// Governed Redis argv invocation tiers for Redis-syntax MCP clients.
    Invocation,
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
        Self::TimeSeries,
        Self::Diagnostics,
        Self::Sessions,
        Self::Transactions,
        Self::Scripting,
        Self::Admin,
        Self::Bulk,
        Self::Invocation,
        Self::Raw,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Essentials => "essentials",
            Self::DataStructures => "data_structures",
            Self::Json => "json",
            Self::Search => "search",
            Self::TimeSeries => "timeseries",
            Self::Diagnostics => "diagnostics",
            Self::Sessions => "sessions",
            Self::Transactions => "transactions",
            Self::Scripting => "scripting",
            Self::Admin => "admin",
            Self::Bulk => "bulk",
            Self::Invocation => "invocation",
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
    /// High-level Redis command family, or `None` for a cross-cutting tool.
    pub fn family(self) -> Option<ToolFamily> {
        match self.bundle {
            ToolBundle::Json => Some(ToolFamily::Json),
            ToolBundle::Search => Some(ToolFamily::Search),
            ToolBundle::TimeSeries => Some(ToolFamily::TimeSeries),
            ToolBundle::Scripting => Some(ToolFamily::Scripting),
            ToolBundle::Essentials => match self.name {
                "redis_pubsub_channels"
                | "redis_pubsub_numsub"
                | "redis_pubsub_numpat"
                | "redis_pubsub_shardchannels"
                | "redis_pubsub_shardnumsub"
                | "redis_publish"
                | "redis_spublish" => Some(ToolFamily::PubSub),
                "redis_get" | "redis_set" | "redis_mget" | "redis_strlen" | "redis_getrange"
                | "redis_mset" | "redis_incr" | "redis_append" | "redis_getex"
                | "redis_setrange" | "redis_decr" | "redis_decrby" | "redis_incrby"
                | "redis_incrbyfloat" | "redis_getdel" => Some(ToolFamily::Strings),
                _ => Some(ToolFamily::Keyspace),
            },
            ToolBundle::DataStructures => {
                let name = self.name;
                if name.starts_with("redis_ar") {
                    Some(ToolFamily::Arrays)
                } else if matches!(
                    name,
                    "redis_getbit"
                        | "redis_setbit"
                        | "redis_bitcount"
                        | "redis_bitpos"
                        | "redis_bitfield_ro"
                        | "redis_bitfield"
                        | "redis_bitop"
                ) {
                    Some(ToolFamily::Bitmaps)
                } else if name.starts_with("redis_geo") {
                    Some(ToolFamily::Geospatial)
                } else if name.starts_with("redis_pf") {
                    Some(ToolFamily::HyperLogLog)
                } else if name.starts_with("redis_v") {
                    Some(ToolFamily::VectorSets)
                } else if name.starts_with("redis_x") {
                    Some(ToolFamily::Streams)
                } else if name.starts_with("redis_z") {
                    Some(ToolFamily::SortedSets)
                } else if name.starts_with("redis_s") {
                    Some(ToolFamily::Sets)
                } else if name.starts_with("redis_l")
                    || matches!(name, "redis_rpush" | "redis_rpop")
                {
                    Some(ToolFamily::Lists)
                } else if name.starts_with("redis_h") {
                    Some(ToolFamily::Hashes)
                } else if matches!(
                    name,
                    "redis_delex" | "redis_digest" | "redis_increx" | "redis_msetex"
                ) {
                    Some(ToolFamily::Strings)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn is_compiled(self) -> bool {
        self.family().map_or_else(
            || match self.bundle {
                ToolBundle::Diagnostics => cfg!(feature = "diagnostics"),
                ToolBundle::Sessions => cfg!(feature = "sessions"),
                ToolBundle::Transactions => cfg!(feature = "transactions"),
                ToolBundle::Admin => cfg!(feature = "admin"),
                ToolBundle::Raw => true,
                ToolBundle::Bulk => true,
                ToolBundle::Invocation => true,
                ToolBundle::Essentials
                | ToolBundle::DataStructures
                | ToolBundle::Json
                | ToolBundle::Search
                | ToolBundle::TimeSeries
                | ToolBundle::Scripting => false,
            },
            ToolFamily::is_compiled,
        )
    }

    /// Optional Redis capability required by this tool.
    pub const fn required_module(self) -> Option<RedisModule> {
        match self.bundle {
            ToolBundle::Json => Some(RedisModule::Json),
            ToolBundle::Search => Some(RedisModule::Search),
            ToolBundle::TimeSeries => Some(RedisModule::TimeSeries),
            _ => None,
        }
    }

    /// Version, module, and command requirements used for discovery-aware
    /// catalog filtering and stable preflight errors.
    pub fn capability_requirements(self) -> ToolCapabilityRequirements {
        let minimum_redis_version = match self.name {
            "redis_slowlog" | "redis_transaction" => Some(RedisVersion::new(2, 2, 0)),
            "redis_latency_history" => Some(RedisVersion::new(2, 8, 0)),
            "redis_cluster_info" => Some(RedisVersion::new(3, 0, 0)),
            "redis_memory_stats"
            | "redis_memory_summary"
            | "redis_module_list"
            | "redis_key_summary"
            | "redis_hotkeys" => Some(RedisVersion::new(4, 0, 0)),
            "redis_acl_whoami"
            | "redis_acl_categories"
            | "redis_acl_users"
            | "redis_acl_user"
            | "redis_acl_rules"
            | "redis_acl_log"
            | "redis_acl_log_reset" => Some(RedisVersion::new(6, 0, 0)),
            "redis_acl_dryrun" => Some(RedisVersion::new(7, 0, 0)),
            "redis_backup_status" | "redis_backup_files" => Some(RedisVersion::new(8, 10, 0)),
            "redis_cluster_inspect" => Some(RedisVersion::new(8, 4, 0)),
            "redis_cluster_slot_stats" => Some(RedisVersion::new(8, 2, 0)),
            "redis_cluster_slot" => Some(RedisVersion::new(5, 0, 0)),
            "redis_client_control" => Some(RedisVersion::new(6, 2, 0)),
            "redis_server_state" => Some(RedisVersion::new(2, 8, 0)),
            "redis_latency_overview" => Some(RedisVersion::new(7, 0, 0)),
            "redis_latency_reset" => Some(RedisVersion::new(2, 8, 0)),
            "redis_memory_diagnostics" | "redis_memory_purge" => Some(RedisVersion::new(4, 0, 0)),
            "redis_slowlog_len" | "redis_slowlog_reset" => Some(RedisVersion::new(2, 2, 0)),
            "redis_hotkeys_get" | "redis_hotkeys_control" => Some(RedisVersion::new(8, 6, 0)),
            "redis_publish"
            | "redis_subscribe"
            | "redis_psubscribe"
            | "redis_pubsub_read"
            | "redis_pubsub_unsubscribe"
            | "redis_pubsub_close" => Some(RedisVersion::new(2, 0, 0)),
            "redis_pubsub_channels" | "redis_pubsub_numsub" | "redis_pubsub_numpat" => {
                Some(RedisVersion::new(2, 8, 0))
            }
            "redis_spublish"
            | "redis_ssubscribe"
            | "redis_pubsub_shardchannels"
            | "redis_pubsub_shardnumsub" => Some(RedisVersion::new(7, 0, 0)),
            "redis_sort" | "redis_zintercard" => Some(RedisVersion::new(7, 0, 0)),
            "redis_eval"
            | "redis_evalsha"
            | "redis_script_exists"
            | "redis_script_load"
            | "redis_script_flush"
            | "redis_script_kill" => Some(RedisVersion::new(2, 6, 0)),
            "redis_eval_ro"
            | "redis_evalsha_ro"
            | "redis_fcall"
            | "redis_fcall_ro"
            | "redis_function_list"
            | "redis_function_stats"
            | "redis_function_dump"
            | "redis_function_load"
            | "redis_function_restore"
            | "redis_function_delete"
            | "redis_function_flush"
            | "redis_function_kill" => Some(RedisVersion::new(7, 0, 0)),
            "redis_dump" => Some(RedisVersion::new(2, 6, 0)),
            "redis_getbit" | "redis_setbit" => Some(RedisVersion::new(2, 2, 0)),
            "redis_bitcount" | "redis_bitop" => Some(RedisVersion::new(2, 6, 0)),
            "redis_bitpos" => Some(RedisVersion::new(2, 8, 7)),
            "redis_pfadd" | "redis_pfcount" | "redis_pfmerge" => Some(RedisVersion::new(2, 8, 9)),
            "redis_bitfield" | "redis_geoadd" | "redis_geodist" | "redis_geohash"
            | "redis_geopos" => Some(RedisVersion::new(3, 2, 0)),
            "redis_bitfield_ro" => Some(RedisVersion::new(6, 0, 0)),
            "redis_geosearch" | "redis_geosearchstore" => Some(RedisVersion::new(6, 2, 0)),
            "redis_vadd" | "redis_vcard" | "redis_vdim" | "redis_vemb" | "redis_vgetattr"
            | "redis_vinfo" | "redis_vlinks" | "redis_vrandmember" | "redis_vrem"
            | "redis_vsetattr" | "redis_vsim" | "redis_hgetdel" | "redis_hgetex"
            | "redis_hsetex" => Some(RedisVersion::new(8, 0, 0)),
            "redis_vismember" | "redis_xackdel" | "redis_xdelex" => {
                Some(RedisVersion::new(8, 2, 0))
            }
            "redis_delex" | "redis_digest" | "redis_msetex" | "redis_vrange" => {
                Some(RedisVersion::new(8, 4, 0))
            }
            "redis_arcount" | "redis_ardel" | "redis_ardelrange" | "redis_arget"
            | "redis_argetrange" | "redis_argrep" | "redis_arinfo" | "redis_arinsert"
            | "redis_arlastitems" | "redis_arlen" | "redis_armget" | "redis_armset"
            | "redis_arnext" | "redis_arop" | "redis_arring" | "redis_arscan" | "redis_arseek"
            | "redis_arset" | "redis_increx" | "redis_xnack" => Some(RedisVersion::new(8, 8, 0)),
            "redis_lmovem" | "redis_sdiffcard" | "redis_sunioncard" => {
                Some(RedisVersion::new(8, 10, 0))
            }
            "redis_touch" | "redis_hstrlen" => Some(RedisVersion::new(3, 2, 0)),
            "redis_memory_usage" | "redis_object_inspect" | "redis_unlink" => {
                Some(RedisVersion::new(4, 0, 0))
            }
            "redis_restore"
            | "redis_restore_replace"
            | "redis_zpopmax"
            | "redis_zpopmin"
            | "redis_xlen"
            | "redis_xrange"
            | "redis_xrevrange"
            | "redis_xread"
            | "redis_xinfo_stream"
            | "redis_xinfo_groups"
            | "redis_xinfo_consumers"
            | "redis_xpending"
            | "redis_xadd"
            | "redis_xgroup_create"
            | "redis_xgroup_setid"
            | "redis_xreadgroup"
            | "redis_xack"
            | "redis_xclaim"
            | "redis_xdel"
            | "redis_xtrim"
            | "redis_xgroup_destroy"
            | "redis_xgroup_delconsumer" => Some(RedisVersion::new(5, 0, 0)),
            "redis_lpos" => Some(RedisVersion::new(6, 0, 0)),
            "redis_xgroup_createconsumer" | "redis_xautoclaim" => Some(RedisVersion::new(6, 2, 0)),
            "redis_copy" | "redis_copy_replace" | "redis_getdel" | "redis_getex"
            | "redis_hrandfield" | "redis_lmove" | "redis_lpop" | "redis_rpop"
            | "redis_smismember" | "redis_zadd" | "redis_zdiffstore" | "redis_zmscore"
            | "redis_zrange" | "redis_zrangestore" => Some(RedisVersion::new(6, 2, 0)),
            "redis_hexpire" | "redis_hexpire_delete" | "redis_hpersist" | "redis_httl" => {
                Some(RedisVersion::new(7, 4, 0))
            }
            _ => None,
        };
        let minimum_module_version = match self.name {
            "redis_ts_del" => Some(RedisVersion::new(1, 6, 0)),
            "redis_json_get"
            | "redis_json_type"
            | "redis_json_mget"
            | "redis_json_strlen"
            | "redis_json_objkeys"
            | "redis_json_objlen"
            | "redis_json_arrlen"
            | "redis_json_set"
            | "redis_json_numincrby"
            | "redis_json_toggle"
            | "redis_json_arrappend"
            | "redis_json_arrinsert"
            | "redis_json_del"
            | "redis_json_clear"
            | "redis_json_arrpop"
            | "redis_json_arrtrim" => Some(RedisVersion::new(2, 0, 0)),
            "redis_json_merge" => Some(RedisVersion::new(2, 6, 0)),
            "redis_ft_list" => Some(RedisVersion::new(2, 0, 0)),
            "redis_ft_aggregate" | "redis_ft_cursor_read" | "redis_ft_cursor_del" => {
                Some(RedisVersion::new(1, 1, 0))
            }
            "redis_ft_synupdate" | "redis_ft_syndump" => Some(RedisVersion::new(1, 2, 0)),
            "redis_ft_dictadd" | "redis_ft_dictdel" | "redis_ft_dictdump" => {
                Some(RedisVersion::new(1, 4, 0))
            }
            "redis_ft_profile" => Some(RedisVersion::new(2, 2, 0)),
            "redis_vector_get_hash"
            | "redis_vector_set_hash"
            | "redis_ft_vector_search"
            | "redis_ft_hybrid_search" => Some(RedisVersion::new(2, 4, 0)),
            _ => None,
        };
        let required_commands = match self.name {
            "redis_transaction" => &["MULTI", "EXEC", "WATCH"] as &'static [&'static str],
            "redis_ts_create" => &["TS.CREATE"],
            "redis_ts_alter" => &["TS.ALTER"],
            "redis_ts_add" => &["TS.ADD"],
            "redis_ts_madd" => &["TS.MADD"],
            "redis_ts_incrby" => &["TS.INCRBY"],
            "redis_ts_decrby" => &["TS.DECRBY"],
            "redis_ts_del" => &["TS.DEL"],
            "redis_ts_createrule" => &["TS.CREATERULE"],
            "redis_ts_deleterule" => &["TS.DELETERULE"],
            "redis_ts_range" => &["TS.RANGE"],
            "redis_ts_revrange" => &["TS.REVRANGE"],
            "redis_ts_mrange" => &["TS.MRANGE"],
            "redis_ts_mrevrange" => &["TS.MREVRANGE"],
            "redis_ts_get" => &["TS.GET"],
            "redis_ts_mget" => &["TS.MGET"],
            "redis_ts_info" => &["TS.INFO"],
            "redis_ts_queryindex" => &["TS.QUERYINDEX"],
            "redis_ping" => &["PING"] as &'static [&'static str],
            "redis_info" => &["INFO"],
            "redis_client_list" | "redis_connection_summary" => &["CLIENT"],
            "redis_cluster_info" => &["CLUSTER"],
            "redis_memory_stats" | "redis_memory_summary" => &["MEMORY"],
            "redis_module_list" => &["MODULE"],
            "redis_slowlog" => &["SLOWLOG"],
            "redis_latency_history" => &["LATENCY"],
            "redis_acl_whoami" => &["ACL"],
            "redis_acl_categories"
            | "redis_acl_users"
            | "redis_acl_user"
            | "redis_acl_rules"
            | "redis_acl_dryrun"
            | "redis_acl_log"
            | "redis_acl_log_reset" => &["ACL"],
            "redis_backup_status" | "redis_backup_files" => &["BACKUP"],
            "redis_cluster_inspect" | "redis_cluster_slot" | "redis_cluster_slot_stats" => {
                &["CLUSTER"]
            }
            "redis_config_get" | "redis_config_set" | "redis_config_resetstat" => &["CONFIG"],
            "redis_server_state" => &["TIME", "LASTSAVE", "ROLE"],
            "redis_latency_overview" | "redis_latency_reset" => &["LATENCY"],
            "redis_memory_diagnostics" | "redis_memory_purge" => &["MEMORY"],
            "redis_slowlog_len" | "redis_slowlog_reset" => &["SLOWLOG"],
            "redis_hotkeys_get" | "redis_hotkeys_control" => &["HOTKEYS"],
            "redis_client_control" => &["CLIENT"],
            "redis_flush" => &["FLUSHDB", "FLUSHALL"],
            "redis_swapdb" => &["SWAPDB"],
            "redis_health_check" | "redis_keyspace_summary" => &["INFO"],
            "redis_key_summary" => &["TYPE", "TTL", "MEMORY", "OBJECT"],
            "redis_hotkeys" => &["SCAN", "TYPE", "MEMORY"],
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
            "redis_sort" => &["SORT_RO", "EXISTS"],
            "redis_eval" => &["EVAL"],
            "redis_eval_ro" => &["EVAL_RO"],
            "redis_evalsha" => &["EVALSHA"],
            "redis_evalsha_ro" => &["EVALSHA_RO"],
            "redis_fcall" => &["FCALL"],
            "redis_fcall_ro" => &["FCALL_RO"],
            "redis_script_exists"
            | "redis_script_load"
            | "redis_script_flush"
            | "redis_script_kill" => &["SCRIPT"],
            "redis_function_list"
            | "redis_function_stats"
            | "redis_function_dump"
            | "redis_function_load"
            | "redis_function_restore"
            | "redis_function_delete"
            | "redis_function_flush"
            | "redis_function_kill" => &["FUNCTION"],
            "redis_publish" => &["PUBLISH"],
            "redis_spublish" => &["SPUBLISH"],
            "redis_subscribe" => &["SUBSCRIBE"],
            "redis_psubscribe" => &["PSUBSCRIBE"],
            "redis_ssubscribe" => &["SSUBSCRIBE"],
            "redis_pubsub_channels"
            | "redis_pubsub_numsub"
            | "redis_pubsub_numpat"
            | "redis_pubsub_shardchannels"
            | "redis_pubsub_shardnumsub" => &["PUBSUB"],
            "redis_hget" => &["HGET", "EXISTS"],
            "redis_hgetall" => &["HGETALL"],
            "redis_hexists" => &["HEXISTS", "EXISTS"],
            "redis_hkeys" => &["HKEYS"],
            "redis_hlen" => &["HLEN"],
            "redis_hmget" => &["HMGET", "EXISTS"],
            "redis_hscan" => &["HSCAN"],
            "redis_hstrlen" => &["HSTRLEN", "HEXISTS", "EXISTS"],
            "redis_httl" => &["HTTL", "HPTTL", "HEXPIRETIME", "HPEXPIRETIME", "EXISTS"],
            "redis_hvals" => &["HVALS"],
            "redis_hrandfield" => &["HRANDFIELD", "EXISTS"],
            "redis_lindex" => &["LINDEX", "EXISTS"],
            "redis_llen" => &["LLEN"],
            "redis_lpos" => &["LPOS", "EXISTS"],
            "redis_lrange" => &["LRANGE", "EXISTS"],
            "redis_scard" => &["SCARD"],
            "redis_sdiff" => &["SDIFF"],
            "redis_sdiffcard" => &["SDIFFCARD"],
            "redis_sdiffstore" => &["SDIFFSTORE"],
            "redis_sinter" => &["SINTER"],
            "redis_sinterstore" => &["SINTERSTORE"],
            "redis_sismember" => &["SISMEMBER", "EXISTS"],
            "redis_smembers" => &["SMEMBERS"],
            "redis_smismember" => &["SMISMEMBER", "EXISTS"],
            "redis_sscan" => &["SSCAN", "EXISTS"],
            "redis_sunion" => &["SUNION"],
            "redis_sunioncard" => &["SUNIONCARD"],
            "redis_sunionstore" => &["SUNIONSTORE"],
            "redis_zcard" => &["ZCARD"],
            "redis_zcount" => &["ZCOUNT", "EXISTS"],
            "redis_zdiffstore" => &["ZDIFFSTORE"],
            "redis_zintercard" => &["ZINTERCARD"],
            "redis_zinterstore" => &["ZINTERSTORE"],
            "redis_zmscore" => &["ZMSCORE", "EXISTS"],
            "redis_zrange" => &["ZRANGE", "EXISTS"],
            "redis_zrangestore" => &["ZRANGESTORE"],
            "redis_zrank" => &["ZRANK", "EXISTS"],
            "redis_zrevrank" => &["ZREVRANK", "EXISTS"],
            "redis_zscan" => &["ZSCAN", "EXISTS"],
            "redis_zscore" => &["ZSCORE", "EXISTS"],
            "redis_zunionstore" => &["ZUNIONSTORE"],
            "redis_getbit" => &["GETBIT"],
            "redis_bitcount" => &["BITCOUNT"],
            "redis_bitpos" => &["BITPOS"],
            "redis_bitfield_ro" => &["BITFIELD_RO"],
            "redis_geodist" => &["GEODIST"],
            "redis_geohash" => &["GEOHASH"],
            "redis_geopos" => &["GEOPOS"],
            "redis_geosearch" => &["GEOSEARCH"],
            "redis_pfcount" => &["PFCOUNT"],
            "redis_arcount" => &["ARCOUNT"],
            "redis_ardel" => &["ARDEL"],
            "redis_ardelrange" => &["ARDELRANGE"],
            "redis_arget" => &["ARGET"],
            "redis_argetrange" => &["ARGETRANGE"],
            "redis_argrep" => &["ARGREP"],
            "redis_arinfo" => &["ARINFO"],
            "redis_arinsert" => &["ARINSERT"],
            "redis_arlastitems" => &["ARLASTITEMS"],
            "redis_arlen" => &["ARLEN"],
            "redis_armget" => &["ARMGET"],
            "redis_armset" => &["ARMSET"],
            "redis_arnext" => &["ARNEXT"],
            "redis_arop" => &["AROP"],
            "redis_arring" => &["ARRING"],
            "redis_arscan" => &["ARSCAN"],
            "redis_arseek" => &["ARSEEK"],
            "redis_arset" => &["ARSET"],
            "redis_digest" => &["DIGEST"],
            "redis_vcard" => &["VCARD"],
            "redis_vdim" => &["VDIM"],
            "redis_vemb" => &["VEMB"],
            "redis_vgetattr" => &["VGETATTR"],
            "redis_vinfo" => &["VINFO"],
            "redis_vismember" => &["VISMEMBER"],
            "redis_vlinks" => &["VLINKS"],
            "redis_vrandmember" => &["VRANDMEMBER"],
            "redis_vrange" => &["VRANGE"],
            "redis_vsim" => &["VSIM"],
            "redis_xlen" => &["XLEN", "EXISTS"],
            "redis_xrange" => &["XRANGE"],
            "redis_xrevrange" => &["XREVRANGE"],
            "redis_xread" => &["XREAD"],
            "redis_xinfo_stream" | "redis_xinfo_groups" | "redis_xinfo_consumers" => &["XINFO"],
            "redis_xpending" => &["XPENDING"],
            "redis_json_get" => &["JSON.GET", "EXISTS"],
            "redis_json_type" => &["JSON.TYPE", "EXISTS"],
            "redis_json_mget" => &["JSON.MGET", "EXISTS"],
            "redis_json_strlen" => &["JSON.STRLEN", "JSON.TYPE", "EXISTS"],
            "redis_json_objkeys" => &["JSON.OBJKEYS", "JSON.TYPE", "EXISTS"],
            "redis_json_objlen" => &["JSON.OBJLEN", "JSON.TYPE", "EXISTS"],
            "redis_json_arrlen" => &["JSON.ARRLEN", "JSON.TYPE", "EXISTS"],
            "redis_ft_list" => &["FT._LIST"],
            "redis_ft_info" => &["FT.INFO"],
            "redis_ft_search" => &["FT.SEARCH"],
            "redis_ft_aggregate" => &["FT.AGGREGATE"],
            "redis_ft_cursor_read" | "redis_ft_cursor_del" => &["FT.CURSOR"],
            "redis_ft_explain" => &["FT.EXPLAIN"],
            "redis_ft_profile" => &["FT.PROFILE"],
            "redis_ft_tagvals" => &["FT.TAGVALS"],
            "redis_ft_dictdump" => &["FT.DICTDUMP"],
            "redis_ft_syndump" => &["FT.SYNDUMP"],
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
            "redis_sort_store" => &["SORT"],
            "redis_hset" => &["HSET"],
            "redis_hexpire" => &["HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT"],
            "redis_hexpire_delete" => &["HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT"],
            "redis_hincrby" => &["HINCRBY"],
            "redis_hincrbyfloat" => &["HINCRBYFLOAT"],
            "redis_hpersist" => &["HPERSIST"],
            "redis_lpush" => &["LPUSH"],
            "redis_rpush" => &["RPUSH"],
            "redis_sadd" => &["SADD"],
            "redis_zadd" => &["ZADD"],
            "redis_zincrby" => &["ZINCRBY"],
            "redis_setbit" => &["SETBIT"],
            "redis_bitfield" => &["BITFIELD"],
            "redis_geoadd" => &["GEOADD"],
            "redis_pfadd" => &["PFADD"],
            "redis_hgetex" => &["HGETEX"],
            "redis_hsetex" => &["HSETEX"],
            "redis_increx" => &["INCREX"],
            "redis_msetex" => &["MSETEX"],
            "redis_vadd" => &["VADD"],
            "redis_vsetattr" => &["VSETATTR"],
            "redis_xadd" => &["XADD"],
            "redis_xgroup_create" | "redis_xgroup_setid" | "redis_xgroup_createconsumer" => {
                &["XGROUP"]
            }
            "redis_xreadgroup" => &["XREADGROUP"],
            "redis_xack" => &["XACK"],
            "redis_xclaim" => &["XCLAIM"],
            "redis_xautoclaim" => &["XAUTOCLAIM"],
            "redis_json_set" => &["JSON.SET"],
            "redis_json_numincrby" => &["JSON.NUMINCRBY", "JSON.TYPE", "EXISTS"],
            "redis_json_toggle" => &["JSON.TOGGLE", "JSON.TYPE", "EXISTS"],
            "redis_json_arrappend" => &["JSON.ARRAPPEND", "JSON.TYPE", "EXISTS"],
            "redis_json_arrinsert" => &["JSON.ARRINSERT", "JSON.TYPE", "EXISTS"],
            "redis_ft_create" => &["FT.CREATE"],
            "redis_ft_alter" => &["FT.ALTER"],
            "redis_ft_synupdate" => &["FT.SYNUPDATE"],
            "redis_ft_dictadd" => &["FT.DICTADD"],
            "redis_ft_aliasadd" => &["FT.ALIASADD"],
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
            "redis_bitop" => &["BITOP"],
            "redis_geosearchstore" => &["GEOSEARCHSTORE"],
            "redis_pfmerge" => &["PFMERGE"],
            "redis_delex" => &["DELEX"],
            "redis_hgetdel" => &["HGETDEL"],
            "redis_lmovem" => &["LMOVEM"],
            "redis_vrem" => &["VREM"],
            "redis_xackdel" => &["XACKDEL"],
            "redis_xdelex" => &["XDELEX"],
            "redis_xnack" => &["XNACK"],
            "redis_xdel" => &["XDEL"],
            "redis_xtrim" => &["XTRIM"],
            "redis_xgroup_destroy" | "redis_xgroup_delconsumer" => &["XGROUP"],
            "redis_unlink" => &["UNLINK"],
            "redis_getdel" => &["GETDEL"],
            "redis_rename" => &["RENAME"],
            "redis_renamenx" => &["RENAMENX"],
            "redis_json_del" => &["JSON.DEL", "EXISTS"],
            "redis_json_clear" => &["JSON.CLEAR", "JSON.TYPE", "EXISTS"],
            "redis_json_arrpop" => &["JSON.ARRPOP", "JSON.TYPE", "EXISTS"],
            "redis_json_arrtrim" => &["JSON.ARRTRIM", "JSON.TYPE", "EXISTS"],
            "redis_json_merge" => &["JSON.MERGE", "EXISTS"],
            "redis_ft_dropindex" => &["FT.DROPINDEX"],
            "redis_ft_aliasupdate" => &["FT.ALIASUPDATE"],
            "redis_ft_aliasdel" => &["FT.ALIASDEL"],
            "redis_ft_dictdel" => &["FT.DICTDEL"],
            "redis_command" => &[],
            _ => &[],
        };
        let deployment = match self.name {
            "redis_cluster_info"
            | "redis_cluster_inspect"
            | "redis_cluster_slot"
            | "redis_cluster_slot_stats" => ToolDeploymentRequirement::Cluster,
            // redis-rs routes these no-key or cursor commands to one node, or
            // returns a fan-out shape the tool does not aggregate. Advertising
            // database-wide semantics on Cluster would therefore mislead.
            "redis_info"
            | "redis_dbsize"
            | "redis_scan"
            | "redis_randomkey"
            | "redis_hotkeys"
            | "redis_ft_list"
            | "redis_acl_categories"
            | "redis_acl_users"
            | "redis_acl_user"
            | "redis_acl_rules"
            | "redis_acl_dryrun"
            | "redis_acl_log"
            | "redis_acl_log_reset"
            | "redis_backup_status"
            | "redis_backup_files"
            | "redis_config_get"
            | "redis_server_state"
            | "redis_client_control"
            | "redis_swapdb" => ToolDeploymentRequirement::Standalone,
            // RedisTimeSeries multi-series queries only observe the node that
            // answers them; OSS Cluster has no database-wide TS coordinator.
            "redis_ts_mget" | "redis_ts_mrange" | "redis_ts_mrevrange" | "redis_ts_queryindex" => {
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
            "redis_ft_aggregate" | "redis_ft_cursor_read" => ToolOutputPolicy::CursorPaginated,
            "redis_lrange" | "redis_zrange" | "redis_xrange" | "redis_xrevrange"
            | "redis_argrep" | "redis_arscan" | "redis_vrange" | "redis_ts_range"
            | "redis_ts_revrange" => ToolOutputPolicy::RangePaginated,
            "redis_ft_search" | "redis_ft_vector_search" | "redis_ft_hybrid_search" => {
                ToolOutputPolicy::OffsetPaginated
            }
            "redis_info"
            | "redis_client_list"
            | "redis_cluster_info"
            | "redis_memory_stats"
            | "redis_module_list"
            | "redis_slowlog"
            | "redis_latency_history"
            | "redis_health_check"
            | "redis_connection_summary"
            | "redis_keyspace_summary"
            | "redis_hotkeys"
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
            | "redis_geohash"
            | "redis_geopos"
            | "redis_geosearch"
            | "redis_vemb"
            | "redis_vinfo"
            | "redis_vlinks"
            | "redis_xinfo_stream"
            | "redis_xinfo_groups"
            | "redis_xinfo_consumers"
            | "redis_xpending"
            | "redis_json_get"
            | "redis_json_type"
            | "redis_json_mget"
            | "redis_json_strlen"
            | "redis_json_objkeys"
            | "redis_json_objlen"
            | "redis_json_arrlen"
            | "redis_json_numincrby"
            | "redis_json_toggle"
            | "redis_json_arrappend"
            | "redis_json_arrinsert"
            | "redis_json_arrpop"
            | "redis_json_arrtrim"
            | "redis_ft_list"
            | "redis_ft_info"
            | "redis_ft_explain"
            | "redis_ft_profile"
            | "redis_ft_tagvals"
            | "redis_ft_dictdump"
            | "redis_ft_syndump"
            | "redis_vector_get_hash"
            | "redis_pubsub_channels"
            | "redis_pubsub_shardchannels"
            | "redis_pubsub_read"
            | "redis_hrandfield"
            | "redis_sort"
            | "redis_eval"
            | "redis_eval_ro"
            | "redis_evalsha"
            | "redis_evalsha_ro"
            | "redis_fcall"
            | "redis_fcall_ro"
            | "redis_function_list"
            | "redis_function_stats"
            | "redis_function_dump"
            | "redis_acl_categories"
            | "redis_acl_users"
            | "redis_acl_user"
            | "redis_acl_rules"
            | "redis_acl_log"
            | "redis_backup_status"
            | "redis_cluster_inspect"
            | "redis_cluster_slot"
            | "redis_cluster_slot_stats"
            | "redis_config_get"
            | "redis_latency_overview"
            | "redis_memory_diagnostics"
            | "redis_hotkeys_get"
            | "redis_command"
            | "redis_command_readonly"
            | "redis_command_write"
            | "redis_command_inventory"
            | "redis_transaction"
            | "redis_ts_mrange"
            | "redis_ts_mrevrange"
            | "redis_ts_mget"
            | "redis_ts_info"
            | "redis_ts_queryindex" => ToolOutputPolicy::BudgetGuarded,
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
        name: "redis_client_list",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_cluster_info",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_memory_stats",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_module_list",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_slowlog",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_latency_history",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_acl_whoami",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_health_check",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_connection_summary",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_keyspace_summary",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_memory_summary",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_key_summary",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hotkeys",
        bundle: ToolBundle::Diagnostics,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_acl_categories",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_acl_users",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_acl_user",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_acl_rules",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_acl_dryrun",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_acl_log",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_acl_log_reset",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_backup_status",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_backup_files",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_cluster_inspect",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_cluster_slot",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_cluster_slot_stats",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_config_get",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_config_set",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_config_resetstat",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_server_state",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_latency_overview",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_latency_reset",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_memory_diagnostics",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_memory_purge",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_slowlog_len",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_slowlog_reset",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hotkeys_get",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hotkeys_control",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_client_control",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_flush",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_swapdb",
        bundle: ToolBundle::Admin,
        required_access: AccessMode::Full,
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
        name: "redis_sort",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pubsub_channels",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pubsub_numsub",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pubsub_numpat",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pubsub_shardchannels",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pubsub_shardnumsub",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_subscribe",
        bundle: ToolBundle::Sessions,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_psubscribe",
        bundle: ToolBundle::Sessions,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ssubscribe",
        bundle: ToolBundle::Sessions,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pubsub_read",
        bundle: ToolBundle::Sessions,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pubsub_unsubscribe",
        bundle: ToolBundle::Sessions,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pubsub_close",
        bundle: ToolBundle::Sessions,
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
        name: "redis_hrandfield",
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
        name: "redis_sdiffcard",
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
        name: "redis_sunioncard",
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
        name: "redis_zintercard",
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
        name: "redis_getbit",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_bitcount",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_bitpos",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_bitfield_ro",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_geodist",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_geohash",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_geopos",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_geosearch",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pfcount",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arcount",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arget",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_argetrange",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_argrep",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arinfo",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arlastitems",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arlen",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_armget",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arnext",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arop",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arscan",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_digest",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vcard",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vdim",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vemb",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vgetattr",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vinfo",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vismember",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vlinks",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vrandmember",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vrange",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vsim",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xlen",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xrange",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xrevrange",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xread",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xinfo_stream",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xinfo_groups",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xinfo_consumers",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xpending",
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
        name: "redis_json_mget",
        bundle: ToolBundle::Json,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_strlen",
        bundle: ToolBundle::Json,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_objkeys",
        bundle: ToolBundle::Json,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_objlen",
        bundle: ToolBundle::Json,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_arrlen",
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
        name: "redis_ft_aggregate",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_cursor_read",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_explain",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_profile",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_tagvals",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_dictdump",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_syndump",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_eval_ro",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_evalsha_ro",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_fcall_ro",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_script_exists",
        bundle: ToolBundle::Scripting,
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
        name: "redis_publish",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_spublish",
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
        name: "redis_setbit",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_bitfield",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_geoadd",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pfadd",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arinsert",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_armset",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arring",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arseek",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_arset",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hgetex",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hsetex",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_increx",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_msetex",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vadd",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vsetattr",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xadd",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xgroup_create",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xgroup_setid",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xgroup_createconsumer",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xreadgroup",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xack",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xclaim",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xautoclaim",
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
        name: "redis_json_numincrby",
        bundle: ToolBundle::Json,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_toggle",
        bundle: ToolBundle::Json,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_arrappend",
        bundle: ToolBundle::Json,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_arrinsert",
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
        name: "redis_ft_cursor_del",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_alter",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_synupdate",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_dictadd",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_aliasadd",
        bundle: ToolBundle::Search,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_eval",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_evalsha",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_fcall",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_script_load",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_script_flush",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_script_kill",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_function_list",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_function_stats",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_function_dump",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_function_load",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_function_restore",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_function_delete",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_function_flush",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_function_kill",
        bundle: ToolBundle::Scripting,
        required_access: AccessMode::Full,
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
        name: "redis_sdiffstore",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_sinterstore",
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
        name: "redis_sunionstore",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zdiffstore",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_zinterstore",
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
        name: "redis_zrangestore",
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
        name: "redis_zunionstore",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_bitop",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_geosearchstore",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_pfmerge",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ardel",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ardelrange",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_delex",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hgetdel",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_lmovem",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_vrem",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xackdel",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xdelex",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xnack",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xdel",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xtrim",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xgroup_destroy",
        bundle: ToolBundle::DataStructures,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_xgroup_delconsumer",
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
        name: "redis_sort_store",
        bundle: ToolBundle::Essentials,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_hexpire_delete",
        bundle: ToolBundle::DataStructures,
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
        name: "redis_json_clear",
        bundle: ToolBundle::Json,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_arrpop",
        bundle: ToolBundle::Json,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_arrtrim",
        bundle: ToolBundle::Json,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_json_merge",
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
        name: "redis_ft_aliasupdate",
        bundle: ToolBundle::Search,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_aliasdel",
        bundle: ToolBundle::Search,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ft_dictdel",
        bundle: ToolBundle::Search,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_create",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_alter",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_add",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_madd",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_incrby",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_decrby",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_del",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_createrule",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_deleterule",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::Full,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_range",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_revrange",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_mrange",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_mrevrange",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_get",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_mget",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_info",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_ts_queryindex",
        bundle: ToolBundle::TimeSeries,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: false,
    },
    ToolMetadata {
        name: "redis_transaction",
        bundle: ToolBundle::Transactions,
        required_access: AccessMode::Full,
        requires_raw_opt_in: true,
    },
    ToolMetadata {
        name: "redis_command_readonly",
        bundle: ToolBundle::Invocation,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: true,
    },
    ToolMetadata {
        name: "redis_command_write",
        bundle: ToolBundle::Invocation,
        required_access: AccessMode::ReadWrite,
        requires_raw_opt_in: true,
    },
    ToolMetadata {
        name: "redis_command_metadata",
        bundle: ToolBundle::Invocation,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: true,
    },
    ToolMetadata {
        name: "redis_command_inventory",
        bundle: ToolBundle::Invocation,
        required_access: AccessMode::ReadOnly,
        requires_raw_opt_in: true,
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
    static COMPILED_CATALOG: OnceLock<Box<[ToolMetadata]>> = OnceLock::new();
    COMPILED_CATALOG.get_or_init(|| {
        CATALOG
            .iter()
            .copied()
            .filter(|metadata| metadata.is_compiled())
            .collect::<Vec<_>>()
            .into_boxed_slice()
    })
}

pub(crate) fn selected_tool_names(
    access: AccessMode,
    bundles: &[ToolBundle],
    raw_enabled: bool,
) -> Vec<&'static str> {
    let mut names = tool_catalog()
        .iter()
        .filter(|tool| access.permits(tool.required_access))
        .filter(|tool| {
            if tool.requires_raw_opt_in {
                // The redis_command escape hatch is selected by the raw policy
                // alone; other raw-gated tools also require their bundle.
                raw_enabled && (tool.bundle == ToolBundle::Raw || bundles.contains(&tool.bundle))
            } else {
                bundles.contains(&tool.bundle)
            }
        })
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
}

pub(crate) fn selected_family_tool_names(
    access: AccessMode,
    families: &[ToolFamily],
) -> Vec<&'static str> {
    let mut names = tool_catalog()
        .iter()
        .filter(|tool| access.permits(tool.required_access))
        .filter(|tool| {
            tool.family()
                .is_some_and(|family| families.contains(&family))
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
    fn every_data_tool_has_a_family() {
        for tool in CATALOG {
            if matches!(
                tool.bundle,
                ToolBundle::Essentials
                    | ToolBundle::DataStructures
                    | ToolBundle::Json
                    | ToolBundle::Search
                    | ToolBundle::Scripting
            ) {
                assert!(tool.family().is_some(), "{} has no family", tool.name);
            }
        }
    }

    #[test]
    fn module_bundles_are_explicit_and_not_defaulted() {
        assert!(!ToolBundle::DEFAULTS.contains(&ToolBundle::Json));
        assert!(!ToolBundle::DEFAULTS.contains(&ToolBundle::Search));
        assert!(!ToolBundle::DEFAULTS.contains(&ToolBundle::TimeSeries));
        for tool in CATALOG {
            match tool.bundle {
                ToolBundle::Json => assert_eq!(tool.required_module(), Some(RedisModule::Json)),
                ToolBundle::Search => {
                    assert_eq!(tool.required_module(), Some(RedisModule::Search));
                }
                ToolBundle::TimeSeries => {
                    assert_eq!(tool.required_module(), Some(RedisModule::TimeSeries));
                }
                _ => assert_eq!(tool.required_module(), None),
            }
        }
    }
}
