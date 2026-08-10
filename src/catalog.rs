//! Stable catalog metadata and host-selectable tool bundles.

use std::fmt;

use crate::AccessMode;

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

    /// Dominant output-bounding strategy for this tool.
    pub fn output_policy(self) -> ToolOutputPolicy {
        match self.name {
            "redis_scan" | "redis_hscan" | "redis_sscan" | "redis_zscan" => {
                ToolOutputPolicy::CursorPaginated
            }
            "redis_lrange" | "redis_zrange" => ToolOutputPolicy::RangePaginated,
            "redis_ft_search" => ToolOutputPolicy::OffsetPaginated,
            "redis_info" | "redis_get" | "redis_mget" | "redis_randomkey" | "redis_hget"
            | "redis_hgetall" | "redis_smembers" | "redis_json_get" | "redis_json_type"
            | "redis_ft_list" | "redis_ft_info" | "redis_command" => {
                ToolOutputPolicy::BudgetGuarded
            }
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
        name: "redis_hscan",
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
        name: "redis_smembers",
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
        name: "redis_zrange",
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
        name: "redis_hset",
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
