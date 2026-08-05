//! Stable catalog metadata and host-selectable tool bundles.

use std::fmt;

use crate::AccessMode;

/// Coherent groups of Redis tools that hosts can compose deliberately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ToolBundle {
    /// Broadly useful connection, key, and data access tools.
    Essentials,
    /// Native Redis collection and data-structure operations.
    DataStructures,
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
    pub const DEFAULTS: &'static [Self] = &[Self::Essentials, Self::Diagnostics];

    /// Every bundle understood by this library version.
    pub const ALL: &'static [Self] = &[
        Self::Essentials,
        Self::DataStructures,
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
        name: "redis_set",
        bundle: ToolBundle::Essentials,
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
}
