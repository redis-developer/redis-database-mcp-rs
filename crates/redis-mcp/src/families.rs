//! Public Redis command-family composition markers.
//!
//! Cargo features decide which handlers are compiled. [`ToolFamily`] values
//! independently decide which of those compiled handlers a router advertises
//! at runtime. Select several families on one [`crate::RedisMcpBuilder`] to
//! preserve one shared executor and policy state, then merge the resulting
//! `McpRouter` into a host router with Tower-MCP.

use std::fmt;

/// A Redis command family that can be selected independently at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ToolFamily {
    Keyspace,
    Strings,
    Hashes,
    Lists,
    Sets,
    SortedSets,
    Streams,
    Bitmaps,
    Arrays,
    HyperLogLog,
    Geospatial,
    VectorSets,
    PubSub,
    Scripting,
    Json,
    Search,
    TimeSeries,
}

impl ToolFamily {
    /// Every command family understood by this library version.
    pub const ALL: &'static [Self] = &[
        Self::Keyspace,
        Self::Strings,
        Self::Hashes,
        Self::Lists,
        Self::Sets,
        Self::SortedSets,
        Self::Streams,
        Self::Bitmaps,
        Self::Arrays,
        Self::HyperLogLog,
        Self::Geospatial,
        Self::VectorSets,
        Self::PubSub,
        Self::Scripting,
        Self::Json,
        Self::Search,
        Self::TimeSeries,
    ];

    /// Data families represented by the legacy curated-default bundles.
    pub const DEFAULTS: &'static [Self] = &[
        Self::Keyspace,
        Self::Strings,
        Self::Hashes,
        Self::Lists,
        Self::Sets,
        Self::SortedSets,
        Self::Streams,
        Self::Bitmaps,
        Self::Arrays,
        Self::HyperLogLog,
        Self::Geospatial,
        Self::VectorSets,
        Self::PubSub,
    ];

    /// Stable Cargo feature name for this family.
    pub const fn feature_name(self) -> &'static str {
        match self {
            Self::Keyspace => "keyspace",
            Self::Strings => "strings",
            Self::Hashes => "hashes",
            Self::Lists => "lists",
            Self::Sets => "sets",
            Self::SortedSets => "sorted-sets",
            Self::Streams => "streams",
            Self::Bitmaps => "bitmaps",
            Self::Arrays => "arrays",
            Self::HyperLogLog => "hyperloglog",
            Self::Geospatial => "geospatial",
            Self::VectorSets => "vector-sets",
            Self::PubSub => "pubsub",
            Self::Scripting => "scripting",
            Self::Json => "json",
            Self::Search => "search",
            Self::TimeSeries => "timeseries",
        }
    }

    /// Whether this family was included in the current crate build.
    pub const fn is_compiled(self) -> bool {
        match self {
            Self::Keyspace => cfg!(feature = "keyspace"),
            Self::Strings => cfg!(feature = "strings"),
            Self::Hashes => cfg!(feature = "hashes"),
            Self::Lists => cfg!(feature = "lists"),
            Self::Sets => cfg!(feature = "sets"),
            Self::SortedSets => cfg!(feature = "sorted-sets"),
            Self::Streams => cfg!(feature = "streams"),
            Self::Bitmaps => cfg!(feature = "bitmaps"),
            Self::Arrays => cfg!(feature = "arrays"),
            Self::HyperLogLog => cfg!(feature = "hyperloglog"),
            Self::Geospatial => cfg!(feature = "geospatial"),
            Self::VectorSets => cfg!(feature = "vector-sets"),
            Self::PubSub => cfg!(feature = "pubsub"),
            Self::Scripting => cfg!(feature = "scripting"),
            Self::Json => cfg!(feature = "json"),
            Self::Search => cfg!(feature = "search"),
            Self::TimeSeries => cfg!(feature = "timeseries"),
        }
    }
}

impl fmt::Display for ToolFamily {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.feature_name())
    }
}

/// Families included in the current Cargo feature set.
pub fn compiled_tool_families() -> impl Iterator<Item = ToolFamily> {
    ToolFamily::ALL
        .iter()
        .copied()
        .filter(|family| family.is_compiled())
}

macro_rules! family_module {
    ($module:ident, $variant:ident, $feature:literal) => {
        #[doc = concat!("Composition marker for the Cargo `", $feature, "` family feature.")]
        pub mod $module {
            use super::ToolFamily;

            /// Select this family with [`crate::RedisMcpBuilder::family`].
            pub const FAMILY: ToolFamily = ToolFamily::$variant;
        }
    };
}

family_module!(keyspace, Keyspace, "keyspace");
family_module!(strings, Strings, "strings");
family_module!(hashes, Hashes, "hashes");
family_module!(lists, Lists, "lists");
family_module!(sets, Sets, "sets");
family_module!(sorted_sets, SortedSets, "sorted-sets");
family_module!(streams, Streams, "streams");
family_module!(bitmaps, Bitmaps, "bitmaps");
family_module!(arrays, Arrays, "arrays");
family_module!(hyperloglog, HyperLogLog, "hyperloglog");
family_module!(geospatial, Geospatial, "geospatial");
family_module!(vector_sets, VectorSets, "vector-sets");
family_module!(pubsub, PubSub, "pubsub");
family_module!(scripting, Scripting, "scripting");
family_module!(json, Json, "json");
family_module!(search, Search, "search");
family_module!(timeseries, TimeSeries, "timeseries");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_names_are_unique() {
        let mut names = ToolFamily::ALL
            .iter()
            .map(|family| family.feature_name())
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ToolFamily::ALL.len());
    }
}
