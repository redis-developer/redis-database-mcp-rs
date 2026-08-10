//! MCP tool output limits.

/// Default maximum size of one serialized MCP tool result.
///
/// The measurement includes both the structured result and the text rendering
/// carried by [`tower_mcp::CallToolResult`].
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 256 * 1024;

/// Default maximum number of entries returned by one collection tool call.
pub const DEFAULT_MAX_OUTPUT_ENTRIES: usize = 1_000;

/// Hard limits applied to every Redis MCP tool result.
///
/// Byte limits are checked against the complete serialized
/// [`tower_mcp::CallToolResult`], after binary values have been base64 encoded.
/// Collection limits are checked by tools that expose lists, maps, sets, search
/// results, or Redis cursor pages. A zero limit is rejected by
/// [`crate::RedisMcpBuilder::try_build`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutputBudget {
    max_bytes: usize,
    max_collection_entries: usize,
}

impl OutputBudget {
    /// Create an output budget from an encoded-result byte ceiling and a
    /// collection-entry ceiling.
    pub const fn new(max_bytes: usize, max_collection_entries: usize) -> Self {
        Self {
            max_bytes,
            max_collection_entries,
        }
    }

    /// Maximum serialized bytes permitted for one complete MCP tool result.
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Maximum collection entries permitted in one tool result.
    pub const fn max_collection_entries(self) -> usize {
        self.max_collection_entries
    }
}

impl Default for OutputBudget {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_OUTPUT_BYTES, DEFAULT_MAX_OUTPUT_ENTRIES)
    }
}
