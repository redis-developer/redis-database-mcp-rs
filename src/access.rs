//! Access policy for the Redis tool surface.

/// Maximum side-effect level exposed by a RedisMcp router.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum AccessMode {
    /// Expose only tools that do not mutate Redis.
    #[default]
    ReadOnly,
    /// Also expose ordinary writes, but not destructive operations.
    ReadWrite,
    /// Expose destructive operations and, when separately enabled, raw commands.
    Full,
}

impl AccessMode {
    pub(crate) fn permits(self, required: Self) -> bool {
        self >= required
    }
}
