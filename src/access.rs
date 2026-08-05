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
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::ReadWrite => "read_write",
            Self::Full => "full",
        }
    }

    pub(crate) fn permits(self, required: Self) -> bool {
        self >= required
    }
}

impl std::fmt::Display for AccessMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
