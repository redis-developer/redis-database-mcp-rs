# Packaged-library consumer fixture

This crate is intentionally excluded from the workspace. The release check
patches its exact crates.io dependency to Cargo's extracted `redis-mcp` package
and compiles it with minimal, default, and full library feature selections.
