//! Policy for the optional raw Redis command escape hatch.

use crate::{
    AccessMode, NativeCommandMetadata, RedisError, RedisErrorKind, RedisModule, RedisVersion,
};

/// How the `redis_command` tool handles command names.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum RawCommandPolicy {
    /// Do not expose `redis_command`.
    #[default]
    Disabled,
    /// Expose only commands explicitly classified as bounded request/response
    /// operations by this library version. Unknown commands fail closed.
    Classified,
    /// Permit commands not known to the classifier, while retaining hard
    /// blocks for connection-state, session, streaming, and unbounded forms.
    Unrestricted,
}

impl RawCommandPolicy {
    pub(crate) fn is_enabled(self) -> bool {
        self != Self::Disabled
    }
}

pub(crate) fn classify_command(
    command: &[u8],
    arguments: &[Vec<u8>],
    policy: RawCommandPolicy,
) -> Result<NativeCommandMetadata, RedisError> {
    let command = command
        .iter()
        .map(u8::to_ascii_uppercase)
        .collect::<Vec<_>>();
    if command.is_empty()
        || command.len() > 128
        || !command
            .iter()
            .copied()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            "command must be one Redis command name containing only letters, digits, '.', '-', or '_'",
        )
        .with_code("INVALID_COMMAND_NAME"));
    }
    let command = String::from_utf8(command).expect("validated Redis command is ASCII");

    if policy == RawCommandPolicy::Disabled {
        return Err(RedisError::new(
            RedisErrorKind::Authorization,
            "native Redis command execution is disabled",
        )
        .with_code("RAW_COMMANDS_DISABLED"));
    }

    if let Some((code, category)) = unsupported_boundary(&command) {
        return unsupported(&command, category, code);
    }

    if matches!(command.as_str(), "XREAD" | "XREADGROUP")
        && arguments
            .iter()
            .any(|argument| eq_ascii_case(argument, b"BLOCK"))
    {
        return unsupported(
            &command,
            "blocking form requires a dedicated session API",
            "BLOCKING_COMMAND_UNSUPPORTED",
        );
    }

    if command == "SCRIPT"
        && arguments
            .first()
            .is_some_and(|argument| eq_ascii_case(argument, b"DEBUG"))
    {
        return unsupported(
            &command,
            "DEBUG requires a dedicated script session API",
            "SCRIPT_COMMAND_UNSUPPORTED",
        );
    }

    if command == "MODULE"
        && arguments.first().is_some_and(|argument| {
            eq_ascii_case(argument, b"LOAD")
                || eq_ascii_case(argument, b"LOADEX")
                || eq_ascii_case(argument, b"UNLOAD")
        })
    {
        return unsupported(
            &command,
            "module lifecycle operations are outside native request/response invocation",
            "MODULE_LIFECYCLE_COMMAND_UNSUPPORTED",
        );
    }

    let classified_access = classified_access(&command, arguments);
    let classified = classified_access.is_some();
    if policy == RawCommandPolicy::Classified && !classified {
        return Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            format!(
                "{command} is not classified for native execution; enable the unrestricted raw policy explicitly to allow unknown request/response commands"
            ),
        )
        .with_code("COMMAND_UNCLASSIFIED"));
    }

    let required_access = classified_access.unwrap_or(AccessMode::Full);
    let (required_module, minimum_module_version) = module_requirement(&command);
    Ok(NativeCommandMetadata::new(
        command.clone(),
        required_access,
        classified,
        minimum_redis_version(&command),
        required_module,
        minimum_module_version,
    ))
}

fn unsupported(
    command: &str,
    reason: &str,
    code: &'static str,
) -> Result<NativeCommandMetadata, RedisError> {
    Err(RedisError::new(
        RedisErrorKind::InvalidRequest,
        format!("{command} {reason}"),
    )
    .with_code(code))
}

fn unsupported_boundary(command: &str) -> Option<(&'static str, &'static str)> {
    match command {
        "AUTH" | "CLIENT" | "HELLO" | "QUIT" | "READONLY" | "READWRITE" | "RESET" | "SELECT" => {
            Some((
                "SESSION_COMMAND_UNSUPPORTED",
                "requires a dedicated connection-session API",
            ))
        }
        "EXEC" | "MULTI" | "UNWATCH" | "WATCH" => Some((
            "TRANSACTION_COMMAND_UNSUPPORTED",
            "requires a dedicated transaction-session API",
        )),
        "PSUBSCRIBE" | "PUNSUBSCRIBE" | "SSUBSCRIBE" | "SUBSCRIBE" | "SUNSUBSCRIBE"
        | "UNSUBSCRIBE" => Some((
            "SUBSCRIPTION_COMMAND_UNSUPPORTED",
            "requires a dedicated subscription-session API",
        )),
        "MONITOR" | "PSYNC" | "SYNC" => Some((
            "STREAMING_COMMAND_UNSUPPORTED",
            "requires a dedicated streaming API",
        )),
        "BLMOVE" | "BLMPOP" | "BLPOP" | "BRPOP" | "BRPOPLPUSH" | "BZMPOP" | "BZPOPMAX"
        | "BZPOPMIN" | "WAIT" | "WAITAOF" => Some((
            "BLOCKING_COMMAND_UNSUPPORTED",
            "requires a dedicated blocking-operation API",
        )),
        "EVAL" | "EVALSHA" | "EVALSHA_RO" | "EVAL_RO" | "FCALL" | "FCALL_RO" => Some((
            "SCRIPT_COMMAND_UNSUPPORTED",
            "requires a dedicated script execution API",
        )),
        "BGREWRITEAOF" | "BGSAVE" | "DEBUG" | "FAILOVER" | "MIGRATE" | "REPLICAOF" | "SAVE"
        | "SHUTDOWN" | "SLAVEOF" => Some((
            "SERVER_LIFECYCLE_COMMAND_UNSUPPORTED",
            "is outside native request/response invocation",
        )),
        _ => None,
    }
}

fn eq_ascii_case(value: &[u8], expected: &[u8]) -> bool {
    value.eq_ignore_ascii_case(expected)
}

fn parse_i64(value: &[u8]) -> Option<i64> {
    std::str::from_utf8(value).ok()?.parse().ok()
}

// This is intentionally an explicit allowlist rather than an attempted copy
// of Redis' full command table. There is no default read-only branch: every
// addition must choose an access tier alongside its semantics.
fn classified_access(command: &str, arguments: &[Vec<u8>]) -> Option<AccessMode> {
    if command == "HEXPIRE"
        && arguments
            .get(1)
            .and_then(|seconds| parse_i64(seconds))
            .is_some_and(|seconds| seconds <= 0)
    {
        return Some(AccessMode::Full);
    }
    let access = match command {
        "ACL" | "DEL" | "GETDEL" | "HDEL" | "JSON.ARRPOP" | "JSON.ARRTRIM" | "JSON.CLEAR"
        | "JSON.DEL" | "LPOP" | "LMOVE" | "LMPOP" | "LREM" | "LSET" | "LTRIM" | "RENAME"
        | "RENAMENX" | "RPOP" | "RPOPLPUSH" | "SMOVE" | "SPOP" | "SREM" | "UNLINK" | "XDEL"
        | "XTRIM" | "ZMPOP" | "ZPOPMAX" | "ZPOPMIN" | "ZREM" | "ZREMRANGEBYLEX"
        | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" => AccessMode::Full,
        "COPY" | "RESTORE"
            if arguments
                .iter()
                .any(|argument| eq_ascii_case(argument, b"REPLACE")) =>
        {
            AccessMode::Full
        }
        "MEMORY"
            if arguments
                .first()
                .is_some_and(|argument| eq_ascii_case(argument, b"PURGE")) =>
        {
            AccessMode::Full
        }
        "APPEND" | "COPY" | "DECR" | "DECRBY" | "EXPIRE" | "EXPIREAT" | "GETEX" | "GETSET"
        | "HEXPIRE" | "HINCRBY" | "HINCRBYFLOAT" | "HMSET" | "HPERSIST" | "HSET" | "HSETNX"
        | "INCR" | "INCRBY" | "INCRBYFLOAT" | "JSON.ARRAPPEND" | "JSON.ARRINSERT"
        | "JSON.NUMINCRBY" | "JSON.SET" | "JSON.TOGGLE" | "LINSERT" | "LPUSH" | "LPUSHX"
        | "MSET" | "MSETNX" | "PERSIST" | "PEXPIRE" | "PEXPIREAT" | "PSETEX" | "RESTORE"
        | "RPUSH" | "RPUSHX" | "SADD" | "SET" | "SETEX" | "TOUCH" | "XACK" | "XADD"
        | "XREADGROUP" | "ZADD" | "ZINCRBY" => AccessMode::ReadWrite,
        "MODULE" => AccessMode::Full,
        "COMMAND" | "DBSIZE" | "DUMP" | "ECHO" | "EXISTS" | "EXPIRETIME" | "GET" | "GETRANGE"
        | "HEXISTS" | "HGET" | "HGETALL" | "HKEYS" | "HLEN" | "HMGET" | "HSCAN" | "HSTRLEN"
        | "HTTL" | "HVALS" | "INFO" | "JSON.ARRLEN" | "JSON.GET" | "JSON.MGET" | "JSON.OBJKEYS"
        | "JSON.OBJLEN" | "JSON.STRLEN" | "JSON.TYPE" | "LCS" | "LINDEX" | "LLEN" | "LPOS"
        | "LRANGE" | "MEMORY" | "MGET" | "OBJECT" | "PEXPIRETIME" | "PING" | "PTTL"
        | "RANDOMKEY" | "SCAN" | "SCARD" | "SDIFF" | "SINTER" | "SINTERCARD" | "SISMEMBER"
        | "SMEMBERS" | "SMISMEMBER" | "SRANDMEMBER" | "SSCAN" | "STRLEN" | "SUNION" | "TTL"
        | "TYPE" | "XINFO" | "XLEN" | "XPENDING" | "XRANGE" | "XREAD" | "XREVRANGE" | "ZCARD"
        | "ZCOUNT" | "ZDIFF" | "ZINTER" | "ZLEXCOUNT" | "ZMSCORE" | "ZRANDMEMBER" | "ZRANGE"
        | "ZRANK" | "ZREVRANK" | "ZSCAN" | "ZSCORE" | "ZUNION" => AccessMode::ReadOnly,
        _ => return None,
    };
    Some(access)
}

fn minimum_redis_version(command: &str) -> Option<RedisVersion> {
    let version = match command {
        "SCAN" | "HSCAN" | "SSCAN" | "ZSCAN" => (2, 8),
        "HSTRLEN" | "TOUCH" => (3, 2),
        "MEMORY" | "UNLINK" => (4, 0),
        "XACK" | "XADD" | "XDEL" | "XINFO" | "XLEN" | "XPENDING" | "XRANGE" | "XREAD"
        | "XREADGROUP" | "XREVRANGE" | "XTRIM" => (5, 0),
        "COPY" | "GETDEL" | "GETEX" | "LMOVE" | "SMISMEMBER" | "ZDIFF" | "ZINTER" | "ZMSCORE"
        | "ZRANDMEMBER" | "ZUNION" => (6, 2),
        "EXPIRETIME" | "LCS" | "LMPOP" | "PEXPIRETIME" | "SINTERCARD" | "ZMPOP" => (7, 0),
        "HEXPIRE" | "HPERSIST" | "HTTL" => (7, 4),
        _ => return None,
    };
    Some(RedisVersion::new(version.0, version.1, 0))
}

fn module_requirement(command: &str) -> (Option<RedisModule>, Option<RedisVersion>) {
    if command.starts_with("JSON.") {
        (Some(RedisModule::Json), None)
    } else {
        (None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invocation(command: &str, arguments: &[&str]) -> Result<NativeCommandMetadata, RedisError> {
        classify_command(
            command.as_bytes(),
            &arguments
                .iter()
                .map(|argument| argument.as_bytes().to_vec())
                .collect::<Vec<_>>(),
            RawCommandPolicy::Classified,
        )
    }

    #[test]
    fn classified_policy_fails_closed_for_unknown_commands() {
        let error = classify_command(b"NEW.MODULE.COMMAND", &[], RawCommandPolicy::Classified)
            .expect_err("unknown command must fail closed");
        assert!(error.message().contains("not classified"));
        let metadata = classify_command(b"NEW.MODULE.COMMAND", &[], RawCommandPolicy::Unrestricted)
            .expect("unrestricted unknown command");
        assert_eq!(metadata.name(), "NEW.MODULE.COMMAND");
        assert_eq!(metadata.required_access(), AccessMode::Full);
        assert!(!metadata.is_classified());
    }

    #[test]
    fn hard_blocks_apply_to_unrestricted_policy() {
        for (command, code) in [
            ("AUTH", "SESSION_COMMAND_UNSUPPORTED"),
            ("MULTI", "TRANSACTION_COMMAND_UNSUPPORTED"),
            ("SUBSCRIBE", "SUBSCRIPTION_COMMAND_UNSUPPORTED"),
            ("MONITOR", "STREAMING_COMMAND_UNSUPPORTED"),
            ("BLPOP", "BLOCKING_COMMAND_UNSUPPORTED"),
            ("EVAL", "SCRIPT_COMMAND_UNSUPPORTED"),
            ("BGSAVE", "SERVER_LIFECYCLE_COMMAND_UNSUPPORTED"),
        ] {
            let error = classify_command(command.as_bytes(), &[], RawCommandPolicy::Unrestricted)
                .expect_err("hard boundary");
            assert_eq!(error.code(), Some(code), "{command}");
        }
        assert!(
            classify_command(
                b"xread",
                &[b"BLOCK".to_vec(), b"0".to_vec()],
                RawCommandPolicy::Unrestricted,
            )
            .is_err()
        );
        assert!(
            classify_command(
                b"script",
                &[b"debug".to_vec(), b"yes".to_vec()],
                RawCommandPolicy::Unrestricted,
            )
            .is_err()
        );
    }

    #[test]
    fn classified_commands_report_access_and_capability_metadata() {
        assert_eq!(
            invocation("GET", &["key"])
                .expect("GET metadata")
                .required_access(),
            AccessMode::ReadOnly
        );
        assert_eq!(
            invocation("SET", &["key", "value"])
                .expect("SET metadata")
                .required_access(),
            AccessMode::ReadWrite
        );
        assert_eq!(
            invocation("DEL", &["key"])
                .expect("DEL metadata")
                .required_access(),
            AccessMode::Full
        );
        let json = invocation("JSON.GET", &["doc"]).expect("JSON.GET metadata");
        assert_eq!(json.required_module(), Some(RedisModule::Json));
        let getdel = invocation("GETDEL", &["key"]).expect("GETDEL metadata");
        assert_eq!(
            getdel.minimum_redis_version(),
            Some(RedisVersion::new(6, 2, 0))
        );
        let httl = invocation("HTTL", &["key", "FIELDS", "1", "field"]).expect("HTTL metadata");
        assert_eq!(
            httl.minimum_redis_version(),
            Some(RedisVersion::new(7, 4, 0))
        );
    }

    #[test]
    fn replace_forms_are_destructive() {
        assert_eq!(
            invocation("COPY", &["source", "target"])
                .expect("COPY metadata")
                .required_access(),
            AccessMode::ReadWrite
        );
        assert_eq!(
            invocation("COPY", &["source", "target", "REPLACE"])
                .expect("COPY REPLACE metadata")
                .required_access(),
            AccessMode::Full
        );
    }

    #[test]
    fn nonpositive_hash_expiration_is_destructive() {
        assert_eq!(
            invocation("HEXPIRE", &["key", "60", "FIELDS", "1", "field"])
                .expect("positive HEXPIRE metadata")
                .required_access(),
            AccessMode::ReadWrite
        );
        for seconds in ["0", "-1"] {
            assert_eq!(
                invocation("HEXPIRE", &["key", seconds, "FIELDS", "1", "field"])
                    .expect("deleting HEXPIRE metadata")
                    .required_access(),
                AccessMode::Full
            );
        }
    }
}
