//! Policy for the optional raw Redis command escape hatch.

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

pub(crate) fn validate_command(
    command: &str,
    arguments: &[String],
    policy: RawCommandPolicy,
) -> Result<String, String> {
    let command = command.trim().to_ascii_uppercase();
    if command.is_empty()
        || command.len() > 128
        || !command
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(
            "command must be one Redis command name containing only letters, digits, '.', '-', or '_'"
                .to_string(),
        );
    }

    if policy == RawCommandPolicy::Disabled {
        return Err("raw command execution is disabled".to_string());
    }

    const ALWAYS_REJECTED: &[&str] = &[
        "AUTH",
        "BGREWRITEAOF",
        "BGSAVE",
        "BLMOVE",
        "BLMPOP",
        "BLPOP",
        "BRPOP",
        "BRPOPLPUSH",
        "BZMPOP",
        "BZPOPMAX",
        "BZPOPMIN",
        "CLIENT",
        "DEBUG",
        "EVAL",
        "EVALSHA",
        "EVALSHA_RO",
        "EVAL_RO",
        "EXEC",
        "FCALL",
        "FCALL_RO",
        "FAILOVER",
        "HELLO",
        "MIGRATE",
        "MONITOR",
        "MULTI",
        "PSUBSCRIBE",
        "PSYNC",
        "PUNSUBSCRIBE",
        "QUIT",
        "READONLY",
        "READWRITE",
        "REPLICAOF",
        "RESET",
        "SAVE",
        "SELECT",
        "SHUTDOWN",
        "SLAVEOF",
        "SSUBSCRIBE",
        "SUBSCRIBE",
        "SUNSUBSCRIBE",
        "SYNC",
        "UNSUBSCRIBE",
        "UNWATCH",
        "WAIT",
        "WAITAOF",
        "WATCH",
    ];
    if ALWAYS_REJECTED.contains(&command.as_str()) {
        return Err(format!(
            "{command} is not supported by the request/response raw tool"
        ));
    }

    if matches!(command.as_str(), "XREAD" | "XREADGROUP")
        && arguments
            .iter()
            .any(|argument| argument.eq_ignore_ascii_case("BLOCK"))
    {
        return Err("blocking XREAD/XREADGROUP is not supported by the raw tool".to_string());
    }

    if command == "SCRIPT"
        && arguments
            .first()
            .is_some_and(|argument| argument.eq_ignore_ascii_case("DEBUG"))
    {
        return Err("SCRIPT DEBUG is not supported by the raw tool".to_string());
    }

    if command == "MODULE"
        && arguments.first().is_some_and(|argument| {
            argument.eq_ignore_ascii_case("LOAD")
                || argument.eq_ignore_ascii_case("LOADEX")
                || argument.eq_ignore_ascii_case("UNLOAD")
        })
    {
        return Err(
            "loading or unloading server modules is not supported by the raw tool".to_string(),
        );
    }

    if policy == RawCommandPolicy::Classified && !CLASSIFIED_COMMANDS.contains(&command.as_str()) {
        return Err(format!(
            "{command} is not classified for raw execution; enable the unrestricted raw policy explicitly to allow unknown request/response commands"
        ));
    }

    Ok(command)
}

// This is intentionally an allowlist rather than an attempted copy of Redis'
// full command table. Additions are contract changes that should be reviewed
// alongside command semantics and timeout/cancellation behavior.
const CLASSIFIED_COMMANDS: &[&str] = &[
    "ACL",
    "APPEND",
    "COMMAND",
    "COPY",
    "DBSIZE",
    "DECR",
    "DECRBY",
    "DEL",
    "DUMP",
    "ECHO",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "EXPIRETIME",
    "GET",
    "GETDEL",
    "GETEX",
    "GETRANGE",
    "GETSET",
    "HDEL",
    "HEXISTS",
    "HGET",
    "HGETALL",
    "HINCRBY",
    "HLEN",
    "HMGET",
    "HMSET",
    "HSCAN",
    "HSET",
    "HSETNX",
    "HSTRLEN",
    "INCR",
    "INCRBY",
    "INFO",
    "JSON.ARRAPPEND",
    "JSON.ARRINSERT",
    "JSON.ARRLEN",
    "JSON.ARRPOP",
    "JSON.ARRTRIM",
    "JSON.CLEAR",
    "JSON.DEL",
    "JSON.GET",
    "JSON.MGET",
    "JSON.NUMINCRBY",
    "JSON.OBJKEYS",
    "JSON.OBJLEN",
    "JSON.SET",
    "JSON.STRLEN",
    "JSON.TOGGLE",
    "JSON.TYPE",
    "LCS",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LMOVE",
    "LMPOP",
    "LPOP",
    "LPOS",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "MEMORY",
    "MGET",
    "MSET",
    "MSETNX",
    "OBJECT",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PEXPIRETIME",
    "PING",
    "PTTL",
    "RANDOMKEY",
    "RENAME",
    "RENAMENX",
    "RESTORE",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SCAN",
    "SCARD",
    "SDIFF",
    "SINTER",
    "SINTERCARD",
    "SISMEMBER",
    "SMEMBERS",
    "SMISMEMBER",
    "SMOVE",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "SSCAN",
    "STRLEN",
    "SUNION",
    "TOUCH",
    "TTL",
    "TYPE",
    "UNLINK",
    "XACK",
    "XADD",
    "XDEL",
    "XINFO",
    "XLEN",
    "XPENDING",
    "XRANGE",
    "XREAD",
    "XREADGROUP",
    "XREVRANGE",
    "XTRIM",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZDIFF",
    "ZINCRBY",
    "ZINTER",
    "ZLEXCOUNT",
    "ZMPOP",
    "ZMSCORE",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANDMEMBER",
    "ZRANGE",
    "ZRANK",
    "ZREM",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZREVRANK",
    "ZSCAN",
    "ZSCORE",
    "ZUNION",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classified_policy_fails_closed_for_unknown_commands() {
        let error = validate_command("NEW.MODULE.COMMAND", &[], RawCommandPolicy::Classified)
            .expect_err("unknown command must fail closed");
        assert!(error.contains("not classified"));
        assert_eq!(
            validate_command("NEW.MODULE.COMMAND", &[], RawCommandPolicy::Unrestricted).as_deref(),
            Ok("NEW.MODULE.COMMAND")
        );
    }

    #[test]
    fn hard_blocks_apply_to_unrestricted_policy() {
        assert!(validate_command("AUTH", &[], RawCommandPolicy::Unrestricted).is_err());
        assert!(
            validate_command(
                "xread",
                &["BLOCK".into(), "0".into()],
                RawCommandPolicy::Unrestricted,
            )
            .is_err()
        );
        assert!(
            validate_command(
                "script",
                &["debug".into(), "yes".into()],
                RawCommandPolicy::Unrestricted,
            )
            .is_err()
        );
    }
}
