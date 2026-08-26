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

    if matches!(command.as_str(), "SCRIPT" | "FUNCTION") {
        return unsupported(
            &command,
            "requires the dedicated bounded scripting family",
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
        minimum_redis_version(&command, arguments),
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
        "AUTH" | "CLIENT" | "HELLO" | "HIMPORT" | "QUIT" | "READONLY" | "READWRITE" | "RESET"
        | "SELECT" => Some((
            "SESSION_COMMAND_UNSUPPORTED",
            "requires a dedicated connection-session API",
        )),
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
        | "SHUTDOWN" | "SLAVEOF" | "TRIMSLOTS" => Some((
            "SERVER_LIFECYCLE_COMMAND_UNSUPPORTED",
            "is outside native request/response invocation",
        )),
        "XCFGSET" | "XIDMPRECORD" | "XSETID" => Some((
            "INTERNAL_COMMAND_UNSUPPORTED",
            "is an internal Redis command outside supported invocation",
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
    if command == "XGROUP" {
        return match arguments.first().map(Vec::as_slice) {
            Some(subcommand)
                if eq_ascii_case(subcommand, b"DESTROY")
                    || eq_ascii_case(subcommand, b"DELCONSUMER") =>
            {
                Some(AccessMode::Full)
            }
            Some(subcommand)
                if eq_ascii_case(subcommand, b"CREATE")
                    || eq_ascii_case(subcommand, b"SETID")
                    || eq_ascii_case(subcommand, b"CREATECONSUMER") =>
            {
                Some(AccessMode::ReadWrite)
            }
            _ => None,
        };
    }
    let access = match command {
        "ACL" | "ARDEL" | "ARDELRANGE" | "BITOP" | "DEL" | "DELEX" | "GEOSEARCHSTORE"
        | "GETDEL" | "HDEL" | "HGETDEL" | "JSON.ARRPOP" | "JSON.ARRTRIM" | "JSON.CLEAR"
        | "JSON.DEL" | "LMOVEM" | "LPOP" | "LMOVE" | "LMPOP" | "LREM" | "LSET" | "LTRIM"
        | "PFMERGE" | "RENAME" | "RENAMENX" | "RPOP" | "RPOPLPUSH" | "SMOVE" | "SPOP" | "SREM"
        | "SDIFFSTORE" | "SINTERSTORE" | "SUNIONSTORE" | "UNLINK" | "VREM" | "XACKDEL" | "XDEL"
        | "XDELEX" | "XNACK" | "XTRIM" | "ZDIFFSTORE" | "ZINTERSTORE" | "ZMPOP" | "ZPOPMAX"
        | "ZPOPMIN" | "ZRANGESTORE" | "ZREM" | "ZREMRANGEBYLEX" | "ZREMRANGEBYRANK"
        | "ZREMRANGEBYSCORE" | "ZUNIONSTORE" => AccessMode::Full,
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
        "APPEND" | "ARINSERT" | "ARMSET" | "ARRING" | "ARSEEK" | "ARSET" | "BITFIELD" | "COPY"
        | "DECR" | "DECRBY" | "EXPIRE" | "EXPIREAT" | "GEOADD" | "GETEX" | "GETSET" | "HEXPIRE"
        | "HGETEX" | "HINCRBY" | "HINCRBYFLOAT" | "HMSET" | "HPERSIST" | "HSET" | "HSETEX"
        | "HSETNX" | "INCR" | "INCRBY" | "INCRBYFLOAT" | "INCREX" | "JSON.ARRAPPEND"
        | "JSON.ARRINSERT" | "JSON.NUMINCRBY" | "JSON.SET" | "JSON.TOGGLE" | "LINSERT"
        | "LPUSH" | "LPUSHX" | "MSET" | "MSETEX" | "MSETNX" | "PERSIST" | "PEXPIRE"
        | "PEXPIREAT" | "PFADD" | "PSETEX" | "RESTORE" | "RPUSH" | "RPUSHX" | "SADD" | "SET"
        | "SETBIT" | "SETEX" | "TOUCH" | "VADD" | "VSETATTR" | "XACK" | "XADD" | "XAUTOCLAIM"
        | "XCLAIM" | "XREADGROUP" | "ZADD" | "ZINCRBY" => AccessMode::ReadWrite,
        "MODULE" => AccessMode::Full,
        "ARCOUNT" | "ARGET" | "ARGETRANGE" | "ARGREP" | "ARINFO" | "ARLASTITEMS" | "ARLEN"
        | "ARMGET" | "ARNEXT" | "AROP" | "ARSCAN" | "BITCOUNT" | "BITFIELD_RO" | "BITPOS"
        | "COMMAND" | "DBSIZE" | "DIGEST" | "DUMP" | "ECHO" | "EXISTS" | "EXPIRETIME"
        | "GEODIST" | "GEOHASH" | "GEOPOS" | "GEOSEARCH" | "GET" | "GETBIT" | "GETRANGE"
        | "HEXISTS" | "HGET" | "HGETALL" | "HKEYS" | "HLEN" | "HMGET" | "HSCAN" | "HSTRLEN"
        | "HTTL" | "HVALS" | "INFO" | "JSON.ARRLEN" | "JSON.GET" | "JSON.MGET" | "JSON.OBJKEYS"
        | "JSON.OBJLEN" | "JSON.STRLEN" | "JSON.TYPE" | "LCS" | "LINDEX" | "LLEN" | "LPOS"
        | "LRANGE" | "MEMORY" | "MGET" | "OBJECT" | "PEXPIRETIME" | "PFCOUNT" | "PING" | "PTTL"
        | "RANDOMKEY" | "SCAN" | "SCARD" | "SDIFF" | "SDIFFCARD" | "SINTER" | "SINTERCARD"
        | "SISMEMBER" | "SMEMBERS" | "SMISMEMBER" | "SRANDMEMBER" | "SSCAN" | "STRLEN"
        | "SUNION" | "SUNIONCARD" | "TTL" | "TYPE" | "VCARD" | "VDIM" | "VEMB" | "VGETATTR"
        | "VINFO" | "VISMEMBER" | "VLINKS" | "VRANDMEMBER" | "VRANGE" | "VSIM" | "XINFO"
        | "XLEN" | "XPENDING" | "XRANGE" | "XREAD" | "XREVRANGE" | "ZCARD" | "ZCOUNT" | "ZDIFF"
        | "ZINTER" | "ZINTERCARD" | "ZLEXCOUNT" | "ZMSCORE" | "ZRANDMEMBER" | "ZRANGE"
        | "ZRANK" | "ZREVRANK" | "ZSCAN" | "ZSCORE" | "ZUNION" => AccessMode::ReadOnly,
        _ => return None,
    };
    Some(access)
}

fn minimum_redis_version(command: &str, arguments: &[Vec<u8>]) -> Option<RedisVersion> {
    if matches!(command, "ZINTERSTORE" | "ZUNIONSTORE")
        && arguments.windows(2).any(|arguments| {
            eq_ascii_case(&arguments[0], b"AGGREGATE") && eq_ascii_case(&arguments[1], b"COUNT")
        })
    {
        return Some(RedisVersion::new(8, 8, 0));
    }
    if (command == "BITCOUNT" && arguments.len() >= 4)
        || (command == "BITPOS" && arguments.len() >= 5)
    {
        return Some(RedisVersion::new(7, 0, 0));
    }
    if command == "BITPOS" {
        return Some(RedisVersion::new(2, 8, 7));
    }
    if matches!(command, "PFADD" | "PFCOUNT" | "PFMERGE") {
        return Some(RedisVersion::new(2, 8, 9));
    }
    if command == "XGROUP"
        && arguments
            .first()
            .is_some_and(|subcommand| eq_ascii_case(subcommand, b"CREATECONSUMER"))
    {
        return Some(RedisVersion::new(6, 2, 0));
    }
    if matches!(command, "XADD" | "XTRIM")
        && arguments.iter().any(|argument| {
            eq_ascii_case(argument, b"NOMKSTREAM")
                || eq_ascii_case(argument, b"MINID")
                || eq_ascii_case(argument, b"LIMIT")
        })
    {
        return Some(RedisVersion::new(6, 2, 0));
    }
    if matches!(command, "XRANGE" | "XREVRANGE")
        && arguments
            .iter()
            .skip(1)
            .take(2)
            .any(|argument| argument.starts_with(b"("))
    {
        return Some(RedisVersion::new(6, 2, 0));
    }
    if command == "GEOADD"
        && arguments.get(1).is_some_and(|argument| {
            eq_ascii_case(argument, b"NX")
                || eq_ascii_case(argument, b"XX")
                || eq_ascii_case(argument, b"CH")
        })
    {
        return Some(RedisVersion::new(6, 2, 0));
    }
    let version = match command {
        "GETBIT" | "SETBIT" => (2, 2),
        "BITCOUNT" | "BITOP" => (2, 6),
        "BITFIELD" | "GEOADD" | "GEODIST" | "GEOHASH" | "GEOPOS" => (3, 2),
        "BITFIELD_RO" => (6, 0),
        "GEOSEARCH" | "GEOSEARCHSTORE" => (6, 2),
        "SCAN" | "HSCAN" | "SSCAN" | "ZSCAN" => (2, 8),
        "HSTRLEN" | "TOUCH" => (3, 2),
        "MEMORY" | "UNLINK" => (4, 0),
        "XACK" | "XADD" | "XCLAIM" | "XDEL" | "XGROUP" | "XINFO" | "XLEN" | "XPENDING"
        | "XRANGE" | "XREAD" | "XREADGROUP" | "XREVRANGE" | "XTRIM" => (5, 0),
        "LPOS" => (6, 0),
        "LPOP" | "RPOP" if arguments.len() > 1 => (6, 2),
        "COPY" | "GETDEL" | "GETEX" | "LMOVE" | "SMISMEMBER" | "XAUTOCLAIM" | "ZDIFF"
        | "ZDIFFSTORE" | "ZINTER" | "ZMSCORE" | "ZRANDMEMBER" | "ZRANGESTORE" | "ZUNION" => (6, 2),
        "EXPIRETIME" | "LCS" | "LMPOP" | "PEXPIRETIME" | "SINTERCARD" | "ZINTERCARD" | "ZMPOP" => {
            (7, 0)
        }
        "HEXPIRE" | "HPERSIST" | "HTTL" => (7, 4),
        "HGETDEL" | "HGETEX" | "HSETEX" | "VADD" | "VCARD" | "VDIM" | "VEMB" | "VGETATTR"
        | "VINFO" | "VLINKS" | "VRANDMEMBER" | "VREM" | "VSETATTR" | "VSIM" => (8, 0),
        "VISMEMBER" | "XACKDEL" | "XDELEX" => (8, 2),
        "DELEX" | "DIGEST" | "MSETEX" | "VRANGE" => (8, 4),
        "ARCOUNT" | "ARDEL" | "ARDELRANGE" | "ARGET" | "ARGETRANGE" | "ARGREP" | "ARINFO"
        | "ARINSERT" | "ARLASTITEMS" | "ARLEN" | "ARMGET" | "ARMSET" | "ARNEXT" | "AROP"
        | "ARRING" | "ARSCAN" | "ARSEEK" | "ARSET" | "INCREX" | "XNACK" => (8, 8),
        "LMOVEM" | "SDIFFCARD" | "SUNIONCARD" => (8, 10),
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
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct CoverageLedger {
        commands: Vec<CoveredCommand>,
    }

    #[derive(Deserialize)]
    struct CoveredCommand {
        name: String,
        disposition: String,
        access: String,
        invocation: Option<Vec<String>>,
    }

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
    fn official_native_ledger_entries_stay_fail_closed_and_classified() {
        let ledger: CoverageLedger = serde_json::from_str(include_str!(
            "../tests/fixtures/redis-command-coverage.json"
        ))
        .expect("Redis command coverage ledger");
        let native = ledger
            .commands
            .into_iter()
            .filter(|command| command.disposition == "native")
            .collect::<Vec<_>>();
        assert_eq!(native.len(), 32);

        for covered in native {
            let invocation = covered.invocation.expect("native invocation evidence");
            let (command, arguments) = invocation.split_first().expect("native command name");
            let arguments = arguments
                .iter()
                .map(|argument| argument.as_bytes().to_vec())
                .collect::<Vec<_>>();
            let metadata =
                classify_command(command.as_bytes(), &arguments, RawCommandPolicy::Classified)
                    .unwrap_or_else(|error| {
                        panic!("{} must remain classified: {error}", covered.name)
                    });
            assert!(metadata.is_classified(), "{}", covered.name);
            assert_eq!(
                metadata.required_access().as_str(),
                covered.access,
                "{}",
                covered.name
            );
        }
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
            ("HIMPORT", "SESSION_COMMAND_UNSUPPORTED"),
            ("MULTI", "TRANSACTION_COMMAND_UNSUPPORTED"),
            ("SUBSCRIBE", "SUBSCRIPTION_COMMAND_UNSUPPORTED"),
            ("MONITOR", "STREAMING_COMMAND_UNSUPPORTED"),
            ("BLPOP", "BLOCKING_COMMAND_UNSUPPORTED"),
            ("EVAL", "SCRIPT_COMMAND_UNSUPPORTED"),
            ("SCRIPT", "SCRIPT_COMMAND_UNSUPPORTED"),
            ("FUNCTION", "SCRIPT_COMMAND_UNSUPPORTED"),
            ("BGSAVE", "SERVER_LIFECYCLE_COMMAND_UNSUPPORTED"),
            ("TRIMSLOTS", "SERVER_LIFECYCLE_COMMAND_UNSUPPORTED"),
            ("XCFGSET", "INTERNAL_COMMAND_UNSUPPORTED"),
            ("XIDMPRECORD", "INTERNAL_COMMAND_UNSUPPORTED"),
            ("XSETID", "INTERNAL_COMMAND_UNSUPPORTED"),
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
    fn redis_eight_modern_commands_have_explicit_access_and_version_metadata() {
        let access_groups = [
            (
                AccessMode::ReadOnly,
                &[
                    "ARCOUNT",
                    "ARGET",
                    "ARGETRANGE",
                    "ARGREP",
                    "ARINFO",
                    "ARLASTITEMS",
                    "ARLEN",
                    "ARMGET",
                    "ARNEXT",
                    "AROP",
                    "ARSCAN",
                    "DIGEST",
                    "VCARD",
                    "VDIM",
                    "VEMB",
                    "VGETATTR",
                    "VINFO",
                    "VISMEMBER",
                    "VLINKS",
                    "VRANDMEMBER",
                    "VRANGE",
                    "VSIM",
                ][..],
            ),
            (
                AccessMode::ReadWrite,
                &[
                    "ARINSERT", "ARMSET", "ARRING", "ARSEEK", "ARSET", "HGETEX", "HSETEX",
                    "INCREX", "MSETEX", "VADD", "VSETATTR",
                ][..],
            ),
            (
                AccessMode::Full,
                &[
                    "ARDEL",
                    "ARDELRANGE",
                    "DELEX",
                    "HGETDEL",
                    "LMOVEM",
                    "VREM",
                    "XACKDEL",
                    "XDELEX",
                    "XNACK",
                ][..],
            ),
        ];
        assert_eq!(
            access_groups
                .iter()
                .map(|(_, commands)| commands.len())
                .sum::<usize>(),
            42
        );
        for (expected_access, commands) in access_groups {
            for command in commands {
                let metadata = invocation(command, &[])
                    .unwrap_or_else(|error| panic!("{command} metadata: {error}"));
                assert!(metadata.is_classified(), "{command}");
                assert_eq!(metadata.required_access(), expected_access, "{command}");
            }
        }

        let version_groups = [
            (
                RedisVersion::new(8, 0, 0),
                &[
                    "HGETDEL",
                    "HGETEX",
                    "HSETEX",
                    "VADD",
                    "VCARD",
                    "VDIM",
                    "VEMB",
                    "VGETATTR",
                    "VINFO",
                    "VLINKS",
                    "VRANDMEMBER",
                    "VREM",
                    "VSETATTR",
                    "VSIM",
                ][..],
            ),
            (
                RedisVersion::new(8, 2, 0),
                &["VISMEMBER", "XACKDEL", "XDELEX"][..],
            ),
            (
                RedisVersion::new(8, 4, 0),
                &["DELEX", "DIGEST", "MSETEX", "VRANGE"][..],
            ),
            (
                RedisVersion::new(8, 8, 0),
                &[
                    "ARCOUNT",
                    "ARDEL",
                    "ARDELRANGE",
                    "ARGET",
                    "ARGETRANGE",
                    "ARGREP",
                    "ARINFO",
                    "ARINSERT",
                    "ARLASTITEMS",
                    "ARLEN",
                    "ARMGET",
                    "ARMSET",
                    "ARNEXT",
                    "AROP",
                    "ARRING",
                    "ARSCAN",
                    "ARSEEK",
                    "ARSET",
                    "INCREX",
                    "XNACK",
                ][..],
            ),
            (RedisVersion::new(8, 10, 0), &["LMOVEM"][..]),
        ];
        assert_eq!(
            version_groups
                .iter()
                .map(|(_, commands)| commands.len())
                .sum::<usize>(),
            42
        );
        for (expected_version, commands) in version_groups {
            for command in commands {
                let metadata = invocation(command, &[])
                    .unwrap_or_else(|error| panic!("{command} metadata: {error}"));
                assert_eq!(
                    metadata.minimum_redis_version(),
                    Some(expected_version),
                    "{command}"
                );
            }
        }
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
        for command in [
            "BITCOUNT",
            "BITFIELD_RO",
            "BITPOS",
            "GEODIST",
            "GEOHASH",
            "GEOPOS",
            "GEOSEARCH",
            "GETBIT",
            "PFCOUNT",
        ] {
            assert_eq!(
                invocation(command, &["key"])
                    .unwrap_or_else(|error| panic!("{command} metadata: {error}"))
                    .required_access(),
                AccessMode::ReadOnly,
                "{command}"
            );
        }
        for command in ["BITFIELD", "GEOADD", "PFADD", "SETBIT"] {
            assert_eq!(
                invocation(command, &["key"])
                    .unwrap_or_else(|error| panic!("{command} metadata: {error}"))
                    .required_access(),
                AccessMode::ReadWrite,
                "{command}"
            );
        }
        for command in ["BITOP", "GEOSEARCHSTORE", "PFMERGE"] {
            assert_eq!(
                invocation(command, &["key"])
                    .unwrap_or_else(|error| panic!("{command} metadata: {error}"))
                    .required_access(),
                AccessMode::Full,
                "{command}"
            );
        }
        assert_eq!(
            invocation("BITPOS", &["key", "1"])
                .expect("BITPOS metadata")
                .minimum_redis_version(),
            Some(RedisVersion::new(2, 8, 7))
        );
        assert_eq!(
            invocation("BITCOUNT", &["key", "0", "7", "BIT"])
                .expect("BITCOUNT BIT metadata")
                .minimum_redis_version(),
            Some(RedisVersion::new(7, 0, 0))
        );
        assert_eq!(
            invocation("BITPOS", &["key", "1", "0", "7", "BYTE"])
                .expect("BITPOS BYTE metadata")
                .minimum_redis_version(),
            Some(RedisVersion::new(7, 0, 0))
        );
        assert_eq!(
            invocation("GEOADD", &["places", "NX", "1", "1", "member"])
                .expect("GEOADD NX metadata")
                .minimum_redis_version(),
            Some(RedisVersion::new(6, 2, 0))
        );
        assert_eq!(
            invocation("PFCOUNT", &["key"])
                .expect("PFCOUNT metadata")
                .minimum_redis_version(),
            Some(RedisVersion::new(2, 8, 9))
        );
        assert_eq!(
            invocation("GEOSEARCH", &["key"])
                .expect("GEOSEARCH metadata")
                .minimum_redis_version(),
            Some(RedisVersion::new(6, 2, 0))
        );
        for command in ["ZINTERSTORE", "ZUNIONSTORE"] {
            assert_eq!(
                invocation(
                    command,
                    &["destination", "2", "one", "two", "AGGREGATE", "COUNT"]
                )
                .unwrap_or_else(|error| panic!("{command} COUNT metadata: {error}"))
                .minimum_redis_version(),
                Some(RedisVersion::new(8, 8, 0)),
                "{command}"
            );
            assert_eq!(
                invocation(
                    command,
                    &["destination", "2", "one", "two", "AGGREGATE", "SUM"]
                )
                .unwrap_or_else(|error| panic!("{command} SUM metadata: {error}"))
                .minimum_redis_version(),
                None,
                "{command}"
            );
        }
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
        assert_eq!(
            invocation("LPOS", &["key", "value"])
                .expect("LPOS metadata")
                .minimum_redis_version(),
            Some(RedisVersion::new(6, 0, 0))
        );
        assert_eq!(
            invocation("LPOP", &["key"])
                .expect("single LPOP metadata")
                .minimum_redis_version(),
            None
        );
        assert_eq!(
            invocation("LPOP", &["key", "2"])
                .expect("counted LPOP metadata")
                .minimum_redis_version(),
            Some(RedisVersion::new(6, 2, 0))
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
    fn stream_group_subcommands_have_explicit_access_and_versions() {
        for subcommand in ["CREATE", "SETID"] {
            let metadata = invocation("XGROUP", &[subcommand, "events", "workers"])
                .expect("classified XGROUP write subcommand");
            assert_eq!(
                metadata.required_access(),
                AccessMode::ReadWrite,
                "{subcommand}"
            );
            assert_eq!(
                metadata.minimum_redis_version(),
                Some(RedisVersion::new(5, 0, 0))
            );
        }
        assert_eq!(
            invocation("XGROUP", &["CREATECONSUMER", "events", "workers"])
                .expect("classified XGROUP CREATECONSUMER")
                .minimum_redis_version(),
            Some(RedisVersion::new(6, 2, 0))
        );
        for subcommand in ["DESTROY", "DELCONSUMER"] {
            assert_eq!(
                invocation("XGROUP", &[subcommand, "events", "workers"])
                    .expect("classified XGROUP destructive subcommand")
                    .required_access(),
                AccessMode::Full,
                "{subcommand}"
            );
        }
        for command in ["XCLAIM", "XAUTOCLAIM"] {
            assert_eq!(
                invocation(command, &["events", "workers", "consumer"])
                    .expect("classified claim command")
                    .required_access(),
                AccessMode::ReadWrite,
                "{command}"
            );
        }
        assert_eq!(
            invocation("XAUTOCLAIM", &["events", "workers", "consumer"])
                .expect("XAUTOCLAIM version")
                .minimum_redis_version(),
            Some(RedisVersion::new(6, 2, 0))
        );
        assert!(invocation("XGROUP", &["HELP"]).is_err());
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
