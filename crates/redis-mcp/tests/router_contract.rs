use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use pretty_assertions::assert_eq;
use redis_mcp::{
    AccessMode, CapabilityStatus, OutputBudget, PubSubMessage, PubSubReadRequest, PubSubReadResult,
    PubSubSessionError, PubSubSessionManager, PubSubSessionOwner, PubSubSessionSnapshot,
    PubSubSubscription, PubSubSubscriptionKind, RawCommandPolicy, RedisCapabilities,
    RedisClusterFanout, RedisCommand, RedisDeployment, RedisError, RedisExecutor, RedisMcp,
    RedisMcpBuildError, RedisModule, RedisModuleCapability, RedisValue, RedisVersion, ToolBundle,
    UnavailableToolPolicy, tool_catalog, tool_names, tool_names_for, tool_names_for_capabilities,
};
use tower_mcp::client::{ChannelTransport, McpClient};

#[derive(Clone, Copy)]
struct StubRedis;

#[derive(Clone, Copy)]
struct StubPubSubSessions;

#[derive(Clone, Default)]
struct OwnerRecordingPubSubSessions {
    subscribed_owners: Arc<Mutex<Vec<String>>>,
    closed_owners: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl PubSubSessionManager for StubPubSubSessions {
    async fn subscribe(
        &self,
        _owner: &PubSubSessionOwner,
        kind: PubSubSubscriptionKind,
        subscriptions: Vec<Vec<u8>>,
    ) -> Result<PubSubSessionSnapshot, PubSubSessionError> {
        Ok(PubSubSessionSnapshot {
            session_id: "ps_00000000000000000000000000000000".to_string(),
            subscriptions: subscriptions
                .into_iter()
                .map(|value| PubSubSubscription { kind, value })
                .collect(),
            buffered_messages: 0,
            max_buffered_messages: 100,
            max_message_bytes: 1_048_576,
            idle_timeout: Duration::from_secs(300),
        })
    }

    async fn read(
        &self,
        _owner: &PubSubSessionOwner,
        _session_id: &str,
        _request: PubSubReadRequest,
    ) -> Result<PubSubReadResult, PubSubSessionError> {
        Ok(PubSubReadResult {
            messages: vec![PubSubMessage {
                sequence: 1,
                kind: PubSubSubscriptionKind::Channel,
                channel: b"events".to_vec(),
                pattern: None,
                payload: b"hello".to_vec(),
                age: Duration::from_millis(1),
            }],
            remaining_buffered: 0,
            timed_out: false,
            dropped_buffer_full_total: 0,
            dropped_oversized_total: 0,
        })
    }

    async fn unsubscribe(
        &self,
        _owner: &PubSubSessionOwner,
        session_id: &str,
        _kind: PubSubSubscriptionKind,
        _subscriptions: Vec<Vec<u8>>,
    ) -> Result<PubSubSessionSnapshot, PubSubSessionError> {
        Ok(PubSubSessionSnapshot {
            session_id: session_id.to_string(),
            subscriptions: Vec::new(),
            buffered_messages: 0,
            max_buffered_messages: 100,
            max_message_bytes: 1_048_576,
            idle_timeout: Duration::from_secs(300),
        })
    }

    async fn close(
        &self,
        _owner: &PubSubSessionOwner,
        _session_id: &str,
    ) -> Result<(), PubSubSessionError> {
        Ok(())
    }

    async fn close_owner(&self, _owner: &PubSubSessionOwner) -> usize {
        0
    }

    async fn shutdown(&self) {}
}

#[async_trait]
impl PubSubSessionManager for OwnerRecordingPubSubSessions {
    async fn subscribe(
        &self,
        owner: &PubSubSessionOwner,
        kind: PubSubSubscriptionKind,
        subscriptions: Vec<Vec<u8>>,
    ) -> Result<PubSubSessionSnapshot, PubSubSessionError> {
        self.subscribed_owners
            .lock()
            .expect("record subscribed owner")
            .push(owner.as_str().to_string());
        Ok(PubSubSessionSnapshot {
            session_id: "ps_00000000000000000000000000000000".to_string(),
            subscriptions: subscriptions
                .into_iter()
                .map(|value| PubSubSubscription { kind, value })
                .collect(),
            buffered_messages: 0,
            max_buffered_messages: 100,
            max_message_bytes: 1_048_576,
            idle_timeout: Duration::from_secs(300),
        })
    }

    async fn read(
        &self,
        _owner: &PubSubSessionOwner,
        _session_id: &str,
        _request: PubSubReadRequest,
    ) -> Result<PubSubReadResult, PubSubSessionError> {
        StubPubSubSessions
            .read(
                _owner,
                _session_id,
                PubSubReadRequest {
                    max_messages: 1,
                    max_bytes: 1,
                    wait: Duration::ZERO,
                },
            )
            .await
    }

    async fn unsubscribe(
        &self,
        _owner: &PubSubSessionOwner,
        session_id: &str,
        _kind: PubSubSubscriptionKind,
        _subscriptions: Vec<Vec<u8>>,
    ) -> Result<PubSubSessionSnapshot, PubSubSessionError> {
        Ok(PubSubSessionSnapshot {
            session_id: session_id.to_string(),
            subscriptions: Vec::new(),
            buffered_messages: 0,
            max_buffered_messages: 100,
            max_message_bytes: 1_048_576,
            idle_timeout: Duration::from_secs(300),
        })
    }

    async fn close(
        &self,
        _owner: &PubSubSessionOwner,
        _session_id: &str,
    ) -> Result<(), PubSubSessionError> {
        Ok(())
    }

    async fn close_owner(&self, owner: &PubSubSessionOwner) -> usize {
        self.closed_owners
            .lock()
            .expect("record closed owner")
            .push(owner.as_str().to_string());
        1
    }

    async fn shutdown(&self) {}
}

#[async_trait]
impl RedisExecutor for StubRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let value = match command.name() {
            "PING" => RedisValue::SimpleString("PONG".into()),
            "INFO" => match command.arguments().first().map(Vec::as_slice) {
                Some(b"keyspace") => RedisValue::BulkString(
                    b"# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=5000,subexpiry=0\r\n"
                        .to_vec(),
                ),
                Some(b"ALL") => RedisValue::BulkString(
                    b"# Server\r\nredis_version:8.2.0\r\nredis_mode:standalone\r\nuptime_in_seconds:60\r\n# Clients\r\nconnected_clients:2\r\nblocked_clients:0\r\n# Memory\r\nused_memory:1024\r\nmaxmemory:0\r\nmem_fragmentation_ratio:1.1\r\n# Stats\r\ninstantaneous_ops_per_sec:3\r\ntotal_commands_processed:10\r\nrejected_connections:0\r\n# Persistence\r\nloading:0\r\nrdb_last_bgsave_status:ok\r\naof_last_bgrewrite_status:ok\r\n# Replication\r\nrole:master\r\n# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=5000\r\n".to_vec(),
                ),
                _ => RedisValue::BulkString(b"# Server\r\nredis_version:8.2.0\r\n".to_vec()),
            },
            "CLIENT" => RedisValue::BulkString(
                b"id=1 addr=10.0.0.1:5000 laddr=10.0.0.2:6379 fd=8 name=agent age=120 idle=61 flags=b db=0 sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=16384 rbp=0 obl=0 oll=0 omem=0 tot-mem=18000 events=r cmd=get user=default redir=-1 resp=3 lib-name=test lib-ver=1.0 io-thread=0 tot-net-in=10 tot-net-out=20 tot-cmds=2 type=normal future=value\n".to_vec(),
            ),
            "CLUSTER" => RedisValue::BulkString(
                b"cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:3\r\ncluster_size:3\r\ncluster_current_epoch:7\r\ncluster_stats_messages_sent:10\r\ncluster_stats_messages_received:9\r\nfuture_metric:1\r\n".to_vec(),
            ),
            "DBSIZE" => RedisValue::Integer(2),
            "SCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"alpha".to_vec()),
                    RedisValue::BulkString(b"beta".to_vec()),
                ]),
            ]),
            "GET" | "GETEX" | "GETDEL" => RedisValue::BulkString(b"hello".to_vec()),
            "GETRANGE" => RedisValue::BulkString(b"ell".to_vec()),
            "DUMP" => RedisValue::BulkString(vec![0, 1, 2]),
            "OBJECT" => {
                if command
                    .arguments()
                    .first()
                    .is_some_and(|value| value == b"ENCODING")
                {
                    RedisValue::BulkString(b"embstr".to_vec())
                } else {
                    RedisValue::Integer(1)
                }
            }
            "PUBLISH" | "SPUBLISH" => RedisValue::Integer(1),
            "PUBSUB" => match command.arguments().first().map(Vec::as_slice) {
                Some(b"CHANNELS" | b"SHARDCHANNELS") => RedisValue::Array(vec![
                    RedisValue::BulkString(b"events:alpha".to_vec()),
                    RedisValue::BulkString(vec![0xff, 0x00]),
                ]),
                Some(b"NUMSUB" | b"SHARDNUMSUB") => RedisValue::Array(vec![
                    RedisValue::BulkString(b"events:alpha".to_vec()),
                    RedisValue::Integer(2),
                ]),
                Some(b"NUMPAT") => RedisValue::Integer(1),
                _ => RedisValue::Nil,
            },
            "EXISTS" => RedisValue::Integer(1),
            "MGET" => RedisValue::Array(vec![
                RedisValue::BulkString(b"hello".to_vec()),
                RedisValue::Nil,
            ]),
            "STRLEN" => RedisValue::Integer(5),
            "MEMORY" if command.arguments().first().is_some_and(|arg| arg == b"STATS") => {
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"peak.allocated".to_vec()),
                    RedisValue::Integer(2048),
                    RedisValue::BulkString(b"total.allocated".to_vec()),
                    RedisValue::Integer(1024),
                    RedisValue::BulkString(b"overhead.total".to_vec()),
                    RedisValue::Integer(256),
                    RedisValue::BulkString(b"dataset.bytes".to_vec()),
                    RedisValue::Integer(768),
                    RedisValue::BulkString(b"keys.count".to_vec()),
                    RedisValue::Integer(2),
                    RedisValue::BulkString(b"keys.bytes-per-key".to_vec()),
                    RedisValue::Integer(384),
                    RedisValue::BulkString(b"fragmentation".to_vec()),
                    RedisValue::Double(1.1),
                    RedisValue::BulkString(b"future.stat".to_vec()),
                    RedisValue::BulkString(vec![0xff, 0x00]),
                ])
            }
            "MEMORY" => RedisValue::Integer(64),
            "MODULE" => RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::BulkString(b"name".to_vec()),
                RedisValue::BulkString(b"search".to_vec()),
                RedisValue::BulkString(b"ver".to_vec()),
                RedisValue::Integer(20800),
                RedisValue::BulkString(b"path".to_vec()),
                RedisValue::BulkString(b"/private/module.so".to_vec()),
                RedisValue::BulkString(b"args".to_vec()),
                RedisValue::Array(Vec::new()),
            ])]),
            "SLOWLOG" => RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::Integer(7),
                RedisValue::Integer(1_700_000_000),
                RedisValue::Integer(250),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"SET".to_vec()),
                    RedisValue::BulkString(b"secret-key".to_vec()),
                    RedisValue::BulkString(b"secret-value".to_vec()),
                ]),
                RedisValue::BulkString(b"10.0.0.1:5000".to_vec()),
                RedisValue::BulkString(b"agent".to_vec()),
            ])]),
            "LATENCY" => RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::Integer(1_700_000_000),
                RedisValue::Integer(12),
            ])]),
            "ACL" => RedisValue::BulkString(b"default".to_vec()),
            "RANDOMKEY" => RedisValue::BulkString(b"alpha".to_vec()),
            "HGET" if command.tool_name() == "redis_vector_get_hash" => RedisValue::BulkString(
                [1.0_f32, 2.0_f32]
                    .into_iter()
                    .flat_map(f32::to_le_bytes)
                    .collect(),
            ),
            "HGET" => RedisValue::BulkString(b"Ada".to_vec()),
            "HGETALL" => RedisValue::Map(vec![(
                RedisValue::BulkString(b"name".to_vec()),
                RedisValue::BulkString(b"Ada".to_vec()),
            )]),
            "HEXISTS" => RedisValue::Integer(1),
            "HKEYS" => RedisValue::Array(vec![RedisValue::BulkString(b"name".to_vec())]),
            "HLEN" => RedisValue::Integer(1),
            "HMGET" => RedisValue::Array(vec![
                RedisValue::BulkString(b"Ada".to_vec()),
                RedisValue::Nil,
            ]),
            "HSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"7".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"name".to_vec()),
                    RedisValue::BulkString(b"Ada".to_vec()),
                ]),
            ]),
            "HSTRLEN" => RedisValue::Integer(3),
            "HTTL" => RedisValue::Array(vec![RedisValue::Integer(-1)]),
            "HRANDFIELD" => RedisValue::Array(vec![
                RedisValue::BulkString(b"name".to_vec()),
                RedisValue::BulkString(b"Ada".to_vec()),
                RedisValue::BulkString(b"name".to_vec()),
                RedisValue::BulkString(b"Ada".to_vec()),
            ]),
            "HVALS" => RedisValue::Array(vec![RedisValue::BulkString(b"Ada".to_vec())]),
            "LRANGE" => RedisValue::Array(vec![
                RedisValue::BulkString(b"second".to_vec()),
                RedisValue::BulkString(b"first".to_vec()),
            ]),
            "LINDEX" => RedisValue::BulkString(b"second".to_vec()),
            "LLEN" => RedisValue::Integer(2),
            "LPOS" => RedisValue::Array(vec![RedisValue::Integer(0)]),
            "SCARD" => RedisValue::Integer(2),
            "SDIFF" => RedisValue::Set(vec![RedisValue::BulkString(b"alpha".to_vec())]),
            "SINTER" => RedisValue::Set(vec![RedisValue::BulkString(b"beta".to_vec())]),
            "SISMEMBER" => RedisValue::Integer(1),
            "SMEMBERS" => RedisValue::Set(vec![
                RedisValue::BulkString(b"beta".to_vec()),
                RedisValue::BulkString(b"alpha".to_vec()),
            ]),
            "SMISMEMBER" => RedisValue::Array(vec![RedisValue::Integer(1), RedisValue::Integer(0)]),
            "SSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(vec![RedisValue::BulkString(b"alpha".to_vec())]),
            ]),
            "SUNION" => RedisValue::Set(vec![
                RedisValue::BulkString(b"beta".to_vec()),
                RedisValue::BulkString(b"alpha".to_vec()),
            ]),
            "SDIFFCARD" | "SUNIONCARD" | "ZINTERCARD" => RedisValue::Integer(2),
            "SDIFFSTORE" | "SINTERSTORE" | "SUNIONSTORE" | "ZDIFFSTORE"
            | "ZINTERSTORE" | "ZRANGESTORE" | "ZUNIONSTORE" => RedisValue::Integer(2),
            "ZCARD" => RedisValue::Integer(2),
            "ZCOUNT" => RedisValue::Integer(1),
            "ZSCORE" => RedisValue::BulkString(b"1.5".to_vec()),
            "ZMSCORE" => RedisValue::Array(vec![
                RedisValue::BulkString(b"1.5".to_vec()),
                RedisValue::Nil,
            ]),
            "ZRANK" | "ZREVRANK" => RedisValue::Integer(0),
            "ZRANGE" => {
                if command
                    .arguments()
                    .iter()
                    .any(|argument| argument.eq_ignore_ascii_case(b"WITHSCORES"))
                {
                    RedisValue::Array(vec![
                        RedisValue::BulkString(b"alice".to_vec()),
                        RedisValue::BulkString(b"1.5".to_vec()),
                    ])
                } else {
                    RedisValue::Array(vec![RedisValue::BulkString(b"alice".to_vec())])
                }
            }
            "ZSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"3".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"alice".to_vec()),
                    RedisValue::BulkString(b"1.5".to_vec()),
                ]),
            ]),
            "ZINCRBY" => RedisValue::BulkString(b"2.5".to_vec()),
            "ZPOPMIN" | "ZPOPMAX" => RedisValue::Array(vec![
                RedisValue::BulkString(b"alice".to_vec()),
                RedisValue::BulkString(b"1.5".to_vec()),
            ]),
            "XLEN" => RedisValue::Integer(2),
            "XRANGE" | "XREVRANGE" => RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::BulkString(b"1-0".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"event".to_vec()),
                    RedisValue::BulkString(b"created".to_vec()),
                ]),
            ])]),
            "XREAD" | "XREADGROUP" => RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::BulkString(b"events".to_vec()),
                RedisValue::Array(vec![RedisValue::Array(vec![
                    RedisValue::BulkString(b"1-0".to_vec()),
                    RedisValue::Array(vec![
                        RedisValue::BulkString(b"event".to_vec()),
                        RedisValue::BulkString(b"created".to_vec()),
                    ]),
                ])]),
            ])]),
            "XINFO"
                if command
                    .arguments()
                    .first()
                    .is_some_and(|arg| arg == b"STREAM") =>
            {
                RedisValue::Map(vec![
                    (
                        RedisValue::BulkString(b"length".to_vec()),
                        RedisValue::Integer(2),
                    ),
                    (
                        RedisValue::BulkString(b"radix-tree-keys".to_vec()),
                        RedisValue::Integer(1),
                    ),
                    (
                        RedisValue::BulkString(b"radix-tree-nodes".to_vec()),
                        RedisValue::Integer(2),
                    ),
                    (
                        RedisValue::BulkString(b"last-generated-id".to_vec()),
                        RedisValue::BulkString(b"2-0".to_vec()),
                    ),
                    (
                        RedisValue::BulkString(b"max-deleted-entry-id".to_vec()),
                        RedisValue::BulkString(b"0-0".to_vec()),
                    ),
                    (
                        RedisValue::BulkString(b"entries-added".to_vec()),
                        RedisValue::Integer(2),
                    ),
                    (
                        RedisValue::BulkString(b"recorded-first-entry-id".to_vec()),
                        RedisValue::BulkString(b"1-0".to_vec()),
                    ),
                    (
                        RedisValue::BulkString(b"groups".to_vec()),
                        RedisValue::Integer(1),
                    ),
                    (
                        RedisValue::BulkString(b"first-entry".to_vec()),
                        RedisValue::Array(vec![
                            RedisValue::BulkString(b"1-0".to_vec()),
                            RedisValue::Array(vec![
                                RedisValue::BulkString(b"event".to_vec()),
                                RedisValue::BulkString(b"created".to_vec()),
                            ]),
                        ]),
                    ),
                    (
                        RedisValue::BulkString(b"last-entry".to_vec()),
                        RedisValue::Array(vec![
                            RedisValue::BulkString(b"2-0".to_vec()),
                            RedisValue::Array(vec![
                                RedisValue::BulkString(b"event".to_vec()),
                                RedisValue::BulkString(b"updated".to_vec()),
                            ]),
                        ]),
                    ),
                ])
            }
            "XINFO"
                if command
                    .arguments()
                    .first()
                    .is_some_and(|arg| arg == b"GROUPS") =>
            {
                RedisValue::Array(vec![RedisValue::Map(vec![
                    (
                        RedisValue::BulkString(b"name".to_vec()),
                        RedisValue::BulkString(b"workers".to_vec()),
                    ),
                    (
                        RedisValue::BulkString(b"consumers".to_vec()),
                        RedisValue::Integer(1),
                    ),
                    (
                        RedisValue::BulkString(b"pending".to_vec()),
                        RedisValue::Integer(1),
                    ),
                    (
                        RedisValue::BulkString(b"last-delivered-id".to_vec()),
                        RedisValue::BulkString(b"1-0".to_vec()),
                    ),
                    (
                        RedisValue::BulkString(b"entries-read".to_vec()),
                        RedisValue::Integer(1),
                    ),
                    (
                        RedisValue::BulkString(b"lag".to_vec()),
                        RedisValue::Integer(1),
                    ),
                ])])
            }
            "XINFO" => RedisValue::Array(vec![RedisValue::Map(vec![
                (
                    RedisValue::BulkString(b"name".to_vec()),
                    RedisValue::BulkString(b"worker-1".to_vec()),
                ),
                (
                    RedisValue::BulkString(b"pending".to_vec()),
                    RedisValue::Integer(1),
                ),
                (
                    RedisValue::BulkString(b"idle".to_vec()),
                    RedisValue::Integer(25),
                ),
                (
                    RedisValue::BulkString(b"inactive".to_vec()),
                    RedisValue::Integer(10),
                ),
            ])]),
            "XPENDING" if command.arguments().len() == 2 => RedisValue::Array(vec![
                RedisValue::Integer(1),
                RedisValue::BulkString(b"1-0".to_vec()),
                RedisValue::BulkString(b"1-0".to_vec()),
                RedisValue::Array(vec![RedisValue::Array(vec![
                    RedisValue::BulkString(b"worker-1".to_vec()),
                    RedisValue::BulkString(b"1".to_vec()),
                ])]),
            ]),
            "XPENDING" => RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::BulkString(b"1-0".to_vec()),
                RedisValue::BulkString(b"worker-1".to_vec()),
                RedisValue::Integer(25),
                RedisValue::Integer(1),
            ])]),
            "XADD" => RedisValue::BulkString(b"2-0".to_vec()),
            "XGROUP"
                if command
                    .arguments()
                    .first()
                    .is_some_and(|arg| arg == b"CREATE" || arg == b"SETID") =>
            {
                RedisValue::Okay
            }
            "XGROUP" => RedisValue::Integer(1),
            "XACK" | "XDEL" | "XTRIM" => RedisValue::Integer(1),
            "XCLAIM" if command.arguments().iter().any(|arg| arg == b"JUSTID") => {
                RedisValue::Array(vec![RedisValue::BulkString(b"1-0".to_vec())])
            }
            "XCLAIM" => RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::BulkString(b"1-0".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"event".to_vec()),
                    RedisValue::BulkString(b"created".to_vec()),
                ]),
            ])]),
            "XAUTOCLAIM" => RedisValue::Array(vec![
                RedisValue::BulkString(b"0-0".to_vec()),
                RedisValue::Array(vec![RedisValue::Array(vec![
                    RedisValue::BulkString(b"1-0".to_vec()),
                    RedisValue::Array(vec![
                        RedisValue::BulkString(b"event".to_vec()),
                        RedisValue::BulkString(b"created".to_vec()),
                    ]),
                ])]),
                RedisValue::Array(Vec::new()),
            ]),
            "GETBIT" | "SETBIT" => RedisValue::Integer(1),
            "BITCOUNT" => RedisValue::Integer(3),
            "BITPOS" => RedisValue::Integer(2),
            "BITFIELD" | "BITFIELD_RO" => RedisValue::Array(vec![RedisValue::Integer(7)]),
            "BITOP" => RedisValue::Integer(4),
            "GEOADD" => RedisValue::Integer(1),
            "GEODIST" => RedisValue::BulkString(b"111.2263".to_vec()),
            "GEOHASH" => RedisValue::Array(vec![RedisValue::BulkString(
                b"9q8yyk8ytpx".to_vec(),
            )]),
            "GEOPOS" => RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::BulkString(b"-122.4193999171257019".to_vec()),
                RedisValue::BulkString(b"37.77490001056517124".to_vec()),
            ])]),
            "GEOSEARCH" => RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::BulkString(b"san-francisco".to_vec()),
                RedisValue::BulkString(b"0.0000".to_vec()),
                RedisValue::Integer(1_366_419_482_564_889),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"-122.4193999171257019".to_vec()),
                    RedisValue::BulkString(b"37.77490001056517124".to_vec()),
                ]),
            ])]),
            "GEOSEARCHSTORE" => RedisValue::Integer(1),
            "PFADD" => RedisValue::Integer(1),
            "PFCOUNT" => RedisValue::Integer(42),
            "PFMERGE" => RedisValue::Okay,
            "ARCOUNT" | "ARLEN" => RedisValue::Integer(2),
            "ARGET" => RedisValue::BulkString(b"array-value".to_vec()),
            "ARGETRANGE" | "ARLASTITEMS" | "ARMGET" => RedisValue::Array(vec![
                RedisValue::BulkString(b"array-value".to_vec()),
                RedisValue::Nil,
            ]),
            "ARGREP" if command.arguments().iter().any(|arg| arg == b"WITHVALUES") => {
                RedisValue::Array(vec![
                    RedisValue::Integer(7),
                    RedisValue::BulkString(vec![0xff]),
                ])
            }
            "ARGREP" => RedisValue::Array(vec![RedisValue::Integer(7)]),
            "ARINFO" => RedisValue::Array(vec![
                RedisValue::BulkString(b"count".to_vec()),
                RedisValue::Integer(2),
                RedisValue::BulkString(b"len".to_vec()),
                RedisValue::Integer(8),
            ]),
            "ARNEXT" => RedisValue::Integer(8),
            "AROP" => RedisValue::Integer(2),
            "ARSCAN" => RedisValue::Array(vec![
                RedisValue::Integer(7),
                RedisValue::BulkString(vec![0xff]),
            ]),
            "ARDEL" | "ARDELRANGE" | "ARINSERT" | "ARMSET" | "ARRING" | "ARSEEK"
            | "ARSET" => RedisValue::Integer(1),
            "DIGEST" => RedisValue::BulkString(b"-123456789".to_vec()),
            "DELEX" | "HSETEX" | "MSETEX" | "VADD" | "VISMEMBER" | "VREM"
            | "VSETATTR" => RedisValue::Integer(1),
            "HGETDEL" | "HGETEX" => RedisValue::Array(vec![
                RedisValue::BulkString(b"hash-value".to_vec()),
                RedisValue::Nil,
            ]),
            "INCREX" => RedisValue::Array(vec![
                RedisValue::BulkString(b"3.5".to_vec()),
                RedisValue::BulkString(b"1.5".to_vec()),
            ]),
            "LMOVEM" => RedisValue::Array(vec![RedisValue::BulkString(b"moved".to_vec())]),
            "VCARD" => RedisValue::Integer(3),
            "VDIM" => RedisValue::Integer(2),
            "VEMB" if command.arguments().iter().any(|arg| arg == b"RAW") => {
                RedisValue::BulkString(vec![0, 0, 0, 0, 0, 0, 128, 63])
            }
            "VEMB" => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::BulkString(b"1".to_vec()),
            ]),
            "VGETATTR" => RedisValue::BulkString(br#"{"role":"primary"}"#.to_vec()),
            "VINFO" => RedisValue::Array(vec![
                RedisValue::BulkString(b"size".to_vec()),
                RedisValue::Integer(3),
                RedisValue::BulkString(b"dim".to_vec()),
                RedisValue::Integer(2),
            ]),
            "VLINKS" => RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::BulkString(b"neighbor".to_vec()),
                RedisValue::BulkString(b"0.9".to_vec()),
            ])]),
            "VRANDMEMBER" if command.arguments().len() > 1 => {
                RedisValue::Array(vec![RedisValue::BulkString(b"member".to_vec())])
            }
            "VRANDMEMBER" => RedisValue::BulkString(b"member".to_vec()),
            "VRANGE" => RedisValue::Array(vec![RedisValue::BulkString(b"member".to_vec())]),
            "VSIM" => RedisValue::Array(vec![
                RedisValue::BulkString(b"member".to_vec()),
                RedisValue::BulkString(b"0.1".to_vec()),
            ]),
            "XACKDEL" | "XDELEX" => RedisValue::Array(vec![RedisValue::Integer(1)]),
            "XNACK" => RedisValue::Integer(1),
            "TYPE" => RedisValue::SimpleString("string".into()),
            "TTL" => RedisValue::Integer(-1),
            "SET" | "MSET" | "RENAME" | "RESTORE" | "LSET" | "LTRIM" => RedisValue::Okay,
            "EXPIRE" | "PERSIST" | "COPY" | "TOUCH" | "RENAMENX" => RedisValue::Integer(1),
            "INCR" | "DECR" | "DECRBY" | "INCRBY" => RedisValue::Integer(2),
            "INCRBYFLOAT" => RedisValue::BulkString(b"2.5".to_vec()),
            "SORT_RO" => RedisValue::Array(vec![
                RedisValue::BulkString(b"first".to_vec()),
                RedisValue::BulkString(b"second".to_vec()),
            ]),
            "SORT" => RedisValue::Integer(2),
            "SETRANGE" => RedisValue::Integer(5),
            "APPEND" => RedisValue::Integer(5),
            "HSET" | "SADD" | "SREM" | "ZADD" | "ZREM" | "ZREMRANGEBYSCORE" | "HINCRBY"
            | "HDEL" => RedisValue::Integer(1),
            "HINCRBYFLOAT" => RedisValue::BulkString(b"2.5".to_vec()),
            "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT"
                if command.tool_name() == "redis_hexpire_delete" =>
            {
                RedisValue::Array(vec![RedisValue::Integer(2)])
            }
            "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT" | "HPERSIST" => {
                RedisValue::Array(vec![RedisValue::Integer(1)])
            }
            "LPUSH" | "RPUSH" => RedisValue::Integer(2),
            "LPOP" | "RPOP" => RedisValue::Array(vec![RedisValue::BulkString(b"first".to_vec())]),
            "LMOVE" => RedisValue::BulkString(b"first".to_vec()),
            "LREM" => RedisValue::Integer(1),
            "DEL" | "UNLINK" => RedisValue::Integer(1),
            "JSON.GET" => RedisValue::BulkString(br#"[{"name":"Ada"}]"#.to_vec()),
            "JSON.TYPE" => {
                let value_type = match command.tool_name() {
                    "redis_json_strlen" => "string",
                    "redis_json_numincrby" => "integer",
                    "redis_json_toggle" => "boolean",
                    "redis_json_arrlen"
                    | "redis_json_arrappend"
                    | "redis_json_arrinsert"
                    | "redis_json_arrpop"
                    | "redis_json_arrtrim" => "array",
                    _ => "object",
                };
                RedisValue::Array(vec![RedisValue::BulkString(value_type.as_bytes().to_vec())])
            }
            "JSON.MGET" => RedisValue::Array(vec![RedisValue::BulkString(
                br#"[{"name":"Ada"}]"#.to_vec(),
            )]),
            "JSON.STRLEN" => RedisValue::Array(vec![RedisValue::Integer(3)]),
            "JSON.OBJKEYS" => {
                RedisValue::Array(vec![RedisValue::Array(vec![RedisValue::BulkString(
                    b"name".to_vec(),
                )])])
            }
            "JSON.OBJLEN" | "JSON.ARRLEN" => RedisValue::Array(vec![RedisValue::Integer(1)]),
            "JSON.SET" => RedisValue::Okay,
            "JSON.NUMINCRBY" => RedisValue::BulkString(b"[43]".to_vec()),
            "JSON.TOGGLE" => RedisValue::Array(vec![RedisValue::Integer(0)]),
            "JSON.ARRAPPEND" | "JSON.ARRINSERT" | "JSON.ARRTRIM" => {
                RedisValue::Array(vec![RedisValue::Integer(3)])
            }
            "JSON.DEL" => RedisValue::Integer(1),
            "JSON.CLEAR" => RedisValue::Integer(1),
            "JSON.ARRPOP" => RedisValue::Array(vec![RedisValue::BulkString(b"1".to_vec())]),
            "JSON.MERGE" => RedisValue::Okay,
            "FT._LIST" => RedisValue::Array(vec![RedisValue::BulkString(b"idx:docs".to_vec())]),
            "FT.INFO" => RedisValue::Array(vec![
                RedisValue::BulkString(b"index_name".to_vec()),
                RedisValue::BulkString(b"idx:docs".to_vec()),
                RedisValue::BulkString(b"num_docs".to_vec()),
                RedisValue::Integer(1),
            ]),
            "FT.SEARCH"
                if matches!(
                    command.tool_name(),
                    "redis_ft_vector_search" | "redis_ft_hybrid_search"
                ) =>
            {
                RedisValue::Array(vec![
                    RedisValue::Integer(1),
                    RedisValue::BulkString(b"doc:1".to_vec()),
                    RedisValue::Array(vec![
                        RedisValue::BulkString(b"vector_distance".to_vec()),
                        RedisValue::BulkString(b"0.125".to_vec()),
                        RedisValue::BulkString(b"title".to_vec()),
                        RedisValue::BulkString(b"Redis guide".to_vec()),
                    ]),
                ])
            }
            "FT.SEARCH" => RedisValue::Array(vec![
                RedisValue::Integer(1),
                RedisValue::BulkString(b"doc:1".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"title".to_vec()),
                    RedisValue::BulkString(b"Redis guide".to_vec()),
                ]),
            ]),
            "FT.AGGREGATE" => RedisValue::Array(vec![
                RedisValue::Integer(1),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"category".to_vec()),
                    RedisValue::BulkString(b"docs".to_vec()),
                    RedisValue::BulkString(b"count".to_vec()),
                    RedisValue::BulkString(b"1".to_vec()),
                ]),
            ]),
            "FT.CURSOR" if command.tool_name() == "redis_ft_cursor_read" => {
                RedisValue::Array(vec![
                    RedisValue::Array(vec![
                        RedisValue::Integer(1),
                        RedisValue::Array(vec![
                            RedisValue::BulkString(b"category".to_vec()),
                            RedisValue::BulkString(b"docs".to_vec()),
                        ]),
                    ]),
                    RedisValue::Integer(0),
                ])
            }
            "FT.EXPLAIN" => RedisValue::BulkString(b"INTERSECT { redis }".to_vec()),
            "FT.PROFILE" => RedisValue::Array(vec![
                RedisValue::Array(vec![RedisValue::Integer(0)]),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"Total profile time".to_vec()),
                    RedisValue::Double(0.25),
                ]),
            ]),
            "FT.TAGVALS" => RedisValue::Array(vec![
                RedisValue::BulkString(b"database".to_vec()),
                RedisValue::BulkString(b"search".to_vec()),
            ]),
            "FT.DICTDUMP" => RedisValue::Array(vec![
                RedisValue::BulkString(b"redis".to_vec()),
                RedisValue::BulkString(b"valkey".to_vec()),
            ]),
            "FT.SYNDUMP" => RedisValue::Array(vec![
                RedisValue::BulkString(b"fast".to_vec()),
                RedisValue::Array(vec![RedisValue::BulkString(b"speed".to_vec())]),
            ]),
            "FT.DICTADD" | "FT.DICTDEL" => RedisValue::Integer(1),
            "FT.CREATE" | "FT.DROPINDEX" | "FT.ALTER" | "FT.SYNUPDATE" | "FT.ALIASADD"
            | "FT.ALIASUPDATE" | "FT.ALIASDEL" | "FT.CURSOR" => RedisValue::Okay,
            "ECHO" => RedisValue::BulkString(b"hello".to_vec()),
            _ => RedisValue::Nil,
        };
        Ok(value)
    }
}

async fn client(access: AccessMode, raw: bool) -> McpClient {
    let router = RedisMcp::builder(StubRedis)
        .access(access)
        .raw_commands(raw)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect in-process client");
    client
        .initialize("redis-mcp-contract-test", "0")
        .await
        .expect("initialize client");
    client
}

async fn client_with_budget(
    access: AccessMode,
    raw: bool,
    output_budget: OutputBudget,
) -> McpClient {
    let router = RedisMcp::builder(StubRedis)
        .access(access)
        .raw_commands(raw)
        .output_budget(output_budget)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect in-process budget client");
    client
        .initialize("redis-mcp-budget-test", "0")
        .await
        .expect("initialize budget client");
    client
}

async fn client_for_bundles(
    access: AccessMode,
    bundles: impl IntoIterator<Item = ToolBundle>,
    raw_policy: RawCommandPolicy,
) -> McpClient {
    let bundles = bundles.into_iter().collect::<Vec<_>>();
    let mut builder = RedisMcp::builder(StubRedis)
        .access(access)
        .bundles(bundles.iter().copied())
        .raw_command_policy(raw_policy);
    if bundles.contains(&ToolBundle::Sessions) {
        builder = builder.pubsub_sessions(StubPubSubSessions);
    }
    let router = builder.build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect in-process client");
    client
        .initialize("redis-mcp-contract-test", "0")
        .await
        .expect("initialize client");
    client
}

#[derive(Clone, Default)]
struct RecordingRedis {
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

#[derive(Clone)]
struct FixedRedis {
    response: RedisValue,
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

impl FixedRedis {
    fn new(response: RedisValue) -> Self {
        Self {
            response,
            commands: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl RedisExecutor for FixedRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        self.commands.lock().expect("fixed lock").push(command);
        Ok(self.response.clone())
    }
}

async fn fixed_client(executor: FixedRedis, capabilities: RedisCapabilities) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .capabilities(capabilities)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect fixed client");
    client
        .initialize("redis-mcp-fixed-test", "0")
        .await
        .expect("initialize fixed client");
    client
}

#[tokio::test]
async fn pubsub_publish_is_binary_safe_and_exposes_cluster_count_scope() {
    for tool in ["redis_publish", "redis_spublish"] {
        let executor = FixedRedis::new(RedisValue::Integer(3));
        let commands = executor.commands.clone();
        let client = fixed_client(
            executor,
            RedisCapabilities::unknown()
                .with_redis_version(RedisVersion::new(7, 0, 0))
                .with_deployment(RedisDeployment::Cluster),
        )
        .await;
        let result = client
            .call_tool(
                tool,
                serde_json::json!({
                    "channel": {"value": "/wA=", "encoding": "base64"},
                    "message": {"value": "/gE=", "encoding": "base64"}
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool} structured output"));
        assert_eq!(result["receivers"], 3, "{tool}");
        assert_eq!(result["receiver_count_scope"], "executing_node", "{tool}");
        assert_eq!(result["channel"]["value"], "/wA=", "{tool}");
        assert_eq!(result["channel"]["encoding"], "base64", "{tool}");

        let commands = commands.lock().expect("recorded Pub/Sub publish");
        assert_eq!(commands.len(), 1, "{tool}");
        assert_eq!(commands[0].arguments()[0], [0xff, 0x00], "{tool}");
        assert_eq!(commands[0].arguments()[1], [0xfe, 0x01], "{tool}");
        assert_eq!(
            commands[0].name(),
            if tool == "redis_publish" {
                "PUBLISH"
            } else {
                "SPUBLISH"
            }
        );
    }
}

#[tokio::test]
async fn pubsub_channel_cluster_aggregation_is_sorted_deduplicated_and_explicit() {
    let executor = FixedRedis::new(RedisValue::ClusterNodes(vec![
        (
            "node-b:6379".to_string(),
            RedisValue::Array(vec![
                RedisValue::BulkString(b"zeta".to_vec()),
                RedisValue::BulkString(b"alpha".to_vec()),
                RedisValue::BulkString(vec![0xff, 0x00]),
            ]),
        ),
        (
            "node-a:6379".to_string(),
            RedisValue::Array(vec![RedisValue::BulkString(b"alpha".to_vec())]),
        ),
        (
            "node-c:6379".to_string(),
            RedisValue::ServerError {
                code: "NOPERM".to_string(),
                message: Some("permission denied".to_string()),
            },
        ),
    ]));
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_deployment(RedisDeployment::Cluster),
    )
    .await;
    let result = client
        .call_tool(
            "redis_pubsub_channels",
            serde_json::json!({
                "pattern": {"value": "events:*"},
                "limit": 4,
                "max_cluster_nodes": 4
            }),
        )
        .await
        .expect("cluster channel inspection")
        .structured_content
        .expect("structured cluster channel inspection");
    assert_eq!(result["count"], 3);
    assert_eq!(result["channels"][0]["value"], "alpha");
    assert_eq!(result["channels"][1]["value"], "zeta");
    assert_eq!(result["channels"][2]["value"], "/wA=");
    assert_eq!(result["channels"][2]["encoding"], "base64");
    assert_eq!(result["cluster"]["nodes_queried"], 3);
    assert_eq!(result["cluster"]["nodes_succeeded"], 2);
    assert_eq!(result["cluster"]["complete"], false);
    assert_eq!(result["cluster"]["failures"][0]["node"], "node-c:6379");

    let commands = commands.lock().expect("recorded channel inspection");
    assert_eq!(commands.len(), 1);
    assert_eq!(
        commands[0].arguments(),
        [b"CHANNELS".to_vec(), b"events:*".to_vec()]
    );
    assert_eq!(commands[0].cluster_node_limit(), Some(4));
}

#[tokio::test]
async fn pubsub_subscriber_counts_sum_cluster_nodes_and_preserve_zeroes() {
    let executor = FixedRedis::new(RedisValue::ClusterNodes(vec![
        (
            "node-a:6379".to_string(),
            RedisValue::Array(vec![
                RedisValue::BulkString(b"alpha".to_vec()),
                RedisValue::Integer(2),
                RedisValue::BulkString(b"beta".to_vec()),
                RedisValue::Integer(0),
            ]),
        ),
        (
            "node-b:6379".to_string(),
            RedisValue::Map(vec![(
                RedisValue::BulkString(b"alpha".to_vec()),
                RedisValue::Integer(3),
            )]),
        ),
    ]));
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_deployment(RedisDeployment::Cluster),
    )
    .await;
    let result = client
        .call_tool(
            "redis_pubsub_numsub",
            serde_json::json!({
                "channels": [
                    {"value": "beta"},
                    {"value": "alpha"},
                    {"value": "alpha"}
                ],
                "max_cluster_nodes": 8
            }),
        )
        .await
        .expect("cluster subscriber counts")
        .structured_content
        .expect("structured cluster subscriber counts");
    assert_eq!(result["count"], 2);
    assert_eq!(result["counts"][0]["channel"]["value"], "alpha");
    assert_eq!(result["counts"][0]["subscribers"], 5);
    assert_eq!(result["counts"][1]["channel"]["value"], "beta");
    assert_eq!(result["counts"][1]["subscribers"], 0);
    assert_eq!(result["cluster"]["complete"], true);

    let commands = commands.lock().expect("recorded subscriber inspection");
    assert_eq!(
        commands[0].arguments(),
        [b"NUMSUB".to_vec(), b"alpha".to_vec(), b"beta".to_vec()]
    );
    assert_eq!(commands[0].cluster_node_limit(), Some(8));
}

#[tokio::test]
async fn pubsub_channel_enumeration_fails_with_a_structured_requested_limit() {
    let client = fixed_client(
        FixedRedis::new(RedisValue::Array(vec![
            RedisValue::BulkString(b"alpha".to_vec()),
            RedisValue::BulkString(b"beta".to_vec()),
        ])),
        RedisCapabilities::unknown(),
    )
    .await;
    let result = client
        .call_tool("redis_pubsub_channels", serde_json::json!({"limit": 1}))
        .await
        .expect("bounded channel inspection");
    assert!(result.is_error);
    let serialized = serde_json::to_value(result).expect("serialize output-limit response");
    assert_eq!(
        serialized["_meta"]["io.redis.mcp/outputLimit"]["dimension"],
        "collection_entries"
    );
    assert_eq!(serialized["_meta"]["io.redis.mcp/outputLimit"]["limit"], 1);
}

#[tokio::test]
async fn pubsub_cluster_aggregation_fails_when_every_node_fails() {
    let client = fixed_client(
        FixedRedis::new(RedisValue::ClusterNodes(vec![(
            "node-a:6379".to_string(),
            RedisValue::ServerError {
                code: "NOPERM".to_string(),
                message: Some("permission denied".to_string()),
            },
        )])),
        RedisCapabilities::unknown().with_deployment(RedisDeployment::Cluster),
    )
    .await;
    let result = client
        .call_tool("redis_pubsub_numpat", serde_json::json!({}))
        .await
        .expect("all-node failure tool result");
    assert!(result.is_error);
    let result = serde_json::to_string(&result).expect("serialize all-node failure");
    assert!(result.contains("failed on every node"), "{result}");
    assert!(result.contains("node-a:6379=NOPERM"), "{result}");
}

#[tokio::test]
async fn redis_six_rejects_sharded_pubsub_before_execution() {
    let executor = FixedRedis::new(RedisValue::Integer(0));
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 2, 0)),
    )
    .await;
    let result = client
        .call_tool(
            "redis_spublish",
            serde_json::json!({
                "channel": {"value": "events"},
                "message": {"value": "hello"}
            }),
        )
        .await
        .expect("known old Redis capability rejection");
    assert!(result.is_error);
    assert!(
        serde_json::to_string(&result)
            .expect("serialize capability error")
            .contains("requires Redis 7.0.0 or newer")
    );
    assert!(
        commands
            .lock()
            .expect("no sharded publish command")
            .is_empty()
    );

    let client = capability_client(
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 2, 0)),
        UnavailableToolPolicy::Advertise,
    )
    .await;
    let result = client
        .call_tool(
            "redis_ssubscribe",
            serde_json::json!({"subscriptions": [{"value": "events:{one}"}]}),
        )
        .await
        .expect("known old Redis session capability rejection");
    assert!(result.is_error);
    assert!(
        serde_json::to_string(&result)
            .expect("serialize session capability error")
            .contains("requires Redis 7.0.0 or newer")
    );
    let result = client
        .call_tool(
            "redis_pubsub_unsubscribe",
            serde_json::json!({
                "session_id": "ps_00000000000000000000000000000000",
                "kind": "sharded",
                "subscriptions": [{"value": "events:{one}"}]
            }),
        )
        .await
        .expect("known old Redis sharded unsubscribe rejection");
    assert!(result.is_error);
    assert!(
        serde_json::to_string(&result)
            .expect("serialize sharded unsubscribe capability error")
            .contains("requires Redis 7.0.0 or newer")
    );
}

#[tokio::test]
async fn specialized_data_version_gates_fail_before_execution() {
    let executor = FixedRedis::new(RedisValue::Integer(0));
    let commands = executor.commands.clone();
    let redis_six = fixed_client(
        executor,
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 2, 0)),
    )
    .await;
    let bit_range = redis_six
        .call_tool(
            "redis_bitcount",
            serde_json::json!({
                "key": "bitmap", "range": {"start": 0, "end": 7, "unit": "bit"}
            }),
        )
        .await
        .expect("Redis 6 bit-range rejection");
    assert!(bit_range.is_error);
    assert!(
        serde_json::to_string(&bit_range)
            .expect("serialize bit-range version rejection")
            .contains("require Redis 7.0 or newer")
    );
    assert!(commands.lock().expect("no BITCOUNT command").is_empty());

    let byte_range = redis_six
        .call_tool(
            "redis_bitcount",
            serde_json::json!({
                "key": "bitmap", "range": {"start": 0, "end": 7, "unit": "byte"}
            }),
        )
        .await
        .expect("Redis 6 byte-range BITCOUNT");
    assert!(!byte_range.is_error);
    {
        let byte_range_commands = commands.lock().expect("BITCOUNT byte-range command");
        assert_eq!(byte_range_commands.len(), 1);
        assert_eq!(
            byte_range_commands[0].arguments(),
            &[b"bitmap".to_vec(), b"0".to_vec(), b"7".to_vec()]
        );
    }

    let executor = FixedRedis::new(RedisValue::Array(Vec::new()));
    let commands = executor.commands.clone();
    let redis_five = fixed_client(
        executor,
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(5, 0, 0)),
    )
    .await;
    let bitfield_ro = redis_five
        .call_tool(
            "redis_bitfield_ro",
            serde_json::json!({
                "key": "bitmap",
                "operations": [{
                    "encoding": {"signed": true, "width": 8},
                    "offset": {"kind": "absolute", "value": 0}
                }]
            }),
        )
        .await
        .expect("Redis 5 BITFIELD_RO rejection");
    assert!(bitfield_ro.is_error);
    assert!(commands.lock().expect("no BITFIELD_RO command").is_empty());

    let geoadd = redis_five
        .call_tool(
            "redis_geoadd",
            serde_json::json!({
                "key": "places",
                "nx": true,
                "members": [{"member": "here", "longitude": 0, "latitude": 0}]
            }),
        )
        .await
        .expect("Redis 5 GEOADD option rejection");
    assert!(geoadd.is_error);
    assert!(commands.lock().expect("no GEOADD command").is_empty());
}

#[tokio::test]
async fn geosearch_any_can_be_combined_with_explicit_sorting() {
    let executor = FixedRedis::new(RedisValue::Array(Vec::new()));
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 2, 0)),
    )
    .await;
    let result = client
        .call_tool(
            "redis_geosearch",
            serde_json::json!({
                "key": "places",
                "center": {"kind": "member", "member": "here"},
                "shape": {"kind": "radius", "radius": 1, "unit": "meters"},
                "sort": "ascending",
                "count": 10,
                "any": true
            }),
        )
        .await
        .expect("sorted GEOSEARCH ANY result");
    assert!(!result.is_error);
    let commands = commands.lock().expect("GEOSEARCH command");
    assert_eq!(commands.len(), 1);
    assert!(
        commands[0]
            .arguments()
            .iter()
            .any(|argument| argument == b"ASC")
    );
    assert!(
        commands[0]
            .arguments()
            .iter()
            .any(|argument| argument == b"ANY")
    );
}

#[tokio::test]
async fn pubsub_inputs_reject_invalid_base64_and_cluster_node_bounds() {
    let client = fixed_client(
        FixedRedis::new(RedisValue::Integer(0)),
        RedisCapabilities::unknown(),
    )
    .await;
    let invalid_base64 = client
        .call_tool(
            "redis_publish",
            serde_json::json!({
                "channel": {"value": "not base64!", "encoding": "base64"},
                "message": {"value": "hello"}
            }),
        )
        .await
        .expect("invalid base64 tool result");
    assert!(invalid_base64.is_error);
    assert!(
        serde_json::to_string(&invalid_base64)
            .expect("serialize invalid base64")
            .contains("not valid standard base64")
    );

    let excessive_nodes = client
        .call_tool(
            "redis_pubsub_numpat",
            serde_json::json!({"max_cluster_nodes": 257}),
        )
        .await
        .expect("cluster node bound tool result");
    assert!(excessive_nodes.is_error);

    let excessive_pattern = client
        .call_tool(
            "redis_pubsub_channels",
            serde_json::json!({"pattern": {"value": "x".repeat(4097)}}),
        )
        .await
        .expect("oversized pattern tool result");
    assert!(excessive_pattern.is_error);
    assert!(
        serde_json::to_string(&excessive_pattern)
            .expect("serialize oversized pattern")
            .contains("maximum is 4096")
    );
}

#[tokio::test]
async fn pubsub_session_inputs_are_bounded_and_binary_safe() {
    let client = client_for_bundles(
        AccessMode::ReadOnly,
        [ToolBundle::Sessions],
        RawCommandPolicy::Disabled,
    )
    .await;
    for (tool, arguments) in [
        ("redis_subscribe", serde_json::json!({"subscriptions": []})),
        (
            "redis_psubscribe",
            serde_json::json!({
                "subscriptions": [{"value": "not base64!", "encoding": "base64"}]
            }),
        ),
        (
            "redis_pubsub_read",
            serde_json::json!({
                "session_id": "ps_00000000000000000000000000000000",
                "max_messages": 1001
            }),
        ),
        (
            "redis_pubsub_read",
            serde_json::json!({
                "session_id": "ps_00000000000000000000000000000000",
                "max_bytes": 1048577
            }),
        ),
        (
            "redis_pubsub_read",
            serde_json::json!({
                "session_id": "ps_00000000000000000000000000000000",
                "wait_ms": 30001
            }),
        ),
        (
            "redis_pubsub_unsubscribe",
            serde_json::json!({
                "session_id": "ps_00000000000000000000000000000000",
                "kind": "channel",
                "subscriptions": []
            }),
        ),
    ] {
        let result = client
            .call_tool(tool, arguments)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(result.is_error, "{tool}: {result:?}");
    }

    let binary = client
        .call_tool(
            "redis_subscribe",
            serde_json::json!({
                "subscriptions": [{"value": "/wA=", "encoding": "base64"}]
            }),
        )
        .await
        .expect("binary-safe session subscribe")
        .structured_content
        .expect("structured binary-safe session subscribe");
    assert_eq!(binary["subscriptions"][0]["value"]["value"], "/wA=");
    assert_eq!(binary["subscriptions"][0]["value"]["encoding"], "base64");

    let control = client
        .call_tool(
            "redis_subscribe",
            serde_json::json!({
                "subscriptions": [{"value": "AA==", "encoding": "base64"}]
            }),
        )
        .await
        .expect("control-byte session subscribe")
        .structured_content
        .expect("structured control-byte session subscribe");
    assert_eq!(control["subscriptions"][0]["value"]["value"], "AA==");
    assert_eq!(control["subscriptions"][0]["value"]["encoding"], "base64");
}

#[tokio::test]
async fn fake_host_assigns_distinct_session_owners_and_cleans_up_on_disconnect() {
    async fn connect(manager: OwnerRecordingPubSubSessions) -> McpClient {
        let router = RedisMcp::builder(StubRedis)
            .bundles([ToolBundle::Sessions])
            .pubsub_sessions(manager)
            .build();
        let client = McpClient::connect(ChannelTransport::new(router))
            .await
            .expect("connect owner-recording client");
        client
            .initialize("redis-mcp-owner-test", "0")
            .await
            .expect("initialize owner-recording client");
        client
    }

    let manager = OwnerRecordingPubSubSessions::default();
    let first = connect(manager.clone()).await;
    let second = connect(manager.clone()).await;
    for client in [&first, &second] {
        let result = client
            .call_tool(
                "redis_subscribe",
                serde_json::json!({"subscriptions": [{"value": "events"}]}),
            )
            .await
            .expect("record owner through subscribe");
        assert!(!result.is_error);
    }
    let subscribed = manager
        .subscribed_owners
        .lock()
        .expect("subscribed owner records")
        .clone();
    assert_eq!(subscribed.len(), 2);
    assert_ne!(subscribed[0], subscribed[1]);

    drop(first);
    drop(second);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if manager
                .closed_owners
                .lock()
                .expect("closed owner records")
                .len()
                == 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("router teardown closes both owners");
    let mut closed = manager
        .closed_owners
        .lock()
        .expect("closed owner records")
        .clone();
    let mut subscribed = subscribed;
    closed.sort();
    subscribed.sort();
    assert_eq!(closed, subscribed);
}

#[tokio::test]
async fn set_is_binary_safe_and_reports_prior_conditional_semantics() {
    let executor = FixedRedis::new(RedisValue::BulkString(vec![0xfd]));
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
    )
    .await;

    let result = client
        .call_tool(
            "redis_set",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "value": "/gE=",
                "value_encoding": "base64",
                "condition": "xx",
                "get": true,
                "expiration": {"type": "unix_milliseconds", "value": 123}
            }),
        )
        .await
        .expect("binary SET")
        .structured_content
        .expect("structured binary SET");
    assert_eq!(result["applied"], true);
    assert_eq!(result["previous_exists"], true);
    assert_eq!(result["previous_value"], "/Q==");
    assert_eq!(result["previous_value_encoding"], "base64");

    let commands = commands.lock().expect("recorded binary SET");
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].arguments()[0], [0xff, 0x00]);
    assert_eq!(commands[0].arguments()[1], [0xfe, 0x01]);
    assert_eq!(
        &commands[0].arguments()[2..],
        [
            b"XX".to_vec(),
            b"GET".to_vec(),
            b"PXAT".to_vec(),
            b"123".to_vec()
        ]
    );
}

#[tokio::test]
async fn set_distinguishes_applied_noop_and_nil_across_conditions() {
    for (response, condition, get, applied, previous_exists) in [
        (RedisValue::Okay, "nx", false, true, None),
        (RedisValue::Nil, "xx", false, false, None),
        (RedisValue::Nil, "nx", true, true, Some(false)),
        (
            RedisValue::BulkString(b"old".to_vec()),
            "nx",
            true,
            false,
            Some(true),
        ),
        (
            RedisValue::BulkString(b"old".to_vec()),
            "xx",
            true,
            true,
            Some(true),
        ),
        (RedisValue::Nil, "xx", true, false, Some(false)),
    ] {
        let client = fixed_client(
            FixedRedis::new(response),
            RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
        )
        .await;
        let result = client
            .call_tool(
                "redis_set",
                serde_json::json!({
                    "key": "condition",
                    "value": "new",
                    "condition": condition,
                    "get": get
                }),
            )
            .await
            .expect("conditional SET")
            .structured_content
            .expect("structured conditional SET");
        assert_eq!(result["applied"], applied, "{condition} get={get}");
        assert_eq!(
            result["previous_exists"],
            previous_exists.map_or(serde_json::Value::Null, serde_json::Value::Bool),
            "{condition} get={get}"
        );
    }
}

#[tokio::test]
async fn known_redis_six_rejects_set_nx_get_without_execution() {
    let executor = FixedRedis::new(RedisValue::Nil);
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 2, 0)),
    )
    .await;
    let result = client
        .call_tool(
            "redis_set",
            serde_json::json!({
                "key": "versioned",
                "value": "new",
                "condition": "nx",
                "get": true
            }),
        )
        .await
        .expect("version-gated SET result");
    assert!(result.is_error);
    assert!(
        serde_json::to_string(&result)
            .expect("serialize version error")
            .contains("Redis 7.0")
    );
    assert!(commands.lock().expect("version commands").is_empty());
}

#[tokio::test]
async fn side_effectful_value_returns_omit_oversized_payloads_but_report_outcomes() {
    let prior = vec![b'x'; 16];
    let client = fixed_client(
        FixedRedis::new(RedisValue::BulkString(prior.clone())),
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
    )
    .await;
    let set = client
        .call_tool(
            "redis_set",
            serde_json::json!({
                "key": "bounded-set",
                "value": "new",
                "condition": "xx",
                "get": true,
                "max_previous_bytes": 8
            }),
        )
        .await
        .expect("bounded SET")
        .structured_content
        .expect("structured bounded SET");
    assert_eq!(set["applied"], true);
    assert_eq!(set["previous_exists"], true);
    assert_eq!(set["previous_value_bytes"], 16);
    assert_eq!(set["previous_value_omitted"], true);
    assert_eq!(set["previous_value"], serde_json::Value::Null);

    for (tool, input) in [
        (
            "redis_getex",
            serde_json::json!({
                "key": "bounded-getex",
                "expiration": {"type": "seconds", "value": 60},
                "max_value_bytes": 8
            }),
        ),
        (
            "redis_getdel",
            serde_json::json!({"key": "bounded-getdel", "max_value_bytes": 8}),
        ),
    ] {
        let client = fixed_client(
            FixedRedis::new(RedisValue::BulkString(prior.clone())),
            RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
        )
        .await;
        let result = client
            .call_tool(tool, input)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(result["exists"], true, "{tool}");
        assert_eq!(result["value_bytes"], 16, "{tool}");
        assert_eq!(result["value_omitted"], true, "{tool}");
        assert_eq!(result["value"], serde_json::Value::Null, "{tool}");
    }

    let client = fixed_client(
        FixedRedis::new(RedisValue::Array(vec![
            RedisValue::BulkString(prior),
            RedisValue::BulkString(b"1.25".to_vec()),
        ])),
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
    )
    .await;
    let popped = client
        .call_tool(
            "redis_zpopmin",
            serde_json::json!({
                "key": "bounded-zpop",
                "count": 1,
                "max_returned_bytes": 8
            }),
        )
        .await
        .expect("bounded ZPOPMIN")
        .structured_content
        .expect("structured bounded ZPOPMIN");
    assert_eq!(popped["count"], 1);
    assert_eq!(popped["member_bytes"], 16);
    assert_eq!(popped["members_omitted"], true);
    assert_eq!(popped["members"], serde_json::json!([]));
}

#[tokio::test]
async fn typed_expiration_variants_emit_exactly_one_redis_modifier() {
    for (expiration, expected) in [
        (
            serde_json::json!({"type": "seconds", "value": 10}),
            vec![b"EX".to_vec(), b"10".to_vec()],
        ),
        (
            serde_json::json!({"type": "milliseconds", "value": 20}),
            vec![b"PX".to_vec(), b"20".to_vec()],
        ),
        (
            serde_json::json!({"type": "unix_seconds", "value": 30}),
            vec![b"EXAT".to_vec(), b"30".to_vec()],
        ),
        (
            serde_json::json!({"type": "unix_milliseconds", "value": 40}),
            vec![b"PXAT".to_vec(), b"40".to_vec()],
        ),
        (
            serde_json::json!({"type": "keep_ttl"}),
            vec![b"KEEPTTL".to_vec()],
        ),
    ] {
        let executor = FixedRedis::new(RedisValue::Okay);
        let commands = executor.commands.clone();
        let client = fixed_client(
            executor,
            RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
        )
        .await;
        let result = client
            .call_tool(
                "redis_set",
                serde_json::json!({
                    "key": "expiration",
                    "value": "value",
                    "expiration": expiration
                }),
            )
            .await
            .expect("typed SET expiration");
        assert!(!result.is_error, "{result:?}");
        let commands = commands.lock().expect("SET expiration command");
        assert_eq!(&commands[0].arguments()[2..], expected);
    }

    for (expiration, expected) in [
        (
            serde_json::json!({"type": "seconds", "value": 10}),
            vec![b"EX".to_vec(), b"10".to_vec()],
        ),
        (
            serde_json::json!({"type": "milliseconds", "value": 20}),
            vec![b"PX".to_vec(), b"20".to_vec()],
        ),
        (
            serde_json::json!({"type": "unix_seconds", "value": 30}),
            vec![b"EXAT".to_vec(), b"30".to_vec()],
        ),
        (
            serde_json::json!({"type": "unix_milliseconds", "value": 40}),
            vec![b"PXAT".to_vec(), b"40".to_vec()],
        ),
        (
            serde_json::json!({"type": "persist"}),
            vec![b"PERSIST".to_vec()],
        ),
    ] {
        let executor = FixedRedis::new(RedisValue::Nil);
        let commands = executor.commands.clone();
        let client = fixed_client(
            executor,
            RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 0, 0)),
        )
        .await;
        let result = client
            .call_tool(
                "redis_getex",
                serde_json::json!({"key": "expiration", "expiration": expiration}),
            )
            .await
            .expect("typed GETEX expiration");
        assert!(!result.is_error, "{result:?}");
        let commands = commands.lock().expect("GETEX expiration command");
        assert_eq!(&commands[0].arguments()[1..], expected);
    }
}

#[async_trait]
impl RedisExecutor for RecordingRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let response = match command.name() {
            "HSET" => RedisValue::Integer(1),
            "HMGET" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xfd]), RedisValue::Nil]),
            "HDEL" => RedisValue::Integer(1),
            "HEXPIRE" | "HPEXPIRE" if command.tool_name() == "redis_hexpire_delete" => {
                RedisValue::Array(vec![RedisValue::Integer(2)])
            }
            "HEXPIRE" | "HPEXPIRE" => RedisValue::Array(vec![RedisValue::Integer(1)]),
            "HPEXPIRETIME" => RedisValue::Array(vec![RedisValue::Integer(4_102_444_800_000)]),
            "HRANDFIELD" => RedisValue::Array(vec![
                RedisValue::BulkString(vec![0xfe]),
                RedisValue::BulkString(vec![0xfd]),
                RedisValue::BulkString(vec![0xfe]),
                RedisValue::BulkString(vec![0xfc]),
            ]),
            "SORT_RO" => RedisValue::Array(vec![
                RedisValue::BulkString(b"alpha".to_vec()),
                RedisValue::Nil,
            ]),
            "SORT" => RedisValue::Integer(2),
            "EXISTS" => RedisValue::Integer(1),
            "FT.SEARCH" => RedisValue::Array(vec![
                RedisValue::Integer(1),
                RedisValue::BulkString(b"doc:1".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"vector_distance".to_vec()),
                    RedisValue::BulkString(b"0".to_vec()),
                ]),
            ]),
            _ => RedisValue::Nil,
        };
        self.commands.lock().expect("recording lock").push(command);
        Ok(response)
    }
}

#[tokio::test]
async fn vector_values_remain_binary_safe_in_curated_commands() {
    let executor = RecordingRedis::default();
    let commands = executor.commands.clone();
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Search])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect recording client");
    client
        .initialize("redis-mcp-vector-recording-test", "0")
        .await
        .expect("initialize recording client");

    let vector = [1.0_f64, -2.5_f64];
    let mut expected = Vec::new();
    for value in vector {
        expected.extend_from_slice(&(value as f32).to_le_bytes());
    }
    let stored = client
        .call_tool(
            "redis_vector_set_hash",
            serde_json::json!({
                "key": "doc:1",
                "field": "embedding",
                "data_type": "FLOAT32",
                "vector": vector
            }),
        )
        .await
        .expect("store vector");
    assert!(!stored.is_error, "{stored:?}");

    let searched = client
        .call_tool(
            "redis_ft_vector_search",
            serde_json::json!({
                "index": "idx:docs",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": vector,
                "top_k": 1,
                "limit_num": 1
            }),
        )
        .await
        .expect("search vector");
    assert!(!searched.is_error, "{searched:?}");

    let commands = commands.lock().expect("recorded commands");
    let hset = commands
        .iter()
        .find(|command| command.tool_name() == "redis_vector_set_hash")
        .expect("recorded vector HSET");
    assert_eq!(hset.arguments()[2], expected);
    let search = commands
        .iter()
        .find(|command| command.tool_name() == "redis_ft_vector_search")
        .expect("recorded vector FT.SEARCH");
    let blob_position = search
        .arguments()
        .iter()
        .position(|argument| argument == b"BLOB")
        .expect("BLOB parameter");
    assert_eq!(search.arguments()[blob_position + 1], expected);
}

#[tokio::test]
async fn hash_multi_field_commands_preserve_binary_argv_and_request_order() {
    let executor = RecordingRedis::default();
    let commands = executor.commands.clone();
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::DataStructures])
        .capabilities(RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 4, 0)))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect hash recording client");
    client
        .initialize("redis-mcp-hash-recording-test", "0")
        .await
        .expect("initialize hash recording client");

    let set = client
        .call_tool(
            "redis_hset",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "entries": [{
                    "field": "/g==",
                    "field_encoding": "base64",
                    "value": "/Q==",
                    "value_encoding": "base64"
                }]
            }),
        )
        .await
        .expect("binary HSET");
    assert!(!set.is_error, "{set:?}");

    let get = client
        .call_tool(
            "redis_hmget",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "fields": [
                    {"field": "/g==", "field_encoding": "base64"},
                    "missing"
                ]
            }),
        )
        .await
        .expect("binary HMGET")
        .structured_content
        .expect("structured binary HMGET");
    assert_eq!(get["values"][0]["value"], "/Q==");
    assert_eq!(get["values"][0]["value_encoding"], "base64");
    assert_eq!(get["values"][1]["exists"], false);

    client
        .call_tool(
            "redis_hexpire",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "seconds": 60,
                "condition": "gt",
                "fields": [{"field": "/g==", "field_encoding": "base64"}]
            }),
        )
        .await
        .expect("binary HEXPIRE");
    client
        .call_tool(
            "redis_hdel",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "fields": [{"field": "/g==", "field_encoding": "base64"}]
            }),
        )
        .await
        .expect("binary HDEL");

    let commands = commands.lock().expect("recorded hash commands");
    let hset = commands
        .iter()
        .find(|command| command.tool_name() == "redis_hset")
        .expect("recorded HSET");
    assert_eq!(
        hset.arguments(),
        &[vec![0xff, 0x00], vec![0xfe], vec![0xfd]]
    );

    let hmget = commands
        .iter()
        .find(|command| command.tool_name() == "redis_hmget")
        .expect("recorded HMGET");
    assert_eq!(
        hmget.arguments(),
        &[vec![0xff, 0x00], vec![0xfe], b"missing".to_vec()]
    );

    let hexpire = commands
        .iter()
        .find(|command| command.tool_name() == "redis_hexpire")
        .expect("recorded HEXPIRE");
    assert_eq!(
        hexpire.arguments(),
        &[
            vec![0xff, 0x00],
            b"60".to_vec(),
            b"GT".to_vec(),
            b"FIELDS".to_vec(),
            b"1".to_vec(),
            vec![0xfe]
        ]
    );

    let hdel = commands
        .iter()
        .find(|command| command.tool_name() == "redis_hdel")
        .expect("recorded HDEL");
    assert_eq!(hdel.arguments(), &[vec![0xff, 0x00], vec![0xfe]]);
}

#[tokio::test]
async fn hash_expiry_sampling_and_sort_contracts_emit_explicit_bounded_argv() {
    let executor = RecordingRedis::default();
    let commands = executor.commands.clone();
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Essentials, ToolBundle::DataStructures])
        .capabilities(RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 2, 0)))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect #62 recording client");
    client
        .initialize("redis-mcp-issue-62-recording-test", "0")
        .await
        .expect("initialize #62 recording client");

    let expiration = client
        .call_tool(
            "redis_hexpire",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "expiration": 1500,
                "mode": "relative_milliseconds",
                "condition": "gt",
                "fields": [{"field": "/g==", "field_encoding": "base64"}]
            }),
        )
        .await
        .expect("HPEXPIRE contract")
        .structured_content
        .expect("structured HPEXPIRE");
    assert_eq!(expiration["mode"], "relative_milliseconds");
    assert_eq!(expiration["expiration"], 1500);
    assert_eq!(expiration["expirations_set"], 1);

    let inspected = client
        .call_tool(
            "redis_httl",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "mode": "unix_milliseconds",
                "fields": [{"field": "/g==", "field_encoding": "base64"}]
            }),
        )
        .await
        .expect("HPEXPIRETIME contract")
        .structured_content
        .expect("structured HPEXPIRETIME");
    assert_eq!(inspected["mode"], "unix_milliseconds");
    assert_eq!(inspected["fields"][0]["value"], 4_102_444_800_000_u64);
    assert_eq!(
        inspected["fields"][0]["ttl_seconds"],
        serde_json::Value::Null
    );

    let deleted = client
        .call_tool(
            "redis_hexpire_delete",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "mode": "relative_milliseconds",
                "condition": "lt",
                "fields": [{"field": "/g==", "field_encoding": "base64"}]
            }),
        )
        .await
        .expect("destructive HPEXPIRE contract")
        .structured_content
        .expect("structured destructive HPEXPIRE");
    assert_eq!(deleted["mode"], "relative_milliseconds");
    assert_eq!(deleted["deleted"], 1);
    assert_eq!(deleted["fields"][0]["status"], "deleted");

    let sampled = client
        .call_tool(
            "redis_hrandfield",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "count": -2,
                "with_values": true
            }),
        )
        .await
        .expect("HRANDFIELD contract")
        .structured_content
        .expect("structured HRANDFIELD");
    assert_eq!(sampled["duplicates_allowed"], true);
    assert_eq!(sampled["returned"], 2);
    assert_eq!(sampled["entries"][0]["field"], "/g==");
    assert_eq!(sampled["entries"][0]["value"], "/Q==");

    let sorted = client
        .call_tool(
            "redis_sort",
            serde_json::json!({
                "key": "source",
                "by": "weight:*",
                "get": ["#"],
                "offset": 1,
                "count": 2,
                "order": "descending",
                "alpha": true
            }),
        )
        .await
        .expect("SORT_RO contract")
        .structured_content
        .expect("structured SORT_RO");
    assert_eq!(sorted["source_exists"], true);
    assert_eq!(sorted["returned"], 2);
    assert_eq!(sorted["values"][0]["value"], "alpha");
    assert_eq!(sorted["values"][1]["value"], serde_json::Value::Null);

    let stored = client
        .call_tool(
            "redis_sort_store",
            serde_json::json!({
                "key": "source:{tenant}",
                "destination": "sorted:{tenant}",
                "count": 3
            }),
        )
        .await
        .expect("SORT STORE contract")
        .structured_content
        .expect("structured SORT STORE");
    assert_eq!(stored["stored"], 2);
    assert_eq!(stored["destination_overwritten"], true);
    assert_eq!(stored["cluster_requires_same_slot"], true);

    let commands = commands.lock().expect("recorded #62 commands");
    let command = |tool_name: &str| {
        commands
            .iter()
            .find(|command| command.tool_name() == tool_name)
            .unwrap_or_else(|| panic!("recorded {tool_name}"))
    };
    assert_eq!(command("redis_hexpire").name(), "HPEXPIRE");
    assert_eq!(
        command("redis_hexpire").arguments(),
        &[
            vec![0xff, 0x00],
            b"1500".to_vec(),
            b"GT".to_vec(),
            b"FIELDS".to_vec(),
            b"1".to_vec(),
            vec![0xfe]
        ]
    );
    assert_eq!(command("redis_httl").name(), "HPEXPIRETIME");
    assert_eq!(command("redis_hexpire_delete").name(), "HPEXPIRE");
    assert_eq!(
        command("redis_hexpire_delete").arguments(),
        &[
            vec![0xff, 0x00],
            b"0".to_vec(),
            b"LT".to_vec(),
            b"FIELDS".to_vec(),
            b"1".to_vec(),
            vec![0xfe]
        ]
    );
    assert_eq!(
        command("redis_hrandfield").arguments(),
        &[vec![0xff, 0x00], b"-2".to_vec(), b"WITHVALUES".to_vec()]
    );
    assert_eq!(
        command("redis_sort").arguments(),
        &[
            b"source".to_vec(),
            b"BY".to_vec(),
            b"weight:*".to_vec(),
            b"LIMIT".to_vec(),
            b"1".to_vec(),
            b"2".to_vec(),
            b"GET".to_vec(),
            b"#".to_vec(),
            b"DESC".to_vec(),
            b"ALPHA".to_vec()
        ]
    );
    assert_eq!(
        command("redis_sort_store").arguments(),
        &[
            b"source:{tenant}".to_vec(),
            b"LIMIT".to_vec(),
            b"0".to_vec(),
            b"3".to_vec(),
            b"ASC".to_vec(),
            b"STORE".to_vec(),
            b"sorted:{tenant}".to_vec()
        ]
    );
}

#[tokio::test]
async fn hash_sampling_sort_and_absolute_expiry_reject_unsafe_or_unbounded_forms() {
    let executor = RecordingRedis::default();
    let commands = executor.commands.clone();
    let client = fixed_client(
        FixedRedis {
            response: RedisValue::Array(Vec::new()),
            commands: commands.clone(),
        },
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 2, 0)),
    )
    .await;

    for (tool, input) in [
        (
            "redis_hrandfield",
            serde_json::json!({"key": "hash", "with_values": true}),
        ),
        (
            "redis_hexpire",
            serde_json::json!({
                "key": "hash",
                "expiration": 1,
                "mode": "unix_seconds",
                "fields": ["field"]
            }),
        ),
        (
            "redis_sort",
            serde_json::json!({
                "key": "source",
                "count": 1000,
                "get": ["#", "#"]
            }),
        ),
    ] {
        let result = client
            .call_tool(tool, input)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(result.is_error, "{tool}: {result:?}");
    }
    assert!(
        commands.lock().expect("invalid input commands").is_empty(),
        "invalid inputs must fail before Redis execution"
    );

    let cluster_executor = RecordingRedis::default();
    let cluster_commands = cluster_executor.commands.clone();
    let router = RedisMcp::builder(cluster_executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::Essentials])
        .capabilities(
            RedisCapabilities::unknown()
                .with_redis_version(RedisVersion::new(8, 2, 0))
                .with_deployment(RedisDeployment::Cluster),
        )
        .build();
    let cluster_client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect Cluster SORT contract client");
    cluster_client
        .initialize("redis-mcp-sort-cluster-contract-test", "0")
        .await
        .expect("initialize Cluster SORT contract client");

    let local = cluster_client
        .call_tool(
            "redis_sort",
            serde_json::json!({"key": "source:{tenant}", "by": "nosort", "get": ["#"]}),
        )
        .await
        .expect("local-only Cluster SORT patterns");
    assert!(!local.is_error, "{local:?}");
    let external = cluster_client
        .call_tool(
            "redis_sort",
            serde_json::json!({"key": "source:{tenant}", "get": ["object:*->name"]}),
        )
        .await
        .expect("external Cluster SORT pattern result");
    assert!(external.is_error, "{external:?}");
    assert_eq!(
        cluster_commands
            .lock()
            .expect("Cluster SORT commands")
            .iter()
            .filter(|command| command.name() == "SORT_RO")
            .count(),
        1,
        "external Cluster pattern must fail before Redis execution"
    );
}

async fn full_catalog_client() -> McpClient {
    client_for_bundles(
        AccessMode::Full,
        ToolBundle::ALL.iter().copied(),
        RawCommandPolicy::Classified,
    )
    .await
}

async fn capability_client(
    capabilities: RedisCapabilities,
    policy: UnavailableToolPolicy,
) -> McpClient {
    let router = RedisMcp::builder(StubRedis)
        .access(AccessMode::Full)
        .bundles(ToolBundle::ALL.iter().copied())
        .capabilities(capabilities)
        .unavailable_tool_policy(policy)
        .pubsub_sessions(StubPubSubSessions)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect capability-aware client");
    client
        .initialize("redis-mcp-capability-test", "0")
        .await
        .expect("initialize capability-aware client");
    client
}

fn structured_cases() -> Vec<(&'static str, serde_json::Value, &'static str)> {
    vec![
        ("redis_ping", serde_json::json!({}), "response"),
        ("redis_info", serde_json::json!({}), "properties"),
        ("redis_client_list", serde_json::json!({}), "clients"),
        ("redis_cluster_info", serde_json::json!({}), "nodes"),
        ("redis_memory_stats", serde_json::json!({}), "nodes"),
        ("redis_module_list", serde_json::json!({}), "modules"),
        ("redis_slowlog", serde_json::json!({}), "entries"),
        (
            "redis_latency_history",
            serde_json::json!({"event": "command"}),
            "samples",
        ),
        ("redis_acl_whoami", serde_json::json!({}), "identities"),
        ("redis_health_check", serde_json::json!({}), "status"),
        ("redis_connection_summary", serde_json::json!({}), "total"),
        (
            "redis_keyspace_summary",
            serde_json::json!({}),
            "total_keys",
        ),
        ("redis_memory_summary", serde_json::json!({}), "nodes"),
        (
            "redis_key_summary",
            serde_json::json!({"key": "alpha"}),
            "key_type",
        ),
        (
            "redis_hotkeys",
            serde_json::json!({"count": 2, "max_keys": 2, "top": 1}),
            "candidates",
        ),
        ("redis_dbsize", serde_json::json!({}), "key_count"),
        (
            "redis_scan",
            serde_json::json!({"pattern": "*", "count": 10}),
            "keys",
        ),
        ("redis_get", serde_json::json!({"key": "greeting"}), "value"),
        (
            "redis_type",
            serde_json::json!({"key": "greeting"}),
            "key_type",
        ),
        (
            "redis_ttl",
            serde_json::json!({"key": "greeting"}),
            "ttl_seconds",
        ),
        (
            "redis_exists",
            serde_json::json!({"keys": ["greeting"]}),
            "existing",
        ),
        (
            "redis_mget",
            serde_json::json!({"keys": ["greeting", "missing"]}),
            "values",
        ),
        (
            "redis_strlen",
            serde_json::json!({"key": "greeting"}),
            "length_bytes",
        ),
        (
            "redis_memory_usage",
            serde_json::json!({"key": "greeting"}),
            "bytes",
        ),
        ("redis_randomkey", serde_json::json!({}), "key"),
        (
            "redis_sort",
            serde_json::json!({"key": "queue", "count": 2}),
            "values",
        ),
        (
            "redis_hget",
            serde_json::json!({"key": "user:1", "field": "name"}),
            "value",
        ),
        (
            "redis_hgetall",
            serde_json::json!({"key": "user:1"}),
            "entries",
        ),
        (
            "redis_hexists",
            serde_json::json!({"key": "user:1", "field": "name"}),
            "field_exists",
        ),
        (
            "redis_hkeys",
            serde_json::json!({"key": "user:1"}),
            "fields",
        ),
        ("redis_hlen", serde_json::json!({"key": "user:1"}), "length"),
        (
            "redis_hmget",
            serde_json::json!({"key": "user:1", "fields": ["name", "missing"]}),
            "values",
        ),
        (
            "redis_hscan",
            serde_json::json!({"key": "user:1", "count": 10}),
            "page",
        ),
        (
            "redis_hstrlen",
            serde_json::json!({"key": "user:1", "field": "name"}),
            "length_bytes",
        ),
        (
            "redis_hrandfield",
            serde_json::json!({
                "key": "user:1",
                "count": -2,
                "with_values": true
            }),
            "entries",
        ),
        (
            "redis_httl",
            serde_json::json!({"key": "user:1", "fields": ["name"]}),
            "fields",
        ),
        (
            "redis_hvals",
            serde_json::json!({"key": "user:1"}),
            "values",
        ),
        (
            "redis_lindex",
            serde_json::json!({"key": "queue", "index": -1}),
            "value",
        ),
        ("redis_llen", serde_json::json!({"key": "queue"}), "length"),
        (
            "redis_lpos",
            serde_json::json!({"key": "queue", "value": "second", "count": 2}),
            "positions",
        ),
        (
            "redis_lrange",
            serde_json::json!({"key": "queue", "start": 0, "stop": 1}),
            "elements",
        ),
        (
            "redis_scard",
            serde_json::json!({"key": "tags"}),
            "cardinality",
        ),
        (
            "redis_sdiff",
            serde_json::json!({"keys": ["tags", "other"]}),
            "members",
        ),
        (
            "redis_sdiffcard",
            serde_json::json!({"keys": ["tags", "other"], "limit": 10}),
            "cardinality",
        ),
        (
            "redis_sinter",
            serde_json::json!({"keys": ["tags", "other"]}),
            "members",
        ),
        (
            "redis_sismember",
            serde_json::json!({"key": "tags", "member": "alpha"}),
            "is_member",
        ),
        (
            "redis_smembers",
            serde_json::json!({"key": "tags"}),
            "members",
        ),
        (
            "redis_smismember",
            serde_json::json!({"key": "tags", "members": ["alpha", "missing"]}),
            "members",
        ),
        (
            "redis_sscan",
            serde_json::json!({"key": "tags", "count": 10}),
            "page",
        ),
        (
            "redis_sunion",
            serde_json::json!({"keys": ["tags", "other"]}),
            "members",
        ),
        (
            "redis_sunioncard",
            serde_json::json!({"keys": ["tags", "other"], "approximate": true}),
            "cardinality",
        ),
        (
            "redis_zcard",
            serde_json::json!({"key": "leaders"}),
            "cardinality",
        ),
        (
            "redis_zcount",
            serde_json::json!({
                "key": "leaders",
                "min": {"kind": "negative_infinity"},
                "max": {"kind": "inclusive", "value": "2.0"}
            }),
            "count",
        ),
        (
            "redis_zintercard",
            serde_json::json!({"keys": ["leaders", "other"], "limit": 10}),
            "cardinality",
        ),
        (
            "redis_zmscore",
            serde_json::json!({"key": "leaders", "members": ["alice", "missing"]}),
            "members",
        ),
        (
            "redis_zrange",
            serde_json::json!({
                "key": "leaders",
                "range": {"kind": "rank", "start": 0, "stop": 0},
                "withscores": true
            }),
            "members",
        ),
        (
            "redis_zrank",
            serde_json::json!({"key": "leaders", "member": "alice"}),
            "rank",
        ),
        (
            "redis_zrevrank",
            serde_json::json!({"key": "leaders", "member": "alice"}),
            "rank",
        ),
        (
            "redis_zscan",
            serde_json::json!({"key": "leaders", "count": 10}),
            "page",
        ),
        (
            "redis_zscore",
            serde_json::json!({"key": "leaders", "member": "alice"}),
            "score",
        ),
        (
            "redis_getbit",
            serde_json::json!({"key": "bitmap", "offset": 2}),
            "bit",
        ),
        (
            "redis_bitcount",
            serde_json::json!({"key": "bitmap", "range": {"start": 0, "end": 1}}),
            "set_bits",
        ),
        (
            "redis_bitpos",
            serde_json::json!({"key": "bitmap", "bit": true}),
            "position",
        ),
        (
            "redis_bitfield_ro",
            serde_json::json!({
                "key": "bitmap",
                "operations": [{
                    "encoding": {"signed": true, "width": 8},
                    "offset": {"kind": "absolute", "value": 0}
                }]
            }),
            "results",
        ),
        (
            "redis_geodist",
            serde_json::json!({
                "key": "places", "from": "san-francisco", "to": "oakland",
                "unit": "kilometers"
            }),
            "distance",
        ),
        (
            "redis_geohash",
            serde_json::json!({"key": "places", "members": ["san-francisco"]}),
            "members",
        ),
        (
            "redis_geopos",
            serde_json::json!({"key": "places", "members": ["san-francisco"]}),
            "members",
        ),
        (
            "redis_geosearch",
            serde_json::json!({
                "key": "places",
                "center": {"kind": "member", "member": "san-francisco"},
                "shape": {"kind": "radius", "radius": "10", "unit": "kilometers"},
                "count": 10
            }),
            "results",
        ),
        (
            "redis_pfcount",
            serde_json::json!({"keys": ["visitors"]}),
            "estimated_cardinality",
        ),
        ("redis_xlen", serde_json::json!({"key": "events"}), "length"),
        (
            "redis_xrange",
            serde_json::json!({"key": "events", "count": 10}),
            "entries",
        ),
        (
            "redis_xrevrange",
            serde_json::json!({"key": "events", "count": 10}),
            "entries",
        ),
        (
            "redis_xread",
            serde_json::json!({
                "streams": [{
                    "key": "events",
                    "offset": {"type": "explicit", "id": {"milliseconds": 0, "sequence": 0}}
                }],
                "count": 10
            }),
            "streams",
        ),
        (
            "redis_xinfo_stream",
            serde_json::json!({"key": "events"}),
            "last_generated_id",
        ),
        (
            "redis_xinfo_groups",
            serde_json::json!({"key": "events"}),
            "groups",
        ),
        (
            "redis_xinfo_consumers",
            serde_json::json!({"key": "events", "group": {"value": "workers"}}),
            "consumers",
        ),
        (
            "redis_xpending",
            serde_json::json!({"key": "events", "group": {"value": "workers"}}),
            "summary",
        ),
        (
            "redis_json_get",
            serde_json::json!({"key": "doc:1"}),
            "value",
        ),
        (
            "redis_json_type",
            serde_json::json!({"key": "doc:1"}),
            "types",
        ),
        (
            "redis_json_mget",
            serde_json::json!({"keys": ["doc:1"]}),
            "values",
        ),
        (
            "redis_json_strlen",
            serde_json::json!({"key": "doc:1", "path": "$.name"}),
            "values",
        ),
        (
            "redis_json_objkeys",
            serde_json::json!({"key": "doc:1"}),
            "keys",
        ),
        (
            "redis_json_objlen",
            serde_json::json!({"key": "doc:1"}),
            "values",
        ),
        (
            "redis_json_arrlen",
            serde_json::json!({"key": "doc:1", "path": "$.items"}),
            "values",
        ),
        ("redis_ft_list", serde_json::json!({}), "indexes"),
        (
            "redis_ft_info",
            serde_json::json!({"index": "idx:docs"}),
            "attributes",
        ),
        (
            "redis_ft_search",
            serde_json::json!({"index": "idx:docs", "query": "redis"}),
            "response",
        ),
        (
            "redis_vector_get_hash",
            serde_json::json!({"key": "doc:1", "field": "embedding", "data_type": "FLOAT32"}),
            "vector",
        ),
        (
            "redis_ft_vector_search",
            serde_json::json!({
                "index": "idx:docs",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0, 2.0],
                "return_fields": ["title"]
            }),
            "results",
        ),
        (
            "redis_ft_hybrid_search",
            serde_json::json!({
                "index": "idx:docs",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0, 2.0],
                "return_fields": ["title"],
                "filters": [{"type": "text", "field": "title", "value": "Redis"}]
            }),
            "results",
        ),
        (
            "redis_ft_aggregate",
            serde_json::json!({
                "index": "idx:docs",
                "query": "*",
                "stages": [{
                    "type": "group_by",
                    "properties": ["@category"],
                    "reducers": [{"function": "count", "alias": "count"}]
                }]
            }),
            "rows",
        ),
        (
            "redis_ft_cursor_read",
            serde_json::json!({"index": "idx:docs", "cursor_id": 7}),
            "rows",
        ),
        (
            "redis_ft_explain",
            serde_json::json!({"index": "idx:docs", "query": "redis"}),
            "plan",
        ),
        (
            "redis_ft_profile",
            serde_json::json!({"index": "idx:docs", "command": "search", "query": "redis"}),
            "profile",
        ),
        (
            "redis_ft_tagvals",
            serde_json::json!({"index": "idx:docs", "field": "category"}),
            "values",
        ),
        (
            "redis_ft_dictdump",
            serde_json::json!({"dict": "terms"}),
            "terms",
        ),
        (
            "redis_ft_syndump",
            serde_json::json!({"index": "idx:docs"}),
            "entries",
        ),
        (
            "redis_set",
            serde_json::json!({"key": "greeting", "value": "hello"}),
            "applied",
        ),
        (
            "redis_publish",
            serde_json::json!({
                "channel": {"value": "events:alpha"},
                "message": {"value": "hello"}
            }),
            "receivers",
        ),
        (
            "redis_spublish",
            serde_json::json!({
                "channel": {"value": "events:{alpha}"},
                "message": {"value": "hello"}
            }),
            "receivers",
        ),
        (
            "redis_expire",
            serde_json::json!({"key": "greeting", "seconds": 60}),
            "applied",
        ),
        (
            "redis_persist",
            serde_json::json!({"key": "greeting"}),
            "applied",
        ),
        (
            "redis_mset",
            serde_json::json!({"entries": [{"key": "a", "value": "1"}, {"key": "b", "value": "2"}]}),
            "stored",
        ),
        ("redis_incr", serde_json::json!({"key": "counter"}), "value"),
        (
            "redis_append",
            serde_json::json!({"key": "greeting", "value": "!"}),
            "length_bytes",
        ),
        (
            "redis_getrange",
            serde_json::json!({"key": "greeting", "start": 1, "end": 3}),
            "value",
        ),
        (
            "redis_dump",
            serde_json::json!({"key": "greeting"}),
            "payload_base64",
        ),
        (
            "redis_object_inspect",
            serde_json::json!({"key": "greeting", "operation": "encoding"}),
            "encoding",
        ),
        (
            "redis_pubsub_channels",
            serde_json::json!({"pattern": {"value": "events:*"}}),
            "channels",
        ),
        (
            "redis_pubsub_numsub",
            serde_json::json!({"channels": [{"value": "events:alpha"}]}),
            "counts",
        ),
        (
            "redis_pubsub_numpat",
            serde_json::json!({}),
            "pattern_count",
        ),
        (
            "redis_pubsub_shardchannels",
            serde_json::json!({"pattern": {"value": "events:*"}}),
            "channels",
        ),
        (
            "redis_pubsub_shardnumsub",
            serde_json::json!({"channels": [{"value": "events:alpha"}]}),
            "counts",
        ),
        (
            "redis_subscribe",
            serde_json::json!({"subscriptions": [{"value": "events:alpha"}]}),
            "session_id",
        ),
        (
            "redis_psubscribe",
            serde_json::json!({"subscriptions": [{"value": "events:*"}]}),
            "session_id",
        ),
        (
            "redis_ssubscribe",
            serde_json::json!({"subscriptions": [{"value": "events:{alpha}"}]}),
            "session_id",
        ),
        (
            "redis_pubsub_read",
            serde_json::json!({
                "session_id": "ps_00000000000000000000000000000000"
            }),
            "messages",
        ),
        (
            "redis_pubsub_unsubscribe",
            serde_json::json!({
                "session_id": "ps_00000000000000000000000000000000",
                "kind": "channel",
                "subscriptions": [{"value": "events:alpha"}]
            }),
            "subscriptions",
        ),
        (
            "redis_pubsub_close",
            serde_json::json!({
                "session_id": "ps_00000000000000000000000000000000"
            }),
            "closed",
        ),
        (
            "redis_getex",
            serde_json::json!({"key": "greeting", "expiration": {"type": "seconds", "value": 60}}),
            "value",
        ),
        (
            "redis_setrange",
            serde_json::json!({"key": "greeting", "offset": 1, "value": "ell"}),
            "length_bytes",
        ),
        ("redis_decr", serde_json::json!({"key": "counter"}), "value"),
        (
            "redis_decrby",
            serde_json::json!({"key": "counter", "amount": 2}),
            "value",
        ),
        (
            "redis_incrby",
            serde_json::json!({"key": "counter", "amount": 2}),
            "value",
        ),
        (
            "redis_incrbyfloat",
            serde_json::json!({"key": "counter", "amount": 0.5}),
            "value",
        ),
        (
            "redis_copy",
            serde_json::json!({"source": "greeting", "destination": "greeting-copy"}),
            "copied",
        ),
        (
            "redis_touch",
            serde_json::json!({"keys": ["greeting"]}),
            "touched",
        ),
        (
            "redis_restore",
            serde_json::json!({"key": "restored", "payload_base64": "AA=="}),
            "restored",
        ),
        (
            "redis_sort_store",
            serde_json::json!({
                "key": "queue:{tenant}",
                "destination": "sorted:{tenant}",
                "count": 2
            }),
            "stored",
        ),
        (
            "redis_hset",
            serde_json::json!({"key": "user:1", "fields": {"name": "Ada"}}),
            "fields_added",
        ),
        (
            "redis_hexpire",
            serde_json::json!({"key": "user:1", "seconds": 60, "fields": ["name"]}),
            "expirations_set",
        ),
        (
            "redis_hexpire_delete",
            serde_json::json!({"key": "user:1", "fields": ["name"]}),
            "deleted",
        ),
        (
            "redis_hincrby",
            serde_json::json!({"key": "user:1", "field": "visits", "increment": 1}),
            "value",
        ),
        (
            "redis_hincrbyfloat",
            serde_json::json!({"key": "user:1", "field": "score", "increment": 0.5}),
            "value",
        ),
        (
            "redis_hpersist",
            serde_json::json!({"key": "user:1", "fields": ["name"]}),
            "expirations_removed",
        ),
        (
            "redis_lpush",
            serde_json::json!({"key": "queue", "elements": ["first", "second"]}),
            "length",
        ),
        (
            "redis_rpush",
            serde_json::json!({"key": "queue", "elements": ["first", "second"]}),
            "length",
        ),
        (
            "redis_sadd",
            serde_json::json!({"key": "tags", "members": ["alpha", "beta"]}),
            "added",
        ),
        (
            "redis_zadd",
            serde_json::json!({"key": "leaders", "members": [{"score": 1.5, "member": "alice"}]}),
            "affected",
        ),
        (
            "redis_zincrby",
            serde_json::json!({"key": "leaders", "member": "alice", "increment": "0.25"}),
            "score",
        ),
        (
            "redis_setbit",
            serde_json::json!({"key": "bitmap", "offset": 2, "value": true}),
            "previous",
        ),
        (
            "redis_bitfield",
            serde_json::json!({
                "key": "bitmap",
                "operations": [{
                    "operation": "increment",
                    "encoding": {"signed": true, "width": 8},
                    "offset": {"kind": "absolute", "value": 0},
                    "increment": "1",
                    "overflow": "saturate"
                }]
            }),
            "results",
        ),
        (
            "redis_geoadd",
            serde_json::json!({
                "key": "places",
                "members": [{
                    "member": "san-francisco",
                    "longitude": "-122.4194",
                    "latitude": "37.7749"
                }]
            }),
            "affected",
        ),
        (
            "redis_pfadd",
            serde_json::json!({"key": "visitors", "elements": ["alice"]}),
            "register_changed",
        ),
        (
            "redis_xadd",
            serde_json::json!({
                "key": "events",
                "fields": [{"field": "event", "value": "created"}]
            }),
            "id",
        ),
        (
            "redis_xgroup_create",
            serde_json::json!({"key": "events", "group": {"value": "workers"}, "mkstream": true}),
            "applied",
        ),
        (
            "redis_xgroup_setid",
            serde_json::json!({
                "key": "events",
                "group": {"value": "workers"},
                "id": {"type": "beginning"}
            }),
            "applied",
        ),
        (
            "redis_xgroup_createconsumer",
            serde_json::json!({
                "key": "events", "group": {"value": "workers"},
                "consumer": {"value": "worker-1"}
            }),
            "created",
        ),
        (
            "redis_xreadgroup",
            serde_json::json!({
                "group": {"value": "workers"}, "consumer": {"value": "worker-1"},
                "streams": [{"key": "events", "offset": {"type": "new"}}],
                "count": 10
            }),
            "streams",
        ),
        (
            "redis_xack",
            serde_json::json!({
                "key": "events", "group": {"value": "workers"},
                "ids": [{"milliseconds": 1, "sequence": 0}]
            }),
            "acknowledged",
        ),
        (
            "redis_xclaim",
            serde_json::json!({
                "key": "events", "group": {"value": "workers"},
                "consumer": {"value": "worker-2"}, "min_idle_time_ms": 0,
                "ids": [{"milliseconds": 1, "sequence": 0}]
            }),
            "entries",
        ),
        (
            "redis_xautoclaim",
            serde_json::json!({
                "key": "events", "group": {"value": "workers"},
                "consumer": {"value": "worker-2"}, "min_idle_time_ms": 0,
                "start": {"milliseconds": 0, "sequence": 0}, "count": 10
            }),
            "next_start_id",
        ),
        (
            "redis_json_set",
            serde_json::json!({"key": "doc:1", "value": {"name": "Ada"}}),
            "stored",
        ),
        (
            "redis_json_numincrby",
            serde_json::json!({"key": "doc:1", "path": "$.score", "value": 1}),
            "values",
        ),
        (
            "redis_json_toggle",
            serde_json::json!({"key": "doc:1", "path": "$.enabled"}),
            "values",
        ),
        (
            "redis_json_arrappend",
            serde_json::json!({"key": "doc:1", "path": "$.items", "values": [3]}),
            "values",
        ),
        (
            "redis_json_arrinsert",
            serde_json::json!({"key": "doc:1", "path": "$.items", "index": 0, "values": [1]}),
            "values",
        ),
        (
            "redis_ft_create",
            serde_json::json!({
                "index": "idx:docs",
                "on": "JSON",
                "prefixes": ["doc:"],
                "schema": [{"name": "$.name", "alias": "name", "field_type": "TEXT"}]
            }),
            "created",
        ),
        (
            "redis_vector_set_hash",
            serde_json::json!({
                "key": "doc:1",
                "field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0, 2.0]
            }),
            "stored",
        ),
        (
            "redis_ft_cursor_del",
            serde_json::json!({"index": "idx:docs", "cursor_id": 7}),
            "deleted",
        ),
        (
            "redis_ft_alter",
            serde_json::json!({
                "index": "idx:docs",
                "field": {"name": "category", "field_type": "TAG"}
            }),
            "added",
        ),
        (
            "redis_ft_synupdate",
            serde_json::json!({
                "index": "idx:docs", "group_id": "speed", "terms": ["fast", "quick"]
            }),
            "updated",
        ),
        (
            "redis_ft_dictadd",
            serde_json::json!({"dict": "terms", "terms": ["redis"]}),
            "changed",
        ),
        (
            "redis_ft_aliasadd",
            serde_json::json!({"alias": "docs", "index": "idx:docs"}),
            "action",
        ),
        (
            "redis_del",
            serde_json::json!({"keys": ["greeting"]}),
            "deleted",
        ),
        (
            "redis_unlink",
            serde_json::json!({"keys": ["temporary"]}),
            "unlinked",
        ),
        (
            "redis_hdel",
            serde_json::json!({"key": "user:1", "fields": ["name"]}),
            "deleted",
        ),
        (
            "redis_lpop",
            serde_json::json!({"key": "queue", "count": 1}),
            "elements",
        ),
        (
            "redis_lmove",
            serde_json::json!({"source": "queue", "destination": "archive", "from": "left", "to": "right"}),
            "moved",
        ),
        (
            "redis_lrem",
            serde_json::json!({"key": "queue", "count": 1, "value": "first"}),
            "removed",
        ),
        (
            "redis_lset",
            serde_json::json!({"key": "queue", "index": -1, "value": "last"}),
            "replaced",
        ),
        (
            "redis_ltrim",
            serde_json::json!({"key": "queue", "start": 0, "stop": 9}),
            "trimmed",
        ),
        (
            "redis_rpop",
            serde_json::json!({"key": "queue", "count": 1}),
            "elements",
        ),
        (
            "redis_srem",
            serde_json::json!({"key": "tags", "members": ["alpha"]}),
            "removed",
        ),
        (
            "redis_sdiffstore",
            serde_json::json!({"destination": "out:{set}", "keys": ["left:{set}", "right:{set}"]}),
            "destination_cardinality",
        ),
        (
            "redis_sinterstore",
            serde_json::json!({"destination": "out:{set}", "keys": ["left:{set}", "right:{set}"]}),
            "destination_cardinality",
        ),
        (
            "redis_sunionstore",
            serde_json::json!({"destination": "out:{set}", "keys": ["left:{set}", "right:{set}"]}),
            "destination_cardinality",
        ),
        (
            "redis_zdiffstore",
            serde_json::json!({"destination": "out:{zset}", "keys": ["left:{zset}", "right:{zset}"]}),
            "destination_cardinality",
        ),
        (
            "redis_zinterstore",
            serde_json::json!({
                "destination": "out:{zset}",
                "sources": [{"key": "left:{zset}", "weight": 2}, "right:{zset}"],
                "aggregate": "max"
            }),
            "destination_cardinality",
        ),
        (
            "redis_zpopmax",
            serde_json::json!({"key": "leaders", "count": 1}),
            "members",
        ),
        (
            "redis_zpopmin",
            serde_json::json!({"key": "leaders", "count": 1}),
            "members",
        ),
        (
            "redis_zrem",
            serde_json::json!({"key": "leaders", "members": ["alice"]}),
            "removed",
        ),
        (
            "redis_zremrangebyscore",
            serde_json::json!({
                "key": "leaders",
                "min": {"kind": "exclusive", "value": "0"},
                "max": {"kind": "positive_infinity"}
            }),
            "removed",
        ),
        (
            "redis_zrangestore",
            serde_json::json!({
                "destination": "out:{zset}", "source": "leaders:{zset}",
                "range": {"kind": "rank", "start": 0, "stop": 9}
            }),
            "destination_cardinality",
        ),
        (
            "redis_zunionstore",
            serde_json::json!({
                "destination": "out:{zset}",
                "sources": ["left:{zset}", "right:{zset}"],
                "aggregate": "sum"
            }),
            "destination_cardinality",
        ),
        (
            "redis_bitop",
            serde_json::json!({
                "destination": "bitmap:result", "operation": "and",
                "sources": ["bitmap:left", "bitmap:right"]
            }),
            "result_length_bytes",
        ),
        (
            "redis_geosearchstore",
            serde_json::json!({
                "destination": "nearby", "source": "places",
                "center": {"kind": "member", "member": "san-francisco"},
                "shape": {"kind": "radius", "radius": "10", "unit": "kilometers"},
                "count": 10
            }),
            "stored",
        ),
        (
            "redis_pfmerge",
            serde_json::json!({"destination": "all-visitors", "sources": ["visitors"]}),
            "destination_overwritten",
        ),
        (
            "redis_xdel",
            serde_json::json!({"key": "events", "ids": [{"milliseconds": 1, "sequence": 0}]}),
            "deleted",
        ),
        (
            "redis_xtrim",
            serde_json::json!({
                "key": "events",
                "trim": {"type": "max_len", "threshold": 100, "approximate": true}
            }),
            "removed",
        ),
        (
            "redis_xgroup_destroy",
            serde_json::json!({"key": "events", "group": {"value": "workers"}}),
            "applied",
        ),
        (
            "redis_xgroup_delconsumer",
            serde_json::json!({
                "key": "events", "group": {"value": "workers"},
                "consumer": {"value": "worker-1"}
            }),
            "pending_deleted",
        ),
        (
            "redis_getdel",
            serde_json::json!({"key": "greeting"}),
            "value",
        ),
        (
            "redis_copy_replace",
            serde_json::json!({"source": "greeting", "destination": "greeting-copy"}),
            "copied",
        ),
        (
            "redis_rename",
            serde_json::json!({"source": "greeting", "destination": "renamed"}),
            "renamed",
        ),
        (
            "redis_renamenx",
            serde_json::json!({"source": "greeting", "destination": "renamed"}),
            "renamed",
        ),
        (
            "redis_restore_replace",
            serde_json::json!({"key": "restored", "payload_base64": "AA=="}),
            "restored",
        ),
        (
            "redis_json_del",
            serde_json::json!({"key": "doc:1"}),
            "deleted",
        ),
        (
            "redis_json_clear",
            serde_json::json!({"key": "doc:1"}),
            "cleared",
        ),
        (
            "redis_json_arrpop",
            serde_json::json!({"key": "doc:1", "path": "$.items"}),
            "popped",
        ),
        (
            "redis_json_arrtrim",
            serde_json::json!({"key": "doc:1", "path": "$.items", "start": 0, "stop": 1}),
            "values",
        ),
        (
            "redis_json_merge",
            serde_json::json!({"key": "doc:1", "value": {"name": "Ada"}}),
            "merged",
        ),
        (
            "redis_ft_dropindex",
            serde_json::json!({"index": "idx:docs"}),
            "dropped",
        ),
        (
            "redis_ft_aliasupdate",
            serde_json::json!({"alias": "docs", "index": "idx:docs-v2"}),
            "action",
        ),
        (
            "redis_ft_aliasdel",
            serde_json::json!({"alias": "docs"}),
            "action",
        ),
        (
            "redis_ft_dictdel",
            serde_json::json!({"dict": "terms", "terms": ["redis"]}),
            "changed",
        ),
        (
            "redis_command",
            serde_json::json!({"command": "ECHO", "arguments": ["hello"]}),
            "value",
        ),
    ]
}

#[tokio::test]
async fn access_modes_expose_exactly_the_expected_tools() {
    assert_eq!(tool_names(AccessMode::Full, false).len(), 201);
    assert_eq!(tool_names(AccessMode::Full, true).len(), 202);
    for (access, raw) in [
        (AccessMode::ReadOnly, false),
        (AccessMode::ReadWrite, false),
        (AccessMode::Full, false),
        (AccessMode::Full, true),
    ] {
        let client = client(access, raw).await;
        let listed = client.list_tools().await.expect("list tools");
        let actual = listed
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(actual, tool_names(access, raw));

        for tool in listed.tools {
            assert_eq!(tool.input_schema["type"], "object", "{}", tool.name);
            assert_eq!(
                tool.input_schema["additionalProperties"], false,
                "{}",
                tool.name
            );
            assert_eq!(
                tool.output_schema.as_ref().map(|schema| &schema["type"]),
                Some(&serde_json::json!("object")),
                "{}",
                tool.name
            );
            assert!(tool.annotations.is_some(), "{}", tool.name);
        }
    }
}

#[tokio::test]
async fn redis_eight_command_families_have_callable_typed_contracts() {
    let client = client(AccessMode::Full, false).await;
    let cases = vec![
        ("redis_arcount", serde_json::json!({"key": "array"})),
        (
            "redis_ardel",
            serde_json::json!({"key": "array", "indices": [7]}),
        ),
        (
            "redis_ardelrange",
            serde_json::json!({"key": "array", "ranges": [{"start": 7, "end": 8}]}),
        ),
        (
            "redis_arget",
            serde_json::json!({"key": "array", "index": 7}),
        ),
        (
            "redis_argetrange",
            serde_json::json!({"key": "array", "start": 7, "end": 8}),
        ),
        (
            "redis_argrep",
            serde_json::json!({"key": "array", "start": "0", "end": "+", "predicates": [{"type": "exact", "value": {"value": "needle"}}], "limit": 10, "with_values": true}),
        ),
        (
            "redis_arinfo",
            serde_json::json!({"key": "array", "full": true}),
        ),
        (
            "redis_arinsert",
            serde_json::json!({"key": "array", "values": [{"value": "one"}]}),
        ),
        (
            "redis_arlastitems",
            serde_json::json!({"key": "array", "count": 2}),
        ),
        ("redis_arlen", serde_json::json!({"key": "array"})),
        (
            "redis_armget",
            serde_json::json!({"key": "array", "indices": [7, 8]}),
        ),
        (
            "redis_armset",
            serde_json::json!({"key": "array", "entries": [{"index": 7, "value": {"value": "one"}}]}),
        ),
        ("redis_arnext", serde_json::json!({"key": "array"})),
        (
            "redis_arop",
            serde_json::json!({"key": "array", "start": 0, "end": 8, "operation": {"type": "used"}}),
        ),
        (
            "redis_arring",
            serde_json::json!({"key": "array", "size": 10, "values": [{"value": "one"}]}),
        ),
        (
            "redis_arscan",
            serde_json::json!({"key": "array", "start": 0, "end": 8, "limit": 10}),
        ),
        (
            "redis_arseek",
            serde_json::json!({"key": "array", "index": 7}),
        ),
        (
            "redis_arset",
            serde_json::json!({"key": "array", "index": 7, "values": [{"value": "one"}]}),
        ),
        (
            "redis_delex",
            serde_json::json!({"key": "string", "condition": {"type": "value_equals", "value": {"value": "one"}}}),
        ),
        ("redis_digest", serde_json::json!({"key": "string"})),
        (
            "redis_hgetdel",
            serde_json::json!({"key": "hash", "fields": [{"value": "one"}, {"value": "two"}]}),
        ),
        (
            "redis_hgetex",
            serde_json::json!({"key": "hash", "expiration": {"type": "seconds", "value": 30}, "fields": [{"value": "one"}, {"value": "two"}]}),
        ),
        (
            "redis_hsetex",
            serde_json::json!({"key": "hash", "condition": "fields_must_not_exist", "expiration": {"type": "seconds", "value": 30}, "fields": [{"field": {"value": "one"}, "value": {"value": "value"}}]}),
        ),
        (
            "redis_increx",
            serde_json::json!({"key": "number", "increment": {"type": "float", "value": "1.5"}, "expiration": {"type": "seconds", "value": 30} }),
        ),
        (
            "redis_lmovem",
            serde_json::json!({"source": {"value": "list:{x}:source"}, "destination": {"value": "list:{x}:destination"}, "from": "left", "to": "right", "amount": {"type": "up_to", "count": 1, "ordering": "bulk"}}),
        ),
        (
            "redis_msetex",
            serde_json::json!({"entries": [{"key": {"value": "key:{x}:1"}, "value": {"value": "one"}}], "condition": "only_if_missing", "expiration": {"type": "seconds", "value": 30}}),
        ),
        (
            "redis_vadd",
            serde_json::json!({"key": "vectors", "vector": {"type": "values", "values": [0, 1]}, "element": {"value": "one"}, "attributes": {"role": "primary"}}),
        ),
        ("redis_vcard", serde_json::json!({"key": "vectors"})),
        ("redis_vdim", serde_json::json!({"key": "vectors"})),
        (
            "redis_vemb",
            serde_json::json!({"key": "vectors", "element": {"value": "one"}}),
        ),
        (
            "redis_vgetattr",
            serde_json::json!({"key": "vectors", "element": {"value": "one"}}),
        ),
        ("redis_vinfo", serde_json::json!({"key": "vectors"})),
        (
            "redis_vismember",
            serde_json::json!({"key": "vectors", "element": {"value": "one"}}),
        ),
        (
            "redis_vlinks",
            serde_json::json!({"key": "vectors", "element": {"value": "one"}, "with_scores": true}),
        ),
        (
            "redis_vrandmember",
            serde_json::json!({"key": "vectors", "count": 1}),
        ),
        (
            "redis_vrange",
            serde_json::json!({"key": "vectors", "start": {"value": "-"}, "end": {"value": "+"}, "count": 10}),
        ),
        (
            "redis_vrem",
            serde_json::json!({"key": "vectors", "element": {"value": "one"}}),
        ),
        (
            "redis_vsetattr",
            serde_json::json!({"key": "vectors", "element": {"value": "one"}, "attributes": {"role": "primary"}}),
        ),
        (
            "redis_vsim",
            serde_json::json!({"key": "vectors", "query": {"type": "element", "element": {"value": "one"}}, "with_scores": true, "count": 10}),
        ),
        (
            "redis_xackdel",
            serde_json::json!({"key": "stream", "group": {"value": "workers"}, "reference_policy": "delete_references", "ids": [{"milliseconds": 1, "sequence": 0}]}),
        ),
        (
            "redis_xdelex",
            serde_json::json!({"key": "stream", "reference_policy": "only_if_acknowledged", "ids": [{"milliseconds": 1, "sequence": 0}]}),
        ),
        (
            "redis_xnack",
            serde_json::json!({"key": "stream", "group": {"value": "workers"}, "mode": "silent", "ids": [{"milliseconds": 1, "sequence": 0}]}),
        ),
    ];

    assert_eq!(cases.len(), 42);
    for (tool, input) in cases {
        let result = client
            .call_tool(tool, input)
            .await
            .unwrap_or_else(|error| panic!("{tool} invocation failed: {error}"));
        assert!(!result.is_error, "{tool}: {result:?}");
        assert!(result.structured_content.is_some(), "{tool}");
    }
}

#[tokio::test]
async fn redis_eight_complex_tools_emit_exact_bounded_command_shapes() {
    let executor = FixedRedis::new(RedisValue::Array(vec![
        RedisValue::Array(vec![
            RedisValue::Integer(0),
            RedisValue::BulkString(b"alpha".to_vec()),
        ]),
        RedisValue::Array(vec![
            RedisValue::Integer(2),
            RedisValue::BulkString(b"alphabet".to_vec()),
        ]),
    ]));
    let commands = executor.commands.clone();
    let client = fixed_client(executor, RedisCapabilities::unknown()).await;
    let grep = client
        .call_tool(
            "redis_argrep",
            serde_json::json!({
                "key": "array",
                "start": "0",
                "end": "+",
                "predicates": [
                    {"type": "match", "value": {"value": "alpha"}},
                    {"type": "glob", "pattern": "a*"}
                ],
                "combination": "all",
                "no_case": true,
                "with_values": true,
                "limit": 2
            }),
        )
        .await
        .expect("typed ARGREP")
        .structured_content
        .expect("structured ARGREP");
    assert_eq!(grep["count"], 2);
    assert_eq!(grep["complete"], false);
    assert_eq!(grep["next_start"], "3");
    assert_eq!(
        commands.lock().expect("ARGREP command")[0].arguments(),
        &[
            b"array".to_vec(),
            b"0".to_vec(),
            b"+".to_vec(),
            b"MATCH".to_vec(),
            b"alpha".to_vec(),
            b"GLOB".to_vec(),
            b"a*".to_vec(),
            b"AND".to_vec(),
            b"NOCASE".to_vec(),
            b"LIMIT".to_vec(),
            b"2".to_vec(),
            b"WITHVALUES".to_vec(),
        ]
    );

    let executor = FixedRedis::new(RedisValue::Integer(1));
    let commands = executor.commands.clone();
    let client = fixed_client(executor, RedisCapabilities::unknown()).await;
    client
        .call_tool(
            "redis_vadd",
            serde_json::json!({
                "key": "vectors",
                "vector": {"type": "values", "values": ["1.25", -2]},
                "element": {"value": "/wA=", "encoding": "base64"},
                "reduce_dimensions": 1,
                "check_and_set": true,
                "quantization": "no_quantization",
                "build_exploration_factor": 32,
                "attributes": {"kind": "test"},
                "num_links": 16
            }),
        )
        .await
        .expect("typed VADD");
    assert_eq!(
        commands.lock().expect("VADD command")[0].arguments(),
        &[
            b"vectors".to_vec(),
            b"REDUCE".to_vec(),
            b"1".to_vec(),
            b"VALUES".to_vec(),
            b"2".to_vec(),
            b"1.25".to_vec(),
            b"-2".to_vec(),
            vec![0xff, 0x00],
            b"CAS".to_vec(),
            b"NOQUANT".to_vec(),
            b"EF".to_vec(),
            b"32".to_vec(),
            b"SETATTR".to_vec(),
            br#"{"kind":"test"}"#.to_vec(),
            b"M".to_vec(),
            b"16".to_vec(),
        ]
    );

    let executor = FixedRedis::new(RedisValue::Integer(1));
    let commands = executor.commands.clone();
    let client = fixed_client(executor, RedisCapabilities::unknown()).await;
    client
        .call_tool(
            "redis_msetex",
            serde_json::json!({
                "entries": [
                    {"key": {"value": "key:{slot}:one"}, "value": {"value": "one"}},
                    {"key": {"value": "key:{slot}:two"}, "value": {"value": "two"}}
                ],
                "condition": "only_if_existing",
                "expiration": {"type": "milliseconds", "value": 5000}
            }),
        )
        .await
        .expect("typed MSETEX");
    assert_eq!(
        commands.lock().expect("MSETEX command")[0].arguments(),
        &[
            b"2".to_vec(),
            b"key:{slot}:one".to_vec(),
            b"one".to_vec(),
            b"key:{slot}:two".to_vec(),
            b"two".to_vec(),
            b"XX".to_vec(),
            b"PX".to_vec(),
            b"5000".to_vec(),
        ]
    );

    let executor = FixedRedis::new(RedisValue::Array(vec![
        RedisValue::BulkString(b"12.5".to_vec()),
        RedisValue::BulkString(b"2.5".to_vec()),
    ]));
    let commands = executor.commands.clone();
    let client = fixed_client(executor, RedisCapabilities::unknown()).await;
    client
        .call_tool(
            "redis_increx",
            serde_json::json!({
                "key": "number",
                "increment": {"type": "float", "value": "2.5"},
                "saturate": true,
                "lower_bound": "0",
                "upper_bound": "20",
                "expiration": {"type": "seconds", "value": 30},
                "expiration_only_if_missing": true
            }),
        )
        .await
        .expect("typed INCREX");
    assert_eq!(
        commands.lock().expect("INCREX command")[0].arguments(),
        &[
            b"number".to_vec(),
            b"BYFLOAT".to_vec(),
            b"2.5".to_vec(),
            b"SATURATE".to_vec(),
            b"LBOUND".to_vec(),
            b"0".to_vec(),
            b"UBOUND".to_vec(),
            b"20".to_vec(),
            b"EX".to_vec(),
            b"30".to_vec(),
            b"ENX".to_vec(),
        ]
    );

    let executor = FixedRedis::new(RedisValue::Array(vec![RedisValue::BulkString(
        b"moved".to_vec(),
    )]));
    let commands = executor.commands.clone();
    let client = fixed_client(executor, RedisCapabilities::unknown()).await;
    client
        .call_tool(
            "redis_lmovem",
            serde_json::json!({
                "source": {"value": "list:{slot}:source"},
                "destination": {"value": "list:{slot}:destination"},
                "from": "right",
                "to": "left",
                "amount": {"type": "exactly", "count": 3, "ordering": "one_by_one"}
            }),
        )
        .await
        .expect("typed LMOVEM");
    assert_eq!(
        commands.lock().expect("LMOVEM command")[0].arguments(),
        &[
            b"list:{slot}:source".to_vec(),
            b"list:{slot}:destination".to_vec(),
            b"RIGHT".to_vec(),
            b"LEFT".to_vec(),
            b"EXACTLY".to_vec(),
            b"3".to_vec(),
            b"OBO".to_vec(),
        ]
    );

    let executor = FixedRedis::new(RedisValue::Array(vec![
        RedisValue::BulkString(b"a".to_vec()),
        RedisValue::BulkString(vec![0xff]),
    ]));
    let commands = executor.commands.clone();
    let client = fixed_client(executor, RedisCapabilities::unknown()).await;
    let range = client
        .call_tool(
            "redis_vrange",
            serde_json::json!({
                "key": "vectors",
                "start": {"value": "-"},
                "end": {"value": "+"},
                "count": 2
            }),
        )
        .await
        .expect("typed VRANGE")
        .structured_content
        .expect("structured VRANGE");
    assert_eq!(range["complete"], false);
    assert_eq!(range["next_start"]["encoding"], "base64");
    assert_eq!(range["next_start"]["value"], "KP8=");
    assert_eq!(
        commands.lock().expect("VRANGE command")[0].arguments(),
        &[
            b"vectors".to_vec(),
            b"-".to_vec(),
            b"+".to_vec(),
            b"2".to_vec()
        ]
    );

    let executor = FixedRedis::new(RedisValue::Array(vec![
        RedisValue::Integer(1),
        RedisValue::Integer(2),
    ]));
    let commands = executor.commands.clone();
    let client = fixed_client(executor, RedisCapabilities::unknown()).await;
    client
        .call_tool(
            "redis_xackdel",
            serde_json::json!({
                "key": "stream",
                "group": {"value": "workers"},
                "reference_policy": "delete_references",
                "ids": [
                    {"milliseconds": 1, "sequence": 0},
                    {"milliseconds": 2, "sequence": 3}
                ]
            }),
        )
        .await
        .expect("typed XACKDEL");
    assert_eq!(
        commands.lock().expect("XACKDEL command")[0].arguments(),
        &[
            b"stream".to_vec(),
            b"workers".to_vec(),
            b"DELREF".to_vec(),
            b"IDS".to_vec(),
            b"2".to_vec(),
            b"1-0".to_vec(),
            b"2-3".to_vec(),
        ]
    );
}

#[tokio::test]
async fn redis_eight_tools_are_version_gated_and_effect_annotated() {
    let names_at = |major, minor| {
        tool_names_for_capabilities(
            AccessMode::Full,
            [ToolBundle::DataStructures],
            false,
            &RedisCapabilities::unknown().with_redis_version(RedisVersion::new(major, minor, 0)),
            UnavailableToolPolicy::Hide,
        )
    };

    let redis_six = names_at(6, 0);
    for name in ["redis_zdiffstore", "redis_zrangestore"] {
        assert!(!redis_six.contains(&name), "{name}");
    }
    let redis_six_two = names_at(6, 2);
    for name in ["redis_zdiffstore", "redis_zrangestore"] {
        assert!(redis_six_two.contains(&name), "{name}");
    }
    assert!(!redis_six_two.contains(&"redis_zintercard"));

    let redis_seven = names_at(7, 4);
    assert!(redis_seven.contains(&"redis_zintercard"));
    for name in [
        "redis_vadd",
        "redis_hgetex",
        "redis_xackdel",
        "redis_delex",
        "redis_arcount",
        "redis_lmovem",
    ] {
        assert!(!redis_seven.contains(&name), "{name}");
    }

    let redis_eight = names_at(8, 0);
    for name in [
        "redis_vadd",
        "redis_vsim",
        "redis_hgetdel",
        "redis_hgetex",
        "redis_hsetex",
    ] {
        assert!(redis_eight.contains(&name), "{name}");
    }
    assert!(!redis_eight.contains(&"redis_vismember"));
    assert!(!redis_eight.contains(&"redis_xackdel"));

    let redis_eight_two = names_at(8, 2);
    for name in ["redis_vismember", "redis_xackdel", "redis_xdelex"] {
        assert!(redis_eight_two.contains(&name), "{name}");
    }

    let redis_eight_four = names_at(8, 4);
    for name in [
        "redis_delex",
        "redis_digest",
        "redis_msetex",
        "redis_vrange",
    ] {
        assert!(redis_eight_four.contains(&name), "{name}");
    }

    let redis_eight_eight = names_at(8, 8);
    for name in [
        "redis_arcount",
        "redis_arset",
        "redis_increx",
        "redis_xnack",
    ] {
        assert!(redis_eight_eight.contains(&name), "{name}");
    }
    for name in ["redis_lmovem", "redis_sdiffcard", "redis_sunioncard"] {
        assert!(!redis_eight_eight.contains(&name), "{name}");
        assert!(names_at(8, 10).contains(&name), "{name}");
    }

    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list modern tool annotations")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in ["redis_arcount", "redis_digest", "redis_vsim"] {
        let annotations = annotations(name);
        assert!(annotations.read_only_hint, "{name}");
        assert!(!annotations.destructive_hint, "{name}");
        assert!(annotations.idempotent_hint, "{name}");
    }
    for name in [
        "redis_arinsert",
        "redis_hgetex",
        "redis_increx",
        "redis_vadd",
    ] {
        let annotations = annotations(name);
        assert!(!annotations.read_only_hint, "{name}");
        assert!(!annotations.destructive_hint, "{name}");
    }
    for name in [
        "redis_ardel",
        "redis_delex",
        "redis_hgetdel",
        "redis_lmovem",
        "redis_vrem",
        "redis_xackdel",
        "redis_xdelex",
        "redis_xnack",
    ] {
        let annotations = annotations(name);
        assert!(!annotations.read_only_hint, "{name}");
        assert!(annotations.destructive_hint, "{name}");
    }
}

#[tokio::test]
async fn sorted_set_count_aggregation_is_gated_at_redis_eight_eight() {
    let executor = FixedRedis::new(RedisValue::Integer(1));
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 6, 0)),
    )
    .await;
    for tool in ["redis_zinterstore", "redis_zunionstore"] {
        let result = client
            .call_tool(
                tool,
                serde_json::json!({
                    "destination": "out:{tenant}",
                    "sources": ["left:{tenant}", "right:{tenant}"],
                    "aggregate": "count"
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool} COUNT version gate: {error}"));
        assert!(result.is_error, "{tool}: {result:?}");
        assert!(
            serde_json::to_string(&result)
                .expect("serialize COUNT version gate")
                .contains("Redis 8.8"),
            "{tool}: {result:?}"
        );
    }
    assert!(
        commands
            .lock()
            .expect("pre-8.8 sorted-set store commands")
            .is_empty(),
        "COUNT must fail before Redis execution"
    );

    let client = fixed_client(
        FixedRedis::new(RedisValue::Integer(1)),
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 8, 0)),
    )
    .await;
    let supported = client
        .call_tool(
            "redis_zunionstore",
            serde_json::json!({
                "destination": "out:{tenant}",
                "sources": ["left:{tenant}", "right:{tenant}"],
                "aggregate": "count"
            }),
        )
        .await
        .expect("Redis 8.8 COUNT aggregation");
    assert!(!supported.is_error, "{supported:?}");
}

#[tokio::test]
async fn set_annotations_match_read_write_and_destructive_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated set tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in [
        "redis_scard",
        "redis_sdiff",
        "redis_sdiffcard",
        "redis_sinter",
        "redis_sismember",
        "redis_smembers",
        "redis_smismember",
        "redis_sscan",
        "redis_sunion",
        "redis_sunioncard",
    ] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }

    let add = annotations("redis_sadd");
    assert!(!add.read_only_hint);
    assert!(!add.destructive_hint);
    assert!(add.idempotent_hint);

    let remove = annotations("redis_srem");
    assert!(!remove.read_only_hint);
    assert!(remove.destructive_hint);
    assert!(remove.idempotent_hint);

    for (name, idempotent) in [
        ("redis_sdiffstore", false),
        ("redis_sinterstore", true),
        ("redis_sunionstore", true),
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
        assert_eq!(annotation.idempotent_hint, idempotent, "{name}");
    }
}

#[tokio::test]
async fn sorted_set_annotations_match_read_write_and_destructive_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated sorted-set tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in [
        "redis_zcard",
        "redis_zcount",
        "redis_zintercard",
        "redis_zmscore",
        "redis_zrange",
        "redis_zrank",
        "redis_zrevrank",
        "redis_zscan",
        "redis_zscore",
    ] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }

    let add = annotations("redis_zadd");
    assert!(!add.read_only_hint);
    assert!(!add.destructive_hint);
    assert!(add.idempotent_hint);

    let increment = annotations("redis_zincrby");
    assert!(!increment.read_only_hint);
    assert!(!increment.destructive_hint);
    assert!(!increment.idempotent_hint);

    for name in [
        "redis_zpopmax",
        "redis_zpopmin",
        "redis_zrem",
        "redis_zremrangebyscore",
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
    }
    assert!(!annotations("redis_zpopmax").idempotent_hint);
    assert!(!annotations("redis_zpopmin").idempotent_hint);
    assert!(annotations("redis_zrem").idempotent_hint);
    assert!(annotations("redis_zremrangebyscore").idempotent_hint);
    for name in [
        "redis_zdiffstore",
        "redis_zinterstore",
        "redis_zrangestore",
        "redis_zunionstore",
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
        assert!(!annotation.idempotent_hint, "{name}");
    }
}

#[tokio::test]
async fn bitmap_geo_and_hll_annotations_match_effect_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated specialized data tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in [
        "redis_getbit",
        "redis_bitcount",
        "redis_bitpos",
        "redis_bitfield_ro",
        "redis_geodist",
        "redis_geohash",
        "redis_geopos",
        "redis_geosearch",
        "redis_pfcount",
    ] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }

    for name in ["redis_setbit", "redis_geoadd", "redis_pfadd"] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }
    let name = "redis_bitfield";
    let annotation = annotations(name);
    assert!(!annotation.read_only_hint, "{name}");
    assert!(!annotation.destructive_hint, "{name}");
    assert!(!annotation.idempotent_hint, "{name}");
    for name in ["redis_bitop", "redis_geosearchstore", "redis_pfmerge"] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
    }
    assert!(!annotations("redis_bitop").idempotent_hint);
    assert!(!annotations("redis_geosearchstore").idempotent_hint);
    assert!(annotations("redis_pfmerge").idempotent_hint);
}

#[tokio::test]
async fn key_string_annotations_match_access_and_overwrite_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    let inspect = annotations("redis_object_inspect");
    assert!(inspect.read_only_hint);
    assert!(!inspect.destructive_hint);
    assert!(inspect.idempotent_hint);

    let sort = annotations("redis_sort");
    assert!(sort.read_only_hint);
    assert!(!sort.destructive_hint);
    assert!(sort.idempotent_hint);

    let copy = annotations("redis_copy");
    assert!(!copy.read_only_hint);
    assert!(!copy.destructive_hint);
    assert!(copy.idempotent_hint);

    for name in ["redis_set", "redis_expire", "redis_getex", "redis_touch"] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(!annotation.idempotent_hint, "{name}");
    }

    for name in [
        "redis_copy_replace",
        "redis_getdel",
        "redis_rename",
        "redis_renamenx",
        "redis_restore_replace",
        "redis_sort_store",
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
    }
    assert!(!annotations("redis_restore_replace").idempotent_hint);
}

#[tokio::test]
async fn pubsub_annotations_distinguish_inspection_from_message_delivery() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated Pub/Sub tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in [
        "redis_pubsub_channels",
        "redis_pubsub_numsub",
        "redis_pubsub_numpat",
        "redis_pubsub_shardchannels",
        "redis_pubsub_shardnumsub",
    ] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }

    for name in ["redis_publish", "redis_spublish"] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(!annotation.idempotent_hint, "{name}");
    }

    for name in ["redis_subscribe", "redis_psubscribe", "redis_ssubscribe"] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(!annotation.idempotent_hint, "{name}");
    }
    let read = annotations("redis_pubsub_read");
    assert!(read.read_only_hint);
    assert!(!read.destructive_hint);
    assert!(!read.idempotent_hint);
    for name in ["redis_pubsub_unsubscribe", "redis_pubsub_close"] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }
}

#[tokio::test]
async fn hash_annotations_match_read_write_and_destructive_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated hash tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in [
        "redis_hexists",
        "redis_hkeys",
        "redis_hlen",
        "redis_hmget",
        "redis_hrandfield",
        "redis_hstrlen",
        "redis_httl",
        "redis_hvals",
    ] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }

    for name in ["redis_hexpire", "redis_hincrby", "redis_hincrbyfloat"] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(!annotation.idempotent_hint, "{name}");
    }

    let persist = annotations("redis_hpersist");
    assert!(!persist.read_only_hint);
    assert!(!persist.destructive_hint);
    assert!(persist.idempotent_hint);

    let delete = annotations("redis_hdel");
    assert!(!delete.read_only_hint);
    assert!(delete.destructive_hint);
    assert!(delete.idempotent_hint);

    let expire_delete = annotations("redis_hexpire_delete");
    assert!(!expire_delete.read_only_hint);
    assert!(expire_delete.destructive_hint);
    assert!(expire_delete.idempotent_hint);
}

#[tokio::test]
async fn list_annotations_match_read_write_and_destructive_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated list tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in ["redis_lindex", "redis_llen", "redis_lpos", "redis_lrange"] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }

    for name in ["redis_lpush", "redis_rpush"] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(!annotation.idempotent_hint, "{name}");
    }

    for name in [
        "redis_lpop",
        "redis_lmove",
        "redis_lrem",
        "redis_lset",
        "redis_ltrim",
        "redis_rpop",
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
    }
    assert!(annotations("redis_lset").idempotent_hint);
    assert!(!annotations("redis_ltrim").idempotent_hint);
    assert!(!annotations("redis_lmove").idempotent_hint);
}

#[tokio::test]
async fn bundles_are_composable_and_raw_remains_a_separate_opt_in() {
    let diagnostics = client_for_bundles(
        AccessMode::Full,
        [ToolBundle::Diagnostics],
        RawCommandPolicy::Disabled,
    )
    .await;
    let listed = diagnostics.list_tools().await.expect("list diagnostics");
    assert_eq!(
        listed
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "redis_acl_whoami",
            "redis_client_list",
            "redis_cluster_info",
            "redis_connection_summary",
            "redis_health_check",
            "redis_hotkeys",
            "redis_info",
            "redis_key_summary",
            "redis_keyspace_summary",
            "redis_latency_history",
            "redis_memory_stats",
            "redis_memory_summary",
            "redis_module_list",
            "redis_slowlog",
        ]
    );

    let essentials_and_raw = client_for_bundles(
        AccessMode::Full,
        [ToolBundle::Essentials],
        RawCommandPolicy::Classified,
    )
    .await;
    let actual = essentials_and_raw
        .list_tools()
        .await
        .expect("list essentials and raw")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        tool_names_for(AccessMode::Full, [ToolBundle::Essentials], true)
    );
    assert!(actual.iter().any(|name| name == "redis_command"));
    assert!(!actual.iter().any(|name| name == "redis_info"));

    let json_read_only = client_for_bundles(
        AccessMode::ReadOnly,
        [ToolBundle::Json],
        RawCommandPolicy::Disabled,
    )
    .await
    .list_tools()
    .await
    .expect("list JSON read tools")
    .tools
    .into_iter()
    .map(|tool| tool.name)
    .collect::<Vec<_>>();
    assert_eq!(
        json_read_only,
        vec![
            "redis_json_arrlen",
            "redis_json_get",
            "redis_json_mget",
            "redis_json_objkeys",
            "redis_json_objlen",
            "redis_json_strlen",
            "redis_json_type",
        ]
    );

    let search_read_only = client_for_bundles(
        AccessMode::ReadOnly,
        [ToolBundle::Search],
        RawCommandPolicy::Disabled,
    )
    .await
    .list_tools()
    .await
    .expect("list Search read tools")
    .tools
    .into_iter()
    .map(|tool| tool.name)
    .collect::<Vec<_>>();
    assert!(
        search_read_only
            .iter()
            .any(|name| name == "redis_ft_vector_search")
    );
    assert!(
        search_read_only
            .iter()
            .any(|name| name == "redis_ft_hybrid_search")
    );
    assert!(
        !search_read_only
            .iter()
            .any(|name| name == "redis_ft_create")
    );
    assert!(
        !search_read_only
            .iter()
            .any(|name| name == "redis_vector_set_hash")
    );

    let mut search_read_write = client_for_bundles(
        AccessMode::ReadWrite,
        [ToolBundle::Search],
        RawCommandPolicy::Disabled,
    )
    .await
    .list_tools()
    .await
    .expect("list Search read-write tools")
    .tools
    .into_iter()
    .map(|tool| tool.name)
    .collect::<Vec<_>>();
    search_read_write.sort();
    assert_eq!(
        search_read_write,
        vec![
            "redis_ft_aggregate",
            "redis_ft_aliasadd",
            "redis_ft_alter",
            "redis_ft_create",
            "redis_ft_cursor_del",
            "redis_ft_cursor_read",
            "redis_ft_dictadd",
            "redis_ft_dictdump",
            "redis_ft_explain",
            "redis_ft_hybrid_search",
            "redis_ft_info",
            "redis_ft_list",
            "redis_ft_profile",
            "redis_ft_search",
            "redis_ft_syndump",
            "redis_ft_synupdate",
            "redis_ft_tagvals",
            "redis_ft_vector_search",
            "redis_vector_get_hash",
            "redis_vector_set_hash",
        ]
    );
}

#[test]
fn invalid_builder_safety_configuration_is_rejected() {
    assert!(matches!(
        RedisMcp::builder(StubRedis).raw_commands(true).try_build(),
        Err(RedisMcpBuildError::RawCommandsRequireFullAccess)
    ));
    assert!(matches!(
        RedisMcp::builder(StubRedis)
            .command_timeout(Duration::ZERO)
            .try_build(),
        Err(RedisMcpBuildError::ZeroCommandTimeout)
    ));
    assert!(matches!(
        RedisMcp::builder(StubRedis)
            .output_budget(OutputBudget::new(0, 1))
            .try_build(),
        Err(RedisMcpBuildError::ZeroOutputBytes)
    ));
    assert!(matches!(
        RedisMcp::builder(StubRedis)
            .output_budget(OutputBudget::new(1, 0))
            .try_build(),
        Err(RedisMcpBuildError::ZeroOutputEntries)
    ));
    assert!(matches!(
        RedisMcp::builder(StubRedis)
            .bundles([ToolBundle::Sessions])
            .try_build(),
        Err(RedisMcpBuildError::SessionsRequireManager)
    ));
}

fn assert_output_limit(
    result: &tower_mcp::CallToolResult,
    dimension: &str,
    actual: usize,
    limit: usize,
) {
    assert!(result.is_error);
    let error = &result
        .meta
        .as_ref()
        .expect("structured output-limit metadata")["io.redis.mcp/outputLimit"];
    assert_eq!(error["code"], "output_limit_exceeded");
    assert_eq!(error["dimension"], dimension);
    assert_eq!(error["actual"], actual);
    assert_eq!(error["limit"], limit);
    assert_eq!(error["retryable"], true);
}

#[tokio::test]
async fn encoded_output_budget_accepts_exact_limit_and_rejects_one_byte_over() {
    let generous = client_with_budget(
        AccessMode::ReadOnly,
        false,
        OutputBudget::new(1_000_000, 1_000),
    )
    .await;
    let baseline = generous
        .call_tool("redis_get", serde_json::json!({"key": "greeting"}))
        .await
        .expect("baseline GET");
    let encoded_bytes = serde_json::to_vec(&baseline)
        .expect("serialize baseline GET")
        .len();

    let exact = client_with_budget(
        AccessMode::ReadOnly,
        false,
        OutputBudget::new(encoded_bytes, 1_000),
    )
    .await
    .call_tool("redis_get", serde_json::json!({"key": "greeting"}))
    .await
    .expect("exact-limit GET");
    assert!(!exact.is_error);

    let limited = client_with_budget(
        AccessMode::ReadOnly,
        false,
        OutputBudget::new(encoded_bytes - 1, 1_000),
    )
    .await
    .call_tool("redis_get", serde_json::json!({"key": "greeting"}))
    .await
    .expect("over-limit GET");
    assert_output_limit(&limited, "encoded_bytes", encoded_bytes, encoded_bytes - 1);
}

#[tokio::test]
async fn collection_budget_accepts_exact_limit_and_returns_retry_guidance() {
    let exact = client_with_budget(AccessMode::ReadOnly, false, OutputBudget::new(1_000_000, 2))
        .await
        .call_tool("redis_smembers", serde_json::json!({"key": "tags"}))
        .await
        .expect("exact-limit SMEMBERS");
    assert!(!exact.is_error);

    let limited = client_with_budget(AccessMode::ReadOnly, false, OutputBudget::new(1_000_000, 1))
        .await
        .call_tool("redis_smembers", serde_json::json!({"key": "tags"}))
        .await
        .expect("over-limit SMEMBERS");
    assert_output_limit(&limited, "collection_entries", 2, 1);
    assert!(
        limited.meta.as_ref().unwrap()["io.redis.mcp/outputLimit"]["guidance"]
            .as_str()
            .unwrap()
            .contains("redis_sscan")
    );

    let algebra = client_with_budget(AccessMode::ReadOnly, false, OutputBudget::new(1_000_000, 1))
        .await
        .call_tool(
            "redis_sunion",
            serde_json::json!({"keys": ["tags", "other"]}),
        )
        .await
        .expect("over-limit SUNION");
    assert_output_limit(&algebra, "collection_entries", 2, 1);
    assert!(
        algebra.meta.as_ref().unwrap()["io.redis.mcp/outputLimit"]["guidance"]
            .as_str()
            .unwrap()
            .contains("SSCAN")
    );
}

#[tokio::test]
async fn sorted_set_collection_inputs_are_bounded_before_reads_or_destructive_effects() {
    let client = client_with_budget(AccessMode::Full, false, OutputBudget::new(1_000_000, 1)).await;
    for (tool, arguments) in [
        (
            "redis_zmscore",
            serde_json::json!({"key": "leaders", "members": ["alice", "bob"]}),
        ),
        (
            "redis_zrange",
            serde_json::json!({
                "key": "leaders",
                "range": {
                    "kind": "score",
                    "min": {"kind": "negative_infinity"},
                    "max": {"kind": "positive_infinity"},
                    "limit": 2
                }
            }),
        ),
        (
            "redis_zpopmin",
            serde_json::json!({"key": "leaders", "count": 2}),
        ),
        (
            "redis_zrangestore",
            serde_json::json!({
                "destination": "out:{tenant}",
                "source": "leaders:{tenant}",
                "range": {"kind": "rank", "start": 0, "stop": 1}
            }),
        ),
    ] {
        let result = client
            .call_tool(tool, arguments)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(result.is_error, "{tool}");
        assert!(
            serde_json::to_string(&result)
                .expect("serialize bounded sorted-set result")
                .contains("configured output limit of 1 entries"),
            "{tool}: {result:?}"
        );
    }
}

#[tokio::test]
async fn raw_commands_share_the_hard_encoded_response_budget() {
    let arguments = serde_json::json!({"command": "ECHO", "arguments": ["hello"]});
    let baseline = client_with_budget(AccessMode::Full, true, OutputBudget::new(1_000_000, 1_000))
        .await
        .call_tool("redis_command", arguments.clone())
        .await
        .expect("baseline raw command");
    let encoded_bytes = serde_json::to_vec(&baseline)
        .expect("serialize baseline raw result")
        .len();

    let limited = client_with_budget(
        AccessMode::Full,
        true,
        OutputBudget::new(encoded_bytes - 1, 1_000),
    )
    .await
    .call_tool("redis_command", arguments)
    .await
    .expect("over-limit raw command");
    assert_output_limit(&limited, "encoded_bytes", encoded_bytes, encoded_bytes - 1);
}

#[tokio::test]
async fn scan_and_range_outputs_expose_typed_continuations() {
    let client = client(AccessMode::ReadOnly, false).await;
    let scan = client
        .call_tool(
            "redis_hscan",
            serde_json::json!({"key": "user:1", "count": 10}),
        )
        .await
        .expect("HSCAN")
        .structured_content
        .expect("structured HSCAN");
    assert_eq!(scan["page"]["complete"], false);
    assert_eq!(scan["page"]["continuation"]["cursor"], 7);

    let range = client
        .call_tool(
            "redis_lrange",
            serde_json::json!({"key": "queue", "start": 0, "stop": 0}),
        )
        .await
        .expect("LRANGE")
        .structured_content
        .expect("structured LRANGE");
    assert_eq!(range["count"], 1);
    assert_eq!(range["page"]["complete"], false);
    assert_eq!(range["page"]["continuation"]["start"], 1);
}

#[tokio::test]
async fn diagnostics_redact_sensitive_fields_and_require_explicit_full_access() {
    let read_only = client(AccessMode::ReadOnly, false).await;
    let clients = read_only
        .call_tool("redis_client_list", serde_json::json!({}))
        .await
        .expect("redacted CLIENT LIST")
        .structured_content
        .expect("structured CLIENT LIST");
    assert_eq!(clients["clients"][0]["address"], serde_json::Value::Null);
    assert_eq!(clients["clients"][0]["name"], serde_json::Value::Null);
    assert_eq!(clients["clients"][0]["username"], serde_json::Value::Null);
    assert_eq!(
        clients["clients"][0]["unknown_fields"],
        serde_json::Value::Null
    );
    assert_eq!(clients["clients"][0]["unknown_field_count"], 1);
    assert_eq!(clients["clients"][0]["sensitive_fields_redacted"], true);
    let serialized = serde_json::to_string(&clients).expect("serialize redacted clients");
    assert!(!serialized.contains("10.0.0.1"));
    assert!(!serialized.contains("agent"));
    assert!(!serialized.contains("future=value"));

    let slowlog = read_only
        .call_tool("redis_slowlog", serde_json::json!({}))
        .await
        .expect("redacted SLOWLOG")
        .structured_content
        .expect("structured SLOWLOG");
    assert_eq!(slowlog["entries"][0]["arguments"], serde_json::Value::Null);
    assert_eq!(
        slowlog["entries"][0]["client_address"],
        serde_json::Value::Null
    );
    let serialized = serde_json::to_string(&slowlog).expect("serialize redacted SLOWLOG");
    assert!(!serialized.contains("secret-key"));
    assert!(!serialized.contains("secret-value"));
    assert!(!serialized.contains("10.0.0.1"));

    let memory = read_only
        .call_tool("redis_memory_stats", serde_json::json!({}))
        .await
        .expect("forward-compatible MEMORY STATS")
        .structured_content
        .expect("structured MEMORY STATS");
    let future = memory["nodes"][0]["fields"]
        .as_array()
        .expect("MEMORY fields")
        .iter()
        .find(|field| field["key"]["value"] == "future.stat")
        .expect("future MEMORY field");
    assert_eq!(future["value"]["encoding"], "base64");
    assert_eq!(future["value"]["value"], "/wA=");

    let modules = read_only
        .call_tool("redis_module_list", serde_json::json!({}))
        .await
        .expect("redacted MODULE LIST")
        .structured_content
        .expect("structured MODULE LIST");
    assert_eq!(modules["modules"][0]["path"], serde_json::Value::Null);
    assert_eq!(modules["modules"][0]["arguments"], serde_json::Value::Null);

    let denied = read_only
        .call_tool(
            "redis_client_list",
            serde_json::json!({"include_sensitive": true}),
        )
        .await
        .expect("sensitive CLIENT LIST denial");
    assert!(denied.is_error);

    let full = client(AccessMode::Full, false).await;
    let clients = full
        .call_tool(
            "redis_client_list",
            serde_json::json!({"include_sensitive": true}),
        )
        .await
        .expect("authorized CLIENT LIST")
        .structured_content
        .expect("structured authorized CLIENT LIST");
    assert_eq!(clients["clients"][0]["address"]["value"], "10.0.0.1:5000");
    assert_eq!(clients["clients"][0]["name"]["value"], "agent");
    assert_eq!(
        clients["clients"][0]["unknown_fields"]["future"]["value"],
        "value"
    );

    let modules = full
        .call_tool(
            "redis_module_list",
            serde_json::json!({"include_sensitive": true}),
        )
        .await
        .expect("authorized MODULE LIST")
        .structured_content
        .expect("structured authorized MODULE LIST");
    assert_eq!(modules["modules"][0]["path"]["value"], "/private/module.so");

    let slowlog = full
        .call_tool(
            "redis_slowlog",
            serde_json::json!({"include_arguments": true, "include_sensitive": true}),
        )
        .await
        .expect("authorized SLOWLOG")
        .structured_content
        .expect("structured authorized SLOWLOG");
    assert_eq!(slowlog["entries"][0]["arguments"][0]["value"], "secret-key");
    assert_eq!(
        slowlog["entries"][0]["client_address"]["value"],
        "10.0.0.1:5000"
    );
}

#[tokio::test]
async fn diagnostics_cluster_failures_are_structured_and_node_addresses_are_redacted() {
    let executor = FixedRedis::new(RedisValue::ClusterNodes(vec![
        (
            "10.0.0.1:6379".to_string(),
            RedisValue::BulkString(
                b"# Server\r\nredis_version:8.2.0\r\nloading:0\r\nrole:master\r\n".to_vec(),
            ),
        ),
        (
            "10.0.0.2:6379".to_string(),
            RedisValue::ServerError {
                code: "NOPERM".to_string(),
                message: Some("server detail that must be redacted".to_string()),
            },
        ),
    ]));
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_deployment(RedisDeployment::Cluster),
    )
    .await;
    let result = client
        .call_tool(
            "redis_health_check",
            serde_json::json!({"max_cluster_nodes": 4}),
        )
        .await
        .expect("partial cluster health")
        .structured_content
        .expect("structured partial cluster health");
    assert_eq!(result["status"], "degraded");
    assert_eq!(result["nodes"][0]["node"], "node-1");
    assert_eq!(result["cluster"]["nodes_queried"], 2);
    assert_eq!(result["cluster"]["nodes_succeeded"], 1);
    assert_eq!(result["cluster"]["complete"], false);
    assert_eq!(result["cluster"]["failures"][0]["node"], "node-2");
    assert_eq!(result["cluster"]["failures"][0]["code"], "NOPERM");
    let serialized = serde_json::to_string(&result).expect("serialize cluster health");
    assert!(!serialized.contains("10.0.0"));
    assert!(!serialized.contains("server detail"));
    let commands = commands.lock().expect("record diagnostics command");
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].name(), "INFO");
    assert_eq!(commands[0].cluster_node_limit(), Some(4));
}

#[tokio::test]
async fn diagnostics_bounds_and_single_page_hotkey_contract_are_enforced() {
    let client = client(AccessMode::ReadOnly, false).await;
    for (tool, arguments) in [
        ("redis_client_list", serde_json::json!({"max_results": 0})),
        (
            "redis_latency_history",
            serde_json::json!({"event": "command", "limit": 0}),
        ),
        ("redis_slowlog", serde_json::json!({"limit": 1001})),
        (
            "redis_hotkeys",
            serde_json::json!({"count": 2, "max_keys": 1, "top": 2}),
        ),
        (
            "redis_health_check",
            serde_json::json!({"max_cluster_nodes": 257}),
        ),
    ] {
        let result = client
            .call_tool(tool, arguments)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(result.is_error, "{tool}");
    }

    let hotkeys = client
        .call_tool(
            "redis_hotkeys",
            serde_json::json!({"cursor": 0, "count": 2, "max_keys": 2, "top": 1}),
        )
        .await
        .expect("bounded hotkey page")
        .structured_content
        .expect("structured hotkey page");
    assert_eq!(hotkeys["sampled_keys"], 2);
    assert_eq!(hotkeys["candidates"].as_array().map(Vec::len), Some(1));
    assert_eq!(hotkeys["page"]["complete"], true);
    assert_eq!(
        hotkeys["selection_basis"],
        "largest_memory_usage_in_one_explicit_scan_page"
    );
}

#[tokio::test]
async fn tool_calls_return_structured_content() {
    let client = full_catalog_client().await;
    for (name, arguments, expected_field) in structured_cases() {
        let result = client
            .call_tool(name, arguments)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(!result.is_error, "{name}");
        assert!(
            result
                .structured_content
                .as_ref()
                .is_some_and(|value| value.get(expected_field).is_some()),
            "{name}"
        );
    }
}

#[tokio::test]
async fn malformed_and_unbounded_inputs_fail_as_tool_results() {
    let client = full_catalog_client().await;
    let cases = [
        ("redis_exists", serde_json::json!({"keys": []})),
        ("redis_mset", serde_json::json!({"entries": []})),
        (
            "redis_lpush",
            serde_json::json!({"key": "queue", "elements": []}),
        ),
        (
            "redis_rpush",
            serde_json::json!({"key": "queue", "elements": []}),
        ),
        (
            "redis_lpop",
            serde_json::json!({"key": "queue", "count": 0}),
        ),
        (
            "redis_lpos",
            serde_json::json!({"key": "queue", "value": "item", "rank": 0}),
        ),
        (
            "redis_hmget",
            serde_json::json!({"key": "hash", "fields": []}),
        ),
        (
            "redis_httl",
            serde_json::json!({"key": "hash", "fields": []}),
        ),
        ("redis_hset", serde_json::json!({"key": "hash"})),
        (
            "redis_hset",
            serde_json::json!({
                "key": "hash",
                "fields": {"field": "value"},
                "entries": [{"field": "other", "value": "value"}]
            }),
        ),
        (
            "redis_zadd",
            serde_json::json!({
                "key": "leaders",
                "members": [{"score": "NaN", "member": "alice"}]
            }),
        ),
        (
            "redis_zmscore",
            serde_json::json!({"key": "leaders", "members": []}),
        ),
        (
            "redis_zincrby",
            serde_json::json!({"key": "leaders", "member": "alice", "increment": "+inf"}),
        ),
        (
            "redis_zrem",
            serde_json::json!({"key": "leaders", "members": []}),
        ),
        (
            "redis_zpopmin",
            serde_json::json!({"key": "leaders", "count": 0}),
        ),
        (
            "redis_zpopmin",
            serde_json::json!({"key": "leaders", "max_returned_bytes": 0}),
        ),
        ("redis_zintercard", serde_json::json!({"keys": []})),
        (
            "redis_zinterstore",
            serde_json::json!({"destination": "out", "sources": []}),
        ),
        (
            "redis_zinterstore",
            serde_json::json!({
                "destination": "out",
                "sources": [{"key": "leaders", "weight": "NaN"}]
            }),
        ),
        (
            "redis_zunionstore",
            serde_json::json!({
                "destination": "out",
                "sources": [{"key": "leaders", "weight": "+inf"}]
            }),
        ),
        (
            "redis_zrangestore",
            serde_json::json!({
                "destination": "out",
                "source": "leaders",
                "range": {"kind": "rank", "start": 0, "stop": -1}
            }),
        ),
        (
            "redis_hset",
            serde_json::json!({
                "key": "hash",
                "entries": [
                    {"field": "field", "value": "one"},
                    {"field": "ZmllbGQ=", "field_encoding": "base64", "value": "two"}
                ]
            }),
        ),
        (
            "redis_hset",
            serde_json::json!({
                "key": "hash",
                "entries": [{"field": "not-base64", "field_encoding": "base64", "value": "value"}]
            }),
        ),
        (
            "redis_hexpire",
            serde_json::json!({"key": "hash", "seconds": 0, "fields": ["field"]}),
        ),
        (
            "redis_hexpire",
            serde_json::json!({"key": "hash", "seconds": 60, "fields": ["field", "field"]}),
        ),
        (
            "redis_hpersist",
            serde_json::json!({"key": "hash", "fields": []}),
        ),
        (
            "redis_hdel",
            serde_json::json!({"key": "hash", "fields": ["field", "field"]}),
        ),
        (
            "redis_sadd",
            serde_json::json!({"key": "tags", "members": []}),
        ),
        (
            "redis_smismember",
            serde_json::json!({"key": "tags", "members": []}),
        ),
        ("redis_sdiff", serde_json::json!({"keys": []})),
        ("redis_sdiffcard", serde_json::json!({"keys": []})),
        (
            "redis_sdiffstore",
            serde_json::json!({"destination": "out", "keys": []}),
        ),
        (
            "redis_srem",
            serde_json::json!({"key": "tags", "members": []}),
        ),
        (
            "redis_sismember",
            serde_json::json!({
                "key": "tags",
                "member": "not-base64",
                "member_encoding": "base64"
            }),
        ),
        (
            "redis_zadd",
            serde_json::json!({
                "key": "leaders",
                "members": [{"score": 1.0, "member": "alice"}],
                "nx": true,
                "xx": true
            }),
        ),
        (
            "redis_expire",
            serde_json::json!({"key": "greeting", "seconds": 0}),
        ),
        (
            "redis_scan",
            serde_json::json!({"pattern": "*", "count": 0}),
        ),
        (
            "redis_lrange",
            serde_json::json!({"key": "queue", "start": 0, "stop": -1}),
        ),
        (
            "redis_zrange",
            serde_json::json!({"key": "leaders", "start": 0, "stop": 1000}),
        ),
        (
            "redis_zrange",
            serde_json::json!({
                "key": "leaders",
                "range": {
                    "kind": "score",
                    "min": {"kind": "negative_infinity"},
                    "max": {"kind": "positive_infinity"},
                    "start": 0,
                    "limit": 10
                }
            }),
        ),
        (
            "redis_zrange",
            serde_json::json!({
                "key": "leaders",
                "range": {
                    "kind": "lex",
                    "min": {"kind": "inclusive", "value": "not-base64", "encoding": "base64"},
                    "max": {"kind": "positive_infinity"},
                    "limit": 10
                }
            }),
        ),
        (
            "redis_setbit",
            serde_json::json!({"key": "bitmap", "offset": 134217728, "value": true}),
        ),
        (
            "redis_bitfield",
            serde_json::json!({"key": "bitmap", "operations": []}),
        ),
        (
            "redis_bitfield",
            serde_json::json!({
                "key": "bitmap",
                "operations": [{
                    "operation": "get",
                    "encoding": {"signed": false, "width": 64},
                    "offset": {"kind": "absolute", "value": 0}
                }]
            }),
        ),
        (
            "redis_bitop",
            serde_json::json!({
                "destination": "result", "operation": "not", "sources": ["one", "two"]
            }),
        ),
        (
            "redis_geoadd",
            serde_json::json!({
                "key": "places", "nx": true, "xx": true,
                "members": [{"member": "here", "longitude": 0, "latitude": 0}]
            }),
        ),
        (
            "redis_geoadd",
            serde_json::json!({
                "key": "places",
                "members": [{"member": "here", "longitude": 181, "latitude": 0}]
            }),
        ),
        (
            "redis_geosearch",
            serde_json::json!({
                "key": "places",
                "center": {"kind": "member", "member": "here"},
                "shape": {"kind": "radius", "radius": 0, "unit": "meters"},
                "count": 10
            }),
        ),
        (
            "redis_geohash",
            serde_json::json!({
                "key": "places", "members": [{"value": "not-base64", "encoding": "base64"}]
            }),
        ),
        (
            "redis_get",
            serde_json::json!({"key": "greeting", "unknown": true}),
        ),
        (
            "redis_json_set",
            serde_json::json!({
                "key": "doc:1",
                "value": {},
                "nx": true,
                "xx": true
            }),
        ),
        ("redis_json_mget", serde_json::json!({"keys": []})),
        (
            "redis_json_arrappend",
            serde_json::json!({"key": "doc:1", "path": "$.items", "values": []}),
        ),
        (
            "redis_json_arrpop",
            serde_json::json!({
                "key": "doc:1",
                "path": "$.items",
                "max_returned_bytes": 0
            }),
        ),
        (
            "redis_ft_search",
            serde_json::json!({"index": "idx", "query": "*", "limit_num": 101}),
        ),
        (
            "redis_ft_search",
            serde_json::json!({"index": "idx", "query": "*", "explainscore": true}),
        ),
        (
            "redis_ft_search",
            serde_json::json!({
                "index": "idx", "query": "@title:$term",
                "params": [{"name": "term", "value": "redis"}]
            }),
        ),
        (
            "redis_ft_aggregate",
            serde_json::json!({
                "index": "idx", "query": "*", "load_all": true, "load_fields": ["@title"]
            }),
        ),
        (
            "redis_ft_aggregate",
            serde_json::json!({
                "index": "idx", "query": "*",
                "stages": [{
                    "type": "group_by", "properties": [],
                    "reducers": [{"function": "count", "arguments": ["@title"]}]
                }]
            }),
        ),
        (
            "redis_ft_cursor_read",
            serde_json::json!({"index": "idx", "cursor_id": 0}),
        ),
        (
            "redis_ft_dictadd",
            serde_json::json!({"dict": "terms", "terms": []}),
        ),
        (
            "redis_ft_synupdate",
            serde_json::json!({"index": "idx", "group_id": "group", "terms": []}),
        ),
        (
            "redis_ft_create",
            serde_json::json!({"index": "idx", "schema": []}),
        ),
        (
            "redis_vector_set_hash",
            serde_json::json!({
                "key": "doc:1",
                "field": "embedding",
                "data_type": "FLOAT32",
                "vector": []
            }),
        ),
        (
            "redis_ft_vector_search",
            serde_json::json!({
                "index": "idx",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0],
                "top_k": 101
            }),
        ),
        (
            "redis_ft_create",
            serde_json::json!({
                "index": "idx",
                "schema": [{"name": "embedding", "field_type": "VECTOR"}]
            }),
        ),
        (
            "redis_ft_hybrid_search",
            serde_json::json!({
                "index": "idx",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0],
                "top_k": 1,
                "filters": [{"type": "text", "field": "bad-field", "value": "x"}]
            }),
        ),
        (
            "redis_set",
            serde_json::json!({
                "key": "greeting",
                "value": "hello",
                "expiration": {"type": "seconds", "value": 0}
            }),
        ),
        (
            "redis_set",
            serde_json::json!({
                "key": "greeting",
                "value": "hello",
                "expiration": {"type": "seconds", "value": 10, "milliseconds": 20}
            }),
        ),
        (
            "redis_set",
            serde_json::json!({
                "key": "greeting",
                "value": "hello",
                "get": true,
                "max_previous_bytes": 0
            }),
        ),
        (
            "redis_set",
            serde_json::json!({
                "key": "not-base64",
                "key_encoding": "base64",
                "value": "hello"
            }),
        ),
        (
            "redis_getrange",
            serde_json::json!({"key": "greeting", "start": 10, "end": 9}),
        ),
        (
            "redis_getrange",
            serde_json::json!({"key": "greeting", "start": 0, "end": 65536}),
        ),
        (
            "redis_setrange",
            serde_json::json!({"key": "greeting", "offset": 16777216, "value": "x"}),
        ),
        (
            "redis_dump",
            serde_json::json!({"key": "greeting", "max_bytes": 0}),
        ),
        (
            "redis_dump",
            serde_json::json!({"key": "greeting", "max_bytes": 2}),
        ),
        (
            "redis_restore",
            serde_json::json!({"key": "restored", "payload_base64": "not-base64"}),
        ),
        (
            "redis_restore",
            serde_json::json!({
                "key": "restored",
                "payload_base64": "A".repeat(349529)
            }),
        ),
        (
            "redis_restore",
            serde_json::json!({
                "key": "restored",
                "payload_base64": "AA==",
                "idle_time_seconds": 0
            }),
        ),
    ];

    for (name, arguments) in cases {
        let result = client
            .call_tool(name, arguments)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(result.is_error, "{name}");
    }
}

#[derive(Clone, Copy)]
struct BinaryRedis;

#[async_trait]
impl RedisExecutor for BinaryRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        Ok(match command.name() {
            "GET" | "HGET" => RedisValue::BulkString(vec![0xff, 0x00]),
            "MGET" => RedisValue::Array(vec![
                RedisValue::BulkString(vec![0xff, 0x00]),
                RedisValue::Nil,
            ]),
            "HGETALL" => RedisValue::Map(vec![(
                RedisValue::BulkString(vec![0xfe]),
                RedisValue::BulkString(vec![0xff]),
            )]),
            "HMGET" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xff]), RedisValue::Nil]),
            "HKEYS" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xfe])]),
            "HVALS" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xff])]),
            "EXISTS" => RedisValue::Integer(1),
            "LRANGE" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xff])]),
            "SMEMBERS" => RedisValue::Set(vec![RedisValue::BulkString(vec![0xff])]),
            "ZRANGE" => RedisValue::Array(vec![
                RedisValue::BulkString(vec![0xff]),
                RedisValue::BulkString(b"1.5".to_vec()),
            ]),
            _ => RedisValue::Nil,
        })
    }
}

#[derive(Clone, Default)]
struct ListContractRedis {
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

#[async_trait]
impl RedisExecutor for ListContractRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let key = command.arguments().first().map(Vec::as_slice);
        let response = match command.name() {
            "LPUSH" | "RPUSH" => RedisValue::Integer(2),
            "LLEN" if key == Some(b"missing".as_slice()) => RedisValue::Integer(0),
            "LLEN" => RedisValue::Integer(2),
            "LINDEX" if key == Some(b"missing".as_slice()) => RedisValue::Nil,
            "LINDEX" if key == Some(b"empty-value".as_slice()) => {
                RedisValue::BulkString(Vec::new())
            }
            "LINDEX" => RedisValue::BulkString(vec![0xff]),
            "LRANGE" if key == Some(b"missing".as_slice()) => RedisValue::Array(Vec::new()),
            "LRANGE" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xff])]),
            "LPOS" if key == Some(b"missing".as_slice()) => RedisValue::Array(Vec::new()),
            "LPOS" => RedisValue::Array(vec![RedisValue::Integer(1)]),
            "LPOP" | "RPOP" if key == Some(b"missing".as_slice()) => RedisValue::Nil,
            "LPOP" | "RPOP" => RedisValue::Array(vec![RedisValue::BulkString(vec![0xff])]),
            "LMOVE" if key == Some(b"missing".as_slice()) => RedisValue::Nil,
            "LMOVE" => RedisValue::BulkString(vec![0xff]),
            "LREM" => RedisValue::Integer(1),
            "LSET" | "LTRIM" => RedisValue::Okay,
            "EXISTS" if key == Some(b"missing".as_slice()) => RedisValue::Integer(0),
            "EXISTS" => RedisValue::Integer(1),
            _ => RedisValue::Nil,
        };
        self.commands
            .lock()
            .expect("list contract lock")
            .push(command);
        Ok(response)
    }
}

async fn list_contract_client(executor: ListContractRedis) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::DataStructures])
        .capabilities(RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 2, 0)))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect list contract client");
    client
        .initialize("redis-mcp-list-contract-test", "0")
        .await
        .expect("initialize list contract client");
    client
}

#[tokio::test]
async fn list_commands_preserve_binary_argv_and_native_ordering_options() {
    let executor = ListContractRedis::default();
    let commands = executor.commands.clone();
    let client = list_contract_client(executor).await;

    for tool in ["redis_lpush", "redis_rpush"] {
        let result = client
            .call_tool(
                tool,
                serde_json::json!({
                    "key": "/wA=",
                    "key_encoding": "base64",
                    "elements": [
                        {"value": "/g==", "value_encoding": "base64"},
                        "tail"
                    ]
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(!result.is_error, "{tool}: {result:?}");
    }
    client
        .call_tool(
            "redis_lindex",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64", "index": -1}),
        )
        .await
        .expect("binary LINDEX");
    client
        .call_tool(
            "redis_lpos",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "value": "/g==",
                "value_encoding": "base64",
                "rank": -1,
                "count": 2,
                "max_len": 10
            }),
        )
        .await
        .expect("binary LPOS");
    client
        .call_tool(
            "redis_lpop",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64", "count": 2}),
        )
        .await
        .expect("binary LPOP");
    client
        .call_tool(
            "redis_lrem",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "count": -1,
                "value": "/g==",
                "value_encoding": "base64"
            }),
        )
        .await
        .expect("binary LREM");
    client
        .call_tool(
            "redis_lset",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "index": -1,
                "value": "/g==",
                "value_encoding": "base64"
            }),
        )
        .await
        .expect("binary LSET");
    client
        .call_tool(
            "redis_ltrim",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64", "start": -2, "stop": -1}),
        )
        .await
        .expect("binary LTRIM");
    client
        .call_tool(
            "redis_lmove",
            serde_json::json!({
                "source": "/wA=",
                "source_encoding": "base64",
                "destination": "/Q==",
                "destination_encoding": "base64",
                "from": "right",
                "to": "left"
            }),
        )
        .await
        .expect("binary LMOVE");
    client
        .call_tool(
            "redis_rpop",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64", "count": 2}),
        )
        .await
        .expect("binary RPOP");

    let commands = commands.lock().expect("recorded list commands");
    let arguments = |tool: &str| {
        commands
            .iter()
            .find(|command| command.tool_name() == tool)
            .unwrap_or_else(|| panic!("missing {tool}"))
            .arguments()
    };
    for tool in ["redis_lpush", "redis_rpush"] {
        assert_eq!(
            arguments(tool),
            &[vec![0xff, 0x00], vec![0xfe], b"tail".to_vec()]
        );
    }
    assert_eq!(
        arguments("redis_lindex"),
        &[vec![0xff, 0x00], b"-1".to_vec()]
    );
    assert_eq!(
        arguments("redis_lpos"),
        &[
            vec![0xff, 0x00],
            vec![0xfe],
            b"RANK".to_vec(),
            b"-1".to_vec(),
            b"COUNT".to_vec(),
            b"2".to_vec(),
            b"MAXLEN".to_vec(),
            b"10".to_vec(),
        ]
    );
    assert_eq!(arguments("redis_lpop"), &[vec![0xff, 0x00], b"2".to_vec()]);
    assert_eq!(
        arguments("redis_lrem"),
        &[vec![0xff, 0x00], b"-1".to_vec(), vec![0xfe]]
    );
    assert_eq!(
        arguments("redis_lset"),
        &[vec![0xff, 0x00], b"-1".to_vec(), vec![0xfe]]
    );
    assert_eq!(
        arguments("redis_ltrim"),
        &[vec![0xff, 0x00], b"-2".to_vec(), b"-1".to_vec()]
    );
    assert_eq!(
        arguments("redis_lmove"),
        &[
            vec![0xff, 0x00],
            vec![0xfd],
            b"RIGHT".to_vec(),
            b"LEFT".to_vec(),
        ]
    );
    assert_eq!(arguments("redis_rpop"), &[vec![0xff, 0x00], b"2".to_vec()]);
}

#[tokio::test]
async fn list_reads_and_pops_distinguish_binary_empty_and_missing_results() {
    let client = list_contract_client(ListContractRedis::default()).await;

    let binary = client
        .call_tool(
            "redis_lindex",
            serde_json::json!({"key": "present", "index": -1}),
        )
        .await
        .expect("binary LINDEX")
        .structured_content
        .expect("structured binary LINDEX");
    assert_eq!(binary["list_exists"], true);
    assert_eq!(binary["element_exists"], true);
    assert_eq!(binary["value"], "/w==");
    assert_eq!(binary["encoding"], "base64");

    let empty = client
        .call_tool(
            "redis_lindex",
            serde_json::json!({"key": "empty-value", "index": 0}),
        )
        .await
        .expect("empty-value LINDEX")
        .structured_content
        .expect("structured empty-value LINDEX");
    assert_eq!(empty["element_exists"], true);
    assert_eq!(empty["value"], "");
    assert_eq!(empty["encoding"], "utf8");

    let missing = client
        .call_tool(
            "redis_lindex",
            serde_json::json!({"key": "missing", "index": 0}),
        )
        .await
        .expect("missing LINDEX")
        .structured_content
        .expect("structured missing LINDEX");
    assert_eq!(missing["list_exists"], false);
    assert_eq!(missing["element_exists"], false);
    assert_eq!(missing["value"], serde_json::Value::Null);

    let range = client
        .call_tool("redis_lrange", serde_json::json!({"key": "missing"}))
        .await
        .expect("missing LRANGE")
        .structured_content
        .expect("structured missing LRANGE");
    assert_eq!(range["exists"], false);
    assert_eq!(range["elements"], serde_json::json!([]));

    let positions = client
        .call_tool(
            "redis_lpos",
            serde_json::json!({"key": "missing", "value": "needle"}),
        )
        .await
        .expect("missing LPOS")
        .structured_content
        .expect("structured missing LPOS");
    assert_eq!(positions["exists"], false);
    assert_eq!(positions["positions"], serde_json::json!([]));

    for tool in ["redis_lpop", "redis_rpop"] {
        let popped = client
            .call_tool(tool, serde_json::json!({"key": "missing", "count": 2}))
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: no structured content"));
        assert_eq!(popped["found"], false, "{tool}");
        assert_eq!(popped["popped"], 0, "{tool}");
        assert_eq!(popped["elements"], serde_json::json!([]), "{tool}");
    }

    let moved = client
        .call_tool(
            "redis_lmove",
            serde_json::json!({
                "source": "missing",
                "destination": "archive",
                "from": "left",
                "to": "right"
            }),
        )
        .await
        .expect("missing LMOVE")
        .structured_content
        .expect("structured missing LMOVE");
    assert_eq!(moved["moved"], false);
    assert_eq!(moved["value"], serde_json::Value::Null);
}

#[derive(Clone, Default)]
struct SetContractRedis {
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

#[async_trait]
impl RedisExecutor for SetContractRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let key = command.arguments().first().map(Vec::as_slice);
        let response = match command.name() {
            "SCARD" if key == Some(b"missing".as_slice()) => RedisValue::Integer(0),
            "SCARD" => RedisValue::Integer(2),
            "SISMEMBER" => RedisValue::Integer(
                (key != Some(b"missing".as_slice())
                    && command.arguments().get(1).map(Vec::as_slice) != Some(b"missing".as_slice()))
                    as i64,
            ),
            "SMISMEMBER" => RedisValue::Array(
                command
                    .arguments()
                    .iter()
                    .skip(1)
                    .map(|member| {
                        RedisValue::Integer(
                            (key != Some(b"missing".as_slice()) && member.as_slice() != b"missing")
                                as i64,
                        )
                    })
                    .collect(),
            ),
            "SMEMBERS" if key == Some(b"missing".as_slice()) => RedisValue::Set(Vec::new()),
            "SMEMBERS" => RedisValue::Set(vec![
                RedisValue::BulkString(b"zeta".to_vec()),
                RedisValue::BulkString(vec![0xff]),
                RedisValue::BulkString(b"alpha".to_vec()),
            ]),
            "SSCAN" if key == Some(b"missing".as_slice()) => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(Vec::new()),
            ]),
            "SSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"zeta".to_vec()),
                    RedisValue::BulkString(b"alpha".to_vec()),
                ]),
            ]),
            "SDIFF" | "SINTER" | "SUNION" if key == Some(b"missing".as_slice()) => {
                RedisValue::Set(Vec::new())
            }
            "SDIFF" | "SINTER" | "SUNION" => RedisValue::Set(vec![
                RedisValue::BulkString(b"zeta".to_vec()),
                RedisValue::BulkString(vec![0xff]),
                RedisValue::BulkString(b"alpha".to_vec()),
            ]),
            "SDIFFCARD" | "SUNIONCARD" => RedisValue::Integer(2),
            "SDIFFSTORE" | "SINTERSTORE" | "SUNIONSTORE" => RedisValue::Integer(2),
            "SADD" | "SREM" => RedisValue::Integer(1),
            "EXISTS" if key == Some(b"missing".as_slice()) => RedisValue::Integer(0),
            "EXISTS" => RedisValue::Integer(1),
            _ => RedisValue::Nil,
        };
        self.commands
            .lock()
            .expect("set contract lock")
            .push(command);
        Ok(response)
    }
}

async fn set_contract_client(executor: SetContractRedis) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::DataStructures])
        .capabilities(RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 10, 0)))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect set contract client");
    client
        .initialize("redis-mcp-set-contract-test", "0")
        .await
        .expect("initialize set contract client");
    client
}

#[tokio::test]
async fn set_commands_preserve_binary_argv_and_ordered_membership_contracts() {
    let executor = SetContractRedis::default();
    let commands = executor.commands.clone();
    let client = set_contract_client(executor).await;

    for tool in ["redis_sadd", "redis_srem"] {
        let result = client
            .call_tool(
                tool,
                serde_json::json!({
                    "key": "/wA=",
                    "key_encoding": "base64",
                    "members": [
                        {"member": "/g==", "member_encoding": "base64"},
                        "tail"
                    ]
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(!result.is_error, "{tool}: {result:?}");
    }
    client
        .call_tool(
            "redis_scard",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64"}),
        )
        .await
        .expect("binary SCARD");
    client
        .call_tool(
            "redis_sismember",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "member": "/g==",
                "member_encoding": "base64"
            }),
        )
        .await
        .expect("binary SISMEMBER");
    let multiple = client
        .call_tool(
            "redis_smismember",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "members": [
                    {"member": "/g==", "member_encoding": "base64"},
                    "missing"
                ]
            }),
        )
        .await
        .expect("binary SMISMEMBER")
        .structured_content
        .expect("structured SMISMEMBER");
    assert_eq!(multiple["members"][0]["member"], "/g==");
    assert_eq!(multiple["members"][0]["member_encoding"], "base64");
    assert_eq!(multiple["members"][0]["is_member"], true);
    assert_eq!(multiple["members"][1]["member"], "missing");
    assert_eq!(multiple["members"][1]["is_member"], false);

    client
        .call_tool(
            "redis_smembers",
            serde_json::json!({"key": "/wA=", "key_encoding": "base64"}),
        )
        .await
        .expect("binary SMEMBERS");
    client
        .call_tool(
            "redis_sscan",
            serde_json::json!({
                "key": "/wA=",
                "key_encoding": "base64",
                "cursor": 5,
                "pattern": "a*",
                "count": 2
            }),
        )
        .await
        .expect("binary SSCAN");
    for tool in ["redis_sdiff", "redis_sinter", "redis_sunion"] {
        let result = client
            .call_tool(
                tool,
                serde_json::json!({
                    "keys": [
                        {"key": "/wA=", "key_encoding": "base64"},
                        {"key": "/Q==", "key_encoding": "base64"}
                    ]
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(result["ordering"], "byte_sorted", "{tool}");
        assert_eq!(result["members"][0]["value"], "alpha", "{tool}");
        assert_eq!(result["members"][2]["encoding"], "base64", "{tool}");
    }

    let commands = commands.lock().expect("recorded set commands");
    let arguments = |tool: &str| {
        commands
            .iter()
            .find(|command| command.tool_name() == tool)
            .unwrap_or_else(|| panic!("missing {tool}"))
            .arguments()
    };
    for tool in ["redis_sadd", "redis_srem"] {
        assert_eq!(
            arguments(tool),
            &[vec![0xff, 0x00], vec![0xfe], b"tail".to_vec()]
        );
    }
    assert_eq!(arguments("redis_scard"), &[vec![0xff, 0x00]]);
    assert_eq!(
        arguments("redis_sismember"),
        &[vec![0xff, 0x00], vec![0xfe]]
    );
    assert_eq!(
        arguments("redis_smismember"),
        &[vec![0xff, 0x00], vec![0xfe], b"missing".to_vec()]
    );
    assert_eq!(arguments("redis_smembers"), &[vec![0xff, 0x00]]);
    assert_eq!(
        arguments("redis_sscan"),
        &[
            vec![0xff, 0x00],
            b"5".to_vec(),
            b"MATCH".to_vec(),
            b"a*".to_vec(),
            b"COUNT".to_vec(),
            b"2".to_vec(),
        ]
    );
    for tool in ["redis_sdiff", "redis_sinter", "redis_sunion"] {
        assert_eq!(arguments(tool), &[vec![0xff, 0x00], vec![0xfd]]);
    }
}

#[tokio::test]
async fn set_cardinality_and_store_tools_preserve_bounded_binary_contracts() {
    let executor = SetContractRedis::default();
    let commands = executor.commands.clone();
    let client = set_contract_client(executor).await;
    let keys = serde_json::json!([
        {"key": "/wA=", "key_encoding": "base64"},
        {"key": "/Q==", "key_encoding": "base64"}
    ]);

    let difference = client
        .call_tool(
            "redis_sdiffcard",
            serde_json::json!({"keys": keys.clone(), "limit": 2}),
        )
        .await
        .expect("binary SDIFFCARD")
        .structured_content
        .expect("structured SDIFFCARD");
    assert_eq!(difference["operation"], "set_difference");
    assert_eq!(difference["cardinality"], 2);
    assert_eq!(difference["limit_reached"], true);
    assert!(difference.get("members").is_none());

    let union = client
        .call_tool(
            "redis_sunioncard",
            serde_json::json!({"keys": keys.clone(), "approximate": true, "limit": 3}),
        )
        .await
        .expect("binary SUNIONCARD")
        .structured_content
        .expect("structured SUNIONCARD");
    assert_eq!(union["operation"], "set_union");
    assert_eq!(union["approximate"], true);
    assert_eq!(union["limit_reached"], false);

    for tool in ["redis_sdiffstore", "redis_sinterstore", "redis_sunionstore"] {
        let stored = client
            .call_tool(
                tool,
                serde_json::json!({
                    "destination": "/g==",
                    "destination_encoding": "base64",
                    "keys": keys.clone()
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(stored["destination"], "/g==", "{tool}");
        assert_eq!(stored["destination_cardinality"], 2, "{tool}");
        assert_eq!(stored["destination_overwritten"], true, "{tool}");
        assert!(stored.get("members").is_none(), "{tool}");
    }

    let commands = commands.lock().expect("recorded set cardinality commands");
    let arguments = |tool: &str| {
        commands
            .iter()
            .find(|command| command.tool_name() == tool)
            .unwrap_or_else(|| panic!("missing {tool}"))
            .arguments()
    };
    assert_eq!(
        arguments("redis_sdiffcard"),
        &[
            b"2".to_vec(),
            vec![0xff, 0x00],
            vec![0xfd],
            b"LIMIT".to_vec(),
            b"2".to_vec()
        ]
    );
    assert_eq!(
        arguments("redis_sunioncard"),
        &[
            b"2".to_vec(),
            vec![0xff, 0x00],
            vec![0xfd],
            b"APPROX".to_vec(),
            b"LIMIT".to_vec(),
            b"3".to_vec()
        ]
    );
    for tool in ["redis_sdiffstore", "redis_sinterstore", "redis_sunionstore"] {
        assert_eq!(
            arguments(tool),
            &[vec![0xfe], vec![0xff, 0x00], vec![0xfd]],
            "{tool}"
        );
    }
}

#[tokio::test]
async fn set_reads_distinguish_missing_sets_and_empty_algebra_results() {
    let client = set_contract_client(SetContractRedis::default()).await;

    let cardinality = client
        .call_tool("redis_scard", serde_json::json!({"key": "missing"}))
        .await
        .expect("missing SCARD")
        .structured_content
        .expect("structured missing SCARD");
    assert_eq!(cardinality["exists"], false);
    assert_eq!(cardinality["cardinality"], 0);

    let one = client
        .call_tool(
            "redis_sismember",
            serde_json::json!({"key": "missing", "member": "missing"}),
        )
        .await
        .expect("missing SISMEMBER")
        .structured_content
        .expect("structured missing SISMEMBER");
    assert_eq!(one["set_exists"], false);
    assert_eq!(one["is_member"], false);

    let multiple = client
        .call_tool(
            "redis_smismember",
            serde_json::json!({"key": "missing", "members": ["missing", "missing"]}),
        )
        .await
        .expect("missing SMISMEMBER")
        .structured_content
        .expect("structured missing SMISMEMBER");
    assert_eq!(multiple["set_exists"], false);
    assert_eq!(multiple["count"], 2);
    assert!(
        multiple["members"]
            .as_array()
            .expect("membership array")
            .iter()
            .all(|member| member["is_member"] == false)
    );

    for tool in ["redis_smembers", "redis_sscan"] {
        let result = client
            .call_tool(tool, serde_json::json!({"key": "missing"}))
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(result["exists"], false, "{tool}");
        assert_eq!(result["members"], serde_json::json!([]), "{tool}");
    }

    for tool in ["redis_sdiff", "redis_sinter", "redis_sunion"] {
        let result = client
            .call_tool(tool, serde_json::json!({"keys": ["missing"]}))
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(result["count"], 0, "{tool}");
        assert_eq!(result["members"], serde_json::json!([]), "{tool}");
    }
}

#[derive(Clone, Default)]
struct SortedSetContractRedis {
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

#[async_trait]
impl RedisExecutor for SortedSetContractRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let key = command.arguments().first().map(Vec::as_slice);
        let missing_key = key == Some(b"missing".as_slice());
        let response = match command.name() {
            "ZCARD" if missing_key => RedisValue::Integer(0),
            "ZCARD" => RedisValue::Integer(3),
            "ZCOUNT" if missing_key => RedisValue::Integer(0),
            "ZCOUNT" => RedisValue::Integer(2),
            "ZINTERCARD" => RedisValue::Integer(2),
            "ZSCORE"
                if missing_key
                    || command.arguments().get(1).map(Vec::as_slice)
                        == Some(b"missing".as_slice()) =>
            {
                RedisValue::Nil
            }
            "ZSCORE" => RedisValue::BulkString(b"0.10000000000000001".to_vec()),
            "ZMSCORE" => RedisValue::Array(
                command
                    .arguments()
                    .iter()
                    .skip(1)
                    .map(|member| {
                        if missing_key || member.as_slice() == b"missing" {
                            RedisValue::Nil
                        } else {
                            RedisValue::BulkString(b"0.10000000000000001".to_vec())
                        }
                    })
                    .collect(),
            ),
            "ZRANK" | "ZREVRANK"
                if missing_key
                    || command.arguments().get(1).map(Vec::as_slice)
                        == Some(b"missing".as_slice()) =>
            {
                RedisValue::Nil
            }
            "ZRANK" => RedisValue::Integer(1),
            "ZREVRANK" => RedisValue::Integer(2),
            "ZRANGE" if command.arguments().iter().any(|arg| arg == b"WITHSCORES") => {
                RedisValue::Array(vec![
                    RedisValue::BulkString(vec![0xfe]),
                    RedisValue::BulkString(b"0.10000000000000001".to_vec()),
                    RedisValue::BulkString(b"middle".to_vec()),
                    RedisValue::BulkString(b"1.5".to_vec()),
                    RedisValue::BulkString(b"tail".to_vec()),
                    RedisValue::BulkString(b"2".to_vec()),
                ])
            }
            "ZRANGE" if missing_key => RedisValue::Array(Vec::new()),
            "ZRANGE" => RedisValue::Array(vec![
                RedisValue::BulkString(vec![0xfe]),
                RedisValue::BulkString(b"middle".to_vec()),
                RedisValue::BulkString(b"tail".to_vec()),
            ]),
            "ZSCAN" if missing_key => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(Vec::new()),
            ]),
            "ZSCAN" => RedisValue::Array(vec![
                RedisValue::BulkString(b"0".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(vec![0xfe]),
                    RedisValue::BulkString(b"0.10000000000000001".to_vec()),
                ]),
            ]),
            "ZADD" | "ZREM" | "ZREMRANGEBYSCORE" => RedisValue::Integer(1),
            "ZDIFFSTORE" | "ZINTERSTORE" | "ZRANGESTORE" | "ZUNIONSTORE" => RedisValue::Integer(2),
            "ZINCRBY" => RedisValue::BulkString(b"0.30000000000000002".to_vec()),
            "ZPOPMIN" | "ZPOPMAX" => RedisValue::Array(vec![
                RedisValue::BulkString(vec![0xfe]),
                RedisValue::BulkString(b"0.10000000000000001".to_vec()),
            ]),
            "EXISTS" if missing_key => RedisValue::Integer(0),
            "EXISTS" => RedisValue::Integer(1),
            _ => RedisValue::Nil,
        };
        self.commands
            .lock()
            .expect("sorted-set contract lock")
            .push(command);
        Ok(response)
    }
}

async fn sorted_set_contract_client(executor: SortedSetContractRedis) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(AccessMode::Full)
        .bundles([ToolBundle::DataStructures])
        .capabilities(RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 8, 0)))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect sorted-set contract client");
    client
        .initialize("redis-mcp-sorted-set-contract-test", "0")
        .await
        .expect("initialize sorted-set contract client");
    client
}

#[tokio::test]
async fn sorted_set_commands_preserve_binary_argv_exact_scores_and_range_modes() {
    let executor = SortedSetContractRedis::default();
    let commands = executor.commands.clone();
    let client = sorted_set_contract_client(executor).await;
    let binary_key = serde_json::json!({"key": "/wA=", "key_encoding": "base64"});

    client
        .call_tool("redis_zcard", binary_key.clone())
        .await
        .expect("binary ZCARD");
    client
        .call_tool(
            "redis_zcount",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64",
                "min": {"kind": "exclusive", "value": "0.10000000000000001"},
                "max": {"kind": "positive_infinity"}
            }),
        )
        .await
        .expect("binary ZCOUNT");
    let score = client
        .call_tool(
            "redis_zscore",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64",
                "member": "/g==", "member_encoding": "base64"
            }),
        )
        .await
        .expect("binary ZSCORE")
        .structured_content
        .expect("structured ZSCORE");
    assert_eq!(score["score"], "0.10000000000000001");
    let scores = client
        .call_tool(
            "redis_zmscore",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64",
                "members": [{"member": "/g==", "member_encoding": "base64"}, "missing"]
            }),
        )
        .await
        .expect("binary ZMSCORE")
        .structured_content
        .expect("structured ZMSCORE");
    assert_eq!(scores["members"][0]["score"], "0.10000000000000001");
    assert_eq!(scores["members"][1]["score"], serde_json::Value::Null);
    for tool in ["redis_zrank", "redis_zrevrank"] {
        client
            .call_tool(
                tool,
                serde_json::json!({
                    "key": "/wA=", "key_encoding": "base64",
                    "member": "/g==", "member_encoding": "base64"
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
    }
    let rank_page = client
        .call_tool(
            "redis_zrange",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64", "withscores": true,
                "range": {"kind": "rank", "start": 0, "stop": 1}
            }),
        )
        .await
        .expect("rank ZRANGE")
        .structured_content
        .expect("structured rank ZRANGE");
    assert_eq!(rank_page["count"], 2);
    assert_eq!(rank_page["page"]["continuation"]["start"], 2);
    assert_eq!(rank_page["members"][0]["encoding"], "base64");
    assert_eq!(rank_page["members"][0]["score"], "0.10000000000000001");

    let score_page = client
        .call_tool(
            "redis_zrange",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64", "rev": true,
                "range": {
                    "kind": "score",
                    "min": {"kind": "negative_infinity"},
                    "max": {"kind": "exclusive", "value": "1.5"},
                    "offset": 2,
                    "limit": 2
                }
            }),
        )
        .await
        .expect("score ZRANGE")
        .structured_content
        .expect("structured score ZRANGE");
    assert_eq!(score_page["page"]["continuation"]["offset"], 4);

    client
        .call_tool(
            "redis_zrange",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64",
                "range": {
                    "kind": "lex",
                    "min": {"kind": "inclusive", "value": "/g==", "encoding": "base64"},
                    "max": {"kind": "positive_infinity"},
                    "limit": 2
                }
            }),
        )
        .await
        .expect("lex ZRANGE");
    client
        .call_tool(
            "redis_zscan",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64", "cursor": 5,
                "pattern": "*", "count": 2
            }),
        )
        .await
        .expect("binary ZSCAN");
    client
        .call_tool(
            "redis_zadd",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64",
                "members": [{
                    "score": "0.10000000000000001",
                    "member": {"member": "/g==", "member_encoding": "base64"}
                }]
            }),
        )
        .await
        .expect("binary ZADD");
    client
        .call_tool(
            "redis_zincrby",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64",
                "member": "/g==", "member_encoding": "base64",
                "increment": "0.20000000000000001"
            }),
        )
        .await
        .expect("binary ZINCRBY");
    client
        .call_tool(
            "redis_zrem",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64",
                "members": [{"member": "/g==", "member_encoding": "base64"}]
            }),
        )
        .await
        .expect("binary ZREM");
    for tool in ["redis_zpopmin", "redis_zpopmax"] {
        client
            .call_tool(
                tool,
                serde_json::json!({"key": "/wA=", "key_encoding": "base64", "count": 2}),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
    }
    client
        .call_tool(
            "redis_zremrangebyscore",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64",
                "min": {"kind": "exclusive", "value": "-2"},
                "max": {"kind": "inclusive", "value": "3"}
            }),
        )
        .await
        .expect("binary ZREMRANGEBYSCORE");

    let commands = commands.lock().expect("recorded sorted-set commands");
    let arguments = |tool: &str, occurrence: usize| {
        commands
            .iter()
            .filter(|command| command.tool_name() == tool)
            .nth(occurrence)
            .unwrap_or_else(|| panic!("missing {tool} occurrence {occurrence}"))
            .arguments()
    };
    assert_eq!(arguments("redis_zcard", 0), &[vec![0xff, 0x00]]);
    assert_eq!(
        arguments("redis_zcount", 0),
        &[
            vec![0xff, 0x00],
            b"(0.10000000000000001".to_vec(),
            b"+inf".to_vec()
        ]
    );
    assert_eq!(
        arguments("redis_zscore", 0),
        &[vec![0xff, 0x00], vec![0xfe]]
    );
    assert_eq!(
        arguments("redis_zmscore", 0),
        &[vec![0xff, 0x00], vec![0xfe], b"missing".to_vec()]
    );
    assert_eq!(arguments("redis_zrank", 0), &[vec![0xff, 0x00], vec![0xfe]]);
    assert_eq!(
        arguments("redis_zrevrank", 0),
        &[vec![0xff, 0x00], vec![0xfe]]
    );
    assert_eq!(
        arguments("redis_zrange", 0),
        &[
            vec![0xff, 0x00],
            b"0".to_vec(),
            b"2".to_vec(),
            b"WITHSCORES".to_vec()
        ]
    );
    assert_eq!(
        arguments("redis_zrange", 1),
        &[
            vec![0xff, 0x00],
            b"(1.5".to_vec(),
            b"-inf".to_vec(),
            b"BYSCORE".to_vec(),
            b"LIMIT".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
            b"REV".to_vec()
        ]
    );
    assert_eq!(
        arguments("redis_zrange", 2),
        &[
            vec![0xff, 0x00],
            vec![b'[', 0xfe],
            b"+".to_vec(),
            b"BYLEX".to_vec(),
            b"LIMIT".to_vec(),
            b"0".to_vec(),
            b"3".to_vec()
        ]
    );
    assert_eq!(
        arguments("redis_zscan", 0),
        &[
            vec![0xff, 0x00],
            b"5".to_vec(),
            b"MATCH".to_vec(),
            b"*".to_vec(),
            b"COUNT".to_vec(),
            b"2".to_vec()
        ]
    );
    assert_eq!(
        arguments("redis_zadd", 0),
        &[
            vec![0xff, 0x00],
            b"0.10000000000000001".to_vec(),
            vec![0xfe]
        ]
    );
    assert_eq!(
        arguments("redis_zincrby", 0),
        &[
            vec![0xff, 0x00],
            b"0.20000000000000001".to_vec(),
            vec![0xfe]
        ]
    );
    assert_eq!(arguments("redis_zrem", 0), &[vec![0xff, 0x00], vec![0xfe]]);
    for tool in ["redis_zpopmin", "redis_zpopmax"] {
        assert_eq!(arguments(tool, 0), &[vec![0xff, 0x00], b"2".to_vec()]);
    }
    assert_eq!(
        arguments("redis_zremrangebyscore", 0),
        &[vec![0xff, 0x00], b"(-2".to_vec(), b"3".to_vec()]
    );
}

#[tokio::test]
async fn sorted_set_cardinality_and_store_tools_preserve_typed_argv() {
    let executor = SortedSetContractRedis::default();
    let commands = executor.commands.clone();
    let client = sorted_set_contract_client(executor).await;
    let binary_keys = serde_json::json!([
        {"key": "/wA=", "key_encoding": "base64"},
        {"key": "/Q==", "key_encoding": "base64"}
    ]);

    let cardinality = client
        .call_tool(
            "redis_zintercard",
            serde_json::json!({"keys": binary_keys.clone(), "limit": 2}),
        )
        .await
        .expect("binary ZINTERCARD")
        .structured_content
        .expect("structured ZINTERCARD");
    assert_eq!(cardinality["operation"], "sorted_set_intersection");
    assert_eq!(cardinality["cardinality"], 2);
    assert!(cardinality.get("members").is_none());

    let difference = client
        .call_tool(
            "redis_zdiffstore",
            serde_json::json!({
                "destination": "/g==",
                "destination_encoding": "base64",
                "keys": binary_keys.clone()
            }),
        )
        .await
        .expect("binary ZDIFFSTORE")
        .structured_content
        .expect("structured ZDIFFSTORE");
    assert_eq!(difference["operation"], "difference");
    assert_eq!(difference["destination_cardinality"], 2);

    let intersection = client
        .call_tool(
            "redis_zinterstore",
            serde_json::json!({
                "destination": "/g==",
                "destination_encoding": "base64",
                "sources": [
                    {"key": "/wA=", "key_encoding": "base64", "weight": "0.10000000000000001"},
                    {"key": "/Q==", "key_encoding": "base64"}
                ],
                "aggregate": "max"
            }),
        )
        .await
        .expect("binary weighted ZINTERSTORE")
        .structured_content
        .expect("structured ZINTERSTORE");
    assert_eq!(intersection["weighted"], true);
    assert_eq!(intersection["aggregate"], "max");
    assert!(intersection.get("members").is_none());

    client
        .call_tool(
            "redis_zunionstore",
            serde_json::json!({
                "destination": "out:{tenant}",
                "sources": ["left:{tenant}", "right:{tenant}"],
                "aggregate": "count"
            }),
        )
        .await
        .expect("COUNT ZUNIONSTORE");

    let range = client
        .call_tool(
            "redis_zrangestore",
            serde_json::json!({
                "destination": "/g==",
                "destination_encoding": "base64",
                "source": "/wA=",
                "source_encoding": "base64",
                "rev": true,
                "range": {
                    "kind": "score",
                    "min": {"kind": "negative_infinity"},
                    "max": {"kind": "exclusive", "value": "1.5"},
                    "offset": 2,
                    "limit": 2
                }
            }),
        )
        .await
        .expect("bounded ZRANGESTORE")
        .structured_content
        .expect("structured ZRANGESTORE");
    assert_eq!(range["requested_maximum"], 2);
    assert_eq!(range["destination_cardinality"], 2);

    let commands = commands.lock().expect("recorded sorted-set store commands");
    let arguments = |tool: &str| {
        commands
            .iter()
            .find(|command| command.tool_name() == tool)
            .unwrap_or_else(|| panic!("missing {tool}"))
            .arguments()
    };
    assert_eq!(
        arguments("redis_zintercard"),
        &[
            b"2".to_vec(),
            vec![0xff, 0x00],
            vec![0xfd],
            b"LIMIT".to_vec(),
            b"2".to_vec()
        ]
    );
    assert_eq!(
        arguments("redis_zdiffstore"),
        &[vec![0xfe], b"2".to_vec(), vec![0xff, 0x00], vec![0xfd]]
    );
    assert_eq!(
        arguments("redis_zinterstore"),
        &[
            vec![0xfe],
            b"2".to_vec(),
            vec![0xff, 0x00],
            vec![0xfd],
            b"WEIGHTS".to_vec(),
            b"0.10000000000000001".to_vec(),
            b"1".to_vec(),
            b"AGGREGATE".to_vec(),
            b"MAX".to_vec()
        ]
    );
    assert_eq!(
        arguments("redis_zunionstore"),
        &[
            b"out:{tenant}".to_vec(),
            b"2".to_vec(),
            b"left:{tenant}".to_vec(),
            b"right:{tenant}".to_vec(),
            b"AGGREGATE".to_vec(),
            b"COUNT".to_vec()
        ]
    );
    assert_eq!(
        arguments("redis_zrangestore"),
        &[
            vec![0xfe],
            vec![0xff, 0x00],
            b"(1.5".to_vec(),
            b"-inf".to_vec(),
            b"BYSCORE".to_vec(),
            b"LIMIT".to_vec(),
            b"2".to_vec(),
            b"2".to_vec(),
            b"REV".to_vec()
        ]
    );
}

#[tokio::test]
async fn sorted_set_reads_distinguish_missing_keys_members_and_nil_scores() {
    let client = sorted_set_contract_client(SortedSetContractRedis::default()).await;
    let card = client
        .call_tool("redis_zcard", serde_json::json!({"key": "missing"}))
        .await
        .expect("missing ZCARD")
        .structured_content
        .expect("structured missing ZCARD");
    assert_eq!(card["exists"], false);
    assert_eq!(card["cardinality"], 0);

    for tool in ["redis_zscore", "redis_zrank", "redis_zrevrank"] {
        let result = client
            .call_tool(
                tool,
                serde_json::json!({"key": "missing", "member": "missing"}),
            )
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(result["zset_exists"], false, "{tool}");
        assert_eq!(result["member_exists"], false, "{tool}");
    }
    let scores = client
        .call_tool(
            "redis_zmscore",
            serde_json::json!({"key": "missing", "members": ["missing", "missing"]}),
        )
        .await
        .expect("missing ZMSCORE")
        .structured_content
        .expect("structured missing ZMSCORE");
    assert_eq!(scores["zset_exists"], false);
    assert_eq!(scores["members"][0]["score"], serde_json::Value::Null);

    for tool in ["redis_zrange", "redis_zscan"] {
        let result = client
            .call_tool(tool, serde_json::json!({"key": "missing"}))
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{tool}: structured result"));
        assert_eq!(result["exists"], false, "{tool}");
        assert_eq!(result["members"], serde_json::json!([]), "{tool}");
    }
}

#[derive(Clone, Copy)]
struct HashEdgeRedis;

#[async_trait]
impl RedisExecutor for HashEdgeRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let key = command.arguments().first().map(Vec::as_slice);
        Ok(match command.name() {
            "HGET"
                if key == Some(b"empty".as_slice())
                    && command.arguments().get(1).map(Vec::as_slice)
                        == Some(b"present".as_slice()) =>
            {
                RedisValue::BulkString(Vec::new())
            }
            "HGET" => RedisValue::Nil,
            "HMGET" if key == Some(b"empty".as_slice()) => {
                RedisValue::Array(vec![RedisValue::BulkString(Vec::new()), RedisValue::Nil])
            }
            "HMGET" => RedisValue::Array(vec![RedisValue::Nil, RedisValue::Nil]),
            "HSTRLEN" => RedisValue::Integer(0),
            "HEXISTS"
                if key == Some(b"empty".as_slice())
                    && command.arguments().get(1).map(Vec::as_slice)
                        == Some(b"present".as_slice()) =>
            {
                RedisValue::Integer(1)
            }
            "HEXISTS" => RedisValue::Integer(0),
            "EXISTS" if key == Some(b"missing-hash".as_slice()) => RedisValue::Integer(0),
            "EXISTS" => RedisValue::Integer(1),
            _ => RedisValue::Nil,
        })
    }
}

#[tokio::test]
async fn hash_reads_distinguish_empty_missing_field_and_missing_hash() {
    let router = RedisMcp::builder(HashEdgeRedis)
        .access(AccessMode::ReadOnly)
        .bundles([ToolBundle::DataStructures])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect hash edge client");
    client
        .initialize("redis-mcp-hash-edge-test", "0")
        .await
        .expect("initialize hash edge client");

    let empty = client
        .call_tool(
            "redis_hget",
            serde_json::json!({"key": "empty", "field": "present"}),
        )
        .await
        .expect("empty HGET")
        .structured_content
        .expect("structured empty HGET");
    assert_eq!(empty["hash_exists"], true);
    assert_eq!(empty["field_exists"], true);
    assert_eq!(empty["value"], "");
    assert_eq!(empty["encoding"], "utf8");

    let missing_field = client
        .call_tool(
            "redis_hget",
            serde_json::json!({"key": "empty", "field": "missing"}),
        )
        .await
        .expect("missing field HGET")
        .structured_content
        .expect("structured missing field HGET");
    assert_eq!(missing_field["hash_exists"], true);
    assert_eq!(missing_field["field_exists"], false);
    assert_eq!(missing_field["value"], serde_json::Value::Null);

    let missing_hash = client
        .call_tool(
            "redis_hget",
            serde_json::json!({"key": "missing-hash", "field": "missing"}),
        )
        .await
        .expect("missing hash HGET")
        .structured_content
        .expect("structured missing hash HGET");
    assert_eq!(missing_hash["hash_exists"], false);
    assert_eq!(missing_hash["field_exists"], false);

    let multiple = client
        .call_tool(
            "redis_hmget",
            serde_json::json!({"key": "empty", "fields": ["present", "missing"]}),
        )
        .await
        .expect("edge HMGET")
        .structured_content
        .expect("structured edge HMGET");
    assert_eq!(multiple["hash_exists"], true);
    assert_eq!(multiple["values"][0]["exists"], true);
    assert_eq!(multiple["values"][0]["value"], "");
    assert_eq!(multiple["values"][1]["exists"], false);

    let length = client
        .call_tool(
            "redis_hstrlen",
            serde_json::json!({"key": "empty", "field": "present"}),
        )
        .await
        .expect("empty HSTRLEN")
        .structured_content
        .expect("structured empty HSTRLEN");
    assert_eq!(length["length_bytes"], 0);
    assert_eq!(length["field_exists"], true);
    assert_eq!(length["hash_exists"], true);
}

#[tokio::test]
async fn binary_and_nil_values_are_explicit_across_curated_reads() {
    let router = RedisMcp::builder(BinaryRedis)
        .access(AccessMode::ReadOnly)
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect binary client");
    client
        .initialize("redis-mcp-binary-test", "0")
        .await
        .expect("initialize binary client");

    let get = client
        .call_tool("redis_get", serde_json::json!({"key": "binary"}))
        .await
        .expect("binary get")
        .structured_content
        .expect("structured get");
    assert_eq!(get["encoding"], "base64");
    assert_eq!(get["value"], "/wA=");

    let mget = client
        .call_tool(
            "redis_mget",
            serde_json::json!({"keys": ["binary", "missing"]}),
        )
        .await
        .expect("binary mget")
        .structured_content
        .expect("structured mget");
    assert_eq!(mget["values"][0]["encoding"], "base64");
    assert_eq!(mget["values"][1]["exists"], false);
    assert_eq!(mget["values"][1]["value"], serde_json::Value::Null);

    for (name, arguments, path) in [
        (
            "redis_hget",
            serde_json::json!({"key": "hash", "field": "field"}),
            "/encoding",
        ),
        (
            "redis_hgetall",
            serde_json::json!({"key": "hash"}),
            "/entries/0/field_encoding",
        ),
        (
            "redis_hmget",
            serde_json::json!({"key": "hash", "fields": ["field", "missing"]}),
            "/values/0/value_encoding",
        ),
        (
            "redis_hkeys",
            serde_json::json!({"key": "hash"}),
            "/fields/0/encoding",
        ),
        (
            "redis_hvals",
            serde_json::json!({"key": "hash"}),
            "/values/0/encoding",
        ),
        (
            "redis_lrange",
            serde_json::json!({"key": "list"}),
            "/elements/0/encoding",
        ),
        (
            "redis_smembers",
            serde_json::json!({"key": "set"}),
            "/members/0/encoding",
        ),
        (
            "redis_zrange",
            serde_json::json!({"key": "zset", "withscores": true}),
            "/members/0/encoding",
        ),
    ] {
        let structured = client
            .call_tool(name, arguments)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"))
            .structured_content
            .unwrap_or_else(|| panic!("{name}: no structured content"));
        assert_eq!(structured.pointer(path), Some(&serde_json::json!("base64")));
    }
}

#[tokio::test]
async fn encoded_budget_is_measured_after_binary_base64_expansion() {
    let generous_router = RedisMcp::builder(BinaryRedis)
        .output_budget(OutputBudget::new(1_000_000, 1_000))
        .build();
    let generous = McpClient::connect(ChannelTransport::new(generous_router))
        .await
        .expect("connect generous binary client");
    generous
        .initialize("redis-mcp-binary-budget-test", "0")
        .await
        .expect("initialize generous binary client");
    let baseline = generous
        .call_tool("redis_get", serde_json::json!({"key": "binary"}))
        .await
        .expect("baseline binary GET");
    assert_eq!(
        baseline.structured_content.as_ref().unwrap()["value"],
        "/wA="
    );
    let encoded_bytes = serde_json::to_vec(&baseline)
        .expect("serialize binary GET")
        .len();

    let limited_router = RedisMcp::builder(BinaryRedis)
        .output_budget(OutputBudget::new(encoded_bytes - 1, 1_000))
        .build();
    let limited = McpClient::connect(ChannelTransport::new(limited_router))
        .await
        .expect("connect limited binary client");
    limited
        .initialize("redis-mcp-binary-budget-test", "0")
        .await
        .expect("initialize limited binary client");
    let result = limited
        .call_tool("redis_get", serde_json::json!({"key": "binary"}))
        .await
        .expect("over-limit binary GET");
    assert_output_limit(&result, "encoded_bytes", encoded_bytes, encoded_bytes - 1);
}

#[tokio::test]
async fn classified_raw_commands_fail_closed() {
    let client = client(AccessMode::Full, true).await;
    let result = client
        .call_tool(
            "redis_command",
            serde_json::json!({"command": "NEW.MODULE.COMMAND", "arguments": []}),
        )
        .await
        .expect("tool errors are returned as MCP results");
    assert!(result.is_error);
    assert!(
        serde_json::to_string(&result)
            .expect("serialize result")
            .contains("not classified")
    );
}

#[tokio::test]
async fn unrestricted_raw_policy_allows_unknown_names_but_keeps_hard_blocks() {
    let client = client_for_bundles(AccessMode::Full, [], RawCommandPolicy::Unrestricted).await;
    let unknown = client
        .call_tool(
            "redis_command",
            serde_json::json!({"command": "NEW.MODULE.COMMAND", "arguments": []}),
        )
        .await
        .expect("unknown command reaches unrestricted executor");
    assert!(!unknown.is_error);

    let blocked = client
        .call_tool(
            "redis_command",
            serde_json::json!({"command": "AUTH", "arguments": ["secret"]}),
        )
        .await
        .expect("hard block is represented as an MCP result");
    assert!(blocked.is_error);
    assert!(
        serde_json::to_string(&blocked)
            .expect("serialize blocked result")
            .contains("SESSION_COMMAND_UNSUPPORTED")
    );
}

#[derive(Clone, Copy)]
struct MissingModulesRedis;

#[async_trait]
impl RedisExecutor for MissingModulesRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        assert_eq!(command.required_module(), Some(RedisModule::Json));
        Err(RedisError::new(
            redis_mcp::RedisErrorKind::Server,
            "ERR unknown command 'JSON.GET', with args beginning with: 'secret-key'",
        ))
    }
}

#[tokio::test]
async fn module_absence_is_stable_for_custom_executors() {
    let router = RedisMcp::builder(MissingModulesRedis)
        .bundles([ToolBundle::Json])
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect missing-module client");
    client
        .initialize("redis-mcp-missing-module-test", "0")
        .await
        .expect("initialize missing-module client");

    let result = client
        .call_tool("redis_json_get", serde_json::json!({"key": "secret-key"}))
        .await
        .expect("module error is a tool result");
    let text = serde_json::to_string(&result).expect("serialize module error");
    assert!(result.is_error);
    assert!(text.contains("ModuleUnavailable"));
    assert!(text.contains("RedisJSON"));
    assert!(!text.contains("secret-key"));
}

#[tokio::test]
async fn unknown_capabilities_preserve_custom_executor_compatibility() {
    let client = capability_client(RedisCapabilities::unknown(), UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list unknown-capability tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(names.iter().any(|name| name == "redis_json_get"));
    assert!(names.iter().any(|name| name == "redis_memory_usage"));

    let result = client
        .call_tool("redis_ping", serde_json::json!({}))
        .await
        .expect("unknown capabilities allow execution");
    assert!(!result.is_error);
}

#[tokio::test]
async fn known_old_redis_can_hide_only_version_incompatible_tools() {
    let old = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(3, 2, 12));
    let client = capability_client(old.clone(), UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list old Redis tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "redis_memory_usage"));
    assert!(!names.iter().any(|name| name == "redis_object_inspect"));
    assert!(!names.iter().any(|name| name == "redis_unlink"));
    assert!(!names.iter().any(|name| name == "redis_copy"));
    assert!(!names.iter().any(|name| name == "redis_getdel"));
    assert!(!names.iter().any(|name| name == "redis_restore"));
    assert!(names.iter().any(|name| name == "redis_get"));
    assert!(names.iter().any(|name| name == "redis_dump"));
    assert!(names.iter().any(|name| name == "redis_touch"));
    assert!(names.iter().any(|name| name == "redis_lindex"));
    for name in ["redis_lpos", "redis_lpop", "redis_lmove", "redis_rpop"] {
        assert!(!names.iter().any(|candidate| candidate == name), "{name}");
    }
    assert!(!names.iter().any(|name| name == "redis_smismember"));
    for name in ["redis_zpopmax", "redis_zpopmin"] {
        assert!(!names.iter().any(|candidate| candidate == name), "{name}");
    }

    let helper_names = tool_names_for_capabilities(
        AccessMode::Full,
        ToolBundle::ALL.iter().copied(),
        false,
        &old,
        UnavailableToolPolicy::Hide,
    );
    assert!(!helper_names.contains(&"redis_memory_usage"));

    let new = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(4, 0, 0));
    let new_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::Essentials],
        false,
        &new,
        UnavailableToolPolicy::Hide,
    );
    assert!(new_names.contains(&"redis_memory_usage"));
    assert!(new_names.contains(&"redis_object_inspect"));
    assert!(new_names.contains(&"redis_unlink"));
    assert!(!new_names.contains(&"redis_restore"));

    let supported = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 2, 0));
    let redis_six = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 0, 0));
    let redis_six_list_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::DataStructures],
        false,
        &redis_six,
        UnavailableToolPolicy::Hide,
    );
    assert!(redis_six_list_names.contains(&"redis_lpos"));
    for name in ["redis_lpop", "redis_lmove", "redis_rpop"] {
        assert!(!redis_six_list_names.contains(&name), "{name}");
    }
    assert!(!redis_six_list_names.contains(&"redis_smismember"));
    for name in ["redis_zadd", "redis_zmscore", "redis_zrange"] {
        assert!(!redis_six_list_names.contains(&name), "{name}");
    }
    for name in ["redis_zpopmax", "redis_zpopmin"] {
        assert!(redis_six_list_names.contains(&name), "{name}");
    }
    let supported_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::Essentials],
        false,
        &supported,
        UnavailableToolPolicy::Hide,
    );
    for name in [
        "redis_copy",
        "redis_getdel",
        "redis_getex",
        "redis_restore",
        "redis_restore_replace",
    ] {
        assert!(supported_names.contains(&name), "{name}");
    }
    let supported_list_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::DataStructures],
        false,
        &supported,
        UnavailableToolPolicy::Hide,
    );
    for name in [
        "redis_lpos",
        "redis_lpop",
        "redis_lmove",
        "redis_rpop",
        "redis_smismember",
        "redis_zadd",
        "redis_zmscore",
        "redis_zrange",
        "redis_zpopmax",
        "redis_zpopmin",
    ] {
        assert!(supported_list_names.contains(&name), "{name}");
    }

    let redis_six_session_names = tool_names_for_capabilities(
        AccessMode::ReadOnly,
        [ToolBundle::Sessions],
        false,
        &supported,
        UnavailableToolPolicy::Hide,
    );
    for name in [
        "redis_subscribe",
        "redis_psubscribe",
        "redis_pubsub_read",
        "redis_pubsub_unsubscribe",
        "redis_pubsub_close",
    ] {
        assert!(redis_six_session_names.contains(&name), "{name}");
    }
    assert!(!redis_six_session_names.contains(&"redis_ssubscribe"));

    let pre_field_expiration =
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 2, 0));
    let pre_field_expiration_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::DataStructures],
        false,
        &pre_field_expiration,
        UnavailableToolPolicy::Hide,
    );
    for name in [
        "redis_hexpire",
        "redis_hexpire_delete",
        "redis_hpersist",
        "redis_httl",
    ] {
        assert!(!pre_field_expiration_names.contains(&name), "{name}");
    }
    assert!(pre_field_expiration_names.contains(&"redis_hstrlen"));

    let field_expiration =
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 4, 0));
    let field_expiration_names = tool_names_for_capabilities(
        AccessMode::Full,
        [ToolBundle::DataStructures],
        false,
        &field_expiration,
        UnavailableToolPolicy::Hide,
    );
    for name in [
        "redis_hexpire",
        "redis_hexpire_delete",
        "redis_hpersist",
        "redis_httl",
    ] {
        assert!(field_expiration_names.contains(&name), "{name}");
    }
}

#[tokio::test]
async fn known_missing_modules_commands_and_module_versions_filter_precisely() {
    let missing_modules = RedisCapabilities::unknown().with_module_inventory([]);
    let client = capability_client(missing_modules, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list missing-module tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name.starts_with("redis_json_")));
    assert!(!names.iter().any(|name| name.starts_with("redis_ft_")));
    assert!(!names.iter().any(|name| name.starts_with("redis_vector_")));

    let missing_get = RedisCapabilities::unknown().with_command_inventory(["MGET"]);
    let client = capability_client(missing_get, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list command-filtered tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "redis_get"));
    assert!(names.iter().any(|name| name == "redis_mget"));

    let legacy_json = RedisCapabilities::unknown().with_module(
        RedisModule::Json,
        RedisModuleCapability::available(Some(RedisVersion::new(1, 0, 0))),
    );
    let client = capability_client(legacy_json, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list legacy RedisJSON tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name.starts_with("redis_json_")));

    let pre_merge_json = RedisCapabilities::unknown().with_module(
        RedisModule::Json,
        RedisModuleCapability::available(Some(RedisVersion::new(2, 4, 0))),
    );
    let client = capability_client(pre_merge_json, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list pre-merge RedisJSON tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(names.iter().any(|name| name == "redis_json_arrappend"));
    assert!(!names.iter().any(|name| name == "redis_json_merge"));

    let old_search = RedisCapabilities::unknown().with_module(
        RedisModule::Search,
        RedisModuleCapability::available(Some(RedisVersion::new(1, 8, 0))),
    );
    let client = capability_client(old_search, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list old Search tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "redis_ft_list"));
    assert!(names.iter().any(|name| name == "redis_ft_search"));
    assert!(!names.iter().any(|name| name == "redis_ft_vector_search"));
    assert!(!names.iter().any(|name| name == "redis_ft_hybrid_search"));
    assert!(!names.iter().any(|name| name == "redis_vector_get_hash"));
    assert!(!names.iter().any(|name| name == "redis_vector_set_hash"));

    let pre_vector_search = RedisCapabilities::unknown().with_module(
        RedisModule::Search,
        RedisModuleCapability::available(Some(RedisVersion::new(2, 2, 0))),
    );
    let client = capability_client(pre_vector_search, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list pre-vector Search tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(names.iter().any(|name| name == "redis_ft_list"));
    assert!(names.iter().any(|name| name == "redis_ft_search"));
    assert!(!names.iter().any(|name| name == "redis_ft_vector_search"));

    let pre_dialect_search = RedisCapabilities::unknown().with_module(
        RedisModule::Search,
        RedisModuleCapability::available(Some(RedisVersion::new(2, 4, 2))),
    );
    let client = capability_client(pre_dialect_search, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool(
            "redis_ft_search",
            serde_json::json!({"index": "idx", "query": "*", "dialect": 2}),
        )
        .await
        .expect("conditional Search dialect version result");
    assert!(result.is_error);
    assert!(format!("{result:?}").contains("2.4.3"));

    let pre_profile_search = RedisCapabilities::unknown().with_module(
        RedisModule::Search,
        RedisModuleCapability::available(Some(RedisVersion::new(2, 0, 0))),
    );
    let client = capability_client(pre_profile_search, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list pre-profile Search tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "redis_ft_profile"));
    assert!(names.iter().any(|name| name == "redis_ft_aggregate"));
}

#[tokio::test]
async fn known_cluster_mode_hides_tools_with_unimplemented_cluster_wide_semantics() {
    let cluster = RedisCapabilities::unknown().with_deployment(RedisDeployment::Cluster);
    let client = capability_client(cluster, UnavailableToolPolicy::Hide).await;
    let names = client
        .list_tools()
        .await
        .expect("list cluster-compatible tools")
        .tools
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    for standalone_only in [
        "redis_info",
        "redis_dbsize",
        "redis_scan",
        "redis_randomkey",
        "redis_ft_list",
    ] {
        assert!(!names.iter().any(|name| name == standalone_only));
    }
    assert!(names.iter().any(|name| name == "redis_get"));
    assert!(names.iter().any(|name| name == "redis_mget"));
    assert!(names.iter().any(|name| name == "redis_ft_vector_search"));
    assert!(names.iter().any(|name| name == "redis_ft_hybrid_search"));
}

#[tokio::test]
async fn advertised_capability_failures_have_stable_categories_and_codes() {
    let old = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(3, 2, 12));
    let client = capability_client(old, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool(
            "redis_memory_usage",
            serde_json::json!({"key": "never-sent"}),
        )
        .await
        .expect("version failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize version failure");
    assert!(result.is_error);
    assert!(text.contains("CapabilityUnavailable"));
    assert!(text.contains("REDIS_VERSION_UNAVAILABLE"));

    let pre_field_expiration =
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(7, 2, 0));
    let client = capability_client(pre_field_expiration, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool(
            "redis_hexpire",
            serde_json::json!({"key": "never-sent", "seconds": 60, "fields": ["field"]}),
        )
        .await
        .expect("hash field expiration version failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize hash version error");
    assert!(result.is_error);
    assert!(text.contains("CapabilityUnavailable"));
    assert!(text.contains("REDIS_VERSION_UNAVAILABLE"));
    assert!(!text.contains("never-sent"));

    let missing_json = RedisCapabilities::unknown()
        .with_module(RedisModule::Json, RedisModuleCapability::unavailable());
    let client = capability_client(missing_json, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool("redis_json_get", serde_json::json!({"key": "never-sent"}))
        .await
        .expect("module failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize module failure");
    assert!(result.is_error);
    assert!(text.contains("ModuleUnavailable"));
    assert!(text.contains("MODULE_UNAVAILABLE"));

    let old_search = RedisCapabilities::unknown().with_module(
        RedisModule::Search,
        RedisModuleCapability::available(Some(RedisVersion::new(1, 8, 0))),
    );
    let client = capability_client(old_search, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool("redis_ft_list", serde_json::json!({}))
        .await
        .expect("module version failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize module version failure");
    assert!(result.is_error);
    assert!(text.contains("ModuleUnavailable"));
    assert!(text.contains("MODULE_VERSION_UNAVAILABLE"));

    let pre_vector_search = RedisCapabilities::unknown().with_module(
        RedisModule::Search,
        RedisModuleCapability::available(Some(RedisVersion::new(2, 2, 0))),
    );
    let client = capability_client(pre_vector_search, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool(
            "redis_ft_vector_search",
            serde_json::json!({
                "index": "idx",
                "vector_field": "embedding",
                "data_type": "FLOAT32",
                "vector": [1.0],
                "top_k": 1,
                "limit_num": 1
            }),
        )
        .await
        .expect("vector module version failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize vector version failure");
    assert!(result.is_error);
    assert!(text.contains("MODULE_VERSION_UNAVAILABLE"));

    let missing_command =
        RedisCapabilities::unknown().with_command("PING", CapabilityStatus::Unavailable);
    let client = capability_client(missing_command, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool("redis_ping", serde_json::json!({}))
        .await
        .expect("command failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize command failure");
    assert!(result.is_error);
    assert!(text.contains("CapabilityUnavailable"));
    assert!(text.contains("COMMAND_UNAVAILABLE"));

    let cluster = RedisCapabilities::unknown().with_deployment(RedisDeployment::Cluster);
    let client = capability_client(cluster, UnavailableToolPolicy::Advertise).await;
    let result = client
        .call_tool("redis_dbsize", serde_json::json!({}))
        .await
        .expect("deployment failure is a tool result");
    let text = serde_json::to_string(&result).expect("serialize deployment failure");
    assert!(result.is_error);
    assert!(text.contains("CapabilityUnavailable"));
    assert!(text.contains("DEPLOYMENT_UNAVAILABLE"));
}

#[derive(Clone, Copy)]
struct SlowRedis;

#[async_trait]
impl RedisExecutor for SlowRedis {
    async fn execute(&self, _command: RedisCommand) -> Result<RedisValue, RedisError> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(RedisValue::SimpleString("PONG".into()))
    }
}

#[tokio::test]
async fn executor_futures_are_bounded_by_the_library_timeout() {
    let router = RedisMcp::builder(SlowRedis)
        .command_timeout(Duration::from_millis(5))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect timeout client");
    client
        .initialize("redis-mcp-timeout-test", "0")
        .await
        .expect("initialize timeout client");

    let result = client
        .call_tool("redis_ping", serde_json::json!({}))
        .await
        .expect("timeout is represented as a tool result");
    assert!(result.is_error);
    assert!(
        serde_json::to_string(&result)
            .expect("serialize result")
            .contains("timed out")
    );
}

#[tokio::test]
async fn stream_commands_preserve_binary_argv_and_explicit_bounds() {
    let capabilities = RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 2, 0));

    let executor = FixedRedis::new(RedisValue::BulkString(b"9-1".to_vec()));
    let commands = executor.commands.clone();
    let client = fixed_client(executor, capabilities.clone()).await;
    let added = client
        .call_tool(
            "redis_xadd",
            serde_json::json!({
                "key": "/wA=", "key_encoding": "base64",
                "id": {"type": "explicit", "id": {"milliseconds": 9, "sequence": 1}},
                "no_mkstream": true,
                "trim": {"type": "max_len", "threshold": 50, "approximate": true, "limit": 5},
                "fields": [{
                    "field": "/g==", "field_encoding": "base64",
                    "value": "/Q==", "value_encoding": "base64"
                }]
            }),
        )
        .await
        .expect("binary XADD");
    assert!(!added.is_error, "{added:?}");
    assert_eq!(
        commands.lock().expect("XADD commands")[0].arguments(),
        &[
            vec![0xff, 0x00],
            b"NOMKSTREAM".to_vec(),
            b"MAXLEN".to_vec(),
            b"~".to_vec(),
            b"50".to_vec(),
            b"LIMIT".to_vec(),
            b"5".to_vec(),
            b"9-1".to_vec(),
            vec![0xfe],
            vec![0xfd],
        ]
    );

    let executor = FixedRedis::new(RedisValue::Nil);
    let commands = executor.commands.clone();
    let client = fixed_client(executor, capabilities.clone()).await;
    let read = client
        .call_tool(
            "redis_xread",
            serde_json::json!({
                "streams": [
                    {"key": "/w==", "key_encoding": "base64", "offset": {"type": "latest"}},
                    {"key": "events{slot}", "offset": {"type": "explicit", "id": {"milliseconds": 3, "sequence": 2}}}
                ],
                "count": 7,
                "block_ms": 25
            }),
        )
        .await
        .expect("bounded binary XREAD");
    assert!(!read.is_error, "{read:?}");
    assert_eq!(read.structured_content.unwrap()["timed_out"], true);
    assert_eq!(
        commands.lock().expect("XREAD commands")[0].arguments(),
        &[
            b"COUNT".to_vec(),
            b"7".to_vec(),
            b"BLOCK".to_vec(),
            b"25".to_vec(),
            b"STREAMS".to_vec(),
            vec![0xff],
            b"events{slot}".to_vec(),
            b"$".to_vec(),
            b"3-2".to_vec(),
        ]
    );

    let executor = FixedRedis::new(RedisValue::Array(vec![RedisValue::BulkString(
        b"1-0".to_vec(),
    )]));
    let commands = executor.commands.clone();
    let client = fixed_client(executor, capabilities).await;
    let claimed = client
        .call_tool(
            "redis_xclaim",
            serde_json::json!({
                "key": "events", "group": {"value": "/g==", "encoding": "base64"},
                "consumer": {"value": "/Q==", "encoding": "base64"},
                "min_idle_time_ms": 1000,
                "ids": [{"milliseconds": 1, "sequence": 0}],
                "idle_ms": 25,
                "retry_count": 3,
                "force": true,
                "just_id": true
            }),
        )
        .await
        .expect("binary XCLAIM");
    assert!(!claimed.is_error, "{claimed:?}");
    assert_eq!(
        commands.lock().expect("XCLAIM commands")[0].arguments(),
        &[
            b"events".to_vec(),
            vec![0xfe],
            vec![0xfd],
            b"1000".to_vec(),
            b"1-0".to_vec(),
            b"IDLE".to_vec(),
            b"25".to_vec(),
            b"RETRYCOUNT".to_vec(),
            b"3".to_vec(),
            b"FORCE".to_vec(),
            b"JUSTID".to_vec(),
        ]
    );
}

#[tokio::test]
async fn stream_bounds_and_versioned_forms_fail_before_execution() {
    let executor = FixedRedis::new(RedisValue::Nil);
    let commands = executor.commands.clone();
    let client = fixed_client(
        executor,
        RedisCapabilities::unknown().with_redis_version(RedisVersion::new(5, 0, 0)),
    )
    .await;
    for (tool, arguments, expected) in [
        (
            "redis_xrange",
            serde_json::json!({
                "key": "events",
                "min": {"type": "exclusive", "id": {"milliseconds": 1, "sequence": 0}}
            }),
            "Redis 6.2",
        ),
        (
            "redis_xadd",
            serde_json::json!({
                "key": "events", "no_mkstream": true,
                "fields": [{"field": "event", "value": "created"}]
            }),
            "Redis 6.2",
        ),
        (
            "redis_xtrim",
            serde_json::json!({
                "key": "events",
                "trim": {"type": "min_id", "threshold": {"milliseconds": 1, "sequence": 0}}
            }),
            "Redis 6.2",
        ),
    ] {
        let result = client
            .call_tool(tool, arguments)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(result.is_error, "{tool}: {result:?}");
        assert!(
            serde_json::to_string(&result).unwrap().contains(expected),
            "{tool}: {result:?}"
        );
    }
    assert!(commands.lock().expect("version-gated commands").is_empty());

    let executor = FixedRedis::new(RedisValue::Nil);
    let commands = executor.commands.clone();
    let client = fixed_client(executor, RedisCapabilities::unknown()).await;
    for (tool, arguments) in [
        (
            "redis_xread",
            serde_json::json!({
                "streams": [{"key": "events", "offset": {"type": "latest"}}],
                "block_ms": 0
            }),
        ),
        (
            "redis_xreadgroup",
            serde_json::json!({
                "group": {"value": "workers"}, "consumer": {"value": "one"},
                "streams": [{"key": "events", "offset": {"type": "new"}}],
                "block_ms": 30000
            }),
        ),
        (
            "redis_xautoclaim",
            serde_json::json!({
                "key": "events", "group": {"value": "workers"},
                "consumer": {"value": "one"}, "min_idle_time_ms": 0,
                "start": {"milliseconds": 0, "sequence": 0}, "count": 101
            }),
        ),
    ] {
        let result = client
            .call_tool(tool, arguments)
            .await
            .unwrap_or_else(|error| panic!("{tool}: {error}"));
        assert!(result.is_error, "{tool}: {result:?}");
    }
    assert!(commands.lock().expect("bounded stream commands").is_empty());
}

#[tokio::test]
async fn stream_annotations_match_access_and_destructive_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated stream tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };
    for name in [
        "redis_xlen",
        "redis_xrange",
        "redis_xrevrange",
        "redis_xread",
        "redis_xinfo_stream",
        "redis_xinfo_groups",
        "redis_xinfo_consumers",
        "redis_xpending",
    ] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }
    for (name, idempotent) in [
        ("redis_xadd", false),
        ("redis_xgroup_create", false),
        ("redis_xgroup_setid", true),
        ("redis_xgroup_createconsumer", true),
        ("redis_xreadgroup", false),
        ("redis_xack", true),
        ("redis_xclaim", false),
        ("redis_xautoclaim", false),
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert_eq!(annotation.idempotent_hint, idempotent, "{name}");
    }
    for name in [
        "redis_xdel",
        "redis_xtrim",
        "redis_xgroup_destroy",
        "redis_xgroup_delconsumer",
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }
}

#[tokio::test]
async fn redis_json_annotations_match_access_and_destructive_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated RedisJSON tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in [
        "redis_json_get",
        "redis_json_type",
        "redis_json_mget",
        "redis_json_strlen",
        "redis_json_objkeys",
        "redis_json_objlen",
        "redis_json_arrlen",
    ] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }
    for (name, idempotent) in [
        ("redis_json_set", true),
        ("redis_json_numincrby", false),
        ("redis_json_toggle", false),
        ("redis_json_arrappend", false),
        ("redis_json_arrinsert", false),
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert_eq!(annotation.idempotent_hint, idempotent, "{name}");
    }
    for (name, idempotent) in [
        ("redis_json_del", true),
        ("redis_json_clear", true),
        ("redis_json_arrpop", false),
        ("redis_json_arrtrim", false),
        ("redis_json_merge", true),
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
        assert_eq!(annotation.idempotent_hint, idempotent, "{name}");
    }
}

#[tokio::test]
async fn search_annotations_match_data_and_cursor_semantics() {
    let tools = full_catalog_client()
        .await
        .list_tools()
        .await
        .expect("list annotated Search tools")
        .tools;
    let annotations = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("missing annotations for {name}"))
    };

    for name in [
        "redis_ft_list",
        "redis_ft_info",
        "redis_ft_search",
        "redis_vector_get_hash",
        "redis_ft_vector_search",
        "redis_ft_hybrid_search",
        "redis_ft_explain",
        "redis_ft_profile",
        "redis_ft_tagvals",
        "redis_ft_dictdump",
        "redis_ft_syndump",
    ] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(annotation.idempotent_hint, "{name}");
    }
    for name in ["redis_ft_aggregate", "redis_ft_cursor_read"] {
        let annotation = annotations(name);
        assert!(annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
        assert!(!annotation.idempotent_hint, "{name}");
    }
    for name in [
        "redis_ft_create",
        "redis_vector_set_hash",
        "redis_ft_alter",
        "redis_ft_synupdate",
        "redis_ft_dictadd",
        "redis_ft_aliasadd",
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(!annotation.destructive_hint, "{name}");
    }
    for name in [
        "redis_ft_cursor_del",
        "redis_ft_dropindex",
        "redis_ft_aliasupdate",
        "redis_ft_aliasdel",
        "redis_ft_dictdel",
    ] {
        let annotation = annotations(name);
        assert!(!annotation.read_only_hint, "{name}");
        assert!(annotation.destructive_hint, "{name}");
    }
}

#[derive(Clone, Default)]
struct ScriptingRedis {
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

#[async_trait]
impl RedisExecutor for ScriptingRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let response = match command.name() {
            "EVAL" | "EVAL_RO" | "EVALSHA" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" => {
                RedisValue::BulkString(vec![0xff, 0x00])
            }
            "SCRIPT" => match command.arguments().first().map(Vec::as_slice) {
                Some(b"EXISTS") => RedisValue::Array(vec![RedisValue::Integer(1)]),
                Some(b"LOAD") => {
                    RedisValue::BulkString(b"0123456789abcdef0123456789abcdef01234567".to_vec())
                }
                _ => RedisValue::Okay,
            },
            "FUNCTION" => match command.arguments().first().map(Vec::as_slice) {
                Some(b"DUMP") => RedisValue::BulkString(vec![0xff, 0x00, 0x01]),
                Some(b"LIST") => RedisValue::Array(vec![RedisValue::Map(vec![(
                    RedisValue::BulkString(b"library_name".to_vec()),
                    RedisValue::BulkString(b"agents".to_vec()),
                )])]),
                Some(b"STATS") => RedisValue::Map(vec![(
                    RedisValue::BulkString(b"engines".to_vec()),
                    RedisValue::Map(Vec::new()),
                )]),
                Some(b"LOAD") => RedisValue::BulkString(b"agents".to_vec()),
                _ => RedisValue::Okay,
            },
            _ => RedisValue::Nil,
        };
        self.commands
            .lock()
            .expect("scripting command lock")
            .push(command);
        Ok(response)
    }
}

async fn scripting_client(executor: impl RedisExecutor, access: AccessMode) -> McpClient {
    let router = RedisMcp::builder(executor)
        .access(access)
        .bundles([ToolBundle::Scripting])
        .capabilities(
            RedisCapabilities::unknown()
                .with_redis_version(RedisVersion::new(8, 2, 0))
                .with_deployment(RedisDeployment::Cluster),
        )
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect scripting contract client");
    client
        .initialize("redis-mcp-scripting-contract-test", "0")
        .await
        .expect("initialize scripting contract client");
    client
}

#[tokio::test]
async fn scripting_family_access_annotations_and_binary_argv_are_explicit() {
    let read_only = scripting_client(ScriptingRedis::default(), AccessMode::ReadOnly).await;
    let listed = read_only
        .list_tools()
        .await
        .expect("list read-only scripting tools");
    let names = listed
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "redis_eval_ro",
            "redis_evalsha_ro",
            "redis_fcall_ro",
            "redis_script_exists"
        ]
    );
    assert!(listed.tools.iter().all(|tool| {
        tool.annotations
            .as_ref()
            .is_some_and(|annotations| annotations.read_only_hint)
    }));

    let executor = ScriptingRedis::default();
    let commands = executor.commands.clone();
    let full = scripting_client(executor, AccessMode::Full).await;
    let listed = full.list_tools().await.expect("list full scripting tools");
    assert_eq!(listed.tools.len(), 18);
    for name in ["redis_eval", "redis_fcall", "redis_function_flush"] {
        let annotations = listed
            .tools
            .iter()
            .find(|tool| tool.name == name)
            .and_then(|tool| tool.annotations.as_ref())
            .unwrap_or_else(|| panic!("missing annotations for {name}"));
        assert!(!annotations.read_only_hint, "{name}");
        assert!(annotations.destructive_hint, "{name}");
    }

    let eval = full
        .call_tool(
            "redis_eval_ro",
            serde_json::json!({
                "script": {"value": "cmV0dXJuIEFSR1ZbMV0=", "encoding": "base64"},
                "keys": [{"value": "a2V5Ont0ZW5hbnR9", "encoding": "base64"}],
                "arguments": [{"value": "/wA=", "encoding": "base64"}]
            }),
        )
        .await
        .expect("call binary EVAL_RO");
    assert!(!eval.is_error, "{eval:?}");
    assert_eq!(
        eval.structured_content.as_ref().unwrap()["result"]["encoding"],
        "base64"
    );

    let fcall = full
        .call_tool(
            "redis_fcall",
            serde_json::json!({
                "function": "lookup",
                "keys": [{"value": "key:{tenant}"}],
                "arguments": [{"value": "AAE=", "encoding": "base64"}]
            }),
        )
        .await
        .expect("call binary FCALL");
    assert!(!fcall.is_error, "{fcall:?}");

    let script_load = full
        .call_tool(
            "redis_script_load",
            serde_json::json!({"script": {"value": "return 1"}, "max_cluster_nodes": 7}),
        )
        .await
        .expect("call SCRIPT LOAD");
    assert!(!script_load.is_error, "{script_load:?}");

    let function_restore = full
        .call_tool(
            "redis_function_restore",
            serde_json::json!({
                "payload": {"value": "/wAB", "encoding": "base64"},
                "policy": "replace",
                "max_cluster_nodes": 5
            }),
        )
        .await
        .expect("call FUNCTION RESTORE");
    assert!(!function_restore.is_error, "{function_restore:?}");

    let commands = commands.lock().expect("record scripting commands");
    let command = |tool_name: &str| {
        commands
            .iter()
            .find(|command| command.tool_name() == tool_name)
            .unwrap_or_else(|| panic!("missing {tool_name}"))
    };
    assert_eq!(
        command("redis_eval_ro").arguments(),
        &[
            b"return ARGV[1]".to_vec(),
            b"1".to_vec(),
            b"key:{tenant}".to_vec(),
            vec![0xff, 0x00]
        ]
    );
    assert_eq!(
        command("redis_fcall").arguments(),
        &[
            b"lookup".to_vec(),
            b"1".to_vec(),
            b"key:{tenant}".to_vec(),
            vec![0x00, 0x01]
        ]
    );
    assert_eq!(
        command("redis_script_load").cluster_fanout(),
        Some(RedisClusterFanout::AllNodes)
    );
    assert_eq!(command("redis_script_load").cluster_node_limit(), Some(7));
    assert_eq!(
        command("redis_function_restore").cluster_fanout(),
        Some(RedisClusterFanout::Primaries)
    );
    assert_eq!(
        command("redis_function_restore").arguments(),
        &[
            b"RESTORE".to_vec(),
            vec![0xff, 0x00, 0x01],
            b"REPLACE".to_vec()
        ]
    );
}

#[tokio::test]
async fn scripting_inputs_and_outputs_fail_closed_at_declared_bounds() {
    let executor = ScriptingRedis::default();
    let commands = executor.commands.clone();
    let client = scripting_client(executor, AccessMode::Full).await;

    for (name, input) in [
        (
            "redis_evalsha_ro",
            serde_json::json!({"sha1": "not-a-sha1"}),
        ),
        (
            "redis_script_exists",
            serde_json::json!({"sha1": [], "max_cluster_nodes": 32}),
        ),
        (
            "redis_script_load",
            serde_json::json!({"script": {"value": "return 1"}, "max_cluster_nodes": 0}),
        ),
        (
            "redis_function_list",
            serde_json::json!({"library_name": "not a library"}),
        ),
        ("redis_function_dump", serde_json::json!({"max_bytes": 2})),
    ] {
        let result = client
            .call_tool(name, input)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(result.is_error, "{name}: {result:?}");
    }
    assert_eq!(
        commands.lock().expect("bounded scripting commands").len(),
        1,
        "only FUNCTION DUMP should execute before its response-size check"
    );
}

#[tokio::test]
async fn scripting_results_obey_the_global_encoded_output_budget() {
    let router = RedisMcp::builder(FixedRedis::new(RedisValue::BulkString(vec![b'x'; 8_192])))
        .access(AccessMode::Full)
        .bundles([ToolBundle::Scripting])
        .capabilities(RedisCapabilities::unknown().with_redis_version(RedisVersion::new(8, 2, 0)))
        .output_budget(OutputBudget::new(1_024, 100))
        .build();
    let client = McpClient::connect(ChannelTransport::new(router))
        .await
        .expect("connect scripting output-budget client");
    client
        .initialize("redis-mcp-scripting-output-budget-test", "0")
        .await
        .expect("initialize scripting output-budget client");

    let result = client
        .call_tool(
            "redis_eval_ro",
            serde_json::json!({"script": {"value": "return string.rep('x', 8192)"}}),
        )
        .await
        .expect("large scripting result is a tool result");
    assert!(result.is_error, "{result:?}");
    assert_eq!(
        result.meta.as_ref().expect("output-limit metadata")["io.redis.mcp/outputLimit"]["code"],
        "output_limit_exceeded"
    );
}

#[tokio::test]
async fn scripting_cluster_partial_failures_are_structured_and_messages_redacted() {
    let executor = FixedRedis::new(RedisValue::ClusterNodes(vec![
        ("10.0.0.1:6379".to_string(), RedisValue::Okay),
        (
            "10.0.0.2:6379".to_string(),
            RedisValue::ServerError {
                code: "NOPERM".to_string(),
                message: Some("secret ACL detail".to_string()),
            },
        ),
    ]));
    let client = scripting_client(executor, AccessMode::Full).await;
    let result = client
        .call_tool(
            "redis_function_flush",
            serde_json::json!({"mode": "sync", "max_cluster_nodes": 4}),
        )
        .await
        .expect("call partial FUNCTION FLUSH");
    assert!(!result.is_error, "{result:?}");
    let structured = result
        .structured_content
        .expect("structured cluster result");
    assert_eq!(structured["scope"], "cluster_primaries");
    assert_eq!(structured["cluster"]["complete"], false);
    assert_eq!(structured["cluster"]["nodes_succeeded"], 1);
    assert_eq!(structured["cluster"]["replies"][1]["error_code"], "NOPERM");
    assert!(!structured.to_string().contains("secret ACL detail"));
}

fn canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => {
            let mut values = values.into_iter().collect::<Vec<_>>();
            values.sort_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        value => value,
    }
}

#[tokio::test]
async fn curated_catalog_matches_checked_in_contract_snapshot() {
    let client = full_catalog_client().await;
    let listed = client.list_tools().await.expect("list tools for snapshot");
    let mut contracts = listed
        .tools
        .into_iter()
        .map(|tool| {
            let metadata = tool_catalog()
                .iter()
                .find(|metadata| metadata.name == tool.name)
                .unwrap_or_else(|| panic!("missing catalog metadata for {}", tool.name));
            let requirements = metadata.capability_requirements();
            serde_json::json!({
                "name": metadata.name,
                "family": metadata.family().map(|family| family.feature_name()),
                "bundle": metadata.bundle.as_str(),
                "required_access": metadata.required_access.as_str(),
                "required_module": metadata.required_module().map(|module| module.as_str()),
                "capability_requirements": {
                    "minimum_redis_version": requirements.minimum_redis_version().map(|version| version.to_string()),
                    "required_module": requirements.required_module().map(|module| module.as_str()),
                    "minimum_module_version": requirements.minimum_module_version().map(|version| version.to_string()),
                    "required_commands": requirements.required_commands(),
                    "deployment": requirements.deployment().as_str(),
                },
                "requires_raw_opt_in": metadata.requires_raw_opt_in,
                "output_policy": metadata.output_policy().as_str(),
                "protocol": tool,
            })
        })
        .collect::<Vec<_>>();
    contracts.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));

    let mut structured_results = BTreeMap::new();
    for (name, arguments, _) in structured_cases() {
        let result = client
            .call_tool(name, arguments)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(!result.is_error, "{name}");
        let mut structured = result
            .structured_content
            .unwrap_or_else(|| panic!("{name} returned no structured content"));
        if name == "redis_ping" {
            structured["latency_ms"] = serde_json::json!(0.0);
        }
        if name == "redis_health_check" {
            structured["elapsed_ms"] = serde_json::json!(0.0);
        }
        structured_results.insert(name, structured);
    }

    let actual = serde_json::to_string_pretty(&canonical_json(serde_json::json!({
        "catalog": contracts,
        "structured_results": structured_results,
    })))
    .expect("serialize contract snapshot");
    if std::env::var_os("REDIS_MCP_UPDATE_SNAPSHOTS").is_some() {
        std::fs::write(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/snapshots/curated_catalog.json"
            ),
            format!("{actual}\n"),
        )
        .expect("update contract snapshot");
        return;
    }
    if std::env::var_os("REDIS_MCP_PRINT_SNAPSHOT").is_some() {
        println!("{actual}");
        return;
    }
    assert_eq!(
        actual,
        include_str!("snapshots/curated_catalog.json").trim_end()
    );
}
