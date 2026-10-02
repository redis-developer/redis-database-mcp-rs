//! Connection configuration shared by the direct redis-tower adapters.

use crate::{RedisDeployment, RedisError, RedisErrorKind};
use redis_tower::{commands::RawCommand, reconnect::ConnectionFactory};
use redis_tower_cluster::MultiplexedClusterClient;
#[cfg(unix)]
use redis_tower_core::RedisStream;
use redis_tower_core::{
    ConnectionConfig, ProtocolVersion, RedisConnection, RedisError as TowerError,
};
#[cfg(unix)]
use std::path::PathBuf;
use std::{future::Future, pin::Pin, time::Duration};

pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Validate a fixed Redis target before building a router, without connecting.
///
/// This uses the same URL parser as the bundled direct executors. A host may
/// call it during synchronous startup, then connect lazily on the first tool
/// call. Errors have stable codes and never contain the input URL or secrets.
/// `Unknown` is not a valid deployment choice for a fixed target.
pub fn validate_redis_target_url(url: &str, deployment: RedisDeployment) -> Result<(), RedisError> {
    let target = Target::parse(url)?;
    match deployment {
        RedisDeployment::Standalone => Ok(()),
        RedisDeployment::Cluster => target.parsed_cluster_url().map(|_| ()),
        RedisDeployment::Unknown => Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            "fixed Redis target requires standalone or Cluster mode",
        )
        .with_code("INVALID_REDIS_DEPLOYMENT")),
    }
}

/// Retain redis-rs protocol and authenticated Unix URL options at the
/// application boundary while using redis-tower for the transport.
#[derive(Clone)]
pub(crate) struct Target {
    pub url: String,
    pub config: ConnectionConfig,
    setup: Option<Box<TransportSetup>>,
}

#[derive(Clone)]
enum TransportSetup {
    Unix(UnixSetup),
    TlsIpv6(TlsIpv6Setup),
}

#[derive(Clone)]
struct UnixSetup {
    #[cfg(unix)]
    path: PathBuf,
    username: Option<String>,
    password: Option<String>,
    database: Option<u16>,
}

#[derive(Clone)]
struct TlsIpv6Setup {
    address: String,
    server_name: String,
    username: Option<String>,
    password: Option<String>,
    database: Option<u16>,
}

impl Target {
    pub fn parse(input: &str) -> Result<Self, RedisError> {
        let mut url = url::Url::parse(input).map_err(|_| invalid_url())?;
        let mut protocol = ProtocolVersion::Resp2;
        let mut database = None;
        let mut username = None;
        let mut password = None;
        for (name, value) in url.query_pairs() {
            match name.as_ref() {
                "protocol" => {
                    protocol = match value.as_ref() {
                        "resp2" | "2" => ProtocolVersion::Resp2,
                        "resp3" | "3" => ProtocolVersion::Resp3,
                        _ => return Err(invalid_url()),
                    }
                }
                "db" => database = Some(value.parse::<u16>().map_err(|_| invalid_url())?),
                "user" => username = Some(value.into_owned()),
                "pass" => password = Some(value.into_owned()),
                _ => return Err(invalid_url()),
            }
        }
        let unix = matches!(url.scheme(), "unix" | "redis+unix" | "valkey+unix");
        // Query credentials and database selection are supported on Unix
        // sockets. On TCP/TLS targets they were previously stripped without
        // being applied, which could silently select the wrong identity or DB.
        if !unix && (username.is_some() || password.is_some() || database.is_some()) {
            return Err(invalid_url());
        }
        #[cfg(unix)]
        let unix_path = unix
            .then(|| url.to_file_path().map_err(|_| invalid_url()))
            .transpose()?;
        if unix && username.is_some() && password.is_none() {
            return Err(invalid_url());
        }
        if matches!(url.scheme(), "rediss" | "valkeys") && url.fragment().is_some() {
            // The prior redis-rs feature set could not honor `#insecure` TLS
            // URLs. Reject fragments explicitly instead of silently dropping
            // a caller's certificate-validation intent.
            return Err(invalid_url());
        }
        url.set_query(None);
        url.set_fragment(None);
        if matches!(url.scheme(), "redis+unix" | "valkey+unix") {
            // redis-tower names the same Unix transport `unix`.
            url.set_scheme("unix").map_err(|_| invalid_url())?;
        }
        let tls_ipv6 = matches!(url.scheme(), "rediss" | "valkeys")
            && matches!(url.host(), Some(url::Host::Ipv6(_)));
        if !unix && matches!(url.host(), Some(url::Host::Ipv6(_))) && url.port().is_none() {
            // redis-tower's string parser requires an explicit port to
            // distinguish the final IPv6 segment from a port separator.
            url.set_port(Some(6379)).map_err(|_| invalid_url())?;
        }
        let url = url.to_string();
        let parsed = redis_tower_core::parse_redis_url(&url).map_err(|_| invalid_url())?;
        let tls_ipv6_setup = tls_ipv6.then(|| TlsIpv6Setup {
            address: format!("{}:{}", parsed.host, parsed.port),
            server_name: parsed
                .host
                .strip_prefix('[')
                .and_then(|host| host.strip_suffix(']'))
                .unwrap_or(&parsed.host)
                .to_owned(),
            username: parsed.username.clone(),
            password: parsed.password.clone(),
            database: parsed.database,
        });
        Ok(Self {
            url,
            config: ConnectionConfig::default()
                .with_protocol(protocol)
                .with_connect_timeout(Some(CONNECT_TIMEOUT)),
            setup: if unix {
                Some(Box::new(TransportSetup::Unix(UnixSetup {
                    #[cfg(unix)]
                    path: unix_path.expect("Unix targets have a decoded path"),
                    username,
                    password,
                    database,
                })))
            } else {
                tls_ipv6_setup.map(|setup| Box::new(TransportSetup::TlsIpv6(setup)))
            },
        })
    }

    pub async fn connect(&self) -> Result<RedisConnection, RedisError> {
        self.connect_tower().await.map_err(RedisError::from)
    }

    async fn connect_tower(&self) -> Result<RedisConnection, TowerError> {
        tokio::time::timeout(CONNECT_TIMEOUT, self.connect_unbounded())
            .await
            .map_err(|_| TowerError::ConnectTimeout)?
    }

    async fn connect_unbounded(&self) -> Result<RedisConnection, TowerError> {
        if let Some(TransportSetup::TlsIpv6(setup)) = self.setup.as_deref() {
            // Keep the verified adapter path for TLS IPv6 during this
            // dependency update. The socket address needs brackets, while
            // rustls needs an unbracketed ServerName. redis-tower now handles
            // this split too; the adapter can be simplified separately.
            let initial = self.config.clone().with_protocol(ProtocolVersion::Resp2);
            let tls = redis_tower_core::tls::TlsConfig::default_rustls();
            let connection = RedisConnection::connect_tls_with_config(
                &setup.address,
                &setup.server_name,
                &tls,
                &initial,
            )
            .await?;
            return finish_setup(
                connection,
                setup.username.as_deref(),
                setup.password.as_deref(),
                setup.database,
                self.config.protocol(),
            )
            .await;
        }

        let Some(TransportSetup::Unix(setup)) = self.setup.as_deref() else {
            return RedisConnection::connect_url_with_config(&self.url, &self.config).await;
        };

        // Preserve the verified query-based ACL setup path during this
        // dependency update. redis-tower now supports it directly too;
        // simplifying the adapter is separate from updating the release set.
        // This path is also used for reconnection.
        let initial = self.config.clone().with_protocol(ProtocolVersion::Resp2);
        #[cfg(unix)]
        let connection = {
            let stream = tokio::net::UnixStream::connect(&setup.path)
                .await
                .map_err(|error| TowerError::connection(setup.path.display().to_string(), error))?;
            let mut connection =
                RedisConnection::from_stream_with_config(RedisStream::Unix(stream), &initial);
            identify_client(&mut connection).await?;
            connection
        };
        #[cfg(not(unix))]
        let mut connection = {
            return Err(TowerError::InvalidUrl(
                "unix sockets are not supported on this platform".into(),
            ));
        };
        finish_setup(
            connection,
            setup.username.as_deref(),
            setup.password.as_deref(),
            setup.database,
            self.config.protocol(),
        )
        .await
    }

    pub async fn exclusive_cluster(
        &self,
    ) -> Result<redis_tower_cluster::ClusterConnection, RedisError> {
        let parsed = self.parsed_cluster_url()?;
        let mut builder = redis_tower_cluster::ClusterConnection::builder(format!(
            "{}:{}",
            parsed.host, parsed.port
        ))
        .connection_config(self.config.clone())
        .max_redirects(0);
        if let Some(password) = parsed.password {
            let credentials = match parsed.username {
                Some(user) if !user.is_empty() => {
                    redis_tower::credentials::StaticCredentials::new(user, password)
                }
                _ => redis_tower::credentials::StaticCredentials::password(password),
            };
            builder = builder.credentials(credentials);
        }
        if parsed.tls {
            builder = builder.tls(redis_tower_core::tls::TlsConfig::default_rustls());
        }
        tokio::time::timeout(CONNECT_TIMEOUT, builder.connect())
            .await
            .map_err(|_| {
                RedisError::new(
                    RedisErrorKind::Timeout,
                    "Redis Cluster connection setup timed out",
                )
            })?
            .map_err(RedisError::from)
    }

    pub fn cluster_builder(
        &self,
    ) -> Result<redis_tower_cluster::MultiplexedClusterClientBuilder, RedisError> {
        let parsed = self.parsed_cluster_url()?;
        let mut builder =
            MultiplexedClusterClient::builder(format!("{}:{}", parsed.host, parsed.port))
                .connection_config(self.config.clone());
        if let Some(password) = parsed.password {
            let credentials = match parsed.username {
                Some(user) if !user.is_empty() => {
                    redis_tower::credentials::StaticCredentials::new(user, password)
                }
                _ => redis_tower::credentials::StaticCredentials::password(password),
            };
            builder = builder.credentials(credentials);
        }
        if parsed.tls {
            builder = builder.tls(redis_tower_core::tls::TlsConfig::default_rustls());
        }
        Ok(builder)
    }

    fn parsed_cluster_url(&self) -> Result<redis_tower_core::RedisUrl, RedisError> {
        let parsed = redis_tower_core::parse_redis_url(&self.url).map_err(|_| invalid_url())?;
        if parsed.unix {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "Redis Cluster targets cannot use Unix sockets",
            )
            .with_code("CLUSTER_UNIX_UNSUPPORTED"));
        }
        if parsed.database.is_some_and(|db| db != 0) {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "Redis Cluster targets must use database 0",
            )
            .with_code("CLUSTER_DATABASE_UNSUPPORTED"));
        }
        Ok(parsed)
    }
}

async fn finish_setup(
    mut connection: RedisConnection,
    username: Option<&str>,
    password: Option<&str>,
    database: Option<u16>,
    protocol: ProtocolVersion,
) -> Result<RedisConnection, TowerError> {
    if let Some(password) = password {
        let mut auth = RawCommand::new("AUTH");
        if let Some(username) = username {
            auth = auth.arg(username);
        }
        connection.execute(auth.arg(password)).await?;
    }
    if let Some(database) = database {
        connection
            .execute(RawCommand::new("SELECT").arg(database.to_string()))
            .await?;
    }
    connection.negotiate_protocol(protocol).await?;
    Ok(connection)
}

#[cfg(unix)]
async fn identify_client(connection: &mut RedisConnection) -> Result<(), TowerError> {
    for command in [
        RawCommand::new("CLIENT")
            .arg("SETINFO")
            .arg("LIB-NAME")
            .arg("redis-mcp"),
        RawCommand::new("CLIENT")
            .arg("SETINFO")
            .arg("LIB-VER")
            .arg(env!("CARGO_PKG_VERSION")),
    ] {
        match connection.execute(command).await {
            Ok(_) | Err(TowerError::Redis(_)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

impl ConnectionFactory for Target {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, TowerError>> + Send>> {
        let target = self.clone();
        Box::pin(async move { target.connect_tower().await })
    }
}

pub(crate) async fn connect_cluster(
    targets: &[Target],
) -> Result<MultiplexedClusterClient, RedisError> {
    // Reject an invalid later seed before a healthy earlier seed can hide it.
    for target in targets {
        target.parsed_cluster_url()?;
    }
    connect_cluster_with_timeout(targets, CONNECT_TIMEOUT).await
}

async fn connect_cluster_with_timeout(
    targets: &[Target],
    timeout: Duration,
) -> Result<MultiplexedClusterClient, RedisError> {
    if targets.is_empty() {
        return Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            "at least one Redis Cluster seed URL is required",
        )
        .with_code("EMPTY_CLUSTER_SEEDS"));
    }

    tokio::time::timeout(timeout, async {
        let mut last = None;
        for target in targets {
            match target.cluster_builder()?.connect().await {
                Ok(client) => return Ok(client),
                Err(error) => last = Some(RedisError::from(error)),
            }
        }
        Err(last.expect("non-empty seed list attempted at least once"))
    })
    .await
    .map_err(|_| {
        RedisError::new(
            RedisErrorKind::Timeout,
            "Redis Cluster connection setup timed out",
        )
    })?
}

fn invalid_url() -> RedisError {
    RedisError::new(
        RedisErrorKind::InvalidRequest,
        "invalid Redis target URL or protocol option",
    )
    .with_code("INVALID_REDIS_URL")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_target_validation_is_offline_and_matches_deployment_rules() {
        for url in [
            "redis://unreachable.invalid:6379/1?protocol=resp3",
            "rediss://agent:password@unreachable.invalid:6379/1",
            "unix:///tmp/redis-mcp-not-running.sock?user=agent&pass=password&db=1",
        ] {
            validate_redis_target_url(url, RedisDeployment::Standalone)
                .expect("standalone syntax is valid without dialing");
        }
        for url in [
            "redis://unreachable.invalid:6379/0?protocol=resp3",
            "rediss://agent:password@unreachable.invalid:6379/",
        ] {
            validate_redis_target_url(url, RedisDeployment::Cluster)
                .expect("Cluster seed syntax is valid without dialing");
        }

        let error = validate_redis_target_url(
            "redis://unreachable.invalid:6379/1",
            RedisDeployment::Cluster,
        )
        .expect_err("Cluster rejects a nonzero logical database");
        assert_eq!(error.code(), Some("CLUSTER_DATABASE_UNSUPPORTED"));

        let error = validate_redis_target_url(
            "unix:///tmp/redis-mcp-not-running.sock",
            RedisDeployment::Cluster,
        )
        .expect_err("Cluster rejects Unix sockets");
        assert_eq!(error.code(), Some("CLUSTER_UNIX_UNSUPPORTED"));

        let error = validate_redis_target_url(
            "redis://unreachable.invalid:6379/",
            RedisDeployment::Unknown,
        )
        .expect_err("fixed target deployment must be explicit");
        assert_eq!(error.code(), Some("INVALID_REDIS_DEPLOYMENT"));
    }

    #[test]
    fn fixed_target_validation_fails_closed_without_echoing_secrets() {
        let secret = "unique-redis-mcp-canary-secret";
        for url in [
            format!("redis://unreachable.invalid:6379/?pass={secret}"),
            format!("redis://unreachable.invalid:6379/?db=1&pass={secret}"),
            format!("rediss://unreachable.invalid:6379/?user=agent&pass={secret}"),
            format!("redis://unreachable.invalid:6379/?auth={secret}"),
            format!("redis://agent:{secret}@unreachable.invalid:6379/?protocol=bad"),
        ] {
            let error = validate_redis_target_url(&url, RedisDeployment::Standalone)
                .expect_err("unsupported or malformed target syntax fails closed");
            assert_eq!(error.kind(), RedisErrorKind::InvalidRequest);
            assert_eq!(error.code(), Some("INVALID_REDIS_URL"));
            for rendered in [error.to_string(), format!("{error:?}")] {
                assert!(!rendered.contains(secret), "validation leaked a secret");
                assert!(!rendered.contains(&url), "validation leaked the URL");
            }
        }
    }

    #[tokio::test]
    async fn invalid_later_cluster_seed_fails_before_any_connection_attempt() {
        let targets = [
            Target::parse("redis://127.0.0.1:1/0").unwrap(),
            Target::parse("redis://127.0.0.1:1/1").unwrap(),
        ];
        let result = tokio::time::timeout(Duration::from_millis(100), connect_cluster(&targets))
            .await
            .expect("validation must finish before dialing the first seed");
        let error = match result {
            Ok(_) => panic!("invalid second seed must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.code(), Some("CLUSTER_DATABASE_UNSUPPORTED"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn authenticated_unix_aliases_replay_acl_setup() {
        let directory = tempfile::tempdir().expect("create Redis test directory");
        let socket = directory.path().join("redis socket.sock");
        let acl_file = directory.path().join("users.acl");
        std::fs::write(
            &acl_file,
            "user default on nopass ~* +@all\nuser agent on >secret ~* +@all\n",
        )
        .expect("write Redis ACL file");
        let server = redis_server_wrapper::RedisServer::new()
            .auto_port()
            .bind("127.0.0.1")
            .dir(directory.path())
            .unixsocket(&socket)
            .unixsocketperm(700)
            .acl_file(&acl_file)
            .no_stack_modules()
            .start()
            .await;
        let _server = match server {
            Ok(server) => server,
            Err(redis_server_wrapper::Error::BinaryNotFound { binary }) => {
                eprintln!("skipping local Unix transport test: {binary} is not on PATH");
                return;
            }
            Err(error) => panic!("start isolated Redis: {error}"),
        };

        for scheme in ["redis+unix", "valkey+unix"] {
            let encoded_socket = socket.display().to_string().replace(' ', "%20");
            let url = format!(
                "{scheme}://{}?user=agent&pass=secret&db=1&protocol=resp3",
                encoded_socket
            );
            let target = Target::parse(&url).expect("parse authenticated Unix target");
            assert!(target.url.starts_with("unix:"));
            assert!(target.url.contains("%20"));
            let mut connection = target.connect().await.expect("connect with ACL identity");
            let identity: String = connection
                .execute(RawCommand::new("ACL").arg("WHOAMI").query())
                .await
                .expect("query authenticated identity");
            assert_eq!(identity, "agent");
            assert!(connection.is_resp3());

            let wrong_url = format!(
                "{scheme}://{}?user=agent&pass=wrong&protocol=resp3",
                encoded_socket
            );
            let result = Target::parse(&wrong_url)
                .expect("parse bad credentials")
                .connect()
                .await;
            let error = match result {
                Ok(_) => panic!("wrong ACL password must fail"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), RedisErrorKind::Authentication);
            assert_eq!(error.code(), Some("WRONGPASS"));
        }
    }

    #[test]
    fn ipv6_urls_without_ports_are_normalized_for_tower() {
        for scheme in ["redis", "rediss", "valkey", "valkeys"] {
            let target = Target::parse(&format!("{scheme}://[::1]/?protocol=resp3"))
                .expect("parse default-port IPv6 target");
            let parsed = redis_tower_core::parse_redis_url(&target.url)
                .expect("normalized target is accepted by redis-tower");
            assert_eq!(parsed.host, "[::1]");
            assert_eq!(parsed.port, 6379);
            assert_eq!(target.config.protocol(), ProtocolVersion::Resp3);
            if matches!(scheme, "rediss" | "valkeys") {
                let Some(TransportSetup::TlsIpv6(setup)) = target.setup.as_deref() else {
                    panic!("TLS IPv6 target has split connection names");
                };
                assert_eq!(setup.address, "[::1]:6379");
                assert_eq!(setup.server_name, "::1");
            }
        }
    }

    #[tokio::test]
    async fn tls_ipv6_connection_uses_unbracketed_rustls_server_name() {
        let listener = match tokio::net::TcpListener::bind("[::1]:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::AddrNotAvailable => {
                eprintln!("skipping TLS IPv6 transport test: IPv6 loopback is unavailable");
                return;
            }
            Err(error) => panic!("bind IPv6 loopback: {error}"),
        };
        let port = listener.local_addr().expect("read listener address").port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept TLS client");
            stream.writable().await.expect("wait for writable socket");
            stream
                .try_write(b"not a TLS record")
                .expect("write invalid TLS response");
        });

        let target =
            Target::parse(&format!("rediss://[::1]:{port}/")).expect("parse TLS IPv6 target");
        let error = match target.connect_tower().await {
            Ok(_) => panic!("invalid test server cannot finish TLS"),
            Err(error) => error,
        };
        server.await.expect("join test TLS server");

        match &error {
            TowerError::Connection { source, .. } => assert_ne!(
                source.kind(),
                std::io::ErrorKind::InvalidInput,
                "rustls rejected the bracketed IPv6 server name: {error}"
            ),
            other => panic!("expected TLS connection error, got {other}"),
        }
    }

    #[test]
    fn insecure_tls_fragments_are_rejected_explicitly() {
        for scheme in ["rediss", "valkeys"] {
            let error = match Target::parse(&format!("{scheme}://localhost/#insecure")) {
                Ok(_) => panic!("insecure TLS fragment must not be silently dropped"),
                Err(error) => error,
            };
            assert_eq!(error.code(), Some("INVALID_REDIS_URL"));
        }
    }

    #[tokio::test]
    async fn cluster_seed_attempts_share_one_total_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalled cluster seed");
        let address = listener.local_addr().expect("read listener address");
        let acceptor = tokio::spawn(async move {
            let mut connections = Vec::new();
            while let Ok((connection, _)) = listener.accept().await {
                connections.push(connection);
            }
        });
        let target = Target::parse(&format!("redis://{address}/")).expect("parse seed");
        let started = tokio::time::Instant::now();
        let error =
            connect_cluster_with_timeout(&[target.clone(), target], Duration::from_millis(100))
                .await
                .expect_err("stalled seeds must time out");
        let elapsed = started.elapsed();
        acceptor.abort();

        assert_eq!(error.kind(), RedisErrorKind::Timeout);
        assert!(
            elapsed < Duration::from_millis(175),
            "seed attempts exceeded one total timeout: {elapsed:?}"
        );
    }
}
