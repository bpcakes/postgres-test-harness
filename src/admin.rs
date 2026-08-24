use std::{
    fmt,
    str::FromStr,
    sync::{
        OnceLock,
        mpsc::{self, Sender},
    },
    time::Duration,
};

use postgres::{Row, types::ToSql};
use sha2::{Digest, Sha256};
use tokio::{runtime::Runtime, task::JoinHandle};
use tokio_postgres::NoTls;
use url::Url;

use crate::{Error, Result, metadata::ResourceMetadata, name::DatabaseName};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const LOCK_TIMEOUT: Duration = Duration::from_secs(10);

static CLIENT_DROP_WORKER: OnceLock<Option<Sender<AdminClient>>> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct AdminDatabaseUrl(Url);

impl AdminDatabaseUrl {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        let url = Url::parse(value).map_err(|_| Error::InvalidAdminDatabaseUrl {
            reason: "the value must be an absolute PostgreSQL URL",
        })?;
        if !matches!(url.scheme(), "postgres" | "postgresql") {
            return Err(Error::InvalidAdminDatabaseUrl {
                reason: "the URL scheme must be postgres or postgresql",
            });
        }
        if url.host().is_none() {
            return Err(Error::InvalidAdminDatabaseUrl {
                reason: "the URL must contain a host",
            });
        }
        if url.path().trim_matches('/').is_empty() {
            return Err(Error::InvalidAdminDatabaseUrl {
                reason: "the URL must identify an administrative database",
            });
        }
        Ok(Self(url))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn database_url(&self, database_name: &DatabaseName) -> String {
        let mut url = self.0.clone();
        url.set_path(&format!("/{}", database_name.as_str()));
        url.set_fragment(None);
        url.to_string()
    }
}

impl fmt::Debug for AdminDatabaseUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut redacted = self.0.clone();
        if redacted.password().is_some() {
            let _ = redacted.set_password(Some("[REDACTED]"));
        }
        formatter
            .debug_tuple("AdminDatabaseUrl")
            .field(&redacted.as_str())
            .finish()
    }
}

pub(crate) struct DatabaseRecord {
    pub(crate) name: String,
    pub(crate) comment: Option<String>,
}

/// Tokio-backed PostgreSQL client with a synchronous internal interface.
///
/// Keeping the runtime here lets connection establishment place one deadline
/// around socket connection, PostgreSQL startup, and authentication. All
/// callers already execute admin work through `spawn_blocking`.
pub(crate) struct AdminClient {
    client: tokio_postgres::Client,
    _connection: JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
    runtime: Runtime,
}

impl AdminClient {
    fn connect(
        admin_url: &AdminDatabaseUrl,
        connect_timeout: Duration,
        operation: &'static str,
    ) -> Result<Self> {
        let mut config = tokio_postgres::Config::from_str(admin_url.as_str())
            .map_err(|source| Error::postgres(operation, source))?;
        config.connect_timeout(connect_timeout);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| Error::PostgresRuntime { operation, source })?;
        let connection = runtime
            .block_on(async { tokio::time::timeout(connect_timeout, config.connect(NoTls)).await });
        let (client, connection) = connection
            .map_err(|_| Error::PostgresConnectTimeout {
                operation,
                timeout: connect_timeout,
            })?
            .map_err(|source| Error::postgres(operation, source))?;
        let connection = runtime.spawn(connection);
        Ok(Self {
            client,
            _connection: connection,
            runtime,
        })
    }

    fn batch_execute(&mut self, query: &str) -> std::result::Result<(), postgres::Error> {
        self.runtime.block_on(self.client.batch_execute(query))
    }

    fn query(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> std::result::Result<Vec<Row>, postgres::Error> {
        self.runtime.block_on(self.client.query(query, params))
    }

    fn query_one(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> std::result::Result<Row, postgres::Error> {
        self.runtime.block_on(self.client.query_one(query, params))
    }

    fn query_opt(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> std::result::Result<Option<Row>, postgres::Error> {
        self.runtime.block_on(self.client.query_opt(query, params))
    }
}

/// Admin client that may be retained by async code to hold an advisory lock.
///
/// Dropping a Tokio runtime directly from another Tokio runtime panics. This
/// wrapper transfers the complete client to a plain dedicated thread for
/// destruction.
pub(crate) struct PersistentClient(Option<AdminClient>);

impl PersistentClient {
    pub(crate) fn new(client: AdminClient) -> Self {
        Self(Some(client))
    }

    pub(crate) fn client_mut(&mut self) -> &mut AdminClient {
        self.0
            .as_mut()
            .expect("persistent PostgreSQL client exists until drop")
    }
}

impl Drop for PersistentClient {
    fn drop(&mut self) {
        let Some(client) = self.0.take() else {
            return;
        };
        let worker = CLIENT_DROP_WORKER.get_or_init(|| {
            let (sender, receiver) = mpsc::channel::<AdminClient>();
            match std::thread::Builder::new()
                .name("postgres-test-harness-client-drop".to_owned())
                .spawn(move || {
                    while let Ok(client) = receiver.recv() {
                        drop(client);
                    }
                })
            {
                Ok(_) => Some(sender),
                Err(error) => {
                    eprintln!(
                        "postgres-test-harness: failed to start PostgreSQL client drop worker: {error}"
                    );
                    None
                }
            }
        });
        let Some(worker) = worker else {
            std::mem::forget(client);
            return;
        };
        if let Err(error) = worker.send(client) {
            std::mem::forget(error.0);
        }
    }
}

pub(crate) fn connect_admin(
    admin_url: &AdminDatabaseUrl,
    operation_timeout: Duration,
    operation: &'static str,
) -> Result<AdminClient> {
    connect_admin_with_timeout(admin_url, operation_timeout, operation, CONNECT_TIMEOUT)
}

fn connect_admin_with_timeout(
    admin_url: &AdminDatabaseUrl,
    operation_timeout: Duration,
    operation: &'static str,
    connect_timeout: Duration,
) -> Result<AdminClient> {
    let mut client = AdminClient::connect(admin_url, connect_timeout, operation)?;
    configure_session_timeouts(&mut client, operation_timeout)?;
    Ok(client)
}

fn configure_session_timeouts(client: &mut AdminClient, operation_timeout: Duration) -> Result<()> {
    let statement_timeout = duration_millis(operation_timeout)?;
    let lock_timeout = duration_millis(LOCK_TIMEOUT.min(operation_timeout))?;
    client
        .batch_execute(&format!(
            "SET statement_timeout = {statement_timeout}; SET lock_timeout = {lock_timeout};"
        ))
        .map_err(|source| Error::postgres("configure admin session timeouts", source))
}

fn configure_coordination_timeouts(client: &mut AdminClient, wait_timeout: Duration) -> Result<()> {
    let timeout = duration_millis(wait_timeout)?;
    client
        .batch_execute(&format!(
            "SET statement_timeout = {timeout}; SET lock_timeout = {timeout};"
        ))
        .map_err(|source| Error::postgres("configure template coordination timeouts", source))
}

pub(crate) fn validate_postgres_18(client: &mut AdminClient) -> Result<()> {
    let server_version_num: i32 = client
        .query_one("SELECT current_setting('server_version_num')::integer", &[])
        .map_err(|source| Error::postgres("read PostgreSQL server version", source))?
        .get(0);
    if server_version_num / 10_000 != 18 {
        return Err(Error::UnsupportedPostgresVersion { server_version_num });
    }
    client
        .query_one("SELECT uuidv7()::text", &[])
        .map_err(|_| Error::MissingUuidV7)?;
    Ok(())
}

pub(crate) fn acquire_advisory_lock(client: &mut AdminClient, key: i64) -> Result<()> {
    client
        .query_one("SELECT pg_advisory_lock($1)", &[&key])
        .map_err(|source| Error::postgres("acquire PostgreSQL advisory lock", source))?;
    Ok(())
}

pub(crate) fn acquire_template_advisory_lock(
    client: &mut AdminClient,
    key: i64,
    wait_timeout: Duration,
    operation_timeout: Duration,
) -> Result<()> {
    configure_coordination_timeouts(client, wait_timeout)?;
    client
        .query_one("SELECT pg_advisory_lock($1)", &[&key])
        .map_err(|source| Error::postgres("acquire PostgreSQL template advisory lock", source))?;
    configure_session_timeouts(client, operation_timeout)
}

pub(crate) fn acquire_shared_template_advisory_lock(
    client: &mut AdminClient,
    key: i64,
    wait_timeout: Duration,
    operation_timeout: Duration,
) -> Result<()> {
    configure_coordination_timeouts(client, wait_timeout)?;
    client
        .query_one("SELECT pg_advisory_lock_shared($1)", &[&key])
        .map_err(|source| {
            Error::postgres("acquire shared PostgreSQL template advisory lock", source)
        })?;
    configure_session_timeouts(client, operation_timeout)
}

pub(crate) fn try_acquire_advisory_lock(client: &mut AdminClient, key: i64) -> Result<bool> {
    client
        .query_one("SELECT pg_try_advisory_lock($1)", &[&key])
        .map(|row| row.get(0))
        .map_err(|source| Error::postgres("try PostgreSQL advisory lock", source))
}

pub(crate) fn release_advisory_lock(client: &mut AdminClient, key: i64) -> Result<()> {
    client
        .query_one("SELECT pg_advisory_unlock($1)", &[&key])
        .map_err(|source| Error::postgres("release PostgreSQL advisory lock", source))?;
    Ok(())
}

pub(crate) fn release_shared_advisory_lock(client: &mut AdminClient, key: i64) -> Result<()> {
    client
        .query_one("SELECT pg_advisory_unlock_shared($1)", &[&key])
        .map_err(|source| Error::postgres("release shared PostgreSQL advisory lock", source))?;
    Ok(())
}

fn create_database(
    client: &mut AdminClient,
    database_name: &DatabaseName,
    template_name: &str,
) -> Result<()> {
    let template = quote_identifier(template_name)?;
    client
        .batch_execute(&format!(
            "CREATE DATABASE {} TEMPLATE {template}",
            database_name.quoted()
        ))
        .map_err(|source| Error::postgres("create disposable PostgreSQL database", source))
}

pub(crate) fn create_managed_database(
    client: &mut AdminClient,
    database_name: &DatabaseName,
    template_name: &str,
    metadata: &ResourceMetadata,
) -> Result<()> {
    create_database(client, database_name, template_name)?;
    if let Err(tagging) = set_database_metadata(client, database_name, metadata) {
        return Err(compensate_failed_metadata_write(tagging, || {
            drop_database(client, database_name)
        }));
    }
    Ok(())
}

fn compensate_failed_metadata_write(tagging: Error, cleanup: impl FnOnce() -> Result<()>) -> Error {
    match cleanup() {
        Ok(()) => tagging,
        Err(cleanup) => Error::ManagedDatabaseTagAndCleanup {
            tagging: Box::new(tagging),
            cleanup: Box::new(cleanup),
        },
    }
}

pub(crate) fn drop_database(client: &mut AdminClient, database_name: &DatabaseName) -> Result<()> {
    client
        .batch_execute(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            database_name.quoted()
        ))
        .map_err(|source| Error::postgres("drop disposable PostgreSQL database", source))
}

pub(crate) fn find_database(
    client: &mut AdminClient,
    database_name: &DatabaseName,
) -> Result<Option<DatabaseRecord>> {
    client
        .query_opt(
            "SELECT datname, shobj_description(oid, 'pg_database') \
             FROM pg_database WHERE datname = $1",
            &[&database_name.as_str()],
        )
        .map(|row| {
            row.map(|row| DatabaseRecord {
                name: row.get(0),
                comment: row.get(1),
            })
        })
        .map_err(|source| Error::postgres("find disposable PostgreSQL database", source))
}

pub(crate) fn set_database_metadata(
    client: &mut AdminClient,
    database_name: &DatabaseName,
    metadata: &ResourceMetadata,
) -> Result<()> {
    client
        .batch_execute(&format!(
            "COMMENT ON DATABASE {} IS {}",
            database_name.quoted(),
            quote_literal(&metadata.encode())
        ))
        .map_err(|source| Error::postgres("write disposable PostgreSQL database metadata", source))
}

pub(crate) fn disable_database_connections(
    client: &mut AdminClient,
    database_name: &DatabaseName,
) -> Result<()> {
    client
        .batch_execute(&format!(
            "ALTER DATABASE {} ALLOW_CONNECTIONS false",
            database_name.quoted()
        ))
        .map_err(|source| Error::postgres("disable PostgreSQL template connections", source))
}

pub(crate) fn terminate_database_connections(
    client: &mut AdminClient,
    database_name: &DatabaseName,
    operation_timeout: Duration,
) -> Result<()> {
    let timeout = i64::try_from(duration_millis(operation_timeout)?).map_err(|_| {
        Error::TimeoutOutOfRange {
            duration: operation_timeout,
        }
    })?;
    let rows = client
        .query(
            "SELECT pg_terminate_backend(pid, $2::bigint) \
             FROM pg_stat_activity \
             WHERE datname = $1 AND pid <> pg_backend_pid()",
            &[&database_name.as_str(), &timeout],
        )
        .map_err(|source| Error::postgres("terminate PostgreSQL template connections", source))?;
    if rows.iter().any(|row| !row.get::<_, bool>(0)) {
        return Err(Error::TemplateConnectionsRemain {
            database_name: database_name.as_str().to_owned(),
        });
    }

    let connections_remain: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = $1)",
            &[&database_name.as_str()],
        )
        .map_err(|source| Error::postgres("verify PostgreSQL template connections", source))?
        .get(0);
    if connections_remain {
        return Err(Error::TemplateConnectionsRemain {
            database_name: database_name.as_str().to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn list_databases(client: &mut AdminClient) -> Result<Vec<DatabaseRecord>> {
    client
        .query(
            "SELECT datname, shobj_description(oid, 'pg_database') FROM pg_database",
            &[],
        )
        .map(|rows| {
            rows.into_iter()
                .map(|row| DatabaseRecord {
                    name: row.get(0),
                    comment: row.get(1),
                })
                .collect()
        })
        .map_err(|source| Error::postgres("list PostgreSQL databases for stale cleanup", source))
}

pub(crate) fn advisory_key(domain: &str, identity: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(b"postgres-test-harness-advisory-v1");
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain.as_bytes());
    hasher.update((identity.len() as u64).to_be_bytes());
    hasher.update(identity.as_bytes());
    let digest = hasher.finalize();
    i64::from_be_bytes(digest[..8].try_into().expect("SHA-256 has eight bytes"))
}

fn duration_millis(duration: Duration) -> Result<u64> {
    duration
        .as_millis()
        .try_into()
        .map_err(|_| Error::TimeoutOutOfRange { duration })
}

fn quote_identifier(identifier: &str) -> Result<String> {
    if identifier == "template0"
        || (!identifier.is_empty()
            && identifier.len() <= 63
            && identifier
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
    {
        Ok(format!(r#""{identifier}""#))
    } else {
        Err(Error::InvalidDatabaseName {
            name: identifier.to_owned(),
        })
    }
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use std::{io::Read, net::TcpListener, thread, time::Duration};

    use super::{
        AdminDatabaseUrl, advisory_key, compensate_failed_metadata_write,
        connect_admin_with_timeout, quote_identifier, quote_literal,
    };
    use crate::{Error, FingerprintBuilder, ProjectName, name::DatabaseName};

    #[test]
    fn admin_url_debug_redacts_password_and_rewrites_only_database() {
        let admin = AdminDatabaseUrl::parse(
            "postgres://user:secret@localhost:5432/postgres?application_name=test",
        )
        .unwrap();
        let debug = format!("{admin:?}");
        assert!(!debug.contains("secret"));
        let database = DatabaseName::template(
            &ProjectName::new("creditkit").unwrap(),
            FingerprintBuilder::new("schema").finish(),
        );
        let rewritten = admin.database_url(&database);
        assert!(rewritten.contains(database.as_str()));
        assert!(rewritten.contains("application_name=test"));
    }

    #[test]
    fn sql_quoting_is_narrow() {
        assert_eq!(quote_identifier("template0").unwrap(), "\"template0\"");
        assert!(quote_identifier("unsafe-name").is_err());
        assert_eq!(quote_literal("a'b"), "'a''b'");
    }

    #[test]
    fn admin_connection_deadline_covers_a_silent_postgres_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut startup = Vec::new();
            stream.read_to_end(&mut startup).unwrap();
            startup
        });
        let admin = AdminDatabaseUrl::parse(&format!(
            "postgres://user:secret@127.0.0.1:{port}/postgres?sslmode=disable"
        ))
        .unwrap();

        let error = match connect_admin_with_timeout(
            &admin,
            Duration::from_secs(1),
            "connect to silent PostgreSQL test server",
            Duration::from_millis(50),
        ) {
            Ok(_) => panic!("silent PostgreSQL peer must not complete startup"),
            Err(error) => error,
        };

        assert!(matches!(
            &error,
            Error::PostgresConnectTimeout {
                operation: "connect to silent PostgreSQL test server",
                timeout
            } if *timeout == Duration::from_millis(50)
        ));
        let display = error.to_string();
        assert!(!display.contains("secret"));
        assert!(!display.contains(&port.to_string()));
        assert!(!server.join().unwrap().is_empty());
    }

    #[test]
    fn advisory_keys_are_domain_separated() {
        assert_ne!(
            advisory_key("run", "same"),
            advisory_key("template", "same")
        );
    }

    #[test]
    fn successful_compensation_returns_the_metadata_error() {
        let error = compensate_failed_metadata_write(
            Error::InvalidConfiguration { reason: "tagging" },
            || Ok(()),
        );
        assert!(matches!(
            error,
            Error::InvalidConfiguration { reason: "tagging" }
        ));
    }

    #[test]
    fn failed_compensation_reports_both_errors() {
        let error = compensate_failed_metadata_write(
            Error::InvalidConfiguration { reason: "tagging" },
            || Err(Error::InvalidConfiguration { reason: "cleanup" }),
        );
        assert!(matches!(
            error,
            Error::ManagedDatabaseTagAndCleanup { tagging, cleanup }
                if matches!(*tagging, Error::InvalidConfiguration { reason: "tagging" })
                    && matches!(*cleanup, Error::InvalidConfiguration { reason: "cleanup" })
        ));
    }
}
