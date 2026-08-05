use std::{
    fmt,
    str::FromStr,
    sync::{
        OnceLock,
        mpsc::{self, Sender},
    },
    time::Duration,
};

use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use url::Url;

use crate::{Error, Result, metadata::ResourceMetadata, name::DatabaseName};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const LOCK_TIMEOUT: Duration = Duration::from_secs(10);

static CLIENT_DROP_WORKER: OnceLock<Option<Sender<Client>>> = OnceLock::new();

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

/// Synchronous PostgreSQL client that may be owned by async code.
///
/// `postgres::Client::drop` blocks on its private Tokio runtime and panics if
/// that happens directly on another Tokio runtime. This wrapper transfers the
/// client to a plain dedicated thread for destruction.
pub(crate) struct PersistentClient(Option<Client>);

impl PersistentClient {
    pub(crate) fn new(client: Client) -> Self {
        Self(Some(client))
    }

    pub(crate) fn client_mut(&mut self) -> &mut Client {
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
            let (sender, receiver) = mpsc::channel::<Client>();
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
) -> Result<Client> {
    let mut config = postgres::Config::from_str(admin_url.as_str())
        .map_err(|source| Error::postgres(operation, source))?;
    config.connect_timeout(CONNECT_TIMEOUT);
    let mut client = config
        .connect(NoTls)
        .map_err(|source| Error::postgres(operation, source))?;

    configure_session_timeouts(&mut client, operation_timeout)?;
    Ok(client)
}

fn configure_session_timeouts(client: &mut Client, operation_timeout: Duration) -> Result<()> {
    let statement_timeout = duration_millis(operation_timeout)?;
    let lock_timeout = duration_millis(LOCK_TIMEOUT.min(operation_timeout))?;
    client
        .batch_execute(&format!(
            "SET statement_timeout = {statement_timeout}; SET lock_timeout = {lock_timeout};"
        ))
        .map_err(|source| Error::postgres("configure admin session timeouts", source))
}

fn configure_coordination_timeouts(client: &mut Client, wait_timeout: Duration) -> Result<()> {
    let timeout = duration_millis(wait_timeout)?;
    client
        .batch_execute(&format!(
            "SET statement_timeout = {timeout}; SET lock_timeout = {timeout};"
        ))
        .map_err(|source| Error::postgres("configure template coordination timeouts", source))
}

pub(crate) fn validate_postgres_18(client: &mut Client) -> Result<()> {
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

pub(crate) fn acquire_advisory_lock(client: &mut Client, key: i64) -> Result<()> {
    client
        .query_one("SELECT pg_advisory_lock($1)", &[&key])
        .map_err(|source| Error::postgres("acquire PostgreSQL advisory lock", source))?;
    Ok(())
}

pub(crate) fn acquire_template_advisory_lock(
    client: &mut Client,
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
    client: &mut Client,
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

pub(crate) fn try_acquire_advisory_lock(client: &mut Client, key: i64) -> Result<bool> {
    client
        .query_one("SELECT pg_try_advisory_lock($1)", &[&key])
        .map(|row| row.get(0))
        .map_err(|source| Error::postgres("try PostgreSQL advisory lock", source))
}

pub(crate) fn release_advisory_lock(client: &mut Client, key: i64) -> Result<()> {
    client
        .query_one("SELECT pg_advisory_unlock($1)", &[&key])
        .map_err(|source| Error::postgres("release PostgreSQL advisory lock", source))?;
    Ok(())
}

pub(crate) fn release_shared_advisory_lock(client: &mut Client, key: i64) -> Result<()> {
    client
        .query_one("SELECT pg_advisory_unlock_shared($1)", &[&key])
        .map_err(|source| Error::postgres("release shared PostgreSQL advisory lock", source))?;
    Ok(())
}

pub(crate) fn create_database(
    client: &mut Client,
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

pub(crate) fn drop_database(client: &mut Client, database_name: &DatabaseName) -> Result<()> {
    client
        .batch_execute(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            database_name.quoted()
        ))
        .map_err(|source| Error::postgres("drop disposable PostgreSQL database", source))
}

pub(crate) fn database_exists(client: &mut Client, database_name: &DatabaseName) -> Result<bool> {
    client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&database_name.as_str()],
        )
        .map(|row| row.get(0))
        .map_err(|source| Error::postgres("check disposable PostgreSQL database", source))
}

pub(crate) fn database_comment(
    client: &mut Client,
    database_name: &DatabaseName,
) -> Result<Option<String>> {
    client
        .query_opt(
            "SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = $1",
            &[&database_name.as_str()],
        )
        .map(|row| row.and_then(|row| row.get(0)))
        .map_err(|source| Error::postgres("read disposable PostgreSQL database metadata", source))
}

pub(crate) fn set_database_metadata(
    client: &mut Client,
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
    client: &mut Client,
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
    client: &mut Client,
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

pub(crate) fn list_databases(client: &mut Client) -> Result<Vec<DatabaseRecord>> {
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
    use super::{AdminDatabaseUrl, advisory_key, quote_identifier, quote_literal};
    use crate::{FingerprintBuilder, ProjectName, name::DatabaseName};

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
    fn advisory_keys_are_domain_separated() {
        assert_ne!(
            advisory_key("run", "same"),
            advisory_key("template", "same")
        );
    }
}
