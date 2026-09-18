use std::{
    fmt,
    future::Future,
    str::FromStr,
    sync::{Condvar, Mutex},
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use tokio::{runtime::Runtime, task::JoinHandle};
use tokio_postgres::{NoTls, Row, types::ToSql};
use url::Url;

use crate::{
    Error, Result, admission::ManagedDatabaseCreationFailure, metadata::ResourceMetadata,
    name::DatabaseName,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
// Give PostgreSQL enough time to report its own `statement_timeout` error;
// this client deadline is the backstop for peers that stop responding.
const CLIENT_OPERATION_TIMEOUT_GRACE: Duration = Duration::from_secs(1);
const LOCK_TIMEOUT: Duration = Duration::from_secs(10);
const LIFECYCLE_APPLICATION_NAME_PREFIX: &str = "postgres-test-harness lifecycle:";

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

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DatabaseRecord {
    pub(crate) name: String,
    pub(crate) comment: Option<String>,
}

/// Tokio-backed PostgreSQL client with a synchronous internal interface.
///
/// Keeping the runtime here lets connection establishment place one deadline
/// around socket connection, PostgreSQL startup, and authentication, and lets
/// every subsequent request carry an end-to-end client deadline. All callers
/// already execute admin work through `spawn_blocking`.
pub(crate) struct AdminClient {
    client: tokio_postgres::Client,
    _connection: JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
    runtime: Runtime,
    request_timeout: Duration,
}

impl AdminClient {
    fn connect(
        admin_url: &AdminDatabaseUrl,
        connect_timeout: Duration,
        request_timeout: Duration,
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
            request_timeout,
        })
    }

    fn batch_execute(&mut self, operation: &'static str, query: &str) -> Result<()> {
        self.batch_execute_with_timeout(self.request_timeout, operation, query)
    }

    fn batch_execute_with_timeout(
        &mut self,
        timeout: Duration,
        operation: &'static str,
        query: &str,
    ) -> Result<()> {
        run_admin_operation(
            &self.runtime,
            timeout,
            operation,
            self.client.batch_execute(query),
        )
    }

    fn query(
        &mut self,
        operation: &'static str,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        run_admin_operation(
            &self.runtime,
            self.request_timeout,
            operation,
            self.client.query(query, params),
        )
    }

    fn query_one(
        &mut self,
        operation: &'static str,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row> {
        self.query_one_with_timeout(self.request_timeout, operation, query, params)
    }

    fn query_one_with_timeout(
        &mut self,
        timeout: Duration,
        operation: &'static str,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row> {
        run_admin_operation(
            &self.runtime,
            timeout,
            operation,
            self.client.query_one(query, params),
        )
    }

    fn query_opt(
        &mut self,
        operation: &'static str,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        run_admin_operation(
            &self.runtime,
            self.request_timeout,
            operation,
            self.client.query_opt(query, params),
        )
    }

    fn shutdown_background(self) {
        let Self {
            client,
            _connection: connection,
            runtime,
            request_timeout: _,
        } = self;
        drop(client);
        drop(connection);
        runtime.shutdown_background();
    }
}

fn run_admin_operation<T>(
    runtime: &Runtime,
    timeout: Duration,
    operation: &'static str,
    future: impl Future<Output = std::result::Result<T, tokio_postgres::Error>>,
) -> Result<T> {
    runtime
        .block_on(async { tokio::time::timeout(timeout, future).await })
        .map_err(|_| Error::PostgresOperationTimeout { operation, timeout })?
        .map_err(|source| Error::postgres(operation, source))
}

fn client_operation_timeout(operation_timeout: Duration) -> Duration {
    operation_timeout.saturating_add(CLIENT_OPERATION_TIMEOUT_GRACE)
}

/// Admin client retained across calls or by async code holding an advisory lock.
///
/// Dropping a Tokio runtime directly from another Tokio runtime panics. This
/// wrapper shuts down the client's private runtime without blocking.
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
        if let Some(client) = self.0.take() {
            client.shutdown_background();
        }
    }
}

/// Lazy, bounded pool for disposable database lifecycle work.
///
/// A checked-out client is removed from the state mutex before any connection
/// or PostgreSQL work runs. Failed or panicking operations never return their
/// session to the idle set because the resulting protocol and session state is
/// not known to be reusable.
pub(crate) struct AdminSessionPool {
    admin_url: AdminDatabaseUrl,
    operation_timeout: Duration,
    application_name: String,
    max_size: usize,
    state: Mutex<AdminSessionPoolState>,
    available: Condvar,
}

struct AdminSessionPoolState {
    idle: Vec<PersistentClient>,
    total: usize,
    closed: bool,
}

struct AdminSession<'a> {
    pool: &'a AdminSessionPool,
    client: Option<PersistentClient>,
    reusable: bool,
}

/// Explicit disposition of a completed operation's checked-out admin session.
///
/// The payload carries the operation outcome independently from whether the
/// session is safe to return to the idle pool.
pub(crate) enum AdminSessionDisposition<T> {
    Reuse(T),
    Evict(T),
}

#[derive(Clone, Copy)]
struct Deadline {
    started: Instant,
    timeout: Duration,
}

impl Deadline {
    fn new(timeout: Duration) -> Self {
        Self {
            started: Instant::now(),
            timeout,
        }
    }

    fn remaining(self) -> Option<Duration> {
        self.timeout
            .checked_sub(self.started.elapsed())
            .filter(|remaining| !remaining.is_zero())
    }

    fn is_elapsed(self) -> bool {
        self.started.elapsed() >= self.timeout
    }

    const fn timeout(self) -> Duration {
        self.timeout
    }
}

fn checkout_remaining(deadline: Deadline) -> Result<Duration> {
    deadline
        .remaining()
        .ok_or(Error::AdminSessionCheckoutTimeout {
            timeout: deadline.timeout(),
        })
}

fn checkout_timeout(deadline: Deadline) -> Error {
    Error::AdminSessionCheckoutTimeout {
        timeout: deadline.timeout(),
    }
}

impl AdminSessionPool {
    pub(crate) fn new(
        admin_url: AdminDatabaseUrl,
        operation_timeout: Duration,
        project: &str,
        max_size: usize,
    ) -> Self {
        debug_assert!(max_size > 0);
        Self {
            admin_url,
            operation_timeout,
            application_name: format!("{LIFECYCLE_APPLICATION_NAME_PREFIX}{project}"),
            max_size: max_size.max(1),
            state: Mutex::new(AdminSessionPoolState {
                idle: Vec::new(),
                total: 0,
                closed: false,
            }),
            available: Condvar::new(),
        }
    }

    pub(crate) fn execute<T>(
        &self,
        connect_operation: &'static str,
        operation: impl FnOnce(&mut AdminClient) -> Result<T>,
    ) -> Result<T> {
        self.execute_with_disposition(connect_operation, |client| match operation(client) {
            Ok(value) => AdminSessionDisposition::Reuse(Ok(value)),
            Err(error) => AdminSessionDisposition::Evict(Err(error)),
        })?
    }

    /// Runs an operation whose result and session disposition are independent.
    pub(crate) fn execute_with_disposition<T>(
        &self,
        connect_operation: &'static str,
        operation: impl FnOnce(&mut AdminClient) -> AdminSessionDisposition<T>,
    ) -> Result<T> {
        let mut session = self.checkout(connect_operation)?;
        match operation(session.client_mut()) {
            AdminSessionDisposition::Reuse(value) => {
                session.reusable = true;
                Ok(value)
            }
            AdminSessionDisposition::Evict(value) => Ok(value),
        }
    }

    #[cfg(feature = "containers")]
    pub(crate) fn close(&self) {
        let idle = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.total = state
                .total
                .checked_sub(state.idle.len())
                .expect("idle admin sessions are included in the pool total");
            let idle = std::mem::take(&mut state.idle);
            self.available.notify_all();
            idle
        };
        drop(idle);
    }

    fn checkout(&self, connect_operation: &'static str) -> Result<AdminSession<'_>> {
        let deadline = Deadline::new(self.operation_timeout);
        loop {
            match self.acquire_checkout(deadline)? {
                Some(mut client) => {
                    if prepare_reused_lifecycle_session(
                        client.client_mut(),
                        self.operation_timeout,
                        &self.application_name,
                        deadline,
                    )
                    .is_err()
                    {
                        drop(client);
                        self.release_slot();
                        if deadline.is_elapsed() {
                            return Err(checkout_timeout(deadline));
                        }
                        continue;
                    }
                    return Ok(AdminSession {
                        pool: self,
                        client: Some(client),
                        reusable: false,
                    });
                }
                None => {
                    let connected = AdminClient::connect(
                        &self.admin_url,
                        CONNECT_TIMEOUT,
                        client_operation_timeout(self.operation_timeout),
                        connect_operation,
                    )
                    .map(PersistentClient::new)
                    .and_then(|mut client| {
                        prepare_new_lifecycle_session(
                            client.client_mut(),
                            self.operation_timeout,
                            &self.application_name,
                        )?;
                        Ok(client)
                    });
                    match connected {
                        Ok(client) => {
                            return Ok(AdminSession {
                                pool: self,
                                client: Some(client),
                                reusable: false,
                            });
                        }
                        Err(error) => {
                            self.release_slot();
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    fn acquire_checkout(&self, deadline: Deadline) -> Result<Option<PersistentClient>> {
        let mut state = self.state.lock().map_err(|_| Error::StatePoisoned {
            operation: "lock disposable admin-session pool",
        })?;
        loop {
            if state.closed {
                return Err(Error::AdminSessionPoolClosed);
            }
            if let Some(client) = state.idle.pop() {
                return Ok(Some(client));
            }
            if state.total < self.max_size {
                state.total += 1;
                return Ok(None);
            }
            let remaining = checkout_remaining(deadline)?;
            (state, _) = self.available.wait_timeout(state, remaining).map_err(|_| {
                Error::StatePoisoned {
                    operation: "wait for disposable admin session",
                }
            })?;
        }
    }

    fn release_slot(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.total = state
            .total
            .checked_sub(1)
            .expect("a reserved admin-session slot exists until release");
        self.available.notify_one();
    }
}

impl AdminSession<'_> {
    fn client_mut(&mut self) -> &mut AdminClient {
        self.client
            .as_mut()
            .expect("checked-out admin session exists until drop")
            .client_mut()
    }
}

impl Drop for AdminSession<'_> {
    fn drop(&mut self) {
        let client = self
            .client
            .take()
            .expect("checked-out admin session is returned at most once");
        let mut state = self
            .pool
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.reusable && !state.closed {
            state.idle.push(client);
            self.pool.available.notify_one();
            return;
        }
        state.total = state
            .total
            .checked_sub(1)
            .expect("a checked-out admin session is included in the pool total");
        self.pool.available.notify_one();
        drop(state);
        drop(client);
    }
}

pub(crate) fn connect_admin(
    admin_url: &AdminDatabaseUrl,
    operation_timeout: Duration,
    operation: &'static str,
) -> Result<AdminClient> {
    connect_admin_with_timeout(admin_url, operation_timeout, operation, CONNECT_TIMEOUT)
}

pub(crate) fn connect_admin_with_timeout(
    admin_url: &AdminDatabaseUrl,
    operation_timeout: Duration,
    operation: &'static str,
    connect_timeout: Duration,
) -> Result<AdminClient> {
    let mut client = AdminClient::connect(
        admin_url,
        connect_timeout,
        client_operation_timeout(operation_timeout),
        operation,
    )?;
    configure_session_timeouts(&mut client, operation_timeout)?;
    Ok(client)
}

/// Connect and configure an admin session within one end-to-end time budget.
///
/// The connect timeout still bounds an individual socket/authentication
/// attempt, while `setup_timeout` bounds that attempt together with the
/// session-configuration query that makes the client ready for use.
#[cfg(any(feature = "containers", test))]
pub(crate) fn connect_admin_with_setup_timeout(
    admin_url: &AdminDatabaseUrl,
    operation_timeout: Duration,
    operation: &'static str,
    connect_timeout: Duration,
    setup_timeout: Duration,
) -> Result<AdminClient> {
    let deadline = Deadline::new(setup_timeout);
    let connect_timeout =
        connect_timeout.min(deadline.remaining().ok_or(Error::PostgresConnectTimeout {
            operation,
            timeout: setup_timeout,
        })?);
    let mut client = AdminClient::connect(
        admin_url,
        connect_timeout,
        client_operation_timeout(operation_timeout),
        operation,
    )?;
    let configure_timeout = deadline
        .remaining()
        .ok_or(Error::PostgresOperationTimeout {
            operation: "configure admin session timeouts",
            timeout: setup_timeout,
        })?;
    configure_session_timeouts_with_timeout(&mut client, operation_timeout, configure_timeout)?;
    Ok(client)
}

fn configure_session_timeouts(client: &mut AdminClient, operation_timeout: Duration) -> Result<()> {
    let request_timeout = client.request_timeout;
    configure_session_timeouts_with_timeout(client, operation_timeout, request_timeout)
}

fn configure_session_timeouts_with_timeout(
    client: &mut AdminClient,
    operation_timeout: Duration,
    request_timeout: Duration,
) -> Result<()> {
    let statement_timeout = duration_millis(operation_timeout)?;
    let lock_timeout = duration_millis(LOCK_TIMEOUT.min(operation_timeout))?;
    client.batch_execute_with_timeout(
        request_timeout,
        "configure admin session timeouts",
        &format!("SET statement_timeout = {statement_timeout}; SET lock_timeout = {lock_timeout};"),
    )
}

fn prepare_new_lifecycle_session(
    client: &mut AdminClient,
    operation_timeout: Duration,
    application_name: &str,
) -> Result<()> {
    configure_lifecycle_session(client, operation_timeout, application_name)
}

fn prepare_reused_lifecycle_session(
    client: &mut AdminClient,
    operation_timeout: Duration,
    application_name: &str,
    checkout_deadline: Deadline,
) -> Result<()> {
    client.batch_execute_with_timeout(
        checkout_remaining(checkout_deadline)?,
        "reset pooled admin session",
        "DISCARD ALL",
    )?;
    configure_lifecycle_session_with_timeout(
        client,
        operation_timeout,
        application_name,
        checkout_remaining(checkout_deadline)?,
    )
}

fn configure_lifecycle_session(
    client: &mut AdminClient,
    operation_timeout: Duration,
    application_name: &str,
) -> Result<()> {
    let request_timeout = client.request_timeout;
    configure_lifecycle_session_with_timeout(
        client,
        operation_timeout,
        application_name,
        request_timeout,
    )
}

fn configure_lifecycle_session_with_timeout(
    client: &mut AdminClient,
    operation_timeout: Duration,
    application_name: &str,
    request_timeout: Duration,
) -> Result<()> {
    let statement_timeout = duration_millis(operation_timeout)?;
    let lock_timeout = duration_millis(LOCK_TIMEOUT.min(operation_timeout))?;
    client.batch_execute_with_timeout(
        request_timeout,
        "configure pooled admin session",
        &format!(
            "SET statement_timeout = {statement_timeout}; \
             SET lock_timeout = {lock_timeout}; \
             SET application_name = {}",
            quote_literal(application_name)
        ),
    )
}

fn configure_coordination_timeouts(client: &mut AdminClient, wait_timeout: Duration) -> Result<()> {
    let timeout = duration_millis(wait_timeout)?;
    client.batch_execute(
        "configure template coordination timeouts",
        &format!("SET statement_timeout = {timeout}; SET lock_timeout = {timeout};"),
    )
}

pub(crate) fn validate_postgres_18(client: &mut AdminClient) -> Result<()> {
    let server_version_num: i32 = client
        .query_one(
            "read PostgreSQL server version",
            "SELECT current_setting('server_version_num')::integer",
            &[],
        )?
        .get(0);
    if server_version_num / 10_000 != 18 {
        return Err(Error::UnsupportedPostgresVersion { server_version_num });
    }
    client
        .query_one(
            "check PostgreSQL uuidv7 capability",
            "SELECT uuidv7()::text",
            &[],
        )
        .map_err(|_| Error::MissingUuidV7)?;
    Ok(())
}

pub(crate) fn regular_connection_slots(client: &mut AdminClient) -> Result<usize> {
    let row = client.query_one(
        "read PostgreSQL connection capacity",
        "SELECT current_setting('max_connections')::integer, \
                    current_setting('reserved_connections')::integer, \
                    current_setting('superuser_reserved_connections')::integer",
        &[],
    )?;
    Ok(regular_connection_slots_from_settings(
        row.get(0),
        row.get(1),
        row.get(2),
    ))
}

fn regular_connection_slots_from_settings(
    max_connections: i32,
    reserved_connections: i32,
    superuser_reserved_connections: i32,
) -> usize {
    let max_connections = max_connections.max(0) as usize;
    let reserved_connections = reserved_connections.max(0) as usize;
    let superuser_reserved_connections = superuser_reserved_connections.max(0) as usize;
    max_connections
        .saturating_sub(reserved_connections)
        .saturating_sub(superuser_reserved_connections)
}

pub(crate) fn acquire_advisory_lock(client: &mut AdminClient, key: i64) -> Result<()> {
    client.query_one(
        "acquire PostgreSQL advisory lock",
        "SELECT pg_advisory_lock($1)",
        &[&key],
    )?;
    Ok(())
}

pub(crate) fn acquire_template_advisory_lock(
    client: &mut AdminClient,
    key: i64,
    wait_timeout: Duration,
    operation_timeout: Duration,
) -> Result<()> {
    configure_coordination_timeouts(client, wait_timeout)?;
    client.query_one_with_timeout(
        client_operation_timeout(wait_timeout),
        "acquire PostgreSQL template advisory lock",
        "SELECT pg_advisory_lock($1)",
        &[&key],
    )?;
    configure_session_timeouts(client, operation_timeout)
}

pub(crate) fn acquire_shared_template_advisory_lock(
    client: &mut AdminClient,
    key: i64,
    wait_timeout: Duration,
    operation_timeout: Duration,
) -> Result<()> {
    configure_coordination_timeouts(client, wait_timeout)?;
    client.query_one_with_timeout(
        client_operation_timeout(wait_timeout),
        "acquire shared PostgreSQL template advisory lock",
        "SELECT pg_advisory_lock_shared($1)",
        &[&key],
    )?;
    configure_session_timeouts(client, operation_timeout)
}

pub(crate) fn try_acquire_advisory_lock(client: &mut AdminClient, key: i64) -> Result<bool> {
    client
        .query_one(
            "try PostgreSQL advisory lock",
            "SELECT pg_try_advisory_lock($1)",
            &[&key],
        )
        .map(|row| row.get(0))
}

pub(crate) fn release_advisory_lock(client: &mut AdminClient, key: i64) -> Result<()> {
    client.query_one(
        "release PostgreSQL advisory lock",
        "SELECT pg_advisory_unlock($1)",
        &[&key],
    )?;
    Ok(())
}

pub(crate) fn release_shared_advisory_lock(client: &mut AdminClient, key: i64) -> Result<()> {
    client.query_one(
        "release shared PostgreSQL advisory lock",
        "SELECT pg_advisory_unlock_shared($1)",
        &[&key],
    )?;
    Ok(())
}

fn create_database(
    client: &mut AdminClient,
    database_name: &DatabaseName,
    template_name: &str,
) -> Result<()> {
    let template = quote_identifier(template_name)?;
    client.batch_execute(
        "create disposable PostgreSQL database",
        &format!(
            "CREATE DATABASE {} TEMPLATE {template}",
            database_name.quoted()
        ),
    )
}

/// Performs the raw PostgreSQL creation attempt.
///
/// Callers that create harness-managed databases must route this through the
/// server's [`DatabaseAdmission`](crate::admission::DatabaseAdmission) gate.
pub(crate) fn attempt_managed_database_creation(
    client: &mut AdminClient,
    database_name: &DatabaseName,
    template_name: &str,
    metadata: &ResourceMetadata,
) -> std::result::Result<(), ManagedDatabaseCreationFailure> {
    if let Err(error) = create_database(client, database_name, template_name) {
        return Err(classify_database_creation_failure(error));
    }
    if let Err(tagging) = set_database_metadata(client, database_name, metadata) {
        return Err(compensate_failed_metadata_write(tagging, || {
            drop_database(client, database_name)
        }));
    }
    Ok(())
}

fn classify_database_creation_failure(error: Error) -> ManagedDatabaseCreationFailure {
    if matches!(
        &error,
        Error::Postgres { source, .. } if source.as_db_error().is_some()
    ) {
        ManagedDatabaseCreationFailure::NoResidual(error)
    } else {
        // A client timeout, transport error, or runtime failure can lose the
        // server's completion response after CREATE DATABASE took effect.
        ManagedDatabaseCreationFailure::ResidualPossible(error)
    }
}

fn compensate_failed_metadata_write(
    tagging: Error,
    cleanup: impl FnOnce() -> Result<()>,
) -> ManagedDatabaseCreationFailure {
    match cleanup() {
        Ok(()) => ManagedDatabaseCreationFailure::NoResidual(tagging),
        Err(cleanup) => {
            ManagedDatabaseCreationFailure::ResidualPossible(Error::ManagedDatabaseTagAndCleanup {
                tagging: Box::new(tagging),
                cleanup: Box::new(cleanup),
            })
        }
    }
}

pub(crate) fn drop_database(client: &mut AdminClient, database_name: &DatabaseName) -> Result<()> {
    client.batch_execute(
        "drop disposable PostgreSQL database",
        &format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            database_name.quoted()
        ),
    )
}

pub(crate) fn find_database(
    client: &mut AdminClient,
    database_name: &DatabaseName,
) -> Result<Option<DatabaseRecord>> {
    client
        .query_opt(
            "find disposable PostgreSQL database",
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
}

pub(crate) fn set_database_metadata(
    client: &mut AdminClient,
    database_name: &DatabaseName,
    metadata: &ResourceMetadata,
) -> Result<()> {
    client.batch_execute(
        "write disposable PostgreSQL database metadata",
        &format!(
            "COMMENT ON DATABASE {} IS {}",
            database_name.quoted(),
            quote_literal(&metadata.encode())
        ),
    )
}

pub(crate) fn disable_database_connections(
    client: &mut AdminClient,
    database_name: &DatabaseName,
) -> Result<()> {
    client.batch_execute(
        "disable PostgreSQL template connections",
        &format!(
            "ALTER DATABASE {} ALLOW_CONNECTIONS false",
            database_name.quoted()
        ),
    )
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
    let rows = client.query(
        "terminate PostgreSQL template connections",
        "SELECT pg_terminate_backend(pid, $2::bigint) \
             FROM pg_stat_activity \
             WHERE datname = $1 AND pid <> pg_backend_pid()",
        &[&database_name.as_str(), &timeout],
    )?;
    if rows.iter().any(|row| !row.get::<_, bool>(0)) {
        return Err(Error::TemplateConnectionsRemain {
            database_name: database_name.as_str().to_owned(),
        });
    }

    let connections_remain: bool = client
        .query_one(
            "verify PostgreSQL template connections",
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = $1)",
            &[&database_name.as_str()],
        )?
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
            "list PostgreSQL databases for stale cleanup",
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
    use std::{
        io::{self, Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    use super::{
        AdminClient, AdminDatabaseUrl, AdminSessionPool, Deadline, PersistentClient, advisory_key,
        compensate_failed_metadata_write, connect_admin_with_setup_timeout,
        connect_admin_with_timeout, quote_identifier, quote_literal,
        regular_connection_slots_from_settings,
    };
    use crate::{
        Error, FingerprintBuilder, ProjectName, admission::ManagedDatabaseCreationFailure,
        name::DatabaseName,
    };

    #[test]
    fn regular_connection_capacity_excludes_both_reserved_slot_classes() {
        assert_eq!(regular_connection_slots_from_settings(300, 0, 3), 297);
        assert_eq!(regular_connection_slots_from_settings(20, 5, 3), 12);
        assert_eq!(regular_connection_slots_from_settings(3, 0, 3), 0);
        assert_eq!(regular_connection_slots_from_settings(2, 4, 4), 0);
    }

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
            FingerprintBuilder::new("schema")
                .finish_root()
                .fingerprint(),
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
    fn admin_pool_checkout_wait_is_bounded_by_the_operation_timeout() {
        let timeout = Duration::from_millis(20);
        let pool = AdminSessionPool::new(
            AdminDatabaseUrl::parse("postgres://user@localhost/postgres").unwrap(),
            timeout,
            "checkout_timeout",
            1,
        );
        pool.state.lock().unwrap().total = 1;

        let started = Instant::now();
        let error = match pool.acquire_checkout(Deadline::new(timeout)) {
            Ok(_) => panic!("a saturated admin-session pool must not admit another checkout"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            Error::AdminSessionCheckoutTimeout { timeout: actual } if actual == timeout
        ));
        assert!(started.elapsed() >= timeout);
        pool.release_slot();
    }

    #[test]
    fn admin_pool_reuse_retries_share_one_checkout_deadline() {
        let timeout = Duration::from_millis(50);
        let (first, first_closed, first_server) = connect_stub_admin_client(timeout);
        let (second, second_closed, second_server) = connect_stub_admin_client(timeout);
        let pool = AdminSessionPool::new(
            AdminDatabaseUrl::parse("postgres://user@localhost/postgres").unwrap(),
            timeout,
            "reuse_timeout",
            2,
        );
        {
            let mut state = pool.state.lock().unwrap();
            state.idle = vec![PersistentClient::new(first), PersistentClient::new(second)];
            state.total = 2;
        }

        let error = match pool.checkout("connect after expired pooled sessions") {
            Ok(_) => panic!("silent pooled sessions must exhaust one checkout deadline"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            Error::AdminSessionCheckoutTimeout { timeout: actual } if actual == timeout
        ));
        assert_eq!(
            pool.state.lock().unwrap().idle.len(),
            1,
            "the checkout must stop before probing another idle session"
        );

        drop(pool);
        first_closed
            .recv_timeout(Duration::from_secs(1))
            .expect("first stub connection closes after its reset times out")
            .expect("first stub connection closes cleanly");
        second_closed
            .recv_timeout(Duration::from_secs(1))
            .expect("unprobed stub connection closes with the pool")
            .expect("second stub connection closes cleanly");
        first_server.join().unwrap();
        second_server.join().unwrap();
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

    fn spawn_stub_admin_server() -> (
        AdminDatabaseUrl,
        mpsc::Receiver<io::Result<()>>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (closed_sender, closed_receiver) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut startup_length = [0; 4];
            stream.read_exact(&mut startup_length).unwrap();
            let startup_length = u32::from_be_bytes(startup_length) as usize;
            let mut startup = vec![0; startup_length - 4];
            stream.read_exact(&mut startup).unwrap();
            stream
                .write_all(&[
                    b'R', 0, 0, 0, 8, 0, 0, 0, 0, b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2, b'Z',
                    0, 0, 0, 5, b'I',
                ])
                .unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut remaining = Vec::new();
            let result = stream.read_to_end(&mut remaining).map(|_| ());
            let _ = closed_sender.send(result);
        });
        let admin = AdminDatabaseUrl::parse(&format!(
            "postgres://user@127.0.0.1:{port}/postgres?sslmode=disable"
        ))
        .unwrap();
        (admin, closed_receiver, server)
    }

    fn connect_stub_admin_client(
        operation_timeout: Duration,
    ) -> (
        AdminClient,
        mpsc::Receiver<io::Result<()>>,
        thread::JoinHandle<()>,
    ) {
        let (admin, closed_receiver, server) = spawn_stub_admin_server();
        let client = AdminClient::connect(
            &admin,
            Duration::from_secs(1),
            operation_timeout,
            "connect to stub PostgreSQL server",
        )
        .unwrap();
        (client, closed_receiver, server)
    }

    #[test]
    fn admin_setup_deadline_covers_session_configuration() {
        let timeout = Duration::from_millis(50);
        let (admin, closed, server) = spawn_stub_admin_server();
        let started = Instant::now();

        let error = match connect_admin_with_setup_timeout(
            &admin,
            Duration::from_secs(1),
            "connect before bounded session configuration",
            Duration::from_secs(1),
            timeout,
        ) {
            Ok(_) => panic!("silent PostgreSQL peer must not complete session configuration"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            Error::PostgresOperationTimeout {
                operation: "configure admin session timeouts",
                ..
            }
        ));
        assert!(started.elapsed() >= timeout);
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "session configuration must use the end-to-end setup budget"
        );
        closed
            .recv_timeout(Duration::from_secs(1))
            .expect("timed-out admin connection should close when dropped")
            .expect("stub PostgreSQL connection should close cleanly");
        server.join().unwrap();
    }

    #[test]
    fn admin_operation_deadline_covers_a_silent_postgres_peer() {
        let timeout = Duration::from_millis(50);
        let (mut client, closed, server) = connect_stub_admin_client(timeout);

        let error = match client.query_one(
            "run query against silent PostgreSQL test server",
            "SELECT 1",
            &[],
        ) {
            Ok(_) => panic!("silent PostgreSQL peer must not complete a query"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            Error::PostgresOperationTimeout {
                operation: "run query against silent PostgreSQL test server",
                timeout: actual,
            } if actual == timeout
        ));
        drop(client);
        closed
            .recv_timeout(Duration::from_secs(1))
            .expect("timed-out admin connection should close when dropped")
            .expect("stub PostgreSQL connection should close cleanly");
        server.join().unwrap();
    }

    fn assert_persistent_client_drop_is_safe(runtime: tokio::runtime::Runtime) {
        let (client, closed, server) = connect_stub_admin_client(Duration::from_secs(1));

        runtime.block_on(async move {
            drop(PersistentClient::new(client));
        });

        closed
            .recv_timeout(Duration::from_secs(1))
            .expect("admin connection should close during background runtime shutdown")
            .expect("stub PostgreSQL connection should close cleanly");
        server.join().unwrap();
    }

    #[test]
    fn persistent_client_drops_inside_a_current_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        assert_persistent_client_drop_is_safe(runtime);
    }

    #[test]
    fn persistent_client_drops_inside_a_multithread_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        assert_persistent_client_drop_is_safe(runtime);
    }

    #[test]
    fn advisory_keys_match_known_vectors() {
        assert_eq!(advisory_key("run", "same"), -8_116_341_416_320_315_028);
        assert_eq!(advisory_key("template", "same"), -4_995_278_210_640_012_122);
        assert_eq!(
            advisory_key("template-coordination", "same"),
            -2_684_819_783_531_546_151
        );
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
            ManagedDatabaseCreationFailure::NoResidual(Error::InvalidConfiguration {
                reason: "tagging"
            })
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
            ManagedDatabaseCreationFailure::ResidualPossible(
                Error::ManagedDatabaseTagAndCleanup { tagging, cleanup }
            )
                if matches!(*tagging, Error::InvalidConfiguration { reason: "tagging" })
                    && matches!(*cleanup, Error::InvalidConfiguration { reason: "cleanup" })
        ));
    }
}
