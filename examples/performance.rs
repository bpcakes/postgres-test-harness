//! Reproducible end-to-end performance characterization for the public API.
//!
//! This is deliberately an example rather than a `#[bench]` target: every
//! observation needs a real PostgreSQL server and includes lifecycle work that
//! a microbenchmark harness cannot isolate usefully. The program writes one
//! JSON document to stdout and progress plus a compact summary to stderr.

use std::{
    collections::BTreeMap,
    env,
    error::Error as StdError,
    fmt,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use postgres_test_harness::{
    BoxError, CleanupReport, FingerprintBuilder, HarnessConfig, OwnedContainerProfile,
    POSTGRES_TEST_ADMIN_URL_ENV, POSTGRES_TEST_IMAGE_ENV, PostgresHarness, TemplateSpec,
    cleanup_stale_databases,
};
use serde::Serialize;
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const SCHEMA_VERSION: u32 = 8;
const DEFAULT_IMAGE: &str = "postgres:18";
const OUTPUT_ENV: &str = "PTH_PERF_OUTPUT";
const OWNED_INITDB_NO_SYNC_ENV: &str = "PTH_PERF_OWNED_INITDB_NO_SYNC";
const OWNED_TMPFS_SIZE_BYTES_ENV: &str = "PTH_PERF_OWNED_TMPFS_SIZE_BYTES";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(90);
const TEMPLATE_WAIT_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const DEFAULT_CONNECTION_BUDGET: usize = 120;
const DEFAULT_DOWNSTREAM_POOL_SIZE: usize = 10;
const DEFAULT_PREWARM_DATABASES: usize = 4;
const DEFAULT_SAMPLES: usize = 3;
const DEFAULT_SEQUENTIAL_OPERATIONS: usize = 4;
const DEFAULT_CONCURRENCY: usize = 4;
const DEFAULT_CONCURRENT_OPERATIONS: usize = 8;
const DEFAULT_DRAIN_DATABASES: usize = 4;
const DEFAULT_REPRESENTATIVE_ROWS: usize = 50_000;
const DEFERRED_DRAIN_TIMEOUT: Duration = Duration::from_secs(120);
const EXTERNAL_CLEANUP_RETRY_WINDOW: Duration = Duration::from_secs(120);
const EXTERNAL_CLEANUP_INITIAL_RETRY_INTERVAL: Duration = Duration::from_millis(25);
const EXTERNAL_CLEANUP_MAX_RETRY_INTERVAL: Duration = Duration::from_secs(1);

type AnyError = Box<dyn StdError + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

#[derive(Debug)]
struct OperationAndCleanupError {
    operation: AnyError,
    cleanup: AnyError,
}

#[derive(Clone, Copy)]
struct Deadline(tokio::time::Instant);

struct RetryBackoff {
    next: Duration,
    maximum: Duration,
}

impl Deadline {
    fn after(duration: Duration) -> Self {
        Self(tokio::time::Instant::now() + duration)
    }

    fn reached(self) -> bool {
        tokio::time::Instant::now() >= self.0
    }

    async fn run<F>(self, future: F) -> std::result::Result<F::Output, tokio::time::error::Elapsed>
    where
        F: Future,
    {
        tokio::time::timeout_at(self.0, future).await
    }

    async fn sleep_up_to(self, duration: Duration) {
        let wake_at = (tokio::time::Instant::now() + duration).min(self.0);
        tokio::time::sleep_until(wake_at).await;
    }
}

impl RetryBackoff {
    fn new(initial: Duration, maximum: Duration) -> Self {
        debug_assert!(!initial.is_zero());
        debug_assert!(initial <= maximum);
        Self {
            next: initial,
            maximum,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.maximum);
        delay
    }
}

impl fmt::Display for OperationAndCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; cleanup also failed: {}",
            self.operation, self.cleanup
        )
    }
}

impl StdError for OperationAndCleanupError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.operation.as_ref())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ServerMode {
    Owned,
    External,
}

#[derive(Clone, Copy)]
enum ExternalCleanupValidation {
    CompletedSample { expected_templates: usize },
    FailedSample,
}

impl ServerMode {
    fn detect() -> AnyResult<Self> {
        Self::from_admin_url_environment(env::var(POSTGRES_TEST_ADMIN_URL_ENV))
    }

    fn from_admin_url_environment(
        admin_url: std::result::Result<String, env::VarError>,
    ) -> AnyResult<Self> {
        match admin_url {
            Ok(_) => Ok(Self::External),
            Err(env::VarError::NotPresent) => Ok(Self::Owned),
            Err(env::VarError::NotUnicode(_)) => Err(invalid_input(format!(
                "{POSTGRES_TEST_ADMIN_URL_ENV} must be valid UTF-8"
            ))),
        }
    }

    fn from_harness(harness: &PostgresHarness) -> Self {
        if harness.is_external() {
            Self::External
        } else {
            Self::Owned
        }
    }

    fn ensure_matches(self, actual: Self) -> AnyResult<()> {
        if self == actual {
            return Ok(());
        }
        Err(io::Error::other(format!(
            "requested {} benchmark mode, but the started harness is {}",
            self.as_str(),
            actual.as_str()
        ))
        .into())
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Owned => "owned",
            Self::External => "external",
        }
    }
}

#[derive(Clone, Debug)]
struct BenchmarkConfig {
    project: String,
    mode: ServerMode,
    image: String,
    owned_container_profile: OwnedContainerProfile,
    output: Option<PathBuf>,
    samples: usize,
    sequential_operations: usize,
    concurrency: usize,
    concurrent_operations: usize,
    drain_databases: usize,
    representative_rows: usize,
    connection_budget: usize,
    connections_per_database_override: Option<u32>,
    downstream_pool_size: usize,
    prewarm_databases: usize,
}

impl BenchmarkConfig {
    fn from_environment(project: String) -> AnyResult<Self> {
        let mode = ServerMode::detect()?;
        let connections_per_database_override =
            optional_positive_env("PTH_PERF_CONNECTIONS_PER_DATABASE")?
                .map(|value| {
                    u32::try_from(value).map_err(|_| {
                        invalid_input(
                            "PTH_PERF_CONNECTIONS_PER_DATABASE must fit in a positive u32",
                        )
                    })
                })
                .transpose()?;
        let config = Self {
            project,
            mode,
            image: env::var(POSTGRES_TEST_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_IMAGE.to_owned()),
            owned_container_profile: match mode {
                ServerMode::Owned => owned_container_profile_from_environment()?,
                ServerMode::External => OwnedContainerProfile::default(),
            },
            output: env::var_os(OUTPUT_ENV).map(PathBuf::from),
            samples: positive_env("PTH_PERF_SAMPLES", DEFAULT_SAMPLES)?,
            sequential_operations: positive_env(
                "PTH_PERF_SEQUENTIAL_OPERATIONS",
                DEFAULT_SEQUENTIAL_OPERATIONS,
            )?,
            concurrency: positive_env("PTH_PERF_CONCURRENCY", DEFAULT_CONCURRENCY)?,
            concurrent_operations: positive_env(
                "PTH_PERF_CONCURRENT_OPERATIONS",
                DEFAULT_CONCURRENT_OPERATIONS,
            )?,
            drain_databases: positive_env("PTH_PERF_DRAIN_DATABASES", DEFAULT_DRAIN_DATABASES)?,
            representative_rows: positive_env(
                "PTH_PERF_REPRESENTATIVE_ROWS",
                DEFAULT_REPRESENTATIVE_ROWS,
            )?,
            connection_budget: positive_env(
                "PTH_PERF_CONNECTION_BUDGET",
                DEFAULT_CONNECTION_BUDGET,
            )?,
            connections_per_database_override,
            downstream_pool_size: positive_env(
                "PTH_PERF_DOWNSTREAM_POOL_SIZE",
                DEFAULT_DOWNSTREAM_POOL_SIZE,
            )?,
            prewarm_databases: positive_env(
                "PTH_PERF_PREWARM_DATABASES",
                DEFAULT_PREWARM_DATABASES,
            )?,
        };

        let limits = config.harness_config()?.connection_limits()?;
        let database_capacity = limits.max_simultaneous_leases();
        if config.concurrency > database_capacity {
            return Err(invalid_input(format!(
                "PTH_PERF_CONCURRENCY={} exceeds the configured database capacity of {database_capacity}",
                config.concurrency
            )));
        }
        if config.drain_databases > database_capacity {
            return Err(invalid_input(format!(
                "PTH_PERF_DRAIN_DATABASES={} exceeds the configured database capacity of {database_capacity}",
                config.drain_databases
            )));
        }
        if config.prewarm_databases > database_capacity {
            return Err(invalid_input(format!(
                "PTH_PERF_PREWARM_DATABASES={} exceeds the configured database capacity of {database_capacity}",
                config.prewarm_databases
            )));
        }
        if config.prewarm_databases < 2 {
            return Err(invalid_input(
                "PTH_PERF_PREWARM_DATABASES must be at least 2 to report steady-state lease latency",
            ));
        }
        if config.concurrent_operations < config.concurrency {
            return Err(invalid_input(format!(
                "PTH_PERF_CONCURRENT_OPERATIONS={} must be at least PTH_PERF_CONCURRENCY={}",
                config.concurrent_operations, config.concurrency
            )));
        }
        if config.downstream_pool_size > usize::try_from(limits.connections_per_database())? {
            return Err(invalid_input(format!(
                "PTH_PERF_DOWNSTREAM_POOL_SIZE={} exceeds PTH_PERF_CONNECTIONS_PER_DATABASE={}; per-database permits must cover every eagerly opened downstream connection",
                config.downstream_pool_size,
                limits.connections_per_database()
            )));
        }
        Ok(config)
    }

    fn harness_config(&self) -> AnyResult<HarnessConfig> {
        let config = HarnessConfig::new(self.project.clone())?
            .with_startup_timeout(STARTUP_TIMEOUT)?
            .with_operation_timeout(OPERATION_TIMEOUT)?
            .with_template_wait_timeout(TEMPLATE_WAIT_TIMEOUT)?
            .with_connection_budget(self.connection_budget)?
            .with_cleanup_on_start(false);
        let config = match self.connections_per_database_override {
            Some(connections_per_database) => {
                config.with_connections_per_database(connections_per_database)?
            }
            None => config,
        };
        match self.mode {
            ServerMode::Owned => Ok(config
                .with_image(self.image.clone())?
                .with_owned_container_profile(self.owned_container_profile)),
            ServerMode::External => Ok(config),
        }
    }

    fn report(&self) -> ConfigurationReport {
        let limits = self
            .harness_config()
            .and_then(|config| config.connection_limits().map_err(Into::into))
            .expect("validated benchmark connection limits remain resolvable");
        ConfigurationReport {
            project: self.project.clone(),
            samples: self.samples,
            sequential_operations: self.sequential_operations,
            concurrency: self.concurrency,
            concurrent_operations: self.concurrent_operations,
            drain_databases: self.drain_databases,
            representative_rows: self.representative_rows,
            startup_timeout_ms: millis(STARTUP_TIMEOUT),
            operation_timeout_ms: millis(OPERATION_TIMEOUT),
            template_wait_timeout_ms: millis(TEMPLATE_WAIT_TIMEOUT),
            deferred_drain_timeout_ms: millis(DEFERRED_DRAIN_TIMEOUT),
            external_cleanup_retry_window_ms: millis(EXTERNAL_CLEANUP_RETRY_WINDOW),
            external_cleanup_initial_retry_interval_ms: millis(
                EXTERNAL_CLEANUP_INITIAL_RETRY_INTERVAL,
            ),
            external_cleanup_max_retry_interval_ms: millis(EXTERNAL_CLEANUP_MAX_RETRY_INTERVAL),
            connection_budget: limits.connection_budget(),
            connections_per_database: limits.connections_per_database(),
            max_simultaneous_leases: limits.max_simultaneous_leases(),
            downstream_pool_size: self.downstream_pool_size,
            prewarm_databases: self.prewarm_databases,
            cleanup_on_start: false,
            owned_container_profile: matches!(self.mode, ServerMode::Owned)
                .then(|| OwnedContainerProfileReport::from(self.owned_container_profile)),
        }
    }
}

#[derive(Serialize)]
struct BenchmarkReport {
    schema_version: u32,
    generated_at_unix_ms: u128,
    source: SourceReport,
    environment: EnvironmentReport,
    configuration: ConfigurationReport,
    samples: Vec<SampleReport>,
    summary: Vec<SummaryReport>,
    notes: Vec<&'static str>,
}

#[derive(Serialize)]
struct SourceReport {
    git_commit: Option<String>,
    git_worktree_dirty: Option<bool>,
}

#[derive(Serialize)]
struct EnvironmentReport {
    mode: ServerMode,
    image_reference: Option<String>,
    image_content_id: MetadataValue,
    storage_driver: MetadataValue,
    postgres_version: String,
    postgres_version_num: i32,
    postgres_settings: PostgresSettingsReport,
    cpu_count: usize,
    operating_system: &'static str,
    architecture: &'static str,
    readiness_contract: &'static str,
    admin_session_counter_scope: &'static str,
}

#[derive(Serialize)]
struct MetadataValue {
    value: Option<String>,
    source: &'static str,
}

#[derive(Serialize)]
struct ConfigurationReport {
    project: String,
    samples: usize,
    sequential_operations: usize,
    concurrency: usize,
    concurrent_operations: usize,
    drain_databases: usize,
    representative_rows: usize,
    startup_timeout_ms: u128,
    operation_timeout_ms: u128,
    template_wait_timeout_ms: u128,
    deferred_drain_timeout_ms: u128,
    external_cleanup_retry_window_ms: u128,
    external_cleanup_initial_retry_interval_ms: u128,
    external_cleanup_max_retry_interval_ms: u128,
    connection_budget: usize,
    connections_per_database: u32,
    max_simultaneous_leases: usize,
    downstream_pool_size: usize,
    prewarm_databases: usize,
    cleanup_on_start: bool,
    owned_container_profile: Option<OwnedContainerProfileReport>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct OwnedContainerProfileReport {
    initdb_no_sync: bool,
    storage: OwnedContainerStorageReport,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum OwnedContainerStorageReport {
    Tmpfs { size_bytes: u64 },
    ImageDefault,
}

impl From<OwnedContainerProfile> for OwnedContainerProfileReport {
    fn from(profile: OwnedContainerProfile) -> Self {
        Self {
            initdb_no_sync: profile.initdb_no_sync(),
            storage: profile
                .tmpfs_size_bytes()
                .map_or(OwnedContainerStorageReport::ImageDefault, |size_bytes| {
                    OwnedContainerStorageReport::Tmpfs { size_bytes }
                }),
        }
    }
}

#[derive(Serialize)]
struct SampleReport {
    sample: usize,
    server_startup: StartupReport,
    readiness: ReadinessReport,
    fixtures: Vec<FixtureReport>,
    external_stale_cleanup: Option<ExternalCleanupReport>,
}

#[derive(Serialize)]
struct StartupReport {
    elapsed_ns: u128,
    admin_sessions_after_observer_connect: SessionSnapshotReport,
}

#[derive(Serialize)]
struct ReadinessReport {
    postmaster_started_at: String,
    container_image_content_id: Option<String>,
    postgres_version: String,
    postgres_version_num: i32,
    postgres_settings: PostgresSettingsReport,
}

#[derive(Serialize)]
struct FixtureReport {
    name: &'static str,
    requested_rows: usize,
    template_size_bytes: i64,
    cold_template_acquisition: TimedOperation,
    warm_template_acquisition: TimedOperation,
    sequential_clone_cleanup: BatchOperation,
    bounded_concurrent_clone_cleanup: BatchOperation,
    downstream_pool_checkout_spike: DownstreamPoolCheckoutOperation,
    prewarmed_database_queue: PrewarmedDatabaseQueueOperation,
    explicit_cleanup_drain: BatchOperation,
    deferred_cleanup: DeferredCleanupOperation,
}

#[derive(Serialize)]
struct TimedOperation {
    elapsed_ns: u128,
    admin_sessions: SessionDelta,
}

#[derive(Serialize)]
struct BatchOperation {
    operations: usize,
    method: BatchMethodReport,
    elapsed_ns: u128,
    operations_per_second: f64,
    individual_elapsed_ns: Vec<u128>,
    admin_sessions: SessionDelta,
}

#[derive(Serialize)]
struct DeferredCleanupOperation {
    operations: usize,
    method: BatchMethodReport,
    caller_return_elapsed_ns: u128,
    caller_returns_per_second: f64,
    individual_caller_return_elapsed_ns: Vec<u128>,
    final_drain_elapsed_ns: u128,
    total_elapsed_ns: u128,
    admin_sessions: SessionDelta,
}

#[derive(Serialize)]
struct DownstreamPoolCheckoutOperation {
    leases: usize,
    eager_connections_per_lease: usize,
    eager_connections_total: usize,
    lease_and_connection_checkout_elapsed_ns: u128,
    eager_connections_per_second: f64,
    observed_application_sessions_at_peak: i64,
    cleanup_elapsed_ns: u128,
    total_elapsed_ns: u128,
    admin_sessions: SessionDelta,
}

#[derive(Serialize)]
struct PrewarmedDatabaseQueueOperation {
    capacity: usize,
    initial_fill_elapsed_ns: u128,
    first_lease_elapsed_ns: u128,
    steady_state_lease_elapsed_ns: Vec<u128>,
    steady_state_median_elapsed_ns: u128,
    initial_ready_storage_growth_bytes: i64,
    refill_elapsed_ns: u128,
    refill_databases_per_second: f64,
    ready_after_refill: usize,
    cleanup_elapsed_ns: u128,
    total_elapsed_ns: u128,
    admin_sessions: SessionDelta,
}

#[derive(Serialize)]
struct BatchMethodReport {
    execution: BatchExecutionReport,
    completion: CompletionObservationReport,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BatchExecutionReport {
    CallerBounded { concurrency: usize },
    ImplementationManaged,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CompletionObservationReport {
    OperationReturn,
    AwaitedDrainBarrier,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct PostgresSettingsReport {
    fsync: bool,
    synchronous_commit: String,
    full_page_writes: bool,
    data_checksums: bool,
    wal_level: String,
    max_connections: i32,
    reserved_connections: i32,
    superuser_reserved_connections: i32,
}

#[derive(Clone, Copy, Serialize)]
struct SessionSnapshotReport {
    cumulative: i64,
    active: i64,
}

impl BatchMethodReport {
    fn caller_bounded(concurrency: usize) -> Self {
        Self {
            execution: BatchExecutionReport::CallerBounded { concurrency },
            completion: CompletionObservationReport::OperationReturn,
        }
    }

    fn implementation_managed_barrier() -> Self {
        Self {
            execution: BatchExecutionReport::ImplementationManaged,
            completion: CompletionObservationReport::AwaitedDrainBarrier,
        }
    }
}

#[derive(Clone, Copy, Serialize)]
struct SessionDelta {
    before: i64,
    after: i64,
    delta: i64,
    active_after: i64,
}

#[derive(Serialize)]
struct ExternalCleanupReport {
    elapsed_ns: u128,
    attempts: Vec<CleanupAttemptReport>,
    dropped_test_databases: usize,
    dropped_templates: usize,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct CleanupAttemptReport {
    dropped_test_databases: usize,
    dropped_templates: usize,
    skipped_active: usize,
    skipped_fresh: usize,
    skipped_unrecognized: usize,
}

#[derive(Debug, Serialize)]
struct SummaryReport {
    metric: String,
    fixture: Option<&'static str>,
    observations: usize,
    min_ns: u128,
    median_ns: u128,
    mean_ns: u128,
    max_ns: u128,
}

#[derive(Clone, Copy)]
struct SessionSnapshot {
    cumulative: i64,
    active: i64,
}

impl SessionSnapshot {
    fn delta(self, after: Self) -> SessionDelta {
        SessionDelta {
            before: self.cumulative,
            after: after.cumulative,
            delta: after.cumulative - self.cumulative,
            active_after: after.active,
        }
    }

    fn report(self) -> SessionSnapshotReport {
        SessionSnapshotReport {
            cumulative: self.cumulative,
            active: self.active,
        }
    }
}

struct ServerMetadata {
    version: String,
    version_num: i32,
    postmaster_started_at: String,
    settings: PostgresSettingsReport,
}

impl ServerMetadata {
    fn ensure_comparable_environment(&self, other: &Self) -> AnyResult<()> {
        if self.version == other.version
            && self.version_num == other.version_num
            && self.settings == other.settings
        {
            return Ok(());
        }
        Err(io::Error::other(format!(
            "PostgreSQL environment changed between observations: version {:?} ({}) with settings {:?} became {:?} ({}) with settings {:?}",
            self.version,
            self.version_num,
            self.settings,
            other.version,
            other.version_num,
            other.settings,
        ))
        .into())
    }
}

struct SampleMeasurements {
    report: SampleReport,
    server_metadata: ServerMetadata,
}

enum ReportOutput {
    Stdout,
    File(PreparedReportFile),
}

struct PreparedReportFile {
    destination: PathBuf,
    temporary: PathBuf,
    writer: Option<BufWriter<File>>,
}

struct Observer {
    client: Option<Client>,
    connection: tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
    operation_timeout: Duration,
}

impl Observer {
    async fn connect(database_url: &str, operation_timeout: Duration) -> AnyResult<Self> {
        let (client, connection) = run_observer_operation(
            "connect",
            operation_timeout,
            tokio_postgres::connect(database_url, NoTls),
        )
        .await?;
        Ok(Self {
            client: Some(client),
            connection: tokio::spawn(connection),
            operation_timeout,
        })
    }

    fn client(&self) -> &Client {
        self.client
            .as_ref()
            .expect("observer client is present until close")
    }

    async fn server_metadata(&self) -> AnyResult<ServerMetadata> {
        let row = run_observer_operation(
            "read server metadata",
            self.operation_timeout,
            self.client().query_one(
                "SELECT current_setting('server_version'),
                        current_setting('server_version_num')::integer,
                        pg_postmaster_start_time()::text,
                        current_setting('fsync')::boolean,
                        current_setting('synchronous_commit'),
                        current_setting('full_page_writes')::boolean,
                        current_setting('data_checksums')::boolean,
                        current_setting('wal_level'),
                        current_setting('max_connections')::integer,
                        current_setting('reserved_connections')::integer,
                        current_setting('superuser_reserved_connections')::integer",
                &[],
            ),
        )
        .await?;
        Ok(ServerMetadata {
            version: row.get(0),
            version_num: row.get(1),
            postmaster_started_at: row.get(2),
            settings: PostgresSettingsReport {
                fsync: row.get(3),
                synchronous_commit: row.get(4),
                full_page_writes: row.get(5),
                data_checksums: row.get(6),
                wal_level: row.get(7),
                max_connections: row.get(8),
                reserved_connections: row.get(9),
                superuser_reserved_connections: row.get(10),
            },
        })
    }

    async fn session_snapshot(&self) -> AnyResult<SessionSnapshot> {
        let row = run_observer_operation(
            "read administrative session counters",
            self.operation_timeout,
            self.client().query_one(
                "SELECT sessions,
                        (SELECT count(*) FROM pg_stat_activity
                         WHERE datname = current_database())
                 FROM pg_stat_database
                 WHERE datname = current_database()",
                &[],
            ),
        )
        .await?;
        Ok(SessionSnapshot {
            cumulative: row.get(0),
            active: row.get(1),
        })
    }

    async fn database_size(&self, database_name: &str) -> AnyResult<i64> {
        let row = run_observer_operation(
            "read template database size",
            self.operation_timeout,
            self.client()
                .query_one("SELECT pg_database_size($1)", &[&database_name]),
        )
        .await?;
        Ok(row.get(0))
    }

    async fn database_sizes(&self, database_names: &[String]) -> AnyResult<i64> {
        let row = run_observer_operation(
            "read prewarmed database storage growth",
            self.operation_timeout,
            self.client().query_one(
                "SELECT COALESCE(sum(pg_database_size(name)), 0)::bigint
                 FROM unnest($1::text[]) AS database_names(name)",
                &[&database_names],
            ),
        )
        .await?;
        Ok(row.get(0))
    }

    async fn application_session_count(&self, database_names: &[String]) -> AnyResult<i64> {
        let row = run_observer_operation(
            "count eager downstream sessions",
            self.operation_timeout,
            self.client().query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = ANY($1)",
                &[&database_names],
            ),
        )
        .await?;
        Ok(row.get(0))
    }

    async fn ensure_databases_are_absent(&self, names: &[String]) -> AnyResult<()> {
        let row = run_observer_operation(
            "verify deferred cleanup barrier",
            self.operation_timeout,
            self.client().query_one(
                "SELECT count(*) FROM pg_database WHERE datname = ANY($1)",
                &[&names],
            ),
        )
        .await?;
        let remaining: i64 = row.get(0);
        if remaining == 0 {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "deferred cleanup barrier returned with {remaining} database(s) still present"
            ))
            .into())
        }
    }

    async fn close(mut self) -> AnyResult<()> {
        drop(self.client.take());
        match run_observer_operation(
            "close connection",
            self.operation_timeout,
            &mut self.connection,
        )
        .await
        {
            Ok(connection) => {
                connection?;
                Ok(())
            }
            Err(error) => {
                self.connection.abort();
                Err(error)
            }
        }
    }
}

async fn run_observer_operation<T, E, F>(
    operation: &str,
    timeout: Duration,
    future: F,
) -> AnyResult<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: StdError + Send + Sync + 'static,
{
    match Deadline::after(timeout).run(future).await {
        Ok(result) => result.map_err(|error| Box::new(error) as AnyError),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "benchmark observer did not {operation} within {} ms",
                millis(timeout)
            ),
        )
        .into()),
    }
}

struct FixtureDefinition {
    name: &'static str,
    requested_rows: usize,
    migration_sql: String,
}

impl FixtureDefinition {
    fn small() -> Self {
        Self {
            name: "small",
            requested_rows: 1,
            migration_sql: "
                CREATE TABLE benchmark_items (
                    id bigint PRIMARY KEY,
                    payload text NOT NULL
                );
                INSERT INTO benchmark_items VALUES (1, 'small fixture');
                ANALYZE benchmark_items;"
                .to_owned(),
        }
    }

    fn representative(rows: usize) -> Self {
        Self {
            name: "representative",
            requested_rows: rows,
            migration_sql: format!(
                "CREATE TABLE benchmark_items (
                     id bigint PRIMARY KEY,
                     lookup_key text NOT NULL,
                     payload text NOT NULL
                 );
                 INSERT INTO benchmark_items
                 SELECT value, md5(value::text), repeat(md5(value::text), 8)
                 FROM generate_series(1, {rows}) AS generated(value);
                 CREATE INDEX benchmark_items_lookup_key_idx
                     ON benchmark_items (lookup_key);
                 ANALYZE benchmark_items;"
            ),
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> AnyResult<()> {
    let generated_at_unix_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_millis();
    let invocation = format!("{generated_at_unix_ms}-{}", std::process::id());
    let config = BenchmarkConfig::from_environment(benchmark_project())?;
    let source = source_report();
    let output = ReportOutput::prepare(config.output.as_deref())?;
    let (image_content_id, storage_driver) = environment_metadata(&config)?;
    let expected_owned_image_content_id = image_content_id.value.as_deref();

    eprintln!(
        "postgres-test-harness performance: project={}, mode={}, samples={}, sequential={}, concurrent={} at {}, downstream_pool_size={}, prewarm={}, drains={}",
        config.project,
        config.mode.as_str(),
        config.samples,
        config.sequential_operations,
        config.concurrent_operations,
        config.concurrency,
        config.downstream_pool_size,
        config.prewarm_databases,
        config.drain_databases
    );

    let mut samples = Vec::with_capacity(config.samples);
    let mut first_server_metadata: Option<ServerMetadata> = None;
    for sample in 1..=config.samples {
        eprintln!("sample {sample}/{}", config.samples);
        let (report, metadata) = run_sample(
            &config,
            &invocation,
            sample,
            expected_owned_image_content_id,
        )
        .await?;
        if let Some(first) = &first_server_metadata {
            first.ensure_comparable_environment(&metadata)?;
        } else {
            first_server_metadata = Some(metadata);
        }
        samples.push(report);
    }

    let metadata =
        first_server_metadata.ok_or_else(|| invalid_input("samples must be positive"))?;
    let summary = summarize(&samples);
    print_summary(&summary);
    let report = BenchmarkReport {
        schema_version: SCHEMA_VERSION,
        generated_at_unix_ms,
        source,
        environment: EnvironmentReport {
            mode: config.mode,
            image_reference: matches!(config.mode, ServerMode::Owned).then(|| config.image.clone()),
            image_content_id,
            storage_driver,
            postgres_version: metadata.version,
            postgres_version_num: metadata.version_num,
            postgres_settings: metadata.settings,
            cpu_count: std::thread::available_parallelism()?.get(),
            operating_system: env::consts::OS,
            architecture: env::consts::ARCH,
            readiness_contract: "PostgresHarness::start authenticated through the mapped TCP port within the owned startup deadline and validated PostgreSQL 18; the image's socket-only temporary initdb server cannot satisfy this probe, and the observer then retained the final-server TCP session across a follow-up probe",
            admin_session_counter_scope: "pg_stat_database.sessions for the administrative database; the startup snapshot is taken after the persistent observer connects, phase deltas retain that same observer at both endpoints, and shared external servers may include ambient traffic",
        },
        configuration: config.report(),
        samples,
        summary,
        notes: vec![
            "Durations are observations, never pass/fail thresholds.",
            "Owned startup requires a cached image and excludes image-pull time.",
            "Owned profile settings record initdb synchronization and the effective harness storage mount; image-default storage adds no harness tmpfs mount.",
            "The owned profile does not disable data checksums or lower wal_level; reports record both server settings.",
            "Deferred cleanup records caller-return latency separately from the awaited final drain barrier.",
            "The drain barrier reports worker failures; one post-barrier catalog query verifies that every exact lease name is absent.",
            "The downstream pool spike eagerly opens its configured maximum on every measured lease and verifies the exact peak in pg_stat_activity; ordinary application pools may establish fewer physical connections lazily.",
            "The prewarm phase reports synchronous initial fill, first and steady-state ready-queue lease latency, exact initial clone storage, background dirty-drop/refill throughput, and final idle-pool cleanup; dirty databases are never reused.",
            "Connection permits reserve downstream application capacity only; owner, template-lock, lifecycle-pool, observer, and ambient external-server sessions are separate PostgreSQL connections.",
            "Run both modes under comparable load and compare the versioned JSON output; URLs and credentials are never recorded.",
        ],
    };

    output.write_report(&report)?;
    Ok(())
}

async fn run_sample(
    config: &BenchmarkConfig,
    invocation: &str,
    sample: usize,
    expected_owned_image_content_id: Option<&str>,
) -> AnyResult<(SampleReport, ServerMetadata)> {
    let startup_started = Instant::now();
    let harness = PostgresHarness::start(config.harness_config()?).await?;
    let startup_elapsed = startup_started.elapsed();
    let admin_url = harness.admin_database_url().to_owned();
    let actual_mode = ServerMode::from_harness(&harness);
    let measurements = match config.mode.ensure_matches(actual_mode) {
        Ok(()) => {
            measure_started_sample(
                config,
                &harness,
                invocation,
                sample,
                startup_elapsed,
                expected_owned_image_content_id,
            )
            .await
        }
        Err(error) => Err(error),
    };
    let validation = match &measurements {
        Ok(measurements) => ExternalCleanupValidation::CompletedSample {
            expected_templates: measurements.report.fixtures.len(),
        },
        Err(_) => ExternalCleanupValidation::FailedSample,
    };
    let cleanup = finalize_sample(config, actual_mode, harness, &admin_url, validation).await;
    let (mut measurements, cleanup) = combine_operation_and_cleanup(measurements, cleanup)?;
    measurements.report.external_stale_cleanup = cleanup;

    Ok((measurements.report, measurements.server_metadata))
}

async fn measure_started_sample(
    config: &BenchmarkConfig,
    harness: &PostgresHarness,
    invocation: &str,
    sample: usize,
    startup_elapsed: Duration,
    expected_owned_image_content_id: Option<&str>,
) -> AnyResult<SampleMeasurements> {
    let container_image_content_id =
        verify_started_image(harness, expected_owned_image_content_id)?;
    let admin_url = harness.admin_database_url().to_owned();
    let observer = Observer::connect(&admin_url, OPERATION_TIMEOUT).await?;
    let measurements = measure_with_observer(
        config,
        harness,
        &observer,
        invocation,
        sample,
        startup_elapsed,
        container_image_content_id,
    )
    .await;
    let closed = observer.close().await;
    let (measurements, ()) = combine_operation_and_cleanup(measurements, closed)?;
    Ok(measurements)
}

async fn measure_with_observer(
    config: &BenchmarkConfig,
    harness: &PostgresHarness,
    observer: &Observer,
    invocation: &str,
    sample: usize,
    startup_elapsed: Duration,
    container_image_content_id: Option<String>,
) -> AnyResult<SampleMeasurements> {
    let server_metadata = observer.server_metadata().await?;
    let first_postmaster_start = server_metadata.postmaster_started_at.clone();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let stable_metadata = observer.server_metadata().await?;
    if stable_metadata.postmaster_started_at != first_postmaster_start {
        return Err(io::Error::other(
            "the PostgreSQL postmaster changed after the timed startup; startup did not observe the stable final server",
        )
        .into());
    }
    server_metadata.ensure_comparable_environment(&stable_metadata)?;
    let startup_sessions = observer.session_snapshot().await?;

    let fixtures = vec![
        FixtureDefinition::small(),
        FixtureDefinition::representative(config.representative_rows),
    ];
    let mut fixture_reports = Vec::with_capacity(fixtures.len());
    for fixture in &fixtures {
        eprintln!("  fixture {}", fixture.name);
        fixture_reports
            .push(run_fixture(config, harness, observer, fixture, invocation, sample).await?);
    }

    Ok(SampleMeasurements {
        report: SampleReport {
            sample,
            server_startup: StartupReport {
                elapsed_ns: startup_elapsed.as_nanos(),
                admin_sessions_after_observer_connect: startup_sessions.report(),
            },
            readiness: ReadinessReport {
                postmaster_started_at: server_metadata.postmaster_started_at.clone(),
                container_image_content_id,
                postgres_version: server_metadata.version.clone(),
                postgres_version_num: server_metadata.version_num,
                postgres_settings: server_metadata.settings.clone(),
            },
            fixtures: fixture_reports,
            external_stale_cleanup: None,
        },
        server_metadata,
    })
}

async fn finalize_sample(
    config: &BenchmarkConfig,
    actual_mode: ServerMode,
    harness: PostgresHarness,
    admin_url: &str,
    validation: ExternalCleanupValidation,
) -> AnyResult<Option<ExternalCleanupReport>> {
    let cleanup = match actual_mode {
        ServerMode::Owned => {
            harness.shutdown().await?;
            drop(harness);
            None
        }
        ServerMode::External => {
            drop(harness);
            let cleanup = cleanup_external_resources(admin_url, &config.project).await?;
            if let ExternalCleanupValidation::CompletedSample { expected_templates } = validation {
                validate_completed_external_cleanup(&cleanup, &config.project, expected_templates)?;
            }
            Some(cleanup)
        }
    };
    Ok(cleanup)
}

async fn run_fixture(
    config: &BenchmarkConfig,
    harness: &PostgresHarness,
    observer: &Observer,
    fixture: &FixtureDefinition,
    invocation: &str,
    sample: usize,
) -> AnyResult<FixtureReport> {
    let fingerprint = FingerprintBuilder::new("performance-v1")
        .add("fixture", fixture.name)
        .add("migration", fixture.migration_sql.as_bytes())
        .add("invocation", invocation)
        .add("sample", sample.to_string())
        .finish();
    let spec = TemplateSpec::new(fingerprint);
    let initialized = Arc::new(AtomicBool::new(false));
    let initialized_by_cold_path = initialized.clone();
    let migration_sql = fixture.migration_sql.clone();

    let before = observer.session_snapshot().await?;
    let started = Instant::now();
    let template = harness
        .template(spec, move |database_url| async move {
            initialized_by_cold_path.store(true, Ordering::SeqCst);
            apply_migration(&database_url, &migration_sql).await
        })
        .await?;
    let cold_elapsed = started.elapsed();
    let after = observer.session_snapshot().await?;
    if !initialized.load(Ordering::SeqCst) {
        return Err(io::Error::other(
            "run-unique template unexpectedly reused existing state on the cold path",
        )
        .into());
    }
    let cold_template_acquisition = TimedOperation {
        elapsed_ns: cold_elapsed.as_nanos(),
        admin_sessions: before.delta(after),
    };
    let template_size_bytes = observer.database_size(template.database_name()).await?;

    let before = observer.session_snapshot().await?;
    let started = Instant::now();
    let warm_template = harness
        .template(spec, |_| async {
            Err(io::Error::other("warm template initializer unexpectedly ran").into())
        })
        .await?;
    let warm_elapsed = started.elapsed();
    let after = observer.session_snapshot().await?;
    let warm_template_acquisition = TimedOperation {
        elapsed_ns: warm_elapsed.as_nanos(),
        admin_sessions: before.delta(after),
    };

    let sequential_clone_cleanup =
        run_sequential(&template, observer, config.sequential_operations).await?;
    let bounded_concurrent_clone_cleanup = run_bounded_concurrent(
        &template,
        observer,
        config.concurrent_operations,
        config.concurrency,
    )
    .await?;
    let downstream_pool_checkout_spike = run_downstream_pool_checkout_spike(
        &template,
        observer,
        config.concurrency,
        config.downstream_pool_size,
    )
    .await?;
    let prewarmed_database_queue =
        run_prewarmed_database_queue(harness, &template, observer, config.prewarm_databases)
            .await?;
    let explicit_cleanup_drain =
        run_explicit_drain(&template, observer, config.drain_databases).await?;
    let deferred_cleanup =
        run_deferred_cleanup(harness, &template, observer, config.drain_databases).await?;

    drop(warm_template);
    drop(template);
    Ok(FixtureReport {
        name: fixture.name,
        requested_rows: fixture.requested_rows,
        template_size_bytes,
        cold_template_acquisition,
        warm_template_acquisition,
        sequential_clone_cleanup,
        bounded_concurrent_clone_cleanup,
        downstream_pool_checkout_spike,
        prewarmed_database_queue,
        explicit_cleanup_drain,
        deferred_cleanup,
    })
}

async fn apply_migration(
    database_url: &str,
    migration_sql: &str,
) -> std::result::Result<(), BoxError> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection = tokio::spawn(connection);
    client.batch_execute(migration_sql).await?;
    drop(client);
    connection.await??;
    Ok(())
}

async fn run_sequential(
    template: &postgres_test_harness::DatabaseTemplate,
    observer: &Observer,
    operations: usize,
) -> AnyResult<BatchOperation> {
    let before = observer.session_snapshot().await?;
    let batch_started = Instant::now();
    let mut individual = Vec::with_capacity(operations);
    for _ in 0..operations {
        let started = Instant::now();
        let database = template.database().await?;
        database.cleanup().await?;
        individual.push(started.elapsed().as_nanos());
    }
    let elapsed = batch_started.elapsed();
    let after = observer.session_snapshot().await?;
    Ok(batch_operation(
        operations,
        BatchMethodReport::caller_bounded(1),
        elapsed,
        individual,
        before.delta(after),
    ))
}

async fn run_bounded_concurrent(
    template: &postgres_test_harness::DatabaseTemplate,
    observer: &Observer,
    operations: usize,
    concurrency: usize,
) -> AnyResult<BatchOperation> {
    let before = observer.session_snapshot().await?;
    let limiter = Arc::new(Semaphore::new(concurrency));
    let batch_started = Instant::now();
    let mut tasks = JoinSet::new();
    for _ in 0..operations {
        let template = template.clone();
        let limiter = limiter.clone();
        tasks.spawn(async move {
            let _slot = limiter.acquire_owned().await?;
            let started = Instant::now();
            let database = template.database().await?;
            database.cleanup().await?;
            Ok::<_, AnyError>(started.elapsed().as_nanos())
        });
    }
    let mut individual = Vec::with_capacity(operations);
    while let Some(result) = tasks.join_next().await {
        individual.push(result??);
    }
    let elapsed = batch_started.elapsed();
    let after = observer.session_snapshot().await?;
    Ok(batch_operation(
        operations,
        BatchMethodReport::caller_bounded(concurrency),
        elapsed,
        individual,
        before.delta(after),
    ))
}

struct EagerDatabaseConnections {
    lease: postgres_test_harness::DatabaseLease,
    clients: Vec<Client>,
    connection_tasks: Vec<tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>>,
    operation_timeout: Duration,
}

impl EagerDatabaseConnections {
    async fn close(self) -> AnyResult<()> {
        let Self {
            lease,
            clients,
            connection_tasks,
            operation_timeout,
        } = self;
        drop(clients);
        let connection_shutdown = async {
            for task in connection_tasks {
                run_observer_operation(
                    "close an eager downstream connection",
                    operation_timeout,
                    task,
                )
                .await??;
            }
            Ok(())
        }
        .await;
        let cleanup = lease
            .cleanup()
            .await
            .map_err(|error| Box::new(error) as AnyError);
        combine_operation_and_cleanup(connection_shutdown, cleanup).map(|_| ())
    }
}

async fn open_eager_database_connections(
    template: &postgres_test_harness::DatabaseTemplate,
    connections: usize,
    operation_timeout: Duration,
) -> AnyResult<EagerDatabaseConnections> {
    let lease = template.database().await?;
    let database_url = lease.database_url().to_owned();
    let mut checkouts = JoinSet::new();
    for _ in 0..connections {
        let database_url = database_url.clone();
        checkouts.spawn(async move {
            let (client, connection) = run_observer_operation(
                "open an eager downstream connection",
                operation_timeout,
                tokio_postgres::connect(&database_url, NoTls),
            )
            .await?;
            let connection = tokio::spawn(connection);
            run_observer_operation(
                "probe an eager downstream connection",
                operation_timeout,
                client.simple_query("SELECT 1"),
            )
            .await?;
            Ok::<_, AnyError>((client, connection))
        });
    }

    let mut clients = Vec::with_capacity(connections);
    let mut connection_tasks = Vec::with_capacity(connections);
    while let Some(result) = checkouts.join_next().await {
        let (client, connection) = result??;
        clients.push(client);
        connection_tasks.push(connection);
    }
    Ok(EagerDatabaseConnections {
        lease,
        clients,
        connection_tasks,
        operation_timeout,
    })
}

async fn close_eager_database_connections(
    connections: Vec<EagerDatabaseConnections>,
) -> AnyResult<()> {
    let mut tasks = JoinSet::new();
    for connections in connections {
        tasks.spawn(connections.close());
    }
    let mut first_error = None;
    while let Some(result) = tasks.join_next().await {
        let result = result
            .map_err(|error| Box::new(error) as AnyError)
            .and_then(|result| result);
        if let Err(error) = result
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn run_downstream_pool_checkout_spike(
    template: &postgres_test_harness::DatabaseTemplate,
    observer: &Observer,
    leases: usize,
    connections_per_lease: usize,
) -> AnyResult<DownstreamPoolCheckoutOperation> {
    let eager_connections_total = leases
        .checked_mul(connections_per_lease)
        .ok_or_else(|| invalid_input("eager downstream connection count overflowed usize"))?;
    let expected_application_sessions = i64::try_from(eager_connections_total)
        .map_err(|_| invalid_input("eager downstream connection count must fit in i64"))?;
    let before = observer.session_snapshot().await?;
    let total_started = Instant::now();
    let mut checkouts = JoinSet::new();
    let operation_timeout = observer.operation_timeout;
    for _ in 0..leases {
        let template = template.clone();
        checkouts.spawn(async move {
            open_eager_database_connections(&template, connections_per_lease, operation_timeout)
                .await
        });
    }

    let mut opened = Vec::with_capacity(leases);
    let mut first_error = None;
    while let Some(result) = checkouts.join_next().await {
        match result {
            Ok(Ok(connections)) => opened.push(connections),
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(Box::new(error));
                }
            }
        }
    }
    let checkout_elapsed = total_started.elapsed();

    if let Some(error) = first_error {
        let cleanup = close_eager_database_connections(opened).await;
        return combine_operation_and_cleanup::<(), ()>(Err(error), cleanup)
            .map(|_| unreachable!("an eager checkout error cannot become successful"));
    }

    let database_names = opened
        .iter()
        .map(|connections| connections.lease.database_name().to_owned())
        .collect::<Vec<_>>();
    let observed = observer
        .application_session_count(&database_names)
        .await
        .and_then(|observed| {
            if observed == expected_application_sessions {
                Ok(observed)
            } else {
                Err(io::Error::other(format!(
                    "expected {eager_connections_total} eager downstream sessions across {leases} leases, observed {observed}"
                ))
                .into())
            }
        });
    let cleanup_started = Instant::now();
    let cleanup = close_eager_database_connections(opened).await;
    let cleanup_elapsed = cleanup_started.elapsed();
    let (observed_application_sessions_at_peak, ()) =
        combine_operation_and_cleanup(observed, cleanup)?;
    let total_elapsed = total_started.elapsed();
    let after = observer.session_snapshot().await?;

    Ok(DownstreamPoolCheckoutOperation {
        leases,
        eager_connections_per_lease: connections_per_lease,
        eager_connections_total,
        lease_and_connection_checkout_elapsed_ns: checkout_elapsed.as_nanos(),
        eager_connections_per_second: eager_connections_total as f64
            / checkout_elapsed.as_secs_f64(),
        observed_application_sessions_at_peak,
        cleanup_elapsed_ns: cleanup_elapsed.as_nanos(),
        total_elapsed_ns: total_elapsed.as_nanos(),
        admin_sessions: before.delta(after),
    })
}

struct PrewarmMeasurement {
    initial_fill_elapsed_ns: u128,
    first_lease_elapsed_ns: u128,
    steady_state_lease_elapsed_ns: Vec<u128>,
    steady_state_median_elapsed_ns: u128,
    initial_ready_storage_growth_bytes: i64,
    refill_elapsed_ns: u128,
    refill_databases_per_second: f64,
    ready_after_refill: usize,
}

async fn run_prewarmed_database_queue(
    harness: &PostgresHarness,
    template: &postgres_test_harness::DatabaseTemplate,
    observer: &Observer,
    capacity: usize,
) -> AnyResult<PrewarmedDatabaseQueueOperation> {
    let before = observer.session_snapshot().await?;
    let total_started = Instant::now();
    let fill_started = Instant::now();
    let pool = template.prewarm(capacity).await?;
    let initial_fill_elapsed = fill_started.elapsed();
    let mut leases = Vec::with_capacity(capacity);

    let measurement: AnyResult<PrewarmMeasurement> = async {
        let first_started = Instant::now();
        leases.push(pool.database().await?);
        let first_lease_elapsed = first_started.elapsed();

        let mut steady_state_lease_elapsed_ns = Vec::with_capacity(capacity - 1);
        for _ in 1..capacity {
            let started = Instant::now();
            leases.push(pool.database().await?);
            steady_state_lease_elapsed_ns.push(started.elapsed().as_nanos());
        }
        let database_names = leases
            .iter()
            .map(|lease| lease.database_name().to_owned())
            .collect::<Vec<_>>();
        let initial_ready_storage_growth_bytes =
            observer.database_sizes(&database_names).await?;

        let refill_started = Instant::now();
        let mut return_error = None;
        for lease in std::mem::take(&mut leases) {
            if let Err(error) = lease.defer_cleanup().await
                && return_error.is_none()
            {
                return_error = Some(Box::new(error) as AnyError);
            }
        }
        if let Some(error) = return_error {
            return Err(error);
        }
        harness.drain_deferred_cleanup().await?;
        let refill_elapsed = refill_started.elapsed();
        let ready_after_refill = pool.status().ready();
        if ready_after_refill != capacity {
            return Err(io::Error::other(format!(
                "prewarmed queue drained with {ready_after_refill} ready databases, expected {capacity}"
            ))
            .into());
        }

        let steady_state_median_elapsed_ns =
            median_nanoseconds(&mut steady_state_lease_elapsed_ns);
        Ok(PrewarmMeasurement {
            initial_fill_elapsed_ns: initial_fill_elapsed.as_nanos(),
            first_lease_elapsed_ns: first_lease_elapsed.as_nanos(),
            steady_state_lease_elapsed_ns,
            steady_state_median_elapsed_ns,
            initial_ready_storage_growth_bytes,
            refill_elapsed_ns: refill_elapsed.as_nanos(),
            refill_databases_per_second: capacity as f64 / refill_elapsed.as_secs_f64(),
            ready_after_refill,
        })
    }
    .await;

    let cleanup_started = Instant::now();
    let mut return_error = None;
    for lease in leases {
        if let Err(error) = lease.defer_cleanup().await
            && return_error.is_none()
        {
            return_error = Some(Box::new(error) as AnyError);
        }
    }
    let pool_cleanup = pool
        .shutdown()
        .await
        .map_err(|error| Box::new(error) as AnyError);
    let cleanup = match (return_error, pool_cleanup) {
        (None, Ok(())) => Ok(()),
        (Some(error), Ok(())) | (None, Err(error)) => Err(error),
        (Some(operation), Err(cleanup)) => {
            Err(Box::new(OperationAndCleanupError { operation, cleanup }) as AnyError)
        }
    };
    let cleanup_elapsed = cleanup_started.elapsed();
    let (measurement, ()) = combine_operation_and_cleanup(measurement, cleanup)?;
    let total_elapsed = total_started.elapsed();
    let after = observer.session_snapshot().await?;

    Ok(PrewarmedDatabaseQueueOperation {
        capacity,
        initial_fill_elapsed_ns: measurement.initial_fill_elapsed_ns,
        first_lease_elapsed_ns: measurement.first_lease_elapsed_ns,
        steady_state_lease_elapsed_ns: measurement.steady_state_lease_elapsed_ns,
        steady_state_median_elapsed_ns: measurement.steady_state_median_elapsed_ns,
        initial_ready_storage_growth_bytes: measurement.initial_ready_storage_growth_bytes,
        refill_elapsed_ns: measurement.refill_elapsed_ns,
        refill_databases_per_second: measurement.refill_databases_per_second,
        ready_after_refill: measurement.ready_after_refill,
        cleanup_elapsed_ns: cleanup_elapsed.as_nanos(),
        total_elapsed_ns: total_elapsed.as_nanos(),
        admin_sessions: before.delta(after),
    })
}

async fn run_explicit_drain(
    template: &postgres_test_harness::DatabaseTemplate,
    observer: &Observer,
    databases: usize,
) -> AnyResult<BatchOperation> {
    let mut leases = Vec::with_capacity(databases);
    for _ in 0..databases {
        leases.push(template.database().await?);
    }
    let before = observer.session_snapshot().await?;
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for lease in leases {
        tasks.spawn(async move {
            let operation_started = Instant::now();
            lease.cleanup().await?;
            Ok::<_, AnyError>(operation_started.elapsed().as_nanos())
        });
    }
    let mut individual = Vec::with_capacity(databases);
    while let Some(result) = tasks.join_next().await {
        individual.push(result??);
    }
    let elapsed = started.elapsed();
    let after = observer.session_snapshot().await?;
    Ok(batch_operation(
        databases,
        BatchMethodReport::caller_bounded(databases),
        elapsed,
        individual,
        before.delta(after),
    ))
}

async fn run_deferred_cleanup(
    harness: &PostgresHarness,
    template: &postgres_test_harness::DatabaseTemplate,
    observer: &Observer,
    databases: usize,
) -> AnyResult<DeferredCleanupOperation> {
    let mut leases = Vec::with_capacity(databases);
    let mut names = Vec::with_capacity(databases);
    for _ in 0..databases {
        let lease = template.database().await?;
        names.push(lease.database_name().to_owned());
        leases.push(lease);
    }
    let before = observer.session_snapshot().await?;
    let total_started = Instant::now();
    let caller_started = Instant::now();
    let mut individual = Vec::with_capacity(databases);
    for lease in leases {
        let operation_started = Instant::now();
        lease.defer_cleanup().await?;
        individual.push(operation_started.elapsed().as_nanos());
    }
    let caller_return_elapsed = caller_started.elapsed();
    let drain_started = Instant::now();
    tokio::time::timeout(DEFERRED_DRAIN_TIMEOUT, harness.drain_deferred_cleanup())
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "deferred cleanup barrier did not finish within {} ms",
                    millis(DEFERRED_DRAIN_TIMEOUT)
                ),
            )
        })??;
    let final_drain_elapsed = drain_started.elapsed();
    let total_elapsed = total_started.elapsed();
    observer.ensure_databases_are_absent(&names).await?;
    let after = observer.session_snapshot().await?;
    Ok(DeferredCleanupOperation {
        operations: databases,
        method: BatchMethodReport::implementation_managed_barrier(),
        caller_return_elapsed_ns: caller_return_elapsed.as_nanos(),
        caller_returns_per_second: databases as f64 / caller_return_elapsed.as_secs_f64(),
        individual_caller_return_elapsed_ns: individual,
        final_drain_elapsed_ns: final_drain_elapsed.as_nanos(),
        total_elapsed_ns: total_elapsed.as_nanos(),
        admin_sessions: before.delta(after),
    })
}

fn batch_operation(
    operations: usize,
    method: BatchMethodReport,
    elapsed: Duration,
    individual_elapsed_ns: Vec<u128>,
    admin_sessions: SessionDelta,
) -> BatchOperation {
    BatchOperation {
        operations,
        method,
        elapsed_ns: elapsed.as_nanos(),
        operations_per_second: operations as f64 / elapsed.as_secs_f64(),
        individual_elapsed_ns,
        admin_sessions,
    }
}

async fn cleanup_external_resources(
    admin_url: &str,
    project: &str,
) -> AnyResult<ExternalCleanupReport> {
    let started = Instant::now();
    let retry_deadline = Deadline::after(EXTERNAL_CLEANUP_RETRY_WINDOW);
    let mut retry_backoff = RetryBackoff::new(
        EXTERNAL_CLEANUP_INITIAL_RETRY_INTERVAL,
        EXTERNAL_CLEANUP_MAX_RETRY_INTERVAL,
    );
    let mut attempts = Vec::new();
    loop {
        let cleanup = cleanup_stale_databases(admin_url, project, Duration::ZERO).await?;
        let skipped_active = cleanup.skipped_active;
        attempts.push(CleanupAttemptReport::from_cleanup(cleanup));
        if skipped_active == 0 {
            let report = ExternalCleanupReport::from_attempts(started.elapsed(), attempts);
            validate_external_sweep(&report, project)?;
            return Ok(report);
        }
        if retry_deadline.reached() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "external cleanup for project {project:?} still had {skipped_active} active resource(s) after {} attempt(s) within a {} ms retry window",
                    attempts.len(),
                    millis(EXTERNAL_CLEANUP_RETRY_WINDOW),
                ),
            )
            .into());
        }
        retry_deadline.sleep_up_to(retry_backoff.next_delay()).await;
    }
}

fn validate_external_sweep(report: &ExternalCleanupReport, project: &str) -> AnyResult<()> {
    let final_attempt = report
        .attempts
        .last()
        .expect("external cleanup always records at least one attempt");
    if final_attempt.skipped_fresh == 0 && final_attempt.skipped_unrecognized == 0 {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "external cleanup for project {project:?} left unexpected resources; final skips: active={}, fresh={}, unrecognized={}",
        final_attempt.skipped_active,
        final_attempt.skipped_fresh,
        final_attempt.skipped_unrecognized,
    ))
    .into())
}

fn validate_completed_external_cleanup(
    report: &ExternalCleanupReport,
    project: &str,
    expected_templates: usize,
) -> AnyResult<()> {
    if report.dropped_test_databases == 0 && report.dropped_templates == expected_templates {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "external cleanup for completed project {project:?} dropped {} unexpected test database(s) and {} of {expected_templates} template(s)",
        report.dropped_test_databases, report.dropped_templates,
    ))
    .into())
}

impl CleanupAttemptReport {
    fn from_cleanup(report: CleanupReport) -> Self {
        Self {
            dropped_test_databases: report.dropped_test_databases,
            dropped_templates: report.dropped_templates,
            skipped_active: report.skipped_active,
            skipped_fresh: report.skipped_fresh,
            skipped_unrecognized: report.skipped_unrecognized,
        }
    }
}

impl ExternalCleanupReport {
    fn from_attempts(elapsed: Duration, attempts: Vec<CleanupAttemptReport>) -> Self {
        Self {
            elapsed_ns: elapsed.as_nanos(),
            dropped_test_databases: attempts
                .iter()
                .map(|attempt| attempt.dropped_test_databases)
                .sum(),
            dropped_templates: attempts
                .iter()
                .map(|attempt| attempt.dropped_templates)
                .sum(),
            attempts,
        }
    }
}

fn benchmark_project() -> String {
    let token = Uuid::new_v4().simple().to_string();
    format!("pghp_{}", &token[..11])
}

fn verify_started_image(
    harness: &PostgresHarness,
    expected_content_id: Option<&str>,
) -> AnyResult<Option<String>> {
    if harness.is_external() {
        return Ok(None);
    }
    let expected_content_id = expected_content_id.ok_or_else(|| {
        invalid_input("owned benchmark did not resolve a cached image content ID before startup")
    })?;
    let container_id = harness
        .container_id()
        .ok_or_else(|| invalid_input("owned benchmark started without a container ID"))?;
    let actual_content_id =
        command_output("docker", &["inspect", "--format={{.Image}}", container_id])?;
    if actual_content_id != expected_content_id {
        return Err(io::Error::other(format!(
            "started container image content ID {actual_content_id:?} differs from cached preflight content ID {expected_content_id:?}"
        ))
        .into());
    }
    Ok(Some(actual_content_id))
}

impl ReportOutput {
    fn prepare(destination: Option<&Path>) -> AnyResult<Self> {
        let Some(destination) = destination else {
            return Ok(Self::Stdout);
        };
        if destination.is_dir() {
            return Err(invalid_input(format!(
                "{OUTPUT_ENV} must name a file, got directory {}",
                destination.display()
            )));
        }
        if let Some(parent) = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let file_name = destination.file_name().ok_or_else(|| {
            invalid_input(format!(
                "{OUTPUT_ENV} must name a file, got {}",
                destination.display()
            ))
        })?;
        let mut temporary_name = file_name.to_os_string();
        temporary_name.push(format!(".{}.tmp", Uuid::new_v4().simple()));
        let temporary = destination.with_file_name(temporary_name);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        Ok(Self::File(PreparedReportFile {
            destination: destination.to_owned(),
            temporary,
            writer: Some(BufWriter::new(file)),
        }))
    }

    fn write_report(self, report: &BenchmarkReport) -> AnyResult<()> {
        let mut encoded = Vec::new();
        serde_json::to_writer_pretty(&mut encoded, report)?;
        encoded.push(b'\n');
        self.write_encoded(&encoded)
    }

    fn write_encoded(self, encoded: &[u8]) -> AnyResult<()> {
        match self {
            Self::Stdout => write_stdout(encoded),
            Self::File(file) => {
                let destination = file.destination.clone();
                if let Err(file_error) = file.commit(encoded) {
                    eprintln!(
                        "failed to write structured report to {}; emitting it to stdout: {file_error}",
                        destination.display()
                    );
                    if let Err(stdout_error) = write_stdout(encoded) {
                        return Err(io::Error::other(format!(
                            "failed to write report to {} ({file_error}); stdout fallback also failed ({stdout_error})",
                            destination.display()
                        ))
                        .into());
                    }
                    return Err(file_error);
                }
                Ok(())
            }
        }
    }
}

impl PreparedReportFile {
    fn commit(mut self, encoded: &[u8]) -> AnyResult<()> {
        let mut writer = self
            .writer
            .take()
            .expect("prepared report writer exists until commit");
        writer.write_all(encoded)?;
        writer.flush()?;
        drop(writer);
        fs::rename(&self.temporary, &self.destination)?;
        eprintln!("wrote structured report to {}", self.destination.display());
        Ok(())
    }
}

impl Drop for PreparedReportFile {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.temporary)
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!(
                "failed to remove temporary performance report {}: {error}",
                self.temporary.display()
            );
        }
    }
}

fn write_stdout(encoded: &[u8]) -> AnyResult<()> {
    let stdout = io::stdout();
    let mut writer = stdout.lock();
    writer.write_all(encoded)?;
    writer.flush()?;
    Ok(())
}

fn environment_metadata(config: &BenchmarkConfig) -> AnyResult<(MetadataValue, MetadataValue)> {
    match config.mode {
        ServerMode::Owned => {
            let content_id = command_output(
                "docker",
                &["image", "inspect", "--format={{.Id}}", &config.image],
            )
            .map_err(|error| {
                invalid_input(format!(
                    "owned startup must use a cached image; run `docker pull {}` first ({error})",
                    config.image
                ))
            })?;
            let driver = command_output("docker", &["info", "--format={{.Driver}}"])?;
            Ok((
                MetadataValue {
                    value: Some(content_id),
                    source: "docker_image_inspect_id",
                },
                MetadataValue {
                    value: Some(driver),
                    source: "docker_info",
                },
            ))
        }
        ServerMode::External => Ok((
            external_metadata("PTH_PERF_EXTERNAL_IMAGE_CONTENT_ID"),
            external_metadata("PTH_PERF_EXTERNAL_STORAGE_DRIVER"),
        )),
    }
}

fn external_metadata(name: &'static str) -> MetadataValue {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => MetadataValue {
            value: Some(value),
            source: "environment",
        },
        _ => MetadataValue {
            value: None,
            source: "not_reported_for_external_server",
        },
    }
}

fn source_report() -> SourceReport {
    let git_commit = command_output("git", &["rev-parse", "HEAD"]).ok();
    let git_worktree_dirty = command_output(
        "git",
        &["status", "--porcelain", "--untracked-files=normal"],
    )
    .ok()
    .map(|status| !status.is_empty());
    SourceReport {
        git_commit,
        git_worktree_dirty,
    }
}

fn command_output(program: &str, arguments: &[&str]) -> AnyResult<String> {
    let output = Command::new(program).args(arguments).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        return Err(io::Error::other(format!(
            "`{program} {}` failed with {}: {stderr}",
            arguments.join(" "),
            output.status
        ))
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn positive_env(name: &str, default: usize) -> AnyResult<usize> {
    Ok(optional_positive_env(name)?.unwrap_or(default))
}

fn optional_positive_env(name: &str) -> AnyResult<Option<usize>> {
    let Some(value) = env::var_os(name) else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| invalid_input(format!("{name} must be valid UTF-8")))?;
    let parsed = value
        .parse::<usize>()
        .map_err(|_| invalid_input(format!("{name} must be a positive integer, got {value:?}")))?;
    if parsed == 0 {
        return Err(invalid_input(format!(
            "{name} must be a positive integer, got zero"
        )));
    }
    Ok(Some(parsed))
}

fn owned_container_profile_from_environment() -> AnyResult<OwnedContainerProfile> {
    let profile = OwnedContainerProfile::default()
        .with_initdb_no_sync(boolean_env(OWNED_INITDB_NO_SYNC_ENV, true)?);
    let Some(value) = env::var_os(OWNED_TMPFS_SIZE_BYTES_ENV) else {
        return Ok(profile);
    };
    let value = value
        .into_string()
        .map_err(|_| invalid_input(format!("{OWNED_TMPFS_SIZE_BYTES_ENV} must be valid UTF-8")))?;
    if value.eq_ignore_ascii_case("off") {
        return Ok(profile.without_tmpfs());
    }
    let size_bytes = value.parse::<u64>().map_err(|_| {
        invalid_input(format!(
            "{OWNED_TMPFS_SIZE_BYTES_ENV} must be 'off' or a positive byte count, got {value:?}"
        ))
    })?;
    profile
        .with_tmpfs_size_bytes(size_bytes)
        .map_err(Into::into)
}

fn boolean_env(name: &str, default: bool) -> AnyResult<bool> {
    let Some(value) = env::var_os(name) else {
        return Ok(default);
    };
    let value = value
        .into_string()
        .map_err(|_| invalid_input(format!("{name} must be valid UTF-8")))?;
    match value.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid_input(format!(
            "{name} must be 'true' or 'false', got {value:?}"
        ))),
    }
}

fn invalid_input(message: impl Into<String>) -> AnyError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

fn combine_operation_and_cleanup<T, C>(
    operation: AnyResult<T>,
    cleanup: AnyResult<C>,
) -> AnyResult<(T, C)> {
    match (operation, cleanup) {
        (Ok(value), Ok(cleanup)) => Ok((value, cleanup)),
        (Err(operation), Ok(_)) => Err(operation),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(operation), Err(cleanup)) => {
            Err(Box::new(OperationAndCleanupError { operation, cleanup }))
        }
    }
}

fn summarize(samples: &[SampleReport]) -> Vec<SummaryReport> {
    let mut observations: BTreeMap<(String, Option<&'static str>), Vec<u128>> = BTreeMap::new();
    for sample in samples {
        observations
            .entry(("server_startup".to_owned(), None))
            .or_default()
            .push(sample.server_startup.elapsed_ns);
        for fixture in &sample.fixtures {
            let metrics = [
                (
                    "cold_template_acquisition",
                    fixture.cold_template_acquisition.elapsed_ns,
                ),
                (
                    "warm_template_acquisition",
                    fixture.warm_template_acquisition.elapsed_ns,
                ),
                (
                    "sequential_clone_cleanup",
                    fixture.sequential_clone_cleanup.elapsed_ns,
                ),
                (
                    "bounded_concurrent_clone_cleanup",
                    fixture.bounded_concurrent_clone_cleanup.elapsed_ns,
                ),
                (
                    "downstream_pool_checkout_spike",
                    fixture
                        .downstream_pool_checkout_spike
                        .lease_and_connection_checkout_elapsed_ns,
                ),
                (
                    "downstream_pool_checkout_spike_total",
                    fixture.downstream_pool_checkout_spike.total_elapsed_ns,
                ),
                (
                    "prewarmed_database_queue_initial_fill",
                    fixture.prewarmed_database_queue.initial_fill_elapsed_ns,
                ),
                (
                    "prewarmed_database_queue_first_lease",
                    fixture.prewarmed_database_queue.first_lease_elapsed_ns,
                ),
                (
                    "prewarmed_database_queue_steady_lease",
                    fixture
                        .prewarmed_database_queue
                        .steady_state_median_elapsed_ns,
                ),
                (
                    "prewarmed_database_queue_refill",
                    fixture.prewarmed_database_queue.refill_elapsed_ns,
                ),
                (
                    "prewarmed_database_queue_total",
                    fixture.prewarmed_database_queue.total_elapsed_ns,
                ),
                (
                    "explicit_cleanup_drain",
                    fixture.explicit_cleanup_drain.elapsed_ns,
                ),
                (
                    "deferred_cleanup_caller_return",
                    fixture.deferred_cleanup.caller_return_elapsed_ns,
                ),
                (
                    "deferred_cleanup_final_drain",
                    fixture.deferred_cleanup.final_drain_elapsed_ns,
                ),
                (
                    "deferred_cleanup_total",
                    fixture.deferred_cleanup.total_elapsed_ns,
                ),
            ];
            for (metric, elapsed_ns) in metrics {
                observations
                    .entry((metric.to_owned(), Some(fixture.name)))
                    .or_default()
                    .push(elapsed_ns);
            }
        }
    }

    observations
        .into_iter()
        .map(|((metric, fixture), values)| summary_report(metric, fixture, values))
        .collect()
}

fn summary_report(
    metric: String,
    fixture: Option<&'static str>,
    mut values: Vec<u128>,
) -> SummaryReport {
    let observations = values.len();
    let median_ns = median_nanoseconds(&mut values);
    SummaryReport {
        metric,
        fixture,
        observations,
        min_ns: values[0],
        median_ns,
        mean_ns: values.iter().sum::<u128>() / observations as u128,
        max_ns: values[observations - 1],
    }
}

fn median_nanoseconds(values: &mut [u128]) -> u128 {
    assert!(!values.is_empty(), "a latency median needs observations");
    values.sort_unstable();
    if values.len().is_multiple_of(2) {
        let upper = values[values.len() / 2];
        let lower = values[values.len() / 2 - 1];
        lower + (upper - lower) / 2
    } else {
        values[values.len() / 2]
    }
}

fn print_summary(summary: &[SummaryReport]) {
    eprintln!("summary (milliseconds; min / median / max):");
    for metric in summary {
        let fixture = metric.fixture.unwrap_or("all");
        eprintln!(
            "  {:<34} {:<14} {:>9.3} / {:>9.3} / {:>9.3}",
            metric.metric,
            fixture,
            ns_to_ms(metric.min_ns),
            ns_to_ms(metric.median_ns),
            ns_to_ms(metric.max_ns)
        );
    }
}

fn millis(duration: Duration) -> u128 {
    duration.as_millis()
}

fn ns_to_ms(nanoseconds: u128) -> f64 {
    nanoseconds as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, ffi::OsString, fs, io, time::Duration};

    use postgres_test_harness::{OwnedContainerProfile, ProjectName};
    use uuid::Uuid;

    use super::{
        BatchMethodReport, CleanupAttemptReport, DownstreamPoolCheckoutOperation,
        ExternalCleanupReport, Observer, OperationAndCleanupError, OwnedContainerProfileReport,
        PostgresSettingsReport, PrewarmedDatabaseQueueOperation, ReportOutput, RetryBackoff,
        SCHEMA_VERSION, ServerMode, SessionDelta, SessionSnapshotReport, SummaryReport,
        benchmark_project, combine_operation_and_cleanup, run_observer_operation, summary_report,
        validate_completed_external_cleanup, validate_external_sweep,
    };

    #[test]
    fn summary_retains_range_and_uses_midpoint_median() {
        let SummaryReport {
            observations,
            min_ns,
            median_ns,
            mean_ns,
            max_ns,
            ..
        } = summary_report("metric".to_owned(), Some("fixture"), vec![40, 10, 30, 20]);
        assert_eq!(observations, 4);
        assert_eq!(min_ns, 10);
        assert_eq!(median_ns, 25);
        assert_eq!(mean_ns, 25);
        assert_eq!(max_ns, 40);
    }

    #[test]
    fn cleanup_report_preserves_attempts_and_aggregates_drops() {
        let report = ExternalCleanupReport::from_attempts(
            Duration::from_millis(5),
            vec![
                CleanupAttemptReport {
                    dropped_test_databases: 0,
                    dropped_templates: 1,
                    skipped_active: 1,
                    skipped_fresh: 0,
                    skipped_unrecognized: 0,
                },
                CleanupAttemptReport {
                    dropped_test_databases: 0,
                    dropped_templates: 1,
                    skipped_active: 0,
                    skipped_fresh: 0,
                    skipped_unrecognized: 0,
                },
            ],
        );

        assert_eq!(report.dropped_test_databases, 0);
        assert_eq!(report.dropped_templates, 2);
        assert_eq!(report.attempts.len(), 2);
        assert_eq!(report.attempts[0].skipped_active, 1);
        assert_eq!(report.attempts[1].skipped_active, 0);
        assert!(validate_external_sweep(&report, "project").is_ok());
        assert!(validate_completed_external_cleanup(&report, "project", 2).is_ok());
    }

    #[test]
    fn retry_backoff_grows_exponentially_and_stays_capped() {
        let mut backoff = RetryBackoff::new(Duration::from_millis(25), Duration::from_secs(1));

        assert_eq!(
            (0..9).map(|_| backoff.next_delay()).collect::<Vec<_>>(),
            [25, 50, 100, 200, 400, 800, 1_000, 1_000, 1_000].map(Duration::from_millis)
        );
    }

    #[test]
    fn cleanup_failure_preserves_the_operation_error() {
        let error = combine_operation_and_cleanup::<(), ()>(
            Err(io::Error::other("measurement failed").into()),
            Err(io::Error::other("cleanup failed").into()),
        )
        .expect_err("both failures must be reported");
        let combined = error
            .downcast_ref::<OperationAndCleanupError>()
            .expect("combined error retains both failures");

        assert_eq!(combined.operation.to_string(), "measurement failed");
        assert_eq!(combined.cleanup.to_string(), "cleanup failed");
        assert_eq!(
            error.to_string(),
            "measurement failed; cleanup also failed: cleanup failed"
        );
    }

    #[test]
    fn requested_mode_rejects_invalid_environment_and_detects_mismatches() {
        assert_eq!(
            ServerMode::from_admin_url_environment(Err(std::env::VarError::NotPresent))
                .expect("missing admin URL selects owned mode"),
            ServerMode::Owned
        );
        assert_eq!(
            ServerMode::from_admin_url_environment(Ok(String::new()))
                .expect("a present admin URL selects external mode"),
            ServerMode::External
        );

        let invalid = ServerMode::from_admin_url_environment(Err(std::env::VarError::NotUnicode(
            OsString::from("invalid"),
        )))
        .expect_err("non-UTF-8 configuration must fail before startup");
        assert!(invalid.to_string().contains("must be valid UTF-8"));
        assert!(
            ServerMode::Owned
                .ensure_matches(ServerMode::External)
                .is_err()
        );
    }

    #[test]
    fn report_output_is_prepared_before_work_and_published_atomically() {
        let root = std::env::temp_dir().join(format!(
            "postgres-test-harness-performance-{}",
            Uuid::new_v4().simple()
        ));
        let destination = root.join("reports/performance.json");
        let output = ReportOutput::prepare(Some(&destination)).expect("prepare report output");
        assert!(!destination.exists());

        output
            .write_encoded(b"{\"complete\":true}\n")
            .expect("publish complete report");
        assert_eq!(
            fs::read(&destination).expect("read published report"),
            b"{\"complete\":true}\n"
        );
        assert_eq!(
            fs::read_dir(destination.parent().expect("report parent"))
                .expect("read report directory")
                .count(),
            1
        );

        let non_directory = root.join("not-a-directory");
        fs::write(&non_directory, b"blocker").expect("create non-directory parent");
        assert!(ReportOutput::prepare(Some(&non_directory.join("report.json"))).is_err());

        fs::remove_dir_all(root).expect("remove report test directory");
    }

    #[tokio::test]
    async fn observer_operations_and_shutdown_are_bounded() {
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            run_observer_operation(
                "finish a test operation",
                Duration::from_millis(5),
                std::future::pending::<std::result::Result<(), io::Error>>(),
            ),
        )
        .await
        .expect("test guard elapsed before the benchmark deadline");
        let error = result.expect_err("observer operation must time out");
        assert_eq!(
            error.downcast_ref::<io::Error>().map(io::Error::kind),
            Some(io::ErrorKind::TimedOut)
        );

        let observer = Observer {
            client: None,
            connection: tokio::spawn(std::future::pending::<
                std::result::Result<(), tokio_postgres::Error>,
            >()),
            operation_timeout: Duration::from_millis(5),
        };
        let close_error = tokio::time::timeout(Duration::from_secs(1), observer.close())
            .await
            .expect("test guard elapsed before observer shutdown timeout")
            .expect_err("observer shutdown must time out");
        assert_eq!(
            close_error.downcast_ref::<io::Error>().map(io::Error::kind),
            Some(io::ErrorKind::TimedOut)
        );
    }

    #[test]
    fn generated_projects_are_random_valid_names() {
        let projects = (0..64).map(|_| benchmark_project()).collect::<HashSet<_>>();
        assert_eq!(projects.len(), 64);
        assert!(
            projects
                .iter()
                .all(|project| ProjectName::new(project.clone()).is_ok())
        );
        assert!(projects.iter().all(|project| project.starts_with("pghp_")));
        assert!(projects.iter().all(|project| project.len() == 16));
    }

    #[test]
    fn schema_v8_encodes_prewarm_concurrency_cleanup_storage_and_provenance() {
        assert_eq!(SCHEMA_VERSION, 8);
        assert_eq!(
            serde_json::to_value(BatchMethodReport::caller_bounded(4))
                .expect("serialize caller-bounded method"),
            serde_json::json!({
                "execution": { "kind": "caller_bounded", "concurrency": 4 },
                "completion": { "kind": "operation_return" }
            })
        );
        assert_eq!(
            serde_json::to_value(BatchMethodReport::implementation_managed_barrier())
                .expect("serialize implementation-managed method"),
            serde_json::json!({
                "execution": { "kind": "implementation_managed" },
                "completion": { "kind": "awaited_drain_barrier" }
            })
        );
        assert_eq!(
            serde_json::to_value(DownstreamPoolCheckoutOperation {
                leases: 4,
                eager_connections_per_lease: 5,
                eager_connections_total: 20,
                lease_and_connection_checkout_elapsed_ns: 400_000_000,
                eager_connections_per_second: 50.0,
                observed_application_sessions_at_peak: 20,
                cleanup_elapsed_ns: 100_000_000,
                total_elapsed_ns: 500_000_000,
                admin_sessions: SessionDelta {
                    before: 7,
                    after: 9,
                    delta: 2,
                    active_after: 4,
                },
            })
            .expect("serialize downstream pool checkout spike"),
            serde_json::json!({
                "leases": 4,
                "eager_connections_per_lease": 5,
                "eager_connections_total": 20,
                "lease_and_connection_checkout_elapsed_ns": 400_000_000_u128,
                "eager_connections_per_second": 50.0,
                "observed_application_sessions_at_peak": 20,
                "cleanup_elapsed_ns": 100_000_000_u128,
                "total_elapsed_ns": 500_000_000_u128,
                "admin_sessions": {
                    "before": 7,
                    "after": 9,
                    "delta": 2,
                    "active_after": 4
                }
            })
        );
        assert_eq!(
            serde_json::to_value(PrewarmedDatabaseQueueOperation {
                capacity: 4,
                initial_fill_elapsed_ns: 400_000_000,
                first_lease_elapsed_ns: 10_000,
                steady_state_lease_elapsed_ns: vec![8_000, 6_000, 7_000],
                steady_state_median_elapsed_ns: 7_000,
                initial_ready_storage_growth_bytes: 32_000_000,
                refill_elapsed_ns: 200_000_000,
                refill_databases_per_second: 20.0,
                ready_after_refill: 4,
                cleanup_elapsed_ns: 100_000_000,
                total_elapsed_ns: 700_000_000,
                admin_sessions: SessionDelta {
                    before: 9,
                    after: 11,
                    delta: 2,
                    active_after: 4,
                },
            })
            .expect("serialize prewarmed database queue"),
            serde_json::json!({
                "capacity": 4,
                "initial_fill_elapsed_ns": 400_000_000_u128,
                "first_lease_elapsed_ns": 10_000_u128,
                "steady_state_lease_elapsed_ns": [8_000_u128, 6_000_u128, 7_000_u128],
                "steady_state_median_elapsed_ns": 7_000_u128,
                "initial_ready_storage_growth_bytes": 32_000_000,
                "refill_elapsed_ns": 200_000_000_u128,
                "refill_databases_per_second": 20.0,
                "ready_after_refill": 4,
                "cleanup_elapsed_ns": 100_000_000_u128,
                "total_elapsed_ns": 700_000_000_u128,
                "admin_sessions": {
                    "before": 9,
                    "after": 11,
                    "delta": 2,
                    "active_after": 4
                }
            })
        );
        assert_eq!(
            serde_json::to_value(PostgresSettingsReport {
                fsync: true,
                synchronous_commit: "on".to_owned(),
                full_page_writes: true,
                data_checksums: true,
                wal_level: "replica".to_owned(),
                max_connections: 100,
                reserved_connections: 2,
                superuser_reserved_connections: 3,
            })
            .expect("serialize PostgreSQL settings"),
            serde_json::json!({
                "fsync": true,
                "synchronous_commit": "on",
                "full_page_writes": true,
                "data_checksums": true,
                "wal_level": "replica",
                "max_connections": 100,
                "reserved_connections": 2,
                "superuser_reserved_connections": 3
            })
        );
        assert_eq!(
            serde_json::to_value(OwnedContainerProfileReport::from(
                OwnedContainerProfile::default()
            ))
            .expect("serialize owned-container profile"),
            serde_json::json!({
                "initdb_no_sync": true,
                "storage": {
                    "kind": "tmpfs",
                    "size_bytes": 1_073_741_824_u64
                }
            })
        );
        assert_eq!(
            serde_json::to_value(OwnedContainerProfileReport::from(
                OwnedContainerProfile::default()
                    .with_initdb_no_sync(false)
                    .without_tmpfs()
            ))
            .expect("serialize compatibility owned-container profile"),
            serde_json::json!({
                "initdb_no_sync": false,
                "storage": { "kind": "image_default" }
            })
        );
        assert_eq!(
            serde_json::to_value(SessionSnapshotReport {
                cumulative: 7,
                active: 1,
            })
            .expect("serialize startup session snapshot"),
            serde_json::json!({ "cumulative": 7, "active": 1 })
        );
    }
}
