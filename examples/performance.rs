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
    fs::{self, File},
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
    BoxError, CleanupReport, FingerprintBuilder, HarnessConfig, POSTGRES_TEST_ADMIN_URL_ENV,
    POSTGRES_TEST_IMAGE_ENV, PostgresHarness, TemplateSpec, cleanup_stale_databases,
};
use serde::Serialize;
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const SCHEMA_VERSION: u32 = 2;
const DEFAULT_IMAGE: &str = "postgres:18";
const OUTPUT_ENV: &str = "PTH_PERF_OUTPUT";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(90);
const TEMPLATE_WAIT_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const CONNECTION_BUDGET: usize = 120;
const CONNECTIONS_PER_DATABASE: u32 = 11;
const DEFAULT_SAMPLES: usize = 3;
const DEFAULT_SEQUENTIAL_OPERATIONS: usize = 4;
const DEFAULT_CONCURRENCY: usize = 4;
const DEFAULT_CONCURRENT_OPERATIONS: usize = 8;
const DEFAULT_DRAIN_DATABASES: usize = 4;
const DEFAULT_REPRESENTATIVE_ROWS: usize = 50_000;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(120);
const EXTERNAL_CLEANUP_RETRY_INTERVAL: Duration = Duration::from_millis(25);

type AnyError = Box<dyn StdError + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

#[derive(Debug)]
struct OperationAndCleanupError {
    operation: AnyError,
    cleanup: AnyError,
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

#[derive(Clone, Debug, Serialize)]
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
    fn detect() -> Self {
        if env::var_os(POSTGRES_TEST_ADMIN_URL_ENV).is_some() {
            Self::External
        } else {
            Self::Owned
        }
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
    output: Option<PathBuf>,
    samples: usize,
    sequential_operations: usize,
    concurrency: usize,
    concurrent_operations: usize,
    drain_databases: usize,
    representative_rows: usize,
}

impl BenchmarkConfig {
    fn from_environment(project: String) -> AnyResult<Self> {
        let config = Self {
            project,
            mode: ServerMode::detect(),
            image: env::var(POSTGRES_TEST_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_IMAGE.to_owned()),
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
        };

        let database_capacity = CONNECTION_BUDGET / CONNECTIONS_PER_DATABASE as usize;
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
        if config.concurrent_operations < config.concurrency {
            return Err(invalid_input(format!(
                "PTH_PERF_CONCURRENT_OPERATIONS={} must be at least PTH_PERF_CONCURRENCY={}",
                config.concurrent_operations, config.concurrency
            )));
        }
        Ok(config)
    }

    fn harness_config(&self) -> AnyResult<HarnessConfig> {
        let config = HarnessConfig::new(self.project.clone())?
            .with_startup_timeout(STARTUP_TIMEOUT)?
            .with_operation_timeout(OPERATION_TIMEOUT)?
            .with_template_wait_timeout(TEMPLATE_WAIT_TIMEOUT)?
            .with_connection_budget(CONNECTION_BUDGET)?
            .with_connections_per_database(CONNECTIONS_PER_DATABASE)?
            .with_cleanup_on_start(false);
        match self.mode {
            ServerMode::Owned => Ok(config.with_image(self.image.clone())?),
            ServerMode::External => Ok(config),
        }
    }

    fn report(&self) -> ConfigurationReport {
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
            deferred_drain_timeout_ms: millis(DRAIN_TIMEOUT),
            connection_budget: CONNECTION_BUDGET,
            connections_per_database: CONNECTIONS_PER_DATABASE,
            cleanup_on_start: false,
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
    image_digest: MetadataValue,
    storage_driver: MetadataValue,
    postgres_version: String,
    postgres_version_num: i32,
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
    connection_budget: usize,
    connections_per_database: u32,
    cleanup_on_start: bool,
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
    active_admin_sessions_after_start: i64,
    cumulative_admin_sessions_after_start: i64,
}

#[derive(Serialize)]
struct ReadinessReport {
    postmaster_started_at: String,
    container_image_digest: Option<String>,
    postgres_version: String,
    postgres_version_num: i32,
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
    explicit_cleanup_drain: BatchOperation,
    deferred_cleanup_drain: BatchOperation,
}

#[derive(Serialize)]
struct TimedOperation {
    elapsed_ns: u128,
    admin_sessions: SessionDelta,
}

#[derive(Serialize)]
struct BatchOperation {
    operations: usize,
    concurrency: usize,
    elapsed_ns: u128,
    operations_per_second: f64,
    individual_elapsed_ns: Vec<u128>,
    admin_sessions: SessionDelta,
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
}

struct ServerMetadata {
    version: String,
    version_num: i32,
    postmaster_started_at: String,
}

struct SampleMeasurements {
    report: SampleReport,
    server_metadata: ServerMetadata,
}

struct Observer {
    client: Option<Client>,
    connection: tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
}

impl Observer {
    async fn connect(database_url: &str) -> AnyResult<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
        Ok(Self {
            client: Some(client),
            connection: tokio::spawn(connection),
        })
    }

    fn client(&self) -> &Client {
        self.client
            .as_ref()
            .expect("observer client is present until close")
    }

    async fn server_metadata(&self) -> AnyResult<ServerMetadata> {
        let row = self
            .client()
            .query_one(
                "SELECT current_setting('server_version'),
                        current_setting('server_version_num')::integer,
                        pg_postmaster_start_time()::text",
                &[],
            )
            .await?;
        Ok(ServerMetadata {
            version: row.get(0),
            version_num: row.get(1),
            postmaster_started_at: row.get(2),
        })
    }

    async fn session_snapshot(&self) -> AnyResult<SessionSnapshot> {
        let row = self
            .client()
            .query_one(
                "SELECT sessions,
                        (SELECT count(*) FROM pg_stat_activity
                         WHERE datname = current_database())
                 FROM pg_stat_database
                 WHERE datname = current_database()",
                &[],
            )
            .await?;
        Ok(SessionSnapshot {
            cumulative: row.get(0),
            active: row.get(1),
        })
    }

    async fn database_size(&self, database_name: &str) -> AnyResult<i64> {
        let row = self
            .client()
            .query_one("SELECT pg_database_size($1)", &[&database_name])
            .await?;
        Ok(row.get(0))
    }

    async fn wait_until_databases_are_absent(&self, names: &[String]) -> AnyResult<()> {
        let deadline = Instant::now() + DRAIN_TIMEOUT;
        loop {
            let row = self
                .client()
                .query_one(
                    "SELECT count(*) FROM pg_database WHERE datname = ANY($1)",
                    &[&names],
                )
                .await?;
            let remaining: i64 = row.get(0);
            if remaining == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "deferred cleanup did not drain {remaining} database(s) within {} ms",
                        millis(DRAIN_TIMEOUT)
                    ),
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn close(mut self) -> AnyResult<()> {
        drop(self.client.take());
        self.connection.await??;
        Ok(())
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
    let (image_digest, storage_driver) = environment_metadata(&config)?;
    let expected_owned_image_digest = image_digest.value.as_deref();

    eprintln!(
        "postgres-test-harness performance: project={}, mode={}, samples={}, sequential={}, concurrent={} at {}, drains={}",
        config.project,
        config.mode.as_str(),
        config.samples,
        config.sequential_operations,
        config.concurrent_operations,
        config.concurrency,
        config.drain_databases
    );

    let mut samples = Vec::with_capacity(config.samples);
    let mut first_server_metadata = None;
    for sample in 1..=config.samples {
        eprintln!("sample {sample}/{}", config.samples);
        let (report, metadata) =
            run_sample(&config, &invocation, sample, expected_owned_image_digest).await?;
        first_server_metadata.get_or_insert(metadata);
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
            mode: config.mode.clone(),
            image_reference: matches!(config.mode, ServerMode::Owned).then(|| config.image.clone()),
            image_digest,
            storage_driver,
            postgres_version: metadata.version,
            postgres_version_num: metadata.version_num,
            cpu_count: std::thread::available_parallelism()?.get(),
            operating_system: env::consts::OS,
            architecture: env::consts::ARCH,
            readiness_contract: "PostgresHarness::start returned after a mapped TCP admin connection and PostgreSQL 18 validation; the observer then retained that final-server TCP session across a follow-up probe",
            admin_session_counter_scope: "pg_stat_database.sessions for the administrative database; exact for an owned server and potentially affected by ambient traffic on a shared external server",
        },
        configuration: config.report(),
        samples,
        summary,
        notes: vec![
            "Durations are observations, never pass/fail thresholds.",
            "Owned startup requires a cached image and excludes image-pull time.",
            "Deferred cleanup drain ends only after every dropped lease name is absent from pg_database.",
            "Run both modes under comparable load and compare the versioned JSON output; URLs and credentials are never recorded.",
        ],
    };

    write_report(&report, config.output.as_deref())?;
    Ok(())
}

async fn run_sample(
    config: &BenchmarkConfig,
    invocation: &str,
    sample: usize,
    expected_owned_image_digest: Option<&str>,
) -> AnyResult<(SampleReport, ServerMetadata)> {
    let startup_started = Instant::now();
    let harness = PostgresHarness::start(config.harness_config()?).await?;
    let startup_elapsed = startup_started.elapsed();
    let admin_url = harness.admin_database_url().to_owned();
    let measurements = measure_started_sample(
        config,
        &harness,
        invocation,
        sample,
        startup_elapsed,
        expected_owned_image_digest,
    )
    .await;
    let validation = match &measurements {
        Ok(measurements) => ExternalCleanupValidation::CompletedSample {
            expected_templates: measurements.report.fixtures.len(),
        },
        Err(_) => ExternalCleanupValidation::FailedSample,
    };
    let cleanup = finalize_sample(config, harness, &admin_url, validation).await;
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
    expected_owned_image_digest: Option<&str>,
) -> AnyResult<SampleMeasurements> {
    let container_image_digest =
        verify_started_image(&config.mode, harness, expected_owned_image_digest)?;
    let admin_url = harness.admin_database_url().to_owned();
    let observer = Observer::connect(&admin_url).await?;
    let measurements = measure_with_observer(
        config,
        harness,
        &observer,
        invocation,
        sample,
        startup_elapsed,
        container_image_digest,
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
    container_image_digest: Option<String>,
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
                active_admin_sessions_after_start: startup_sessions.active,
                cumulative_admin_sessions_after_start: startup_sessions.cumulative,
            },
            readiness: ReadinessReport {
                postmaster_started_at: server_metadata.postmaster_started_at.clone(),
                container_image_digest,
                postgres_version: server_metadata.version.clone(),
                postgres_version_num: server_metadata.version_num,
            },
            fixtures: fixture_reports,
            external_stale_cleanup: None,
        },
        server_metadata,
    })
}

async fn finalize_sample(
    config: &BenchmarkConfig,
    harness: PostgresHarness,
    admin_url: &str,
    validation: ExternalCleanupValidation,
) -> AnyResult<Option<ExternalCleanupReport>> {
    let cleanup = match config.mode {
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
    let explicit_cleanup_drain =
        run_explicit_drain(&template, observer, config.drain_databases).await?;
    let deferred_cleanup_drain =
        run_deferred_drain(&template, observer, config.drain_databases).await?;

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
        explicit_cleanup_drain,
        deferred_cleanup_drain,
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
        1,
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
        concurrency,
        elapsed,
        individual,
        before.delta(after),
    ))
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
        databases,
        elapsed,
        individual,
        before.delta(after),
    ))
}

async fn run_deferred_drain(
    template: &postgres_test_harness::DatabaseTemplate,
    observer: &Observer,
    databases: usize,
) -> AnyResult<BatchOperation> {
    let mut leases = Vec::with_capacity(databases);
    let mut names = Vec::with_capacity(databases);
    for _ in 0..databases {
        let lease = template.database().await?;
        names.push(lease.database_name().to_owned());
        leases.push(lease);
    }
    let before = observer.session_snapshot().await?;
    let started = Instant::now();
    drop(leases);
    observer.wait_until_databases_are_absent(&names).await?;
    let elapsed = started.elapsed();
    let after = observer.session_snapshot().await?;
    Ok(batch_operation(
        databases,
        1,
        elapsed,
        Vec::new(),
        before.delta(after),
    ))
}

fn batch_operation(
    operations: usize,
    concurrency: usize,
    elapsed: Duration,
    individual_elapsed_ns: Vec<u128>,
    admin_sessions: SessionDelta,
) -> BatchOperation {
    BatchOperation {
        operations,
        concurrency,
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
    let deadline = started + DRAIN_TIMEOUT;
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
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "external cleanup for project {project:?} still had {skipped_active} active resource(s) after {} attempt(s) and {} ms",
                    attempts.len(),
                    millis(DRAIN_TIMEOUT),
                ),
            )
            .into());
        }
        tokio::time::sleep(EXTERNAL_CLEANUP_RETRY_INTERVAL).await;
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
    mode: &ServerMode,
    harness: &PostgresHarness,
    expected_digest: Option<&str>,
) -> AnyResult<Option<String>> {
    let ServerMode::Owned = mode else {
        return Ok(None);
    };
    let expected_digest = expected_digest.ok_or_else(|| {
        invalid_input("owned benchmark did not resolve a cached image digest before startup")
    })?;
    let container_id = harness
        .container_id()
        .ok_or_else(|| invalid_input("owned benchmark started without a container ID"))?;
    let actual_digest =
        command_output("docker", &["inspect", "--format={{.Image}}", container_id])?;
    if actual_digest != expected_digest {
        return Err(io::Error::other(format!(
            "started container image digest {actual_digest:?} differs from cached preflight digest {expected_digest:?}"
        ))
        .into());
    }
    Ok(Some(actual_digest))
}

fn write_report(report: &BenchmarkReport, output: Option<&Path>) -> AnyResult<()> {
    match output {
        Some(path) => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)?;
            }
            let mut writer = BufWriter::new(File::create(path)?);
            serde_json::to_writer_pretty(&mut writer, report)?;
            writeln!(writer)?;
            writer.flush()?;
            eprintln!("wrote structured report to {}", path.display());
        }
        None => {
            let stdout = io::stdout();
            let mut writer = stdout.lock();
            serde_json::to_writer_pretty(&mut writer, report)?;
            writeln!(writer)?;
        }
    }
    Ok(())
}

fn environment_metadata(config: &BenchmarkConfig) -> AnyResult<(MetadataValue, MetadataValue)> {
    match config.mode {
        ServerMode::Owned => {
            let digest = command_output(
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
                    value: Some(digest),
                    source: "docker_image_inspect",
                },
                MetadataValue {
                    value: Some(driver),
                    source: "docker_info",
                },
            ))
        }
        ServerMode::External => Ok((
            external_metadata("PTH_PERF_EXTERNAL_IMAGE_DIGEST"),
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
    let Some(value) = env::var_os(name) else {
        return Ok(default);
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
    Ok(parsed)
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
                    "explicit_cleanup_drain",
                    fixture.explicit_cleanup_drain.elapsed_ns,
                ),
                (
                    "deferred_cleanup_drain",
                    fixture.deferred_cleanup_drain.elapsed_ns,
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
    values.sort_unstable();
    let observations = values.len();
    let median_ns = if observations.is_multiple_of(2) {
        let upper = values[observations / 2];
        let lower = values[observations / 2 - 1];
        lower + (upper - lower) / 2
    } else {
        values[observations / 2]
    };
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
    use std::{collections::HashSet, io, time::Duration};

    use postgres_test_harness::ProjectName;

    use super::{
        CleanupAttemptReport, ExternalCleanupReport, OperationAndCleanupError, SCHEMA_VERSION,
        SummaryReport, benchmark_project, combine_operation_and_cleanup, summary_report,
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
    fn generated_projects_are_random_valid_names_and_schema_change_is_explicit() {
        let projects = (0..64).map(|_| benchmark_project()).collect::<HashSet<_>>();
        assert_eq!(projects.len(), 64);
        assert!(
            projects
                .iter()
                .all(|project| ProjectName::new(project.clone()).is_ok())
        );
        assert!(projects.iter().all(|project| project.starts_with("pghp_")));
        assert!(projects.iter().all(|project| project.len() == 16));
        assert_eq!(SCHEMA_VERSION, 2);
    }
}
