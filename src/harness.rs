use std::{
    future::Future,
    sync::{
        Arc, Mutex, OnceLock,
        mpsc::{self, Sender},
    },
    time::{Duration, SystemTime},
};

use tokio::sync::OwnedSemaphorePermit;

use crate::{
    BoxError, Error, HarnessConfig, ProjectName, Result, TemplateFingerprint, TemplateSpec,
    admin::{
        AdminClient, AdminDatabaseUrl, DatabaseRecord, PersistentClient,
        acquire_shared_template_advisory_lock, acquire_template_advisory_lock, advisory_key,
        connect_admin, create_managed_database, disable_database_connections, drop_database,
        find_database, list_databases, release_advisory_lock, release_shared_advisory_lock,
        set_database_metadata, terminate_database_connections, try_acquire_advisory_lock,
        validate_postgres_18,
    },
    metadata::{ResourceMetadata, TemplateState},
    name::{DatabaseKind, DatabaseName},
    server::{ServerInner, run_blocking},
};

const DEFAULT_CLEANUP_OPERATION_TIMEOUT: Duration = Duration::from_secs(90);

static DATABASE_CLEANUP_WORKER: OnceLock<Option<Sender<DatabaseLeaseInner>>> = OnceLock::new();

/// Process-local owner of a PostgreSQL 18 server and its database capacity.
#[derive(Clone)]
pub struct PostgresHarness {
    server: Arc<ServerInner>,
}

impl PostgresHarness {
    pub async fn start(config: HarnessConfig) -> Result<Self> {
        let server = ServerInner::start(config).await?;
        let harness = Self { server };
        if harness.server.is_external() && harness.server.cleanup_on_start {
            cleanup_stale_with(
                harness.server.admin_url.clone(),
                harness.server.project.clone(),
                harness.server.stale_after,
                harness.server.operation_timeout,
            )
            .await?;
        }
        Ok(harness)
    }

    pub fn project(&self) -> &ProjectName {
        &self.server.project
    }

    pub fn admin_database_url(&self) -> &str {
        self.server.admin_url.as_str()
    }

    pub fn is_external(&self) -> bool {
        self.server.is_external()
    }

    pub fn container_id(&self) -> Option<&str> {
        self.server.container_id()
    }

    /// Closes database admission and removes an owned container immediately.
    ///
    /// Admission remains closed after the first owned shutdown attempt, even
    /// if container removal fails. External servers and their admission remain
    /// untouched. Repeated and concurrent calls do not retry removal; the call
    /// that completes the terminal worker reports any removal error, while
    /// later calls observe completed shutdown. Active leases remain owned but
    /// can no longer contact a successfully removed server.
    pub async fn shutdown(&self) -> Result<()> {
        self.server.shutdown_container().await
    }

    /// Creates a pristine database from PostgreSQL's built-in `template0`.
    pub async fn empty_database(&self) -> Result<DatabaseLease> {
        create_test_database(self.server.clone(), "template0").await
    }

    /// Gets or initializes one immutable, content-addressed template database.
    pub async fn template<F, Fut>(
        &self,
        spec: TemplateSpec,
        initializer: F,
    ) -> Result<DatabaseTemplate>
    where
        F: FnOnce(String) -> Fut,
        Fut: Future<Output = std::result::Result<(), BoxError>>,
    {
        let fingerprint = spec.fingerprint();
        let name = DatabaseName::template(&self.server.project, fingerprint);
        let server = self.server.clone();
        let preparation = run_blocking(move || begin_template(server, name, fingerprint)).await?;

        let template_lock = match preparation {
            TemplatePreparation::Ready(template_lock) => template_lock,
            TemplatePreparation::Initialize(initialization) => {
                let database_url = initialization
                    .server
                    .admin_url
                    .database_url(&initialization.name);
                match initializer(database_url).await {
                    Ok(()) => run_blocking(move || initialization.finish()).await?,
                    Err(initializer) => {
                        let cleanup = run_blocking(move || initialization.abort()).await;
                        return match cleanup {
                            Ok(()) => Err(Error::TemplateInitializer {
                                source: initializer,
                            }),
                            Err(cleanup) => Err(Error::TemplateInitializerAndCleanup {
                                initializer,
                                cleanup: Box::new(cleanup),
                            }),
                        };
                    }
                }
            }
        };

        Ok(DatabaseTemplate {
            inner: Arc::new(TemplateInner {
                server: self.server.clone(),
                name: DatabaseName::template(&self.server.project, fingerprint),
                fingerprint,
                _shared_lock: Mutex::new(template_lock),
            }),
        })
    }
}

/// Immutable initialized database that can be cloned for individual tests.
#[derive(Clone)]
pub struct DatabaseTemplate {
    inner: Arc<TemplateInner>,
}

impl DatabaseTemplate {
    pub fn database_name(&self) -> &str {
        self.inner.name.as_str()
    }

    pub fn fingerprint(&self) -> TemplateFingerprint {
        self.inner.fingerprint
    }

    pub async fn database(&self) -> Result<DatabaseLease> {
        create_test_database(self.inner.server.clone(), self.inner.name.as_str()).await
    }
}

struct TemplateInner {
    server: Arc<ServerInner>,
    name: DatabaseName,
    fingerprint: TemplateFingerprint,
    _shared_lock: Mutex<PersistentClient>,
}

/// Exclusive ownership of one disposable test database.
pub struct DatabaseLease {
    inner: Option<DatabaseLeaseInner>,
    database_url: String,
}

impl DatabaseLease {
    pub fn database_name(&self) -> &str {
        self.inner
            .as_ref()
            .expect("database lease is present until cleanup consumes it")
            .name
            .as_str()
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    pub async fn cleanup(mut self) -> Result<()> {
        let inner = self
            .inner
            .take()
            .expect("database lease cleanup runs at most once");
        run_blocking(move || inner.cleanup()).await
    }
}

impl std::fmt::Debug for DatabaseLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DatabaseLease")
            .field(
                "database_name",
                &self.inner.as_ref().map(|inner| inner.name.as_str()),
            )
            .field("database_url", &"[REDACTED]")
            .finish()
    }
}

impl Drop for DatabaseLease {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            queue_database_cleanup(inner);
        }
    }
}

struct DatabaseLeaseInner {
    server: Arc<ServerInner>,
    name: DatabaseName,
    _permit: OwnedSemaphorePermit,
}

impl DatabaseLeaseInner {
    fn cleanup(self) -> Result<()> {
        let Self { server, name, .. } = self;
        server.with_lifecycle_admin("connect for disposable database cleanup", |client| {
            drop_database(client, &name)
        })
    }
}

enum TemplatePreparation {
    Ready(PersistentClient),
    Initialize(TemplateInitialization),
}

struct TemplateInitialization {
    server: Arc<ServerInner>,
    name: DatabaseName,
    fingerprint: TemplateFingerprint,
    lock_key: i64,
    coordination_key: i64,
    exclusive_lock: PersistentClient,
}

impl TemplateInitialization {
    fn finish(self) -> Result<PersistentClient> {
        let Self {
            server,
            name,
            fingerprint,
            lock_key,
            coordination_key,
            mut exclusive_lock,
        } = self;
        let client = exclusive_lock.client_mut();
        disable_database_connections(client, &name)?;
        terminate_database_connections(client, &name, server.operation_timeout)?;
        set_database_metadata(
            client,
            &name,
            &ResourceMetadata::template(
                server.project.clone(),
                lock_key,
                fingerprint,
                TemplateState::Ready,
            ),
        )?;
        release_advisory_lock(client, lock_key)?;
        let ready = acquire_and_inspect_shared_template_lock(
            client,
            &server,
            &name,
            fingerprint,
            lock_key,
        )?;
        release_advisory_lock(client, coordination_key)?;
        if ready {
            Ok(exclusive_lock)
        } else {
            Err(Error::InconsistentMetadata {
                database_name: name.as_str().to_owned(),
            })
        }
    }

    fn abort(self) -> Result<()> {
        let Self {
            name,
            lock_key,
            coordination_key,
            mut exclusive_lock,
            ..
        } = self;
        let client = exclusive_lock.client_mut();
        let cleanup = drop_database(client, &name);
        let unlock_template = release_advisory_lock(client, lock_key);
        let unlock_coordination = release_advisory_lock(client, coordination_key);
        cleanup.and(unlock_template).and(unlock_coordination)
    }
}

fn begin_template(
    server: Arc<ServerInner>,
    name: DatabaseName,
    fingerprint: TemplateFingerprint,
) -> Result<TemplatePreparation> {
    let lock_key = template_lock_key(&server.project, fingerprint);
    let coordination_key = template_coordination_key(&server.project, fingerprint);
    let mut persistent_client = PersistentClient::new(connect_admin(
        &server.admin_url,
        server.operation_timeout,
        "connect for PostgreSQL template coordination",
    )?);
    loop {
        let client = persistent_client.client_mut();
        if acquire_and_inspect_shared_template_lock(client, &server, &name, fingerprint, lock_key)?
        {
            return Ok(TemplatePreparation::Ready(persistent_client));
        }

        // Serialize upgrades so a retained ready shared lock cannot strand an
        // earlier coordination attempt waiting for the exclusive lock.
        acquire_template_advisory_lock(
            client,
            coordination_key,
            server.template_wait_timeout,
            server.operation_timeout,
        )?;
        if acquire_and_inspect_shared_template_lock(client, &server, &name, fingerprint, lock_key)?
        {
            release_advisory_lock(client, coordination_key)?;
            return Ok(TemplatePreparation::Ready(persistent_client));
        }

        acquire_template_advisory_lock(
            client,
            lock_key,
            server.template_wait_timeout,
            server.operation_timeout,
        )?;
        if template_is_ready(client, &server, &name, fingerprint, lock_key)? {
            release_advisory_lock(client, lock_key)?;
            let ready = acquire_and_inspect_shared_template_lock(
                client,
                &server,
                &name,
                fingerprint,
                lock_key,
            )?;
            release_advisory_lock(client, coordination_key)?;
            if ready {
                return Ok(TemplatePreparation::Ready(persistent_client));
            }
            continue;
        }
        if let Some(record) = find_database(client, &name)? {
            let recoverable = record
                .comment
                .as_deref()
                .and_then(ResourceMetadata::parse)
                .is_some_and(|metadata| {
                    recoverable_template_initialization(
                        &metadata,
                        &server.project,
                        fingerprint,
                        lock_key,
                    )
                });
            if !recoverable {
                return Err(Error::InconsistentMetadata {
                    database_name: name.as_str().to_owned(),
                });
            }
            drop_database(client, &name)?;
        }
        create_managed_database(
            client,
            &name,
            "template0",
            &ResourceMetadata::template(
                server.project.clone(),
                lock_key,
                fingerprint,
                TemplateState::Initializing,
            ),
        )?;
        return Ok(TemplatePreparation::Initialize(TemplateInitialization {
            server,
            name,
            fingerprint,
            lock_key,
            coordination_key,
            exclusive_lock: persistent_client,
        }));
    }
}

fn template_lock_key(project: &ProjectName, fingerprint: TemplateFingerprint) -> i64 {
    advisory_key(
        "template",
        &format!("{}:{}", project.as_str(), fingerprint.to_hex()),
    )
}

fn template_coordination_key(project: &ProjectName, fingerprint: TemplateFingerprint) -> i64 {
    advisory_key(
        "template-coordination",
        &format!("{}:{}", project.as_str(), fingerprint.to_hex()),
    )
}

fn acquire_and_inspect_shared_template_lock(
    client: &mut AdminClient,
    server: &ServerInner,
    name: &DatabaseName,
    fingerprint: TemplateFingerprint,
    lock_key: i64,
) -> Result<bool> {
    acquire_shared_template_advisory_lock(
        client,
        lock_key,
        server.template_wait_timeout,
        server.operation_timeout,
    )?;
    if template_is_ready(client, server, name, fingerprint, lock_key)? {
        Ok(true)
    } else {
        release_shared_advisory_lock(client, lock_key)?;
        Ok(false)
    }
}

fn recoverable_template_initialization(
    metadata: &ResourceMetadata,
    project: &ProjectName,
    fingerprint: TemplateFingerprint,
    lock_key: i64,
) -> bool {
    matches!(
        metadata,
        ResourceMetadata::Template {
            project: metadata_project,
            lock_key: metadata_lock_key,
            fingerprint: metadata_fingerprint,
            state: TemplateState::Initializing,
            ..
        } if metadata_project == project
            && *metadata_lock_key == lock_key
            && *metadata_fingerprint == fingerprint
    )
}

fn template_is_ready(
    client: &mut AdminClient,
    server: &ServerInner,
    name: &DatabaseName,
    fingerprint: TemplateFingerprint,
    lock_key: i64,
) -> Result<bool> {
    let record = find_database(client, name)?;
    Ok(template_record_is_ready(
        record.as_ref(),
        &server.project,
        fingerprint,
        lock_key,
    ))
}

fn template_record_is_ready(
    record: Option<&DatabaseRecord>,
    project: &ProjectName,
    fingerprint: TemplateFingerprint,
    lock_key: i64,
) -> bool {
    let Some(comment) = record.and_then(|record| record.comment.as_deref()) else {
        return false;
    };
    matches!(
        ResourceMetadata::parse(comment),
        Some(ResourceMetadata::Template {
            project: metadata_project,
            lock_key: metadata_lock_key,
            fingerprint: metadata_fingerprint,
            state: TemplateState::Ready,
            ..
        }) if metadata_project == *project
            && metadata_lock_key == lock_key
            && metadata_fingerprint == fingerprint
    )
}

async fn create_test_database(
    server: Arc<ServerInner>,
    template_name: &str,
) -> Result<DatabaseLease> {
    let permit = server.acquire_database_permit().await?;
    let template_name = template_name.to_owned();
    let name = DatabaseName::test(&server.project);
    let database_url = server.admin_url.database_url(&name);
    run_blocking(move || {
        server.with_lifecycle_admin("connect for disposable database creation", |client| {
            create_managed_database(
                client,
                &name,
                &template_name,
                &ResourceMetadata::test(server.project.clone(), server.owner_key),
            )
        })?;
        Ok(DatabaseLease {
            inner: Some(DatabaseLeaseInner {
                server,
                name,
                _permit: permit,
            }),
            database_url,
        })
    })
    .await
}

fn queue_database_cleanup(inner: DatabaseLeaseInner) {
    let worker = DATABASE_CLEANUP_WORKER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<DatabaseLeaseInner>();
        match std::thread::Builder::new()
            .name("postgres-test-harness-cleanup".to_owned())
            .spawn(move || {
                while let Ok(inner) = receiver.recv() {
                    let database_name = inner.name.as_str().to_owned();
                    if let Err(error) = inner.cleanup() {
                        eprintln!(
                            "postgres-test-harness: fallback cleanup failed for database '{database_name}': {error}"
                        );
                    }
                }
            })
        {
            Ok(_) => Some(sender),
            Err(error) => {
                eprintln!(
                    "postgres-test-harness: failed to start fallback database cleanup worker: {error}"
                );
                None
            }
        }
    });
    let Some(worker) = worker else {
        // The database remains tagged for the next owner-aware stale sweep.
        drop(inner);
        return;
    };
    if let Err(error) = worker.send(inner) {
        // The worker can only disappear during process teardown. Retain the
        // tagged database for the external server's next sweep while releasing
        // the per-harness permit and server ownership.
        drop(error.0);
    }
}

/// Counts from an owner-aware stale database cleanup pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct CleanupReport {
    pub dropped_test_databases: usize,
    pub dropped_templates: usize,
    pub skipped_active: usize,
    pub skipped_fresh: usize,
    pub skipped_unrecognized: usize,
}

impl CleanupReport {
    pub fn total_dropped(self) -> usize {
        self.dropped_test_databases + self.dropped_templates
    }
}

/// Drops only old, tagged resources whose PostgreSQL owner lock is not held.
pub async fn cleanup_stale_databases(
    admin_database_url: &str,
    project: impl Into<String>,
    stale_after: Duration,
) -> Result<CleanupReport> {
    let admin_url = AdminDatabaseUrl::parse(admin_database_url)?;
    let project = ProjectName::new(project)?;
    cleanup_stale_with(
        admin_url,
        project,
        stale_after,
        DEFAULT_CLEANUP_OPERATION_TIMEOUT,
    )
    .await
}

async fn cleanup_stale_with(
    admin_url: AdminDatabaseUrl,
    project: ProjectName,
    stale_after: Duration,
    operation_timeout: Duration,
) -> Result<CleanupReport> {
    run_blocking(move || {
        cleanup_stale_blocking(&admin_url, &project, stale_after, operation_timeout)
    })
    .await
}

fn cleanup_stale_blocking(
    admin_url: &AdminDatabaseUrl,
    project: &ProjectName,
    stale_after: Duration,
    operation_timeout: Duration,
) -> Result<CleanupReport> {
    let mut client = connect_admin(
        admin_url,
        operation_timeout,
        "connect for stale PostgreSQL database cleanup",
    )?;
    validate_postgres_18(&mut client)?;
    let records = list_databases(&mut client)?;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut report = CleanupReport::default();

    for record in records {
        let lock_key = match classify_cleanup_record(project, &record, now, stale_after) {
            CleanupClassification::Ignore => continue,
            CleanupClassification::Unrecognized => {
                report.skipped_unrecognized += 1;
                continue;
            }
            CleanupClassification::Fresh => {
                report.skipped_fresh += 1;
                continue;
            }
            CleanupClassification::Candidate { lock_key, .. } => lock_key,
        };
        if !try_acquire_advisory_lock(&mut client, lock_key)? {
            report.skipped_active += 1;
            continue;
        }
        let cleanup_result = cleanup_candidate_after_lock(
            &mut client,
            project,
            &record,
            lock_key,
            now,
            stale_after,
            &mut report,
        );
        let Some(dropped_kind) = finish_locked_cleanup(cleanup_result, || {
            release_advisory_lock(&mut client, lock_key)
        })?
        else {
            continue;
        };
        match dropped_kind {
            DatabaseKind::Test => report.dropped_test_databases += 1,
            DatabaseKind::Template => report.dropped_templates += 1,
        }
    }
    Ok(report)
}

fn cleanup_candidate_after_lock(
    client: &mut AdminClient,
    project: &ProjectName,
    snapshot: &DatabaseRecord,
    held_key: i64,
    now: u64,
    stale_after: Duration,
    report: &mut CleanupReport,
) -> Result<Option<DatabaseKind>> {
    let snapshot_name = DatabaseName::from_existing(project, snapshot.name.clone())
        .expect("classified cleanup candidate has a valid managed database name");
    let current = find_database(client, &snapshot_name)?;
    let Some(name) = revalidate_cleanup_candidate(
        project,
        snapshot,
        current.as_ref(),
        held_key,
        now,
        stale_after,
        report,
    ) else {
        return Ok(None);
    };
    drop_database(client, &name)?;
    Ok(Some(name.kind()))
}

fn finish_locked_cleanup<T>(
    operation: Result<T>,
    unlock: impl FnOnce() -> Result<()>,
) -> Result<T> {
    let unlock = unlock();
    match operation {
        Ok(value) => {
            unlock?;
            Ok(value)
        }
        Err(error) => Err(error),
    }
}

enum CleanupClassification {
    Ignore,
    Unrecognized,
    Fresh,
    Candidate { name: DatabaseName, lock_key: i64 },
}

fn classify_cleanup_record(
    project: &ProjectName,
    record: &DatabaseRecord,
    now: u64,
    stale_after: Duration,
) -> CleanupClassification {
    let Some(name) = DatabaseName::from_existing(project, record.name.clone()) else {
        return CleanupClassification::Ignore;
    };
    let Some(metadata) = record.comment.as_deref().and_then(ResourceMetadata::parse) else {
        return CleanupClassification::Unrecognized;
    };
    if metadata.project() != project || !metadata_matches_name(&metadata, &name) {
        return CleanupClassification::Unrecognized;
    }
    if now.saturating_sub(metadata.created_at()) < stale_after.as_secs() {
        return CleanupClassification::Fresh;
    }
    CleanupClassification::Candidate {
        name,
        lock_key: metadata.lock_key(),
    }
}

fn revalidate_cleanup_candidate(
    project: &ProjectName,
    snapshot: &DatabaseRecord,
    current: Option<&DatabaseRecord>,
    held_key: i64,
    now: u64,
    stale_after: Duration,
    report: &mut CleanupReport,
) -> Option<DatabaseName> {
    let current = current?;
    match classify_cleanup_record(project, current, now, stale_after) {
        CleanupClassification::Ignore => None,
        CleanupClassification::Unrecognized => {
            report.skipped_unrecognized += 1;
            None
        }
        CleanupClassification::Fresh => {
            report.skipped_fresh += 1;
            None
        }
        CleanupClassification::Candidate { name, lock_key }
            if current == snapshot && lock_key == held_key =>
        {
            Some(name)
        }
        CleanupClassification::Candidate { .. } => None,
    }
}

fn metadata_matches_name(metadata: &ResourceMetadata, name: &DatabaseName) -> bool {
    match (metadata, name.kind()) {
        (ResourceMetadata::Test { .. }, DatabaseKind::Test) => true,
        (
            ResourceMetadata::Template {
                project,
                fingerprint,
                ..
            },
            DatabaseKind::Template,
        ) => DatabaseName::template(project, *fingerprint) == *name,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };

    use tokio::sync::oneshot;

    use super::{
        CleanupClassification, CleanupReport, classify_cleanup_record, finish_locked_cleanup,
        recoverable_template_initialization, revalidate_cleanup_candidate, run_blocking,
        template_coordination_key, template_lock_key, template_record_is_ready,
    };
    use crate::{
        Error, FingerprintBuilder, ProjectName,
        admin::DatabaseRecord,
        metadata::{ResourceMetadata, TemplateState},
        name::{DatabaseKind, DatabaseName},
    };

    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn test_record(
        project: &ProjectName,
        name: &DatabaseName,
        owner_key: i64,
        created_at: u64,
    ) -> DatabaseRecord {
        DatabaseRecord {
            name: name.as_str().to_owned(),
            comment: Some(
                ResourceMetadata::Test {
                    project: project.clone(),
                    owner_key,
                    created_at,
                }
                .encode(),
            ),
        }
    }

    fn template_record(
        project: &ProjectName,
        fingerprint: crate::TemplateFingerprint,
        lock_key: i64,
        state: TemplateState,
        created_at: u64,
    ) -> DatabaseRecord {
        DatabaseRecord {
            name: DatabaseName::template(project, fingerprint)
                .as_str()
                .to_owned(),
            comment: Some(
                ResourceMetadata::Template {
                    project: project.clone(),
                    lock_key,
                    fingerprint,
                    state,
                    created_at,
                }
                .encode(),
            ),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn detached_blocking_results_are_dropped() {
        let cleaned = Arc::new(AtomicBool::new(false));
        let cleaned_for_task = cleaned.clone();
        let (started_sender, started_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = mpsc::channel();

        let task = tokio::spawn(async move {
            run_blocking(move || {
                let _ = started_sender.send(());
                release_receiver.recv().unwrap();
                Ok(DropProbe(cleaned_for_task))
            })
            .await
        });

        started_receiver.await.unwrap();
        task.abort();
        release_sender.send(()).unwrap();
        let join_error = match task.await {
            Ok(_) => panic!("cancelled task unexpectedly completed"),
            Err(error) => error,
        };
        assert!(join_error.is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cleaned.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached blocking result should be cleaned");
    }

    #[test]
    fn template_lock_domains_are_distinct_and_namespaced_by_project() {
        let fingerprint = FingerprintBuilder::new("schema").finish();
        let left = template_lock_key(&ProjectName::new("left").unwrap(), fingerprint);
        let right = template_lock_key(&ProjectName::new("right").unwrap(), fingerprint);
        let coordination =
            template_coordination_key(&ProjectName::new("left").unwrap(), fingerprint);
        assert_ne!(left, right);
        assert_ne!(left, coordination);
    }

    #[test]
    fn only_matching_initializing_templates_are_recoverable() {
        let project = ProjectName::new("creditkit").unwrap();
        let fingerprint = FingerprintBuilder::new("schema").finish();
        let lock_key = template_lock_key(&project, fingerprint);
        let initializing = ResourceMetadata::template(
            project.clone(),
            lock_key,
            fingerprint,
            TemplateState::Initializing,
        );
        let ready = ResourceMetadata::template(
            project.clone(),
            lock_key,
            fingerprint,
            TemplateState::Ready,
        );
        assert!(recoverable_template_initialization(
            &initializing,
            &project,
            fingerprint,
            lock_key
        ));
        assert!(!recoverable_template_initialization(
            &ready,
            &project,
            fingerprint,
            lock_key
        ));
        assert!(!recoverable_template_initialization(
            &initializing,
            &ProjectName::new("another").unwrap(),
            fingerprint,
            lock_key
        ));
    }

    #[test]
    fn exact_template_records_preserve_catalog_states() {
        let project = ProjectName::new("creditkit").unwrap();
        let fingerprint = FingerprintBuilder::new("schema").finish();
        let lock_key = template_lock_key(&project, fingerprint);
        let name = DatabaseName::template(&project, fingerprint)
            .as_str()
            .to_owned();
        let untagged = DatabaseRecord {
            name: name.clone(),
            comment: None,
        };
        let initializing = DatabaseRecord {
            name: name.clone(),
            comment: Some(
                ResourceMetadata::template(
                    project.clone(),
                    lock_key,
                    fingerprint,
                    TemplateState::Initializing,
                )
                .encode(),
            ),
        };
        let ready = DatabaseRecord {
            name,
            comment: Some(
                ResourceMetadata::template(
                    project.clone(),
                    lock_key,
                    fingerprint,
                    TemplateState::Ready,
                )
                .encode(),
            ),
        };

        assert!(!template_record_is_ready(
            None,
            &project,
            fingerprint,
            lock_key
        ));
        assert!(!template_record_is_ready(
            Some(&untagged),
            &project,
            fingerprint,
            lock_key
        ));
        assert!(!template_record_is_ready(
            Some(&initializing),
            &project,
            fingerprint,
            lock_key
        ));
        assert!(template_record_is_ready(
            Some(&ready),
            &project,
            fingerprint,
            lock_key
        ));
    }

    #[test]
    fn cleanup_classification_is_conservative_without_postgres() {
        let project = ProjectName::new("creditkit").unwrap();
        let test_name = DatabaseName::test(&project);
        let metadata = ResourceMetadata::test(project.clone(), 42);
        let created_at = metadata.created_at();

        let unrecognized = classify_cleanup_record(
            &project,
            &DatabaseRecord {
                name: test_name.as_str().to_owned(),
                comment: None,
            },
            created_at + 100,
            Duration::from_secs(10),
        );
        assert!(matches!(unrecognized, CleanupClassification::Unrecognized));

        let fresh = classify_cleanup_record(
            &project,
            &DatabaseRecord {
                name: test_name.as_str().to_owned(),
                comment: Some(metadata.encode()),
            },
            created_at + 5,
            Duration::from_secs(10),
        );
        assert!(matches!(fresh, CleanupClassification::Fresh));

        let candidate = classify_cleanup_record(
            &project,
            &DatabaseRecord {
                name: test_name.as_str().to_owned(),
                comment: Some(metadata.encode()),
            },
            created_at + 10,
            Duration::from_secs(10),
        );
        assert!(matches!(
            candidate,
            CleanupClassification::Candidate { lock_key: 42, .. }
        ));

        let malformed_name = classify_cleanup_record(
            &project,
            &DatabaseRecord {
                name: "pgh_creditkit_test_backup".to_owned(),
                comment: Some(metadata.encode()),
            },
            created_at + 10,
            Duration::from_secs(10),
        );
        assert!(matches!(malformed_name, CleanupClassification::Ignore));
    }

    #[test]
    fn locked_cleanup_requires_an_unchanged_candidate() {
        let project = ProjectName::new("creditkit").unwrap();
        let test_name = DatabaseName::test(&project);
        let snapshot = test_record(&project, &test_name, 42, 10);
        let mut report = CleanupReport::default();

        let unchanged = revalidate_cleanup_candidate(
            &project,
            &snapshot,
            Some(&snapshot),
            42,
            100,
            Duration::from_secs(10),
            &mut report,
        );
        assert_eq!(
            unchanged.as_ref().map(DatabaseName::kind),
            Some(DatabaseKind::Test)
        );

        let refreshed = test_record(&project, &test_name, 42, 95);
        assert!(
            revalidate_cleanup_candidate(
                &project,
                &snapshot,
                Some(&refreshed),
                42,
                100,
                Duration::from_secs(10),
                &mut report,
            )
            .is_none()
        );
        assert_eq!(report.skipped_fresh, 1);

        let changed_key = test_record(&project, &test_name, 84, 10);
        assert!(
            revalidate_cleanup_candidate(
                &project,
                &snapshot,
                Some(&changed_key),
                42,
                100,
                Duration::from_secs(10),
                &mut report,
            )
            .is_none()
        );
        assert!(
            revalidate_cleanup_candidate(
                &project,
                &snapshot,
                None,
                42,
                100,
                Duration::from_secs(10),
                &mut report,
            )
            .is_none()
        );
    }

    #[test]
    fn locked_cleanup_rejects_a_finalized_template_snapshot() {
        let project = ProjectName::new("creditkit").unwrap();
        let fingerprint = FingerprintBuilder::new("cleanup-race").finish();
        let initializing =
            template_record(&project, fingerprint, 42, TemplateState::Initializing, 10);
        let ready = template_record(&project, fingerprint, 42, TemplateState::Ready, 10);
        let mut report = CleanupReport::default();

        assert!(
            revalidate_cleanup_candidate(
                &project,
                &initializing,
                Some(&ready),
                42,
                100,
                Duration::from_secs(10),
                &mut report,
            )
            .is_none()
        );
        assert_eq!(report, CleanupReport::default());
    }

    #[test]
    fn locked_cleanup_keeps_the_operation_error_primary() {
        let unlocked = AtomicBool::new(false);
        let error = finish_locked_cleanup::<()>(
            Err(Error::InvalidConfiguration {
                reason: "locked reread",
            }),
            || {
                unlocked.store(true, Ordering::SeqCst);
                Err(Error::InvalidConfiguration { reason: "unlock" })
            },
        )
        .unwrap_err();
        assert!(unlocked.load(Ordering::SeqCst));
        assert!(matches!(
            error,
            Error::InvalidConfiguration {
                reason: "locked reread"
            }
        ));

        let error = finish_locked_cleanup(Ok(()), || {
            Err(Error::InvalidConfiguration { reason: "unlock" })
        })
        .unwrap_err();
        assert!(matches!(
            error,
            Error::InvalidConfiguration { reason: "unlock" }
        ));
    }
}
