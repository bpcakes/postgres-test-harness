use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

#[cfg(feature = "containers")]
use std::{
    io::Read,
    sync::{
        OnceLock,
        mpsc::{self, RecvTimeoutError, Sender},
    },
    thread::JoinHandle,
    time::{Instant, SystemTime},
};
#[cfg(feature = "containers")]
use testcontainers::{
    Container, ContainerRequest, GenericImage, ImageExt,
    core::{IntoContainerPort, Mount, WaitFor},
    runners::SyncRunner,
};
use tokio::sync::{OwnedSemaphorePermit, watch};
use uuid::Uuid;

use crate::{
    ConnectionLimits, Error, HarnessConfig, ProjectName, Result, TemplateFingerprint,
    admin::{
        AdminClient, AdminDatabaseUrl, AdminSessionDisposition, AdminSessionPool, PersistentClient,
        acquire_advisory_lock, advisory_key, attempt_managed_database_creation, connect_admin,
        regular_connection_slots, validate_postgres_18,
    },
    admission::{DatabaseAdmission, ManagedDatabaseCreationFailure},
    cleanup::DatabaseCleanupQueue,
    harness::PrewarmPoolInner,
    metadata::ResourceMetadata,
    name::DatabaseName,
};

#[cfg(feature = "containers")]
use crate::{
    admin::connect_admin_with_setup_timeout,
    config::{ImageReference, OwnedContainerProfile},
};

#[cfg(feature = "containers")]
const POSTGRES_PORT: u16 = 5432;
#[cfg(feature = "containers")]
const POSTGRES_USER: &str = "postgres";
#[cfg(feature = "containers")]
const POSTGRES_PASSWORD: &str = "postgres";
#[cfg(feature = "containers")]
const POSTGRES_DATABASE: &str = "postgres";
#[cfg(feature = "containers")]
const POSTGRES_STORAGE_PATH: &str = "/var/lib/postgresql";
#[cfg(feature = "containers")]
const POSTGRES_INITDB_NO_SYNC: &str = "--no-sync";
#[cfg(feature = "containers")]
const STARTUP_CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(250);
#[cfg(feature = "containers")]
const CONTAINER_ENGINE_STARTUP_TIMEOUT_FLOOR: Duration = Duration::from_secs(60);
#[cfg(feature = "containers")]
const STARTUP_RETRY_INTERVAL: Duration = Duration::from_millis(25);
#[cfg(feature = "containers")]
const STARTUP_STATUS_INTERVAL: Duration = Duration::from_millis(25);
#[cfg(feature = "containers")]
const STARTUP_LOG_LIMIT_BYTES: u64 = 64 * 1024;
// One harness's lifecycle pool may occupy at most one quarter of PostgreSQL's
// regular connection slots. Separate harnesses and processes do not coordinate
// this local bound.
const LIFECYCLE_ADMIN_CONNECTION_SHARE_DIVISOR: usize = 4;
// Cleanup is I/O-bound, but each worker owns an OS thread while it is idle.
// Reserve at least half of a multi-session lifecycle pool for creates and
// other lifecycle work, and cap the per-server thread footprint explicitly.
const CLEANUP_ADMIN_SESSION_SHARE_DIVISOR: usize = 2;
const MAX_CLEANUP_WORKERS: usize = 4;
#[cfg(feature = "containers")]
const MANAGED_LABEL: &str = "org.postgres-test-harness.managed";
#[cfg(feature = "containers")]
const PROJECT_LABEL: &str = "org.postgres-test-harness.project";
#[cfg(feature = "containers")]
const RUN_LABEL: &str = "org.postgres-test-harness.run";
#[cfg(feature = "containers")]
const CREATED_LABEL: &str = "org.postgres-test-harness.created";

#[cfg(feature = "containers")]
static CONTAINER_REGISTRY: OnceLock<Mutex<Vec<Weak<ContainerOwner>>>> = OnceLock::new();
#[cfg(feature = "containers")]
static EXIT_REGISTRATION: OnceLock<i32> = OnceLock::new();

pub(crate) struct ServerInner {
    pub(crate) admin_url: AdminDatabaseUrl,
    pub(crate) project: ProjectName,
    pub(crate) owner_key: i64,
    pub(crate) operation_timeout: Duration,
    pub(crate) template_wait_timeout: Duration,
    pub(crate) stale_after: Duration,
    pub(crate) cleanup_on_start: bool,
    pub(crate) connection_limits: ConnectionLimits,
    database_admission: Arc<DatabaseAdmission>,
    pub(crate) database_cleanup: Arc<DatabaseCleanupQueue>,
    admin_sessions: Arc<AdminSessionPool>,
    template_cache: Mutex<TemplateCache>,
    prewarm_pools: Mutex<Vec<Weak<PrewarmPoolInner>>>,
    _owner_lock: Mutex<Option<PersistentClient>>,
    container: Option<Arc<ContainerOwner>>,
}

#[derive(Default)]
struct TemplateCache {
    entries: HashMap<TemplateFingerprint, TemplateCacheEntry>,
}

enum TemplateCacheEntry {
    Initializing(Arc<TemplateFlight>),
    // TemplateInner owns its ServerInner, so the server-side cache must not
    // retain a strong reference and form a permanent cycle.
    Ready(Weak<TemplateInner>),
}

impl TemplateCache {
    fn prune_expired_ready_entries(&mut self) {
        self.entries.retain(|_, entry| {
            !matches!(entry, TemplateCacheEntry::Ready(template) if template.strong_count() == 0)
        });
    }
}

struct TemplateFlight {
    completed: watch::Sender<bool>,
    // Registered waiters retain the flight and therefore this successful
    // value until each can clone the exact TemplateInner that won the flight.
    template: Mutex<Option<Arc<TemplateInner>>>,
}

pub(crate) enum TemplateCacheAction {
    Ready(Arc<TemplateInner>),
    Initialize(TemplateCacheInitializer),
    Wait(TemplateCacheWaiter),
}

pub(crate) struct TemplateCacheInitializer {
    server: Arc<ServerInner>,
    fingerprint: TemplateFingerprint,
    flight: Arc<TemplateFlight>,
    active: bool,
}

pub(crate) struct TemplateCacheWaiter {
    flight: Arc<TemplateFlight>,
    completed: watch::Receiver<bool>,
}

pub(crate) struct TemplateInner {
    server: Arc<ServerInner>,
    name: DatabaseName,
    fingerprint: TemplateFingerprint,
    _shared_lock: Mutex<PersistentClient>,
}

impl ServerInner {
    pub(crate) async fn start(config: HarnessConfig) -> Result<Arc<Self>> {
        run_blocking(move || Self::start_blocking(config)).await
    }

    fn start_blocking(config: HarnessConfig) -> Result<Arc<Self>> {
        let connection_limits = config.connection_limits()?;
        let run_id = Uuid::now_v7().simple().to_string();
        let owner_key = advisory_key("run", &run_id);
        if let Some(admin_url) = config.resolved_admin_database_url() {
            return Self::finish_start(
                config,
                connection_limits,
                AdminDatabaseUrl::parse(&admin_url)?,
                None,
                owner_key,
            );
        }

        #[cfg(not(feature = "containers"))]
        return Err(Error::ExternalAdminUrlRequired);

        #[cfg(feature = "containers")]
        {
            let startup_started = Instant::now();
            let image = config.resolved_image()?;
            let profile = config.owned_container_profile;
            let (container, admin_url) = ContainerOwner::start(
                image,
                profile,
                &config.project,
                &run_id,
                config.startup_timeout,
            )?;
            let owner_lock = wait_for_owned_server(
                &admin_url,
                config.operation_timeout,
                startup_started,
                config.startup_timeout,
                profile,
                &container,
            )?;
            container.mark_ready()?;
            Self::finish_start_with_client(
                config,
                connection_limits,
                admin_url,
                Some(container),
                owner_key,
                owner_lock,
            )
        }
    }

    fn finish_start(
        config: HarnessConfig,
        connection_limits: ConnectionLimits,
        admin_url: AdminDatabaseUrl,
        container: Option<Arc<ContainerOwner>>,
        owner_key: i64,
    ) -> Result<Arc<Self>> {
        Self::finish_start_with(
            config,
            connection_limits,
            admin_url,
            container,
            owner_key,
            connect_admin,
        )
    }

    fn finish_start_with<F>(
        config: HarnessConfig,
        connection_limits: ConnectionLimits,
        admin_url: AdminDatabaseUrl,
        container: Option<Arc<ContainerOwner>>,
        owner_key: i64,
        connector: F,
    ) -> Result<Arc<Self>>
    where
        F: FnOnce(&AdminDatabaseUrl, Duration, &'static str) -> Result<AdminClient>,
    {
        let owner_lock = connector(
            &admin_url,
            config.operation_timeout,
            "connect to PostgreSQL test server",
        )?;
        Self::finish_start_with_client(
            config,
            connection_limits,
            admin_url,
            container,
            owner_key,
            owner_lock,
        )
    }

    fn finish_start_with_client(
        config: HarnessConfig,
        connection_limits: ConnectionLimits,
        admin_url: AdminDatabaseUrl,
        container: Option<Arc<ContainerOwner>>,
        owner_key: i64,
        mut owner_lock: AdminClient,
    ) -> Result<Arc<Self>> {
        validate_postgres_18(&mut owner_lock)?;
        let admin_pool_size = per_harness_admin_session_pool_size(
            connection_limits,
            regular_connection_slots(&mut owner_lock)?,
        );
        acquire_advisory_lock(&mut owner_lock, owner_key)?;
        let admin_sessions = Arc::new(AdminSessionPool::new(
            admin_url.clone(),
            config.operation_timeout,
            config.project.as_str(),
            admin_pool_size,
        ));
        let database_admission = Arc::new(DatabaseAdmission::new(
            connection_limits.connection_budget(),
        ));
        let cleanup_limits = cleanup_queue_limits(admin_pool_size);
        // One waiting slot per worker bounds deferred residual databases while
        // the separate worker limit preserves lifecycle-pool headroom.
        let database_cleanup = DatabaseCleanupQueue::new(
            config.project.as_str(),
            cleanup_limits.worker_count,
            cleanup_limits.queue_capacity,
            database_admission.clone(),
        )?;

        Ok(Arc::new(Self {
            admin_url,
            project: config.project,
            owner_key,
            operation_timeout: config.operation_timeout,
            template_wait_timeout: config.template_wait_timeout,
            stale_after: config.stale_after,
            cleanup_on_start: config.cleanup_on_start,
            connection_limits,
            database_admission,
            database_cleanup,
            admin_sessions,
            template_cache: Mutex::new(TemplateCache::default()),
            prewarm_pools: Mutex::new(Vec::new()),
            _owner_lock: Mutex::new(Some(PersistentClient::new(owner_lock))),
            container,
        }))
    }

    pub(crate) fn template_cache_action(
        self: &Arc<Self>,
        fingerprint: TemplateFingerprint,
    ) -> TemplateCacheAction {
        let mut cache = self.lock_template_cache();
        match cache.entries.get(&fingerprint) {
            Some(TemplateCacheEntry::Ready(template)) => {
                if let Some(template) = template.upgrade() {
                    return TemplateCacheAction::Ready(template);
                }
            }
            Some(TemplateCacheEntry::Initializing(flight)) => {
                return TemplateCacheAction::Wait(TemplateCacheWaiter {
                    flight: flight.clone(),
                    completed: flight.completed.subscribe(),
                });
            }
            None => {}
        }

        // Cold acquisitions are the only path that can grow the cache. Prune
        // dead weak entries here so warm lookups stay O(1) while unique,
        // short-lived fingerprints cannot accumulate for the server lifetime.
        cache.prune_expired_ready_entries();
        let flight = Arc::new(TemplateFlight::new());
        cache.entries.insert(
            fingerprint,
            TemplateCacheEntry::Initializing(flight.clone()),
        );
        TemplateCacheAction::Initialize(TemplateCacheInitializer {
            server: self.clone(),
            fingerprint,
            flight,
            active: true,
        })
    }

    fn lock_template_cache(&self) -> std::sync::MutexGuard<'_, TemplateCache> {
        // Cache critical sections never run user code. Recovering the contents
        // lets an initializer's cancellation guard still wake waiters if some
        // unrelated panic poisoned the mutex.
        self.template_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn register_prewarm_pool(&self, pool: &Arc<PrewarmPoolInner>) -> Result<()> {
        let mut pools = self
            .prewarm_pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.database_admission.is_closed() {
            return Err(Error::ConnectionBudgetClosed);
        }
        pools.retain(|pool| pool.strong_count() > 0);
        pools.push(Arc::downgrade(pool));
        Ok(())
    }

    #[cfg(feature = "containers")]
    fn close_prewarm_pools(&self) {
        let pools = std::mem::take(
            &mut *self
                .prewarm_pools
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for pool in pools.into_iter().filter_map(|pool| pool.upgrade()) {
            pool.close_and_queue_ready();
        }
    }

    pub(crate) async fn acquire_database_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.database_admission
            .acquire(self.connection_limits.connections_per_database())
            .await
    }

    pub(crate) fn create_managed_database_classified(
        &self,
        client: &mut AdminClient,
        database_name: &DatabaseName,
        template_name: &str,
        metadata: &ResourceMetadata,
    ) -> std::result::Result<(), ManagedDatabaseCreationFailure> {
        self.database_admission.attempt_managed_creation(|| {
            attempt_managed_database_creation(client, database_name, template_name, metadata)
        })
    }

    pub(crate) fn with_lifecycle_admin<T>(
        &self,
        connect_operation: &'static str,
        operation: impl FnOnce(&mut AdminClient) -> Result<T>,
    ) -> Result<T> {
        self.admin_sessions.execute(connect_operation, operation)
    }

    pub(crate) fn with_lifecycle_admin_disposition<T>(
        &self,
        connect_operation: &'static str,
        operation: impl FnOnce(&mut AdminClient) -> AdminSessionDisposition<T>,
    ) -> Result<T> {
        self.admin_sessions
            .execute_with_disposition(connect_operation, operation)
    }

    pub(crate) fn is_external(&self) -> bool {
        self.container.is_none()
    }

    pub(crate) fn container_id(&self) -> Option<&str> {
        #[cfg(feature = "containers")]
        {
            self.container
                .as_ref()
                .map(|container| container.id.as_str())
        }
        #[cfg(not(feature = "containers"))]
        {
            None
        }
    }

    pub(crate) async fn shutdown_container(&self) -> Result<()> {
        #[cfg(not(feature = "containers"))]
        {
            Ok(())
        }

        #[cfg(feature = "containers")]
        {
            let Some(container) =
                begin_owned_container_shutdown(self.container.as_ref(), &self.database_admission)
            else {
                return Ok(());
            };
            // Queue every idle prewarmed database before fixing the cleanup
            // barrier's target. This also prevents in-flight dirty returns from
            // creating replacements during terminal shutdown.
            self.close_prewarm_pools();
            let database_cleanup = self.database_cleanup.clone();
            let admin_sessions = self.admin_sessions.clone();
            // Keep the whole terminal sequence in one blocking task. Cancellation
            // of the async caller can detach this task, but cannot skip pool closure
            // or container removal after the cleanup barrier has begun.
            let outcome = run_blocking(move || {
                let cleanup = database_cleanup.close_and_begin_drain();
                admin_sessions.close();
                let shutdown = container.shutdown();
                Ok(OwnedShutdownOutcome { cleanup, shutdown })
            })
            .await?;
            outcome.finish()
        }
    }
}

impl TemplateFlight {
    fn new() -> Self {
        let (completed, _) = watch::channel(false);
        Self {
            completed,
            template: Mutex::new(None),
        }
    }

    fn publish(&self, template: Arc<TemplateInner>) {
        let mut published = self
            .template
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(published.is_none());
        *published = Some(template);
    }

    fn published_template(&self) -> Option<Arc<TemplateInner>> {
        self.template
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn complete(&self) {
        self.completed.send_replace(true);
    }
}

impl TemplateCacheInitializer {
    pub(crate) fn publish(mut self, template: Arc<TemplateInner>) -> Arc<TemplateInner> {
        debug_assert_eq!(template.fingerprint(), self.fingerprint);
        let installed = {
            let mut cache = self.server.lock_template_cache();
            let owns_entry = matches!(
                cache.entries.get(&self.fingerprint),
                Some(TemplateCacheEntry::Initializing(flight))
                    if Arc::ptr_eq(flight, &self.flight)
            );
            if owns_entry {
                self.flight.publish(template.clone());
                cache.entries.insert(
                    self.fingerprint,
                    TemplateCacheEntry::Ready(Arc::downgrade(&template)),
                );
            }
            owns_entry
        };
        debug_assert!(
            installed,
            "template flight owner must publish its own entry"
        );
        self.active = false;
        self.flight.complete();
        template
    }
}

impl Drop for TemplateCacheInitializer {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        {
            let mut cache = self.server.lock_template_cache();
            let owns_entry = matches!(
                cache.entries.get(&self.fingerprint),
                Some(TemplateCacheEntry::Initializing(flight))
                    if Arc::ptr_eq(flight, &self.flight)
            );
            if owns_entry {
                cache.entries.remove(&self.fingerprint);
            }
        }
        self.flight.complete();
    }
}

impl TemplateCacheWaiter {
    pub(crate) async fn wait(mut self) -> Option<Arc<TemplateInner>> {
        if !*self.completed.borrow() {
            let _ = self.completed.changed().await;
        }
        self.flight.published_template()
    }
}

impl TemplateInner {
    pub(crate) fn new(
        server: Arc<ServerInner>,
        name: DatabaseName,
        fingerprint: TemplateFingerprint,
        shared_lock: PersistentClient,
    ) -> Self {
        Self {
            server,
            name,
            fingerprint,
            _shared_lock: Mutex::new(shared_lock),
        }
    }

    pub(crate) fn server(&self) -> &Arc<ServerInner> {
        &self.server
    }

    pub(crate) fn name(&self) -> &DatabaseName {
        &self.name
    }

    pub(crate) fn fingerprint(&self) -> TemplateFingerprint {
        self.fingerprint
    }
}

#[cfg(feature = "containers")]
struct OwnedShutdownOutcome {
    cleanup: crate::cleanup::CleanupDrainOutcome,
    shutdown: Result<()>,
}

#[cfg(feature = "containers")]
impl OwnedShutdownOutcome {
    fn finish(self) -> Result<()> {
        combine_cleanup_and_shutdown(self.cleanup.finish(), self.shutdown)
    }
}

#[cfg(feature = "containers")]
fn combine_cleanup_and_shutdown(cleanup: Result<()>, shutdown: Result<()>) -> Result<()> {
    match (cleanup, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(cleanup), Ok(())) => Err(cleanup),
        (Ok(()), Err(shutdown)) => Err(shutdown),
        (Err(cleanup), Err(shutdown)) => Err(Error::CleanupAndContainerShutdown {
            cleanup: Box::new(cleanup),
            shutdown: Box::new(shutdown),
        }),
    }
}

fn per_harness_admin_session_pool_size(
    connection_limits: ConnectionLimits,
    regular_connection_slots: usize,
) -> usize {
    let postgres_headroom_limit =
        (regular_connection_slots / LIFECYCLE_ADMIN_CONNECTION_SHARE_DIVISOR).max(1);
    connection_limits
        .max_simultaneous_leases()
        .min(postgres_headroom_limit)
        .max(1)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CleanupQueueLimits {
    worker_count: usize,
    queue_capacity: usize,
}

fn cleanup_queue_limits(admin_pool_size: usize) -> CleanupQueueLimits {
    debug_assert!(admin_pool_size > 0);
    let worker_count =
        (admin_pool_size / CLEANUP_ADMIN_SESSION_SHARE_DIVISOR).clamp(1, MAX_CLEANUP_WORKERS);
    CleanupQueueLimits {
        worker_count,
        queue_capacity: worker_count,
    }
}

#[cfg(feature = "containers")]
fn begin_owned_container_shutdown(
    container: Option<&Arc<ContainerOwner>>,
    database_admission: &DatabaseAdmission,
) -> Option<Arc<ContainerOwner>> {
    let container = container?.clone();
    database_admission.close();
    Some(container)
}

pub(crate) async fn run_blocking<T, F>(operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|source| Error::BlockingTask { source })?
}

#[cfg(feature = "containers")]
type RemovalResult = std::result::Result<(), testcontainers::TestcontainersError>;

#[cfg(feature = "containers")]
#[derive(Clone, Copy)]
enum ContainerCommand {
    Ready,
    Shutdown,
}

#[cfg(feature = "containers")]
struct ContainerWorker {
    shutdown: Sender<ContainerCommand>,
    handle: JoinHandle<RemovalResult>,
}

#[derive(Clone)]
#[cfg(feature = "containers")]
struct ContainerExit {
    exit_code: Option<i64>,
    output: Vec<u8>,
}

#[cfg(feature = "containers")]
struct StartedContainer {
    id: String,
    host: String,
    port: u16,
}

#[cfg(feature = "containers")]
pub(crate) struct ContainerOwner {
    id: String,
    worker: Mutex<Option<ContainerWorker>>,
    startup_exit: Arc<Mutex<Option<ContainerExit>>>,
}

#[cfg(not(feature = "containers"))]
pub(crate) struct ContainerOwner;

#[cfg(feature = "containers")]
impl ContainerOwner {
    fn start(
        image: ImageReference,
        profile: OwnedContainerProfile,
        project: &ProjectName,
        run_id: &str,
        startup_timeout: Duration,
    ) -> Result<(Arc<Self>, AdminDatabaseUrl)> {
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (shutdown_sender, shutdown_receiver) = mpsc::channel();
        let project_label = project.as_str().to_owned();
        let run_label = run_id.to_owned();
        let created_label = unix_now().to_string();
        let startup_exit = Arc::new(Mutex::new(None));
        let worker_startup_exit = startup_exit.clone();
        let worker = std::thread::Builder::new()
            .name("postgres-test-harness-container".to_owned())
            .spawn(move || {
                let request = container_request(
                    image,
                    profile,
                    startup_timeout,
                    project_label,
                    run_label,
                    created_label,
                );
                let container = match request.start() {
                    Ok(container) => container,
                    Err(source) => {
                        let _ = started_sender.send(Err(source));
                        return Ok(());
                    }
                };
                let started = resolve_started_container_with_retry(&container, startup_timeout);
                if started_sender.send(started).is_err() {
                    return container.rm();
                }

                monitor_container_startup(
                    &container,
                    &shutdown_receiver,
                    &worker_startup_exit,
                );
                let removal_result = container.rm();
                if let Err(error) = &removal_result {
                    eprintln!(
                        "postgres-test-harness: failed to remove owned PostgreSQL container: {error}"
                    );
                }
                removal_result
            })
            .map_err(|source| Error::ContainerWorkerStart { source })?;

        let started = match started_receiver.recv_timeout(startup_timeout) {
            Ok(Ok(started)) => started,
            Ok(Err(source)) => {
                drop(started_receiver);
                drop(shutdown_sender);
                let startup = map_container_start_error(profile, source);
                return Err(combine_startup_and_cleanup(
                    startup,
                    join_failed_startup_worker(worker),
                ));
            }
            Err(RecvTimeoutError::Timeout) => {
                drop(started_receiver);
                drop(shutdown_sender);
                let startup = Error::ContainerStartupTimeout {
                    timeout: startup_timeout,
                };
                return Err(combine_startup_and_cleanup(
                    startup,
                    join_failed_startup_worker(worker),
                ));
            }
            Err(RecvTimeoutError::Disconnected) => {
                drop(started_receiver);
                drop(shutdown_sender);
                return Err(combine_startup_and_cleanup(
                    Error::ContainerWorkerStopped,
                    join_failed_startup_worker(worker),
                ));
            }
        };
        let admin_url = AdminDatabaseUrl::parse(&format!(
            "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{}:{}/{POSTGRES_DATABASE}?sslmode=disable",
            started.host, started.port
        ))?;
        let owner = Arc::new(Self {
            id: started.id,
            worker: Mutex::new(Some(ContainerWorker {
                shutdown: shutdown_sender,
                handle: worker,
            })),
            startup_exit,
        });
        register_container(&owner)?;
        Ok((owner, admin_url))
    }

    fn mark_ready(&self) -> Result<()> {
        let worker = self.worker.lock().map_err(|_| Error::StatePoisoned {
            operation: "lock PostgreSQL container worker",
        })?;
        let Some(worker) = worker.as_ref() else {
            return Err(Error::ContainerWorkerStopped);
        };
        worker
            .shutdown
            .send(ContainerCommand::Ready)
            .map_err(|_| Error::ContainerWorkerStopped)
    }

    fn startup_exit(&self) -> Option<ContainerExit> {
        self.startup_exit.lock().ok().and_then(|exit| exit.clone())
    }

    fn shutdown(&self) -> Result<()> {
        let mut worker = self.worker.lock().map_err(|_| Error::StatePoisoned {
            operation: "lock PostgreSQL container worker",
        })?;
        let Some(ContainerWorker { shutdown, handle }) = worker.take() else {
            return Ok(());
        };

        let _ = shutdown.send(ContainerCommand::Shutdown);
        drop(shutdown);
        handle
            .join()
            .map_err(|_| Error::ContainerWorkerPanicked)?
            .map_err(|source| Error::ContainerRemove { source })
    }
}

#[cfg(feature = "containers")]
fn join_failed_startup_worker(worker: JoinHandle<RemovalResult>) -> Result<()> {
    worker
        .join()
        .map_err(|_| Error::ContainerWorkerPanicked)?
        .map_err(|source| Error::ContainerRemove { source })
}

#[cfg(feature = "containers")]
fn combine_startup_and_cleanup(startup: Error, cleanup: Result<()>) -> Error {
    match cleanup {
        Ok(()) => startup,
        Err(cleanup) => Error::ContainerStartupAndCleanup {
            startup: Box::new(startup),
            cleanup: Box::new(cleanup),
        },
    }
}

#[cfg(feature = "containers")]
impl Drop for ContainerOwner {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            eprintln!(
                "postgres-test-harness: failed to stop owned PostgreSQL container '{}': {error}",
                self.id
            );
        }
    }
}

#[cfg(feature = "containers")]
fn container_request(
    image: ImageReference,
    profile: OwnedContainerProfile,
    startup_timeout: Duration,
    project_label: String,
    run_label: String,
    created_label: String,
) -> ContainerRequest<GenericImage> {
    // Docker Official postgres starts a temporary socket-only server during
    // initialization and logs the normal readiness message for it. Let Docker
    // report that the process started, then prove final readiness with an
    // authenticated connection through the mapped TCP port below.
    let mut request = GenericImage::new(image.repository, image.tag)
        .with_wait_for(WaitFor::Nothing)
        .with_exposed_port(POSTGRES_PORT.tcp())
        .with_env_var("POSTGRES_USER", POSTGRES_USER)
        .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env_var("POSTGRES_DB", POSTGRES_DATABASE)
        .with_cmd([
            "postgres",
            "-c",
            "fsync=off",
            "-c",
            "synchronous_commit=off",
            "-c",
            "full_page_writes=off",
            "-c",
            "max_connections=300",
        ])
        // The harness enforces the caller's deadline outside Testcontainers.
        // A very short Testcontainers timeout can be cancelled after Docker
        // creates a container but before a removable handle is returned.
        .with_startup_timeout(startup_timeout.max(CONTAINER_ENGINE_STARTUP_TIMEOUT_FLOOR))
        .with_label(MANAGED_LABEL, "true")
        .with_label(PROJECT_LABEL, project_label)
        .with_label(RUN_LABEL, run_label)
        .with_label(CREATED_LABEL, created_label);

    if profile.initdb_no_sync() {
        request = request.with_env_var("POSTGRES_INITDB_ARGS", POSTGRES_INITDB_NO_SYNC);
    }
    if let Some(size_bytes) = profile.tmpfs_size_bytes() {
        request = request.with_mount(
            Mount::tmpfs_mount(POSTGRES_STORAGE_PATH)
                .with_size_bytes(size_bytes as i64)
                // Match the Docker Official image's volume-parent mode so its
                // root entrypoint can create and chown the versioned PGDATA.
                .with_mode(0o1777),
        );
    }
    request
}

#[cfg(feature = "containers")]
fn wait_for_owned_server(
    admin_url: &AdminDatabaseUrl,
    operation_timeout: Duration,
    startup_started: Instant,
    startup_timeout: Duration,
    profile: OwnedContainerProfile,
    container: &ContainerOwner,
) -> Result<AdminClient> {
    let mut last_error = None;
    loop {
        if let Some(exit) = container.startup_exit() {
            return Err(container_exit_error(profile, exit));
        }

        let remaining = startup_timeout.saturating_sub(startup_started.elapsed());
        if remaining.as_millis() == 0 {
            if let Some(exit) = container.startup_exit() {
                return Err(container_exit_error(profile, exit));
            }
            return Err(last_error.map_or(
                Error::ContainerStartupTimeout {
                    timeout: startup_timeout,
                },
                |source| Error::ContainerReadinessTimeout {
                    timeout: startup_timeout,
                    source: Box::new(source),
                },
            ));
        }

        let attempt_timeout = STARTUP_CONNECT_ATTEMPT_TIMEOUT.min(remaining);
        match connect_admin_with_setup_timeout(
            admin_url,
            operation_timeout,
            "wait for final owned PostgreSQL TCP server",
            attempt_timeout,
            remaining,
        ) {
            Ok(client) => return Ok(client),
            Err(error) => last_error = Some(error),
        }

        let remaining = startup_timeout.saturating_sub(startup_started.elapsed());
        if remaining.as_millis() > 0 {
            std::thread::sleep(STARTUP_RETRY_INTERVAL.min(remaining));
        }
    }
}

#[cfg(feature = "containers")]
fn monitor_container_startup(
    container: &Container<GenericImage>,
    commands: &mpsc::Receiver<ContainerCommand>,
    startup_exit: &Mutex<Option<ContainerExit>>,
) {
    loop {
        match commands.recv_timeout(STARTUP_STATUS_INTERVAL) {
            Ok(ContainerCommand::Ready) => {
                let _ = commands.recv();
                return;
            }
            Ok(ContainerCommand::Shutdown) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
        }

        if matches!(container.is_running(), Ok(false)) {
            let exit = ContainerExit {
                exit_code: container.exit_code().unwrap_or(None),
                output: bounded_container_output(container),
            };
            if let Ok(mut state) = startup_exit.lock() {
                *state = Some(exit);
            }
            let _ = commands.recv();
            return;
        }
    }
}

#[cfg(feature = "containers")]
fn bounded_container_output(container: &Container<GenericImage>) -> Vec<u8> {
    let mut output = Vec::new();
    let _ = container
        .stderr(false)
        .take(STARTUP_LOG_LIMIT_BYTES)
        .read_to_end(&mut output);
    let remaining = STARTUP_LOG_LIMIT_BYTES.saturating_sub(output.len() as u64);
    if remaining > 0 {
        let _ = container
            .stdout(false)
            .take(remaining)
            .read_to_end(&mut output);
    }
    output
}

#[cfg(feature = "containers")]
fn container_exit_error(profile: OwnedContainerProfile, exit: ContainerExit) -> Error {
    if let Some(tmpfs_size_bytes) = profile.tmpfs_size_bytes() {
        if let Some(evidence) = storage_exhaustion_evidence(&exit.output) {
            return Error::ContainerStorageExhausted {
                tmpfs_size_bytes,
                evidence,
            };
        }
        if let Some(evidence) = memory_exhaustion_evidence(&exit.output, exit.exit_code) {
            return Error::ContainerMemoryExhausted {
                tmpfs_size_bytes,
                evidence,
            };
        }
    }
    Error::ContainerExitedBeforeReady {
        exit_code: exit.exit_code,
    }
}

#[cfg(feature = "containers")]
fn storage_exhaustion_evidence(output: &[u8]) -> Option<&'static str> {
    let output = String::from_utf8_lossy(output).to_ascii_lowercase();
    if output.contains("no space left on device") {
        Some("container log reported no space left on device")
    } else {
        None
    }
}

#[cfg(feature = "containers")]
fn memory_exhaustion_evidence(output: &[u8], exit_code: Option<i64>) -> Option<&'static str> {
    let output = String::from_utf8_lossy(output).to_ascii_lowercase();
    if output.contains("cannot allocate memory") || output.contains("out of memory") {
        Some("container log reported memory exhaustion")
    } else if exit_code == Some(137) {
        Some("container exited with status 137, consistent with an engine OOM kill")
    } else {
        None
    }
}

#[cfg(feature = "containers")]
fn map_container_start_error(
    profile: OwnedContainerProfile,
    source: testcontainers::TestcontainersError,
) -> Error {
    match (
        profile.tmpfs_size_bytes(),
        tmpfs_start_error_evidence(&source),
    ) {
        (Some(tmpfs_size_bytes), true) => Error::ContainerStorageStart {
            tmpfs_size_bytes,
            source,
        },
        _ => Error::ContainerStart { source },
    }
}

#[cfg(feature = "containers")]
fn tmpfs_start_error_evidence(source: &testcontainers::TestcontainersError) -> bool {
    let message = source.to_string().to_ascii_lowercase();
    message.contains("tmpfs")
        || (message.contains("mount") && message.contains(POSTGRES_STORAGE_PATH))
}

#[cfg(feature = "containers")]
fn resolve_started_container(
    container: &Container<GenericImage>,
) -> std::result::Result<StartedContainer, testcontainers::TestcontainersError> {
    Ok(StartedContainer {
        id: container.id().to_owned(),
        host: ipv4_mapped_container_host(container.get_host()?.to_string()),
        port: container.get_host_port_ipv4(POSTGRES_PORT.tcp())?,
    })
}

#[cfg(feature = "containers")]
fn resolve_started_container_with_retry(
    container: &Container<GenericImage>,
    startup_timeout: Duration,
) -> std::result::Result<StartedContainer, testcontainers::TestcontainersError> {
    let started = Instant::now();
    loop {
        match resolve_started_container(container) {
            Ok(container) => return Ok(container),
            Err(error) if started.elapsed() >= startup_timeout => return Err(error),
            Err(error) => {
                if matches!(container.is_running(), Ok(false)) {
                    return Err(error);
                }
                std::thread::sleep(
                    STARTUP_RETRY_INTERVAL.min(startup_timeout.saturating_sub(started.elapsed())),
                );
            }
        }
    }
}

#[cfg(feature = "containers")]
fn ipv4_mapped_container_host(host: String) -> String {
    if host.eq_ignore_ascii_case("localhost") {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}

#[cfg(feature = "containers")]
fn register_container(container: &Arc<ContainerOwner>) -> Result<()> {
    let registration = *EXIT_REGISTRATION.get_or_init(|| {
        // SAFETY: the callback has the required C ABI and only accesses
        // process-global synchronization primitives that intentionally live
        // until process termination.
        unsafe { libc::atexit(cleanup_owned_containers_at_exit) }
    });
    if registration != 0 {
        return Err(Error::ExitCleanupRegistration);
    }
    CONTAINER_REGISTRY
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .map_err(|_| Error::StatePoisoned {
            operation: "register PostgreSQL container for process-exit cleanup",
        })?
        .push(Arc::downgrade(container));
    Ok(())
}

#[cfg(feature = "containers")]
extern "C" fn cleanup_owned_containers_at_exit() {
    let Some(registry) = CONTAINER_REGISTRY.get() else {
        return;
    };
    let Ok(mut containers) = registry.lock() else {
        return;
    };
    for container in containers
        .drain(..)
        .filter_map(|container| container.upgrade())
    {
        if let Err(error) = container.shutdown() {
            eprintln!(
                "postgres-test-harness: process-exit cleanup failed for container '{}': {error}",
                container.id
            );
        }
    }
}

#[cfg(feature = "containers")]
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod template_cache_tests {
    use std::sync::{Arc, Weak};

    use super::{TemplateCache, TemplateCacheEntry, TemplateFlight};
    use crate::FingerprintBuilder;

    #[test]
    fn cold_acquisition_prunes_expired_ready_entries_only() {
        let mut cache = TemplateCache::default();
        for index in 0..3 {
            cache.entries.insert(
                FingerprintBuilder::new(format!("expired-{index}"))
                    .finish_root()
                    .fingerprint(),
                TemplateCacheEntry::Ready(Weak::new()),
            );
        }
        let initializing = FingerprintBuilder::new("initializing")
            .finish_root()
            .fingerprint();
        cache.entries.insert(
            initializing,
            TemplateCacheEntry::Initializing(Arc::new(TemplateFlight::new())),
        );

        cache.prune_expired_ready_entries();

        assert_eq!(cache.entries.len(), 1);
        assert!(matches!(
            cache.entries.get(&initializing),
            Some(TemplateCacheEntry::Initializing(_))
        ));
    }
}

#[cfg(all(test, feature = "containers"))]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

    use super::{
        ContainerCommand, ContainerOwner, ContainerWorker, POSTGRES_INITDB_NO_SYNC,
        POSTGRES_STORAGE_PATH, ServerInner, begin_owned_container_shutdown, cleanup_queue_limits,
        combine_cleanup_and_shutdown, combine_startup_and_cleanup, container_request,
        ipv4_mapped_container_host, map_container_start_error, memory_exhaustion_evidence,
        per_harness_admin_session_pool_size, storage_exhaustion_evidence,
    };
    use crate::{
        ConnectionLimits, Error, HarnessConfig, OwnedContainerProfile, admin::AdminDatabaseUrl,
        admission::DatabaseAdmission, config::ImageReference,
    };

    fn test_container_owner(
        work: impl FnOnce(mpsc::Receiver<ContainerCommand>) -> super::RemovalResult + Send + 'static,
    ) -> Arc<ContainerOwner> {
        let (shutdown, shutdown_receiver) = mpsc::channel();
        Arc::new(ContainerOwner {
            id: "test-container".to_owned(),
            worker: Mutex::new(Some(ContainerWorker {
                shutdown,
                handle: thread::spawn(move || work(shutdown_receiver)),
            })),
            startup_exit: Arc::new(Mutex::new(None)),
        })
    }

    #[test]
    fn ipv4_port_mapping_uses_an_ipv4_loopback_literal() {
        assert_eq!(
            ipv4_mapped_container_host("localhost".to_owned()),
            "127.0.0.1"
        );
        assert_eq!(
            ipv4_mapped_container_host("LOCALHOST".to_owned()),
            "127.0.0.1"
        );
        assert_eq!(
            ipv4_mapped_container_host("192.0.2.10".to_owned()),
            "192.0.2.10"
        );
        assert_eq!(
            ipv4_mapped_container_host("host.docker.internal".to_owned()),
            "host.docker.internal"
        );
    }

    #[test]
    fn invalid_connection_limits_fail_before_server_startup_side_effects() {
        let config = HarnessConfig::new("limits")
            .unwrap()
            .with_admin_database_url("not a PostgreSQL URL")
            .with_connections_per_database(121)
            .unwrap();

        let error = match ServerInner::start_blocking(config) {
            Ok(_) => panic!("incompatible connection limits must fail server startup"),
            Err(error) => error,
        };

        assert!(matches!(error, Error::InvalidConfiguration { .. }));
    }

    #[test]
    fn per_harness_admin_pool_policy_obeys_lifecycle_concurrency_and_server_headroom() {
        let limits = ConnectionLimits {
            connection_budget: 120,
            connections_per_database: 11,
        };
        assert_eq!(per_harness_admin_session_pool_size(limits, 297), 10);
        assert_eq!(per_harness_admin_session_pool_size(limits, 20), 5);
        assert_eq!(per_harness_admin_session_pool_size(limits, 3), 1);
        assert_eq!(per_harness_admin_session_pool_size(limits, 0), 1);

        let single_lifecycle = ConnectionLimits {
            connection_budget: 2,
            connections_per_database: 2,
        };
        assert_eq!(
            per_harness_admin_session_pool_size(single_lifecycle, 297),
            1
        );

        let oversized_application_policy = ConnectionLimits {
            connection_budget: 1_000,
            connections_per_database: 1,
        };
        assert_eq!(
            oversized_application_policy.max_simultaneous_leases(),
            1_000
        );
        assert_eq!(
            per_harness_admin_session_pool_size(oversized_application_policy, 297),
            74,
            "server headroom must still cap the lazy lifecycle pool"
        );
    }

    #[test]
    fn cleanup_policy_preserves_pool_headroom_and_caps_worker_threads() {
        for admin_pool_size in 2..=64 {
            let limits = cleanup_queue_limits(admin_pool_size);
            assert!(limits.worker_count < admin_pool_size);
            assert!(limits.worker_count <= 4);
            assert_eq!(limits.queue_capacity, limits.worker_count);
        }

        assert_eq!(cleanup_queue_limits(1).worker_count, 1);
        assert_eq!(cleanup_queue_limits(2).worker_count, 1);
        assert_eq!(cleanup_queue_limits(4).worker_count, 2);
        assert_eq!(cleanup_queue_limits(10).worker_count, 4);
        assert_eq!(cleanup_queue_limits(100).worker_count, 4);
    }

    #[test]
    fn oversized_application_policy_is_observable_without_inflating_owned_server_default() {
        let limits = HarnessConfig::new("oversized")
            .unwrap()
            .with_connection_budget(1_000)
            .unwrap()
            .with_connections_per_database(100)
            .unwrap()
            .connection_limits()
            .unwrap();
        assert_eq!(limits.max_simultaneous_leases(), 10);
        assert_eq!(
            limits.max_simultaneous_leases()
                * usize::try_from(limits.connections_per_database()).unwrap(),
            1_000
        );

        let request = container_request(
            ImageReference::parse("postgres:18").unwrap(),
            OwnedContainerProfile::default(),
            Duration::from_secs(60),
            "oversized".to_owned(),
            "run".to_owned(),
            "1".to_owned(),
        );
        let command = request
            .cmd()
            .map(|value| value.into_owned())
            .collect::<Vec<_>>();
        assert!(command.iter().any(|value| value == "max_connections=300"));
    }

    #[test]
    fn default_container_request_uses_bounded_tmpfs_and_only_no_sync_initdb() {
        let request = container_request(
            ImageReference::parse("postgres:18").unwrap(),
            OwnedContainerProfile::default(),
            Duration::from_secs(60),
            "project".to_owned(),
            "run".to_owned(),
            "1".to_owned(),
        );
        let environment = request
            .env_vars()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            environment
                .get("POSTGRES_INITDB_ARGS")
                .map(|value| value.as_ref()),
            Some(POSTGRES_INITDB_NO_SYNC)
        );
        assert!(!environment.contains_key("POSTGRES_HOST_AUTH_METHOD"));

        let mounts = request.mounts().collect::<Vec<_>>();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].target(), Some(POSTGRES_STORAGE_PATH));
        assert_eq!(
            mounts[0].mount_type(),
            testcontainers::core::MountType::Tmpfs
        );
        let options = mounts[0].tmpfs_options().expect("bounded tmpfs options");
        assert_eq!(
            options.size_bytes,
            OwnedContainerProfile::default()
                .tmpfs_size_bytes()
                .map(|size| size as i64)
        );
        assert_eq!(options.mode, Some(0o1777));

        let command = request
            .cmd()
            .map(|value| value.into_owned())
            .collect::<Vec<_>>();
        assert!(command.iter().any(|value| value == "max_connections=300"));
        assert!(!command.iter().any(|value| value.contains("wal_level")));
        assert!(
            !environment
                .values()
                .any(|value| value.contains("--no-data-checksums"))
        );
    }

    #[test]
    fn compatibility_profile_omits_official_image_storage_extensions() {
        let profile = OwnedContainerProfile::default()
            .with_initdb_no_sync(false)
            .without_tmpfs();
        let request = container_request(
            ImageReference::parse("registry.example/custom-postgres:18").unwrap(),
            profile,
            Duration::from_secs(60),
            "project".to_owned(),
            "run".to_owned(),
            "1".to_owned(),
        );

        assert!(
            !request
                .env_vars()
                .any(|(name, _)| name == "POSTGRES_INITDB_ARGS")
        );
        assert_eq!(request.mounts().count(), 0);
    }

    #[test]
    fn tmpfs_start_failures_require_storage_specific_evidence() {
        let error = map_container_start_error(
            OwnedContainerProfile::default(),
            testcontainers::TestcontainersError::other("tmpfs mounts are not supported"),
        );

        assert!(matches!(error, Error::ContainerStorageStart { .. }));
        assert!(error.to_string().contains("tmpfs mounts are not supported"));
        assert!(error.to_string().contains("without_tmpfs"));

        let error = map_container_start_error(
            OwnedContainerProfile::default(),
            testcontainers::TestcontainersError::other(format!(
                "invalid mount configuration for {POSTGRES_STORAGE_PATH}"
            )),
        );
        assert!(matches!(error, Error::ContainerStorageStart { .. }));

        let error = map_container_start_error(
            OwnedContainerProfile::default(),
            testcontainers::TestcontainersError::other("daemon unavailable"),
        );
        assert!(matches!(error, Error::ContainerStart { .. }));

        let error = map_container_start_error(
            OwnedContainerProfile::default().without_tmpfs(),
            testcontainers::TestcontainersError::other("tmpfs mounts are not supported"),
        );
        assert!(matches!(error, Error::ContainerStart { .. }));
    }

    #[test]
    fn storage_exhaustion_detection_is_specific_and_case_insensitive() {
        assert_eq!(
            storage_exhaustion_evidence(b"initdb: NO SPACE LEFT ON DEVICE"),
            Some("container log reported no space left on device")
        );
        assert_eq!(
            memory_exhaustion_evidence(b"fatal: cannot allocate memory", Some(1)),
            Some("container log reported memory exhaustion")
        );
        assert_eq!(
            memory_exhaustion_evidence(b"", Some(137)),
            Some("container exited with status 137, consistent with an engine OOM kill")
        );
        assert_eq!(storage_exhaustion_evidence(b"authentication failed"), None);
        assert_eq!(
            memory_exhaustion_evidence(b"authentication failed", Some(1)),
            None
        );
    }

    #[tokio::test]
    async fn owned_shutdown_closes_admission_and_wakes_waiters() {
        let container = test_container_owner(|shutdown| {
            shutdown.recv().unwrap();
            Ok(())
        });
        let admission = Arc::new(DatabaseAdmission::new(1));
        let active_permit = admission.acquire(1).await.unwrap();
        let waiting_admission = admission.clone();
        let waiter = tokio::spawn(async move { waiting_admission.acquire(1).await });
        tokio::task::yield_now().await;

        let owned = begin_owned_container_shutdown(Some(&container), &admission)
            .expect("owned container should begin shutdown");

        assert!(admission.is_closed());
        assert!(waiter.await.unwrap().is_err());
        assert!(owned.shutdown().is_ok());
        drop(active_permit);
        assert!(admission.acquire(1).await.is_err());
    }

    #[test]
    fn external_shutdown_keeps_admission_open() {
        let admission = DatabaseAdmission::new(1);

        assert!(begin_owned_container_shutdown(None, &admission).is_none());
        assert!(!admission.is_closed());
    }

    #[test]
    fn owned_shutdown_preserves_cleanup_and_container_failures() {
        let error = combine_cleanup_and_shutdown(
            Err(Error::InvalidConfiguration { reason: "cleanup" }),
            Err(Error::InvalidConfiguration {
                reason: "container",
            }),
        )
        .unwrap_err();
        let Error::CleanupAndContainerShutdown { cleanup, shutdown } = error else {
            panic!("unexpected combined shutdown error: {error:?}");
        };
        assert!(cleanup.to_string().contains("cleanup"));
        assert!(shutdown.to_string().contains("container"));
    }

    #[test]
    fn failed_startup_preserves_startup_and_cleanup_errors() {
        let startup = Error::ContainerStartupTimeout {
            timeout: Duration::from_millis(50),
        };
        let error = combine_startup_and_cleanup(
            startup,
            Err(Error::ContainerRemove {
                source: testcontainers::TestcontainersError::other("forced removal failure"),
            }),
        );

        let Error::ContainerStartupAndCleanup { startup, cleanup } = error else {
            panic!("unexpected combined startup error: {error:?}");
        };
        assert!(matches!(*startup, Error::ContainerStartupTimeout { .. }));
        assert!(matches!(*cleanup, Error::ContainerRemove { .. }));

        let startup = Error::ContainerWorkerStopped;
        assert!(matches!(
            combine_startup_and_cleanup(startup, Ok(())),
            Error::ContainerWorkerStopped
        ));
    }

    #[test]
    fn failed_owned_shutdown_keeps_admission_terminal() {
        let container = test_container_owner(|shutdown| {
            shutdown.recv().unwrap();
            Err(testcontainers::TestcontainersError::other(
                "forced removal failure",
            ))
        });
        let admission = DatabaseAdmission::new(1);

        let owned = begin_owned_container_shutdown(Some(&container), &admission).unwrap();
        assert!(matches!(
            owned.shutdown().unwrap_err(),
            Error::ContainerRemove { .. }
        ));
        assert!(admission.is_closed());
        assert!(container.shutdown().is_ok());
    }

    #[test]
    fn owned_container_is_shut_down_when_admin_connection_times_out() {
        let (shutdown_request_sender, shutdown_request_receiver) = mpsc::channel();
        let (shutdown_sender, shutdown_receiver) = mpsc::channel();
        let worker = thread::spawn(move || {
            shutdown_request_receiver.recv().unwrap();
            shutdown_sender.send(()).unwrap();
            Ok(())
        });
        let container = Arc::new(ContainerOwner {
            id: "test-container".to_owned(),
            worker: Mutex::new(Some(ContainerWorker {
                shutdown: shutdown_request_sender,
                handle: worker,
            })),
            startup_exit: Arc::new(Mutex::new(None)),
        });
        let config = HarnessConfig::new("cleanup").unwrap();
        let connection_limits = config.connection_limits().unwrap();
        let admin_url = AdminDatabaseUrl::parse(
            "postgres://postgres:secret@127.0.0.1:5432/postgres?sslmode=disable",
        )
        .unwrap();

        let error = match ServerInner::finish_start_with(
            config,
            connection_limits,
            admin_url,
            Some(container),
            1,
            |_, _, operation| {
                Err(Error::PostgresConnectTimeout {
                    operation,
                    timeout: Duration::from_millis(50),
                })
            },
        ) {
            Ok(_) => panic!("forced admin connection timeout must fail startup"),
            Err(error) => error,
        };

        assert!(matches!(error, Error::PostgresConnectTimeout { .. }));
        shutdown_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("owned container shutdown should run before startup returns");
    }

    #[test]
    fn container_shutdown_is_successful_and_idempotent() {
        let removals = Arc::new(AtomicUsize::new(0));
        let worker_removals = removals.clone();
        let container = test_container_owner(move |shutdown| {
            shutdown.recv().unwrap();
            worker_removals.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        container.shutdown().unwrap();
        container.shutdown().unwrap();

        assert_eq!(removals.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_container_shutdown_waits_for_the_terminal_transition() {
        let (removal_started_sender, removal_started_receiver) = mpsc::channel();
        let (release_removal_sender, release_removal_receiver) = mpsc::channel();
        let container = test_container_owner(move |shutdown| {
            shutdown.recv().unwrap();
            removal_started_sender.send(()).unwrap();
            release_removal_receiver.recv().unwrap();
            Ok(())
        });

        let first_container = container.clone();
        let first = thread::spawn(move || first_container.shutdown());
        removal_started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("first shutdown should begin container removal");

        let (second_started_sender, second_started_receiver) = mpsc::channel();
        let (second_finished_sender, second_finished_receiver) = mpsc::channel();
        let second = thread::spawn(move || {
            second_started_sender.send(()).unwrap();
            let result = container.shutdown();
            second_finished_sender.send(()).unwrap();
            result
        });
        second_started_receiver.recv().unwrap();
        assert!(
            second_finished_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "concurrent shutdown returned before removal finished"
        );

        release_removal_sender.send(()).unwrap();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
    }

    #[test]
    fn container_shutdown_returns_the_worker_removal_error() {
        let container = test_container_owner(|shutdown| {
            shutdown.recv().unwrap();
            Err(testcontainers::TestcontainersError::other(
                "forced removal failure",
            ))
        });

        let error = container.shutdown().unwrap_err();

        assert!(matches!(error, Error::ContainerRemove { .. }));
        assert!(error.to_string().contains("forced removal failure"));
        assert!(container.shutdown().is_ok());
    }

    #[test]
    fn container_shutdown_reports_a_worker_panic() {
        let container = test_container_owner(|shutdown| {
            shutdown.recv().unwrap();
            panic!("forced container worker panic");
        });

        assert!(matches!(
            container.shutdown().unwrap_err(),
            Error::ContainerWorkerPanicked
        ));
    }

    #[test]
    fn container_shutdown_uses_the_result_of_an_already_stopped_worker() {
        let container = test_container_owner(|_| Ok(()));

        container.shutdown().unwrap();
    }
}
