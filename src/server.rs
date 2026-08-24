use std::{
    io::Read,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        mpsc::{self, RecvTimeoutError, Sender},
    },
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime},
};

use testcontainers::{
    Container, ContainerRequest, GenericImage, ImageExt,
    core::{IntoContainerPort, Mount, WaitFor},
    runners::SyncRunner,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::{
    Error, HarnessConfig, ProjectName, Result,
    admin::{
        AdminClient, AdminDatabaseUrl, AdminSessionPool, PersistentClient, acquire_advisory_lock,
        advisory_key, connect_admin, connect_admin_with_timeout, regular_connection_slots,
        validate_postgres_18,
    },
    config::{ImageReference, OwnedContainerProfile, ResolvedConnectionLimits},
};

const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "postgres";
const POSTGRES_DATABASE: &str = "postgres";
const POSTGRES_STORAGE_PATH: &str = "/var/lib/postgresql";
const POSTGRES_INITDB_NO_SYNC: &str = "--no-sync";
const STARTUP_CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(250);
const CONTAINER_ENGINE_STARTUP_TIMEOUT_FLOOR: Duration = Duration::from_secs(60);
const STARTUP_RETRY_INTERVAL: Duration = Duration::from_millis(25);
const STARTUP_STATUS_INTERVAL: Duration = Duration::from_millis(25);
const STARTUP_LOG_LIMIT_BYTES: u64 = 64 * 1024;
// One harness's lifecycle pool may occupy at most one quarter of PostgreSQL's
// regular connection slots. Separate harnesses and processes do not coordinate
// this local bound.
const LIFECYCLE_ADMIN_CONNECTION_SHARE_DIVISOR: usize = 4;
const MANAGED_LABEL: &str = "org.postgres-test-harness.managed";
const PROJECT_LABEL: &str = "org.postgres-test-harness.project";
const RUN_LABEL: &str = "org.postgres-test-harness.run";
const CREATED_LABEL: &str = "org.postgres-test-harness.created";

static CONTAINER_REGISTRY: OnceLock<Mutex<Vec<Weak<ContainerOwner>>>> = OnceLock::new();
static EXIT_REGISTRATION: OnceLock<i32> = OnceLock::new();

pub(crate) struct ServerInner {
    pub(crate) admin_url: AdminDatabaseUrl,
    pub(crate) project: ProjectName,
    pub(crate) owner_key: i64,
    pub(crate) operation_timeout: Duration,
    pub(crate) template_wait_timeout: Duration,
    pub(crate) stale_after: Duration,
    pub(crate) cleanup_on_start: bool,
    pub(crate) connections_per_database: u32,
    budget: Arc<Semaphore>,
    admin_sessions: AdminSessionPool,
    _owner_lock: Mutex<Option<PersistentClient>>,
    container: Option<Arc<ContainerOwner>>,
}

impl ServerInner {
    pub(crate) async fn start(config: HarnessConfig) -> Result<Arc<Self>> {
        run_blocking(move || Self::start_blocking(config)).await
    }

    fn start_blocking(config: HarnessConfig) -> Result<Arc<Self>> {
        let connection_limits = config.resolved_connection_limits()?;
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

    fn finish_start(
        config: HarnessConfig,
        connection_limits: ResolvedConnectionLimits,
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
        connection_limits: ResolvedConnectionLimits,
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
        connection_limits: ResolvedConnectionLimits,
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
        let admin_sessions = AdminSessionPool::new(
            admin_url.clone(),
            config.operation_timeout,
            config.project.as_str(),
            admin_pool_size,
        );

        Ok(Arc::new(Self {
            admin_url,
            project: config.project,
            owner_key,
            operation_timeout: config.operation_timeout,
            template_wait_timeout: config.template_wait_timeout,
            stale_after: config.stale_after,
            cleanup_on_start: config.cleanup_on_start,
            connections_per_database: connection_limits.per_database,
            budget: Arc::new(Semaphore::new(connection_limits.budget)),
            admin_sessions,
            _owner_lock: Mutex::new(Some(PersistentClient::new(owner_lock))),
            container,
        }))
    }

    pub(crate) async fn acquire_database_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.budget
            .clone()
            .acquire_many_owned(self.connections_per_database)
            .await
            .map_err(|_| Error::ConnectionBudgetClosed)
    }

    pub(crate) fn with_lifecycle_admin<T>(
        &self,
        connect_operation: &'static str,
        operation: impl FnOnce(&mut AdminClient) -> Result<T>,
    ) -> Result<T> {
        self.admin_sessions.execute(connect_operation, operation)
    }

    pub(crate) fn is_external(&self) -> bool {
        self.container.is_none()
    }

    pub(crate) fn container_id(&self) -> Option<&str> {
        self.container
            .as_ref()
            .map(|container| container.id.as_str())
    }

    pub(crate) async fn shutdown_container(&self) -> Result<()> {
        let Some(container) = begin_owned_container_shutdown(self.container.as_ref(), &self.budget)
        else {
            return Ok(());
        };
        self.admin_sessions.close();
        run_blocking(move || container.shutdown()).await
    }
}

fn per_harness_admin_session_pool_size(
    connection_limits: ResolvedConnectionLimits,
    regular_connection_slots: usize,
) -> usize {
    let lifecycle_concurrency = connection_limits.budget
        / usize::try_from(connection_limits.per_database)
            .expect("u32 per-database connection limit fits usize");
    let postgres_headroom_limit =
        (regular_connection_slots / LIFECYCLE_ADMIN_CONNECTION_SHARE_DIVISOR).max(1);
    lifecycle_concurrency.min(postgres_headroom_limit).max(1)
}

fn begin_owned_container_shutdown(
    container: Option<&Arc<ContainerOwner>>,
    budget: &Semaphore,
) -> Option<Arc<ContainerOwner>> {
    let container = container?.clone();
    budget.close();
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

type RemovalResult = std::result::Result<(), testcontainers::TestcontainersError>;

#[derive(Clone, Copy)]
enum ContainerCommand {
    Ready,
    Shutdown,
}

struct ContainerWorker {
    shutdown: Sender<ContainerCommand>,
    handle: JoinHandle<RemovalResult>,
}

#[derive(Clone)]
struct ContainerExit {
    exit_code: Option<i64>,
    output: Vec<u8>,
}

struct StartedContainer {
    id: String,
    host: String,
    port: u16,
}

pub(crate) struct ContainerOwner {
    id: String,
    worker: Mutex<Option<ContainerWorker>>,
    startup_exit: Arc<Mutex<Option<ContainerExit>>>,
}

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
                join_failed_startup_worker(worker)?;
                return Err(map_container_start_error(profile, source));
            }
            Err(RecvTimeoutError::Timeout) => {
                drop(started_receiver);
                drop(shutdown_sender);
                join_failed_startup_worker(worker)?;
                return Err(Error::ContainerStartupTimeout {
                    timeout: startup_timeout,
                });
            }
            Err(RecvTimeoutError::Disconnected) => {
                drop(started_receiver);
                drop(shutdown_sender);
                join_failed_startup_worker(worker)?;
                return Err(Error::ContainerWorkerStopped);
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

fn join_failed_startup_worker(worker: JoinHandle<RemovalResult>) -> Result<()> {
    worker
        .join()
        .map_err(|_| Error::ContainerWorkerPanicked)?
        .map_err(|source| Error::ContainerRemove { source })
}

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
        match connect_admin_with_timeout(
            admin_url,
            operation_timeout,
            "wait for final owned PostgreSQL TCP server",
            attempt_timeout,
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

fn storage_exhaustion_evidence(output: &[u8]) -> Option<&'static str> {
    let output = String::from_utf8_lossy(output).to_ascii_lowercase();
    if output.contains("no space left on device") {
        Some("container log reported no space left on device")
    } else {
        None
    }
}

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

fn map_container_start_error(
    profile: OwnedContainerProfile,
    source: testcontainers::TestcontainersError,
) -> Error {
    match profile.tmpfs_size_bytes() {
        Some(tmpfs_size_bytes) => Error::ContainerStorageStart {
            tmpfs_size_bytes,
            source,
        },
        None => Error::ContainerStart { source },
    }
}

fn resolve_started_container(
    container: &Container<GenericImage>,
) -> std::result::Result<StartedContainer, testcontainers::TestcontainersError> {
    Ok(StartedContainer {
        id: container.id().to_owned(),
        host: ipv4_mapped_container_host(container.get_host()?.to_string()),
        port: container.get_host_port_ipv4(POSTGRES_PORT.tcp())?,
    })
}

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

fn ipv4_mapped_container_host(host: String) -> String {
    if host.eq_ignore_ascii_case("localhost") {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}

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

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
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
        POSTGRES_STORAGE_PATH, ServerInner, begin_owned_container_shutdown, container_request,
        ipv4_mapped_container_host, map_container_start_error, memory_exhaustion_evidence,
        per_harness_admin_session_pool_size, storage_exhaustion_evidence,
    };
    use crate::{
        Error, HarnessConfig, OwnedContainerProfile,
        admin::AdminDatabaseUrl,
        config::{ImageReference, ResolvedConnectionLimits},
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
        let limits = ResolvedConnectionLimits {
            budget: 120,
            per_database: 11,
        };
        assert_eq!(per_harness_admin_session_pool_size(limits, 297), 10);
        assert_eq!(per_harness_admin_session_pool_size(limits, 20), 5);
        assert_eq!(per_harness_admin_session_pool_size(limits, 3), 1);

        let single_lifecycle = ResolvedConnectionLimits {
            budget: 2,
            per_database: 2,
        };
        assert_eq!(
            per_harness_admin_session_pool_size(single_lifecycle, 297),
            1
        );
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
    fn tmpfs_start_failures_retain_daemon_details_and_an_opt_out() {
        let error = map_container_start_error(
            OwnedContainerProfile::default(),
            testcontainers::TestcontainersError::other("operation not permitted"),
        );

        assert!(matches!(error, Error::ContainerStorageStart { .. }));
        assert!(error.to_string().contains("operation not permitted"));
        assert!(error.to_string().contains("without_tmpfs"));

        let error = map_container_start_error(
            OwnedContainerProfile::default().without_tmpfs(),
            testcontainers::TestcontainersError::other("daemon unavailable"),
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
        let budget = Arc::new(tokio::sync::Semaphore::new(1));
        let active_permit = budget.clone().acquire_owned().await.unwrap();
        let waiting_budget = budget.clone();
        let waiter = tokio::spawn(async move { waiting_budget.acquire_owned().await });
        tokio::task::yield_now().await;

        let owned = begin_owned_container_shutdown(Some(&container), &budget)
            .expect("owned container should begin shutdown");

        assert!(budget.is_closed());
        assert!(waiter.await.unwrap().is_err());
        assert!(owned.shutdown().is_ok());
        drop(active_permit);
        assert!(budget.clone().acquire_owned().await.is_err());
    }

    #[test]
    fn external_shutdown_keeps_admission_open() {
        let budget = tokio::sync::Semaphore::new(1);

        assert!(begin_owned_container_shutdown(None, &budget).is_none());
        assert!(!budget.is_closed());
        assert_eq!(budget.available_permits(), 1);
    }

    #[test]
    fn failed_owned_shutdown_keeps_admission_terminal() {
        let container = test_container_owner(|shutdown| {
            shutdown.recv().unwrap();
            Err(testcontainers::TestcontainersError::other(
                "forced removal failure",
            ))
        });
        let budget = tokio::sync::Semaphore::new(1);

        let owned = begin_owned_container_shutdown(Some(&container), &budget).unwrap();
        assert!(matches!(
            owned.shutdown().unwrap_err(),
            Error::ContainerRemove { .. }
        ));
        assert!(budget.is_closed());
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
        let connection_limits = config.resolved_connection_limits().unwrap();
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
