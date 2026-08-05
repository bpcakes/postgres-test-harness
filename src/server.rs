use std::{
    sync::{
        Arc, Mutex, OnceLock, Weak,
        mpsc::{self, Sender},
    },
    thread::JoinHandle,
    time::{Duration, SystemTime},
};

use testcontainers::{
    Container, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::SyncRunner,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::{
    Error, HarnessConfig, ProjectName, Result,
    admin::{
        AdminDatabaseUrl, PersistentClient, acquire_advisory_lock, advisory_key, connect_admin,
        validate_postgres_18,
    },
    config::ImageReference,
};

const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "postgres";
const POSTGRES_DATABASE: &str = "postgres";
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
    _owner_lock: Mutex<Option<PersistentClient>>,
    container: Option<Arc<ContainerOwner>>,
}

impl ServerInner {
    pub(crate) async fn start(config: HarnessConfig) -> Result<Arc<Self>> {
        run_blocking(move || Self::start_blocking(config)).await
    }

    fn start_blocking(config: HarnessConfig) -> Result<Arc<Self>> {
        let run_id = Uuid::now_v7().simple().to_string();
        let owner_key = advisory_key("run", &run_id);
        let (admin_url, container) = if let Some(admin_url) = config.resolved_admin_database_url() {
            (AdminDatabaseUrl::parse(&admin_url)?, None)
        } else {
            let image = config.resolved_image()?;
            let (container, admin_url) =
                ContainerOwner::start(image, &config.project, &run_id, config.startup_timeout)?;
            (admin_url, Some(container))
        };

        let mut owner_lock = connect_admin(
            &admin_url,
            config.operation_timeout,
            "connect to PostgreSQL test server",
        )?;
        validate_postgres_18(&mut owner_lock)?;
        acquire_advisory_lock(&mut owner_lock, owner_key)?;

        Ok(Arc::new(Self {
            admin_url,
            project: config.project,
            owner_key,
            operation_timeout: config.operation_timeout,
            template_wait_timeout: config.template_wait_timeout,
            stale_after: config.stale_after,
            cleanup_on_start: config.cleanup_on_start,
            connections_per_database: config.connections_per_database,
            budget: Arc::new(Semaphore::new(config.connection_budget)),
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

    pub(crate) fn is_external(&self) -> bool {
        self.container.is_none()
    }

    pub(crate) fn container_id(&self) -> Option<&str> {
        self.container
            .as_ref()
            .map(|container| container.id.as_str())
    }

    pub(crate) async fn shutdown_container(&self) -> Result<()> {
        let Some(container) = self.container.clone() else {
            return Ok(());
        };
        run_blocking(move || container.shutdown()).await
    }
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

enum ContainerCommand {
    Shutdown(Sender<std::result::Result<(), testcontainers::TestcontainersError>>),
}

struct StartedContainer {
    id: String,
    host: String,
    port: u16,
}

pub(crate) struct ContainerOwner {
    id: String,
    commands: Mutex<Option<Sender<ContainerCommand>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl ContainerOwner {
    fn start(
        image: ImageReference,
        project: &ProjectName,
        run_id: &str,
        startup_timeout: Duration,
    ) -> Result<(Arc<Self>, AdminDatabaseUrl)> {
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (command_sender, command_receiver) = mpsc::channel();
        let project_label = project.as_str().to_owned();
        let run_label = run_id.to_owned();
        let created_label = unix_now().to_string();
        let worker = std::thread::Builder::new()
            .name("postgres-test-harness-container".to_owned())
            .spawn(move || {
                let request = GenericImage::new(image.repository, image.tag)
                    .with_wait_for(WaitFor::message_on_stderr(
                        "database system is ready to accept connections",
                    ))
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
                    .with_startup_timeout(startup_timeout)
                    .with_label(MANAGED_LABEL, "true")
                    .with_label(PROJECT_LABEL, project_label)
                    .with_label(RUN_LABEL, run_label)
                    .with_label(CREATED_LABEL, created_label);
                let container = match request.start() {
                    Ok(container) => container,
                    Err(source) => {
                        let _ = started_sender.send(Err(source));
                        return;
                    }
                };
                let started = resolve_started_container(&container);
                if started_sender.send(started).is_err() {
                    let _ = container.rm();
                    return;
                }

                let removal_result = match command_receiver.recv() {
                    Ok(ContainerCommand::Shutdown(result_sender)) => {
                        let result = container.rm();
                        let result_for_sender = result.as_ref().map(|_| ()).map_err(|error| {
                            testcontainers::TestcontainersError::other(error.to_string())
                        });
                        let _ = result_sender.send(result_for_sender);
                        result
                    }
                    Err(_) => container.rm(),
                };
                if let Err(error) = removal_result {
                    eprintln!(
                        "postgres-test-harness: failed to remove owned PostgreSQL container: {error}"
                    );
                }
            })
            .map_err(|source| Error::ContainerWorkerStart { source })?;

        let started = started_receiver
            .recv()
            .map_err(|_| Error::ContainerWorkerStopped)?
            .map_err(|source| Error::ContainerStart { source })?;
        let admin_url = AdminDatabaseUrl::parse(&format!(
            "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{}:{}/{POSTGRES_DATABASE}?sslmode=disable",
            started.host, started.port
        ))?;
        let owner = Arc::new(Self {
            id: started.id,
            commands: Mutex::new(Some(command_sender)),
            worker: Mutex::new(Some(worker)),
        });
        register_container(&owner)?;
        Ok((owner, admin_url))
    }

    fn shutdown(&self) -> Result<()> {
        let command_sender = self
            .commands
            .lock()
            .map_err(|_| Error::StatePoisoned {
                operation: "lock PostgreSQL container command sender",
            })?
            .take();
        if let Some(command_sender) = command_sender {
            let (result_sender, result_receiver) = mpsc::channel();
            if command_sender
                .send(ContainerCommand::Shutdown(result_sender))
                .is_ok()
            {
                result_receiver
                    .recv()
                    .map_err(|_| Error::ContainerWorkerStopped)?
                    .map_err(|source| Error::ContainerRemove { source })?;
            }
        }
        if let Some(worker) = self
            .worker
            .lock()
            .map_err(|_| Error::StatePoisoned {
                operation: "lock PostgreSQL container worker",
            })?
            .take()
        {
            worker.join().map_err(|_| Error::ContainerWorkerPanicked)?;
        }
        Ok(())
    }
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

fn resolve_started_container(
    container: &Container<GenericImage>,
) -> std::result::Result<StartedContainer, testcontainers::TestcontainersError> {
    Ok(StartedContainer {
        id: container.id().to_owned(),
        host: container.get_host()?.to_string(),
        port: container.get_host_port_ipv4(POSTGRES_PORT.tcp())?,
    })
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
