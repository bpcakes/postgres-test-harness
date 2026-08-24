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
        AdminClient, AdminDatabaseUrl, PersistentClient, acquire_advisory_lock, advisory_key,
        connect_admin, validate_postgres_18,
    },
    config::{ImageReference, ResolvedConnectionLimits},
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
        let connection_limits = config.resolved_connection_limits()?;
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

        Self::finish_start(config, connection_limits, admin_url, container, owner_key)
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
        let mut owner_lock = connector(
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
            connections_per_database: connection_limits.per_database,
            budget: Arc::new(Semaphore::new(connection_limits.budget)),
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

type RemovalResult = std::result::Result<(), testcontainers::TestcontainersError>;

struct ContainerWorker {
    shutdown: Sender<()>,
    handle: JoinHandle<RemovalResult>,
}

struct StartedContainer {
    id: String,
    host: String,
    port: u16,
}

pub(crate) struct ContainerOwner {
    id: String,
    worker: Mutex<Option<ContainerWorker>>,
}

impl ContainerOwner {
    fn start(
        image: ImageReference,
        project: &ProjectName,
        run_id: &str,
        startup_timeout: Duration,
    ) -> Result<(Arc<Self>, AdminDatabaseUrl)> {
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (shutdown_sender, shutdown_receiver) = mpsc::channel();
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
                        return Ok(());
                    }
                };
                let started = resolve_started_container(&container);
                if started_sender.send(started).is_err() {
                    return container.rm();
                }

                let _ = shutdown_receiver.recv();
                let removal_result = container.rm();
                if let Err(error) = &removal_result {
                    eprintln!(
                        "postgres-test-harness: failed to remove owned PostgreSQL container: {error}"
                    );
                }
                removal_result
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
            worker: Mutex::new(Some(ContainerWorker {
                shutdown: shutdown_sender,
                handle: worker,
            })),
        });
        register_container(&owner)?;
        Ok((owner, admin_url))
    }

    fn shutdown(&self) -> Result<()> {
        let mut worker = self.worker.lock().map_err(|_| Error::StatePoisoned {
            operation: "lock PostgreSQL container worker",
        })?;
        let Some(ContainerWorker { shutdown, handle }) = worker.take() else {
            return Ok(());
        };

        let _ = shutdown.send(());
        drop(shutdown);
        handle
            .join()
            .map_err(|_| Error::ContainerWorkerPanicked)?
            .map_err(|source| Error::ContainerRemove { source })
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
        host: ipv4_mapped_container_host(container.get_host()?.to_string()),
        port: container.get_host_port_ipv4(POSTGRES_PORT.tcp())?,
    })
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

    use super::{ContainerOwner, ContainerWorker, ServerInner, ipv4_mapped_container_host};
    use crate::{Error, HarnessConfig, admin::AdminDatabaseUrl};

    fn test_container_owner(
        work: impl FnOnce(mpsc::Receiver<()>) -> super::RemovalResult + Send + 'static,
    ) -> Arc<ContainerOwner> {
        let (shutdown, shutdown_receiver) = mpsc::channel();
        Arc::new(ContainerOwner {
            id: "test-container".to_owned(),
            worker: Mutex::new(Some(ContainerWorker {
                shutdown,
                handle: thread::spawn(move || work(shutdown_receiver)),
            })),
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
