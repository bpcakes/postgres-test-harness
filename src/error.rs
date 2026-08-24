use std::time::Duration;

/// Error type accepted from a project-owned template initializer.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Result returned by this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Failures from PostgreSQL test-harness configuration and lifecycle work.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid PostgreSQL test-harness configuration: {reason}")]
    InvalidConfiguration { reason: &'static str },

    #[error("invalid PostgreSQL test project name '{name}': {reason}")]
    InvalidProjectName { name: String, reason: &'static str },

    #[error("invalid PostgreSQL test image reference: {reason}")]
    InvalidImageReference { reason: &'static str },

    #[error("invalid PostgreSQL admin database URL: {reason}")]
    InvalidAdminDatabaseUrl { reason: &'static str },

    #[error("invalid template fingerprint")]
    InvalidTemplateFingerprint,

    #[error("invalid managed test database name '{name}'")]
    InvalidDatabaseName { name: String },

    #[error("PostgreSQL test connection failed during {operation}: {source}")]
    Postgres {
        operation: &'static str,
        #[source]
        source: tokio_postgres::Error,
    },

    #[error("PostgreSQL test connection timed out during {operation} after {timeout:?}")]
    PostgresConnectTimeout {
        operation: &'static str,
        timeout: Duration,
    },

    #[error("failed to initialize PostgreSQL admin runtime during {operation}: {source}")]
    PostgresRuntime {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "PostgreSQL 18 is required; connected server reported server_version_num={server_version_num}"
    )]
    UnsupportedPostgresVersion { server_version_num: i32 },

    #[error("PostgreSQL 18 capability check failed: uuidv7() is unavailable")]
    MissingUuidV7,

    #[error("failed to start the PostgreSQL test container: {source}")]
    ContainerStart {
        #[source]
        source: testcontainers::TestcontainersError,
    },

    #[error(
        "failed to start the PostgreSQL test container with a {tmpfs_size_bytes}-byte tmpfs storage cap; if this Docker daemon does not support tmpfs mounts, disable tmpfs through OwnedContainerProfile::without_tmpfs: {source}"
    )]
    ContainerStorageStart {
        tmpfs_size_bytes: u64,
        #[source]
        source: testcontainers::TestcontainersError,
    },

    #[error(
        "the PostgreSQL test container did not reach its final TCP server within the {timeout:?} startup timeout: {source}"
    )]
    ContainerReadinessTimeout {
        timeout: Duration,
        #[source]
        source: Box<Error>,
    },

    #[error(
        "the PostgreSQL test container stopped before its final TCP server was ready (exit code {exit_code:?})"
    )]
    ContainerExitedBeforeReady { exit_code: Option<i64> },

    #[error(
        "the PostgreSQL test container exhausted its {tmpfs_size_bytes}-byte tmpfs before its final TCP server was ready ({evidence}); increase the cap with OwnedContainerProfile::with_tmpfs_size_bytes or disable tmpfs with OwnedContainerProfile::without_tmpfs"
    )]
    ContainerStorageExhausted {
        tmpfs_size_bytes: u64,
        evidence: &'static str,
    },

    #[error(
        "the PostgreSQL test container encountered memory exhaustion while using a {tmpfs_size_bytes}-byte tmpfs before its final TCP server was ready ({evidence}); reduce memory pressure, increase the Docker memory allowance, or disable tmpfs with OwnedContainerProfile::without_tmpfs"
    )]
    ContainerMemoryExhausted {
        tmpfs_size_bytes: u64,
        evidence: &'static str,
    },

    #[error(
        "the PostgreSQL test container did not finish starting within the {timeout:?} startup timeout"
    )]
    ContainerStartupTimeout { timeout: Duration },

    #[error("failed to remove the PostgreSQL test container: {source}")]
    ContainerRemove {
        #[source]
        source: testcontainers::TestcontainersError,
    },

    #[error("failed to start the PostgreSQL container owner thread: {source}")]
    ContainerWorkerStart {
        #[source]
        source: std::io::Error,
    },

    #[error("the PostgreSQL container owner thread stopped unexpectedly")]
    ContainerWorkerStopped,

    #[error("the PostgreSQL container owner thread panicked")]
    ContainerWorkerPanicked,

    #[error("failed to register PostgreSQL container process-exit cleanup")]
    ExitCleanupRegistration,

    #[error("PostgreSQL test blocking worker failed: {source}")]
    BlockingTask {
        #[source]
        source: tokio::task::JoinError,
    },

    #[error("PostgreSQL test state lock was poisoned during {operation}")]
    StatePoisoned { operation: &'static str },

    #[error("PostgreSQL test connection budget is closed")]
    ConnectionBudgetClosed,

    #[error("the disposable PostgreSQL admin-session pool is closed")]
    AdminSessionPoolClosed,

    #[error("template initialization failed: {source}")]
    TemplateInitializer {
        #[source]
        source: BoxError,
    },

    #[error("template initialization failed ({initializer}); cleanup also failed ({cleanup})")]
    TemplateInitializerAndCleanup {
        #[source]
        initializer: BoxError,
        cleanup: Box<Error>,
    },

    #[error(
        "failed to tag a managed PostgreSQL database ({tagging}); compensating FORCE drop also failed ({cleanup})"
    )]
    ManagedDatabaseTagAndCleanup {
        #[source]
        tagging: Box<Error>,
        cleanup: Box<Error>,
    },

    #[error("managed PostgreSQL resource metadata is inconsistent for database '{database_name}'")]
    InconsistentMetadata { database_name: String },

    #[error("duration {duration:?} is outside the supported millisecond timeout range")]
    TimeoutOutOfRange { duration: Duration },

    #[error(
        "connections to PostgreSQL template database '{database_name}' could not be terminated"
    )]
    TemplateConnectionsRemain { database_name: String },

    #[error("failed to start the PostgreSQL fallback cleanup worker: {source}")]
    CleanupWorkerStart {
        #[source]
        source: std::io::Error,
    },
}

impl Error {
    pub(crate) fn postgres(operation: &'static str, source: tokio_postgres::Error) -> Self {
        Self::Postgres { operation, source }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::Error;

    #[test]
    fn combined_initializer_error_preserves_a_source() {
        let error = Error::TemplateInitializerAndCleanup {
            initializer: std::io::Error::other("initializer").into(),
            cleanup: Box::new(Error::InvalidConfiguration { reason: "cleanup" }),
        };
        assert_eq!(error.source().unwrap().to_string(), "initializer");
    }

    #[test]
    fn combined_managed_database_error_preserves_the_tagging_source() {
        let error = Error::ManagedDatabaseTagAndCleanup {
            tagging: Box::new(Error::InvalidConfiguration { reason: "tagging" }),
            cleanup: Box::new(Error::InvalidConfiguration { reason: "cleanup" }),
        };
        assert_eq!(
            error.source().unwrap().to_string(),
            "invalid PostgreSQL test-harness configuration: tagging"
        );
        assert!(error.to_string().contains("cleanup"));
    }
}
