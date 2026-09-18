#![doc = include_str!("../README.md")]

mod admin;
mod admission;
mod cleanup;
mod config;
mod error;
mod fingerprint;
mod harness;
mod metadata;
mod name;
mod server;

pub use config::{
    ConnectionLimits, DEFAULT_OWNED_CONTAINER_TMPFS_SIZE_BYTES, HarnessConfig,
    OwnedContainerProfile, POSTGRES_TEST_ADMIN_URL_ENV, POSTGRES_TEST_IMAGE_ENV, ProjectName,
};
pub use error::{BoxError, DeferredCleanupFailure, Error, Result};
pub use fingerprint::{FingerprintBuilder, RootSpec, StepSpec, TemplateFingerprint};
pub use harness::{
    CleanupReport, DatabaseLease, DatabaseTemplate, PostgresHarness, PrewarmPoolStatus,
    PrewarmedDatabasePool, cleanup_stale_databases,
};
