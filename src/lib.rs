#![doc = include_str!("../README.md")]

mod admin;
mod config;
mod error;
mod fingerprint;
mod harness;
mod metadata;
mod name;
mod server;

pub use config::{
    HarnessConfig, POSTGRES_TEST_ADMIN_URL_ENV, POSTGRES_TEST_IMAGE_ENV, ProjectName,
};
pub use error::{BoxError, Error, Result};
pub use fingerprint::{FingerprintBuilder, TemplateFingerprint, TemplateSpec};
pub use harness::{
    CleanupReport, DatabaseLease, DatabaseTemplate, PostgresHarness, cleanup_stale_databases,
};
