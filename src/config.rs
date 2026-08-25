use std::{fmt, num::NonZeroU32, time::Duration};

use crate::{Error, Result};

pub const POSTGRES_TEST_ADMIN_URL_ENV: &str = "POSTGRES_TEST_ADMIN_URL";
pub const POSTGRES_TEST_IMAGE_ENV: &str = "POSTGRES_TEST_IMAGE";

#[cfg(feature = "containers")]
const DEFAULT_IMAGE: &str = "postgres:18";
const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(90);
const DEFAULT_TEMPLATE_WAIT_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(60 * 60);
const DEFAULT_CONNECTION_BUDGET: u32 = 120;
const DEFAULT_CONNECTIONS_PER_DATABASE: u32 = 11;
const MAX_PROJECT_NAME_LEN: usize = 16;

/// Default upper bound for the owned container's tmpfs-backed PostgreSQL
/// storage (1 GiB).
pub const DEFAULT_OWNED_CONTAINER_TMPFS_SIZE_BYTES: u64 = 1024 * 1024 * 1024;

/// Performance and storage behavior applied only to harness-owned containers.
///
/// The default profile is intended for disposable test data: it passes
/// `--no-sync` through the Docker Official PostgreSQL image's
/// `POSTGRES_INITDB_ARGS` interface and mounts a size-bounded tmpfs at the
/// image's PostgreSQL volume parent. External-server mode ignores this profile.
///
/// Custom images must implement the same environment-variable, filesystem,
/// command, and mapped-TCP contracts as the Docker Official PostgreSQL 18
/// image. Disable either optimization when a compatible custom image or Docker
/// daemon cannot provide it.
///
/// This type remains available without the `containers` feature so shared
/// configuration code can compile, but external-only startup ignores it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnedContainerProfile {
    initdb_no_sync: bool,
    tmpfs_size_bytes: Option<u64>,
}

impl OwnedContainerProfile {
    /// Returns the default disposable-data performance profile.
    pub const fn performance() -> Self {
        Self {
            initdb_no_sync: true,
            tmpfs_size_bytes: Some(DEFAULT_OWNED_CONTAINER_TMPFS_SIZE_BYTES),
        }
    }

    /// Enables or disables `initdb --no-sync` for an owned container.
    ///
    /// Disable this for custom images that do not honor
    /// `POSTGRES_INITDB_ARGS`. This does not affect the runtime PostgreSQL
    /// durability settings used by the harness.
    pub const fn with_initdb_no_sync(mut self, enabled: bool) -> Self {
        self.initdb_no_sync = enabled;
        self
    }

    /// Sets the maximum number of bytes available to the owned container's
    /// tmpfs-backed PostgreSQL storage.
    pub fn with_tmpfs_size_bytes(mut self, size_bytes: u64) -> Result<Self> {
        if size_bytes == 0 || size_bytes > i64::MAX as u64 {
            return Err(Error::InvalidConfiguration {
                reason: "owned-container tmpfs size must be between 1 and i64::MAX bytes",
            });
        }
        self.tmpfs_size_bytes = Some(size_bytes);
        Ok(self)
    }

    /// Uses the image's normal storage instead of adding a tmpfs mount.
    ///
    /// This is the compatibility opt-out for daemons that do not support
    /// tmpfs mounts and for workloads that may exceed a safe memory-backed
    /// storage limit.
    pub const fn without_tmpfs(mut self) -> Self {
        self.tmpfs_size_bytes = None;
        self
    }

    /// Reports whether the profile requests `initdb --no-sync`.
    pub const fn initdb_no_sync(&self) -> bool {
        self.initdb_no_sync
    }

    /// Reports the tmpfs size cap, or `None` when image-default storage is
    /// selected.
    pub const fn tmpfs_size_bytes(&self) -> Option<u64> {
        self.tmpfs_size_bytes
    }
}

impl Default for OwnedContainerProfile {
    fn default() -> Self {
        Self::performance()
    }
}

/// Validated namespace used for database names, metadata, and container labels.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectName(String);

impl ProjectName {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::InvalidProjectName {
                name,
                reason: "the name must not be empty",
            });
        }
        if name.len() > MAX_PROJECT_NAME_LEN {
            return Err(Error::InvalidProjectName {
                name,
                reason: "the name must be at most 16 ASCII characters",
            });
        }
        if !name
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
        {
            return Err(Error::InvalidProjectName {
                name,
                reason: "the name must start with a lowercase ASCII letter",
            });
        }
        if !name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(Error::InvalidProjectName {
                name,
                reason: "only lowercase ASCII letters, digits, and underscores are allowed",
            });
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Configuration shared by a process-local PostgreSQL test harness.
#[derive(Clone)]
pub struct HarnessConfig {
    pub(crate) project: ProjectName,
    pub(crate) admin_database_url: Option<String>,
    pub(crate) image: Option<String>,
    pub(crate) owned_container_profile: OwnedContainerProfile,
    pub(crate) startup_timeout: Duration,
    pub(crate) operation_timeout: Duration,
    pub(crate) template_wait_timeout: Duration,
    pub(crate) stale_after: Duration,
    connection_budget_override: Option<NonZeroU32>,
    connections_per_database_override: Option<NonZeroU32>,
    pub(crate) cleanup_on_start: bool,
}

/// Resolved connection-permit policy for disposable database leases.
///
/// The budget models downstream application connections. Each live database
/// lease reserves [`Self::connections_per_database`] permits, so the effective
/// lease limit is floor division and any remainder stays unused. Harness-owned
/// owner, template-coordination, and lifecycle-administration sessions are
/// separate from this application budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionLimits {
    pub(crate) connection_budget: usize,
    pub(crate) connections_per_database: u32,
}

impl ConnectionLimits {
    /// Returns the total number of downstream-connection permits.
    pub const fn connection_budget(&self) -> usize {
        self.connection_budget
    }

    /// Returns the permits reserved by every live database lease.
    ///
    /// This value should cover the sum of the maximum sizes of every
    /// application pool or standalone connection that one test can open.
    pub const fn connections_per_database(&self) -> u32 {
        self.connections_per_database
    }

    /// Returns the effective maximum number of simultaneous database leases.
    ///
    /// This is `connection_budget / connections_per_database`, rounded down.
    pub const fn max_simultaneous_leases(&self) -> usize {
        self.connection_budget / self.connections_per_database as usize
    }
}

impl HarnessConfig {
    pub fn new(project: impl Into<String>) -> Result<Self> {
        Ok(Self {
            project: ProjectName::new(project)?,
            admin_database_url: None,
            image: None,
            owned_container_profile: OwnedContainerProfile::default(),
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            template_wait_timeout: DEFAULT_TEMPLATE_WAIT_TIMEOUT,
            stale_after: DEFAULT_STALE_AFTER,
            connection_budget_override: None,
            connections_per_database_override: None,
            cleanup_on_start: true,
        })
    }

    pub fn with_admin_database_url(mut self, admin_database_url: impl Into<String>) -> Self {
        self.admin_database_url = Some(admin_database_url.into());
        self
    }

    /// Selects and validates the image used for owned-container startup.
    ///
    /// The setting is retained but ignored for external-server startup,
    /// including builds compiled without the `containers` feature.
    pub fn with_image(mut self, image: impl Into<String>) -> Result<Self> {
        let image = image.into();
        ImageReference::parse(&image)?;
        self.image = Some(image);
        Ok(self)
    }

    /// Replaces the profile applied when this configuration starts an owned
    /// container. External-server mode, including builds without the
    /// `containers` feature, ignores the profile.
    pub fn with_owned_container_profile(mut self, profile: OwnedContainerProfile) -> Self {
        self.owned_container_profile = profile;
        self
    }

    pub fn with_startup_timeout(mut self, timeout: Duration) -> Result<Self> {
        validate_millisecond_timeout(timeout)?;
        self.startup_timeout = timeout;
        Ok(self)
    }

    pub fn with_operation_timeout(mut self, timeout: Duration) -> Result<Self> {
        validate_postgres_timeout(timeout)?;
        self.operation_timeout = timeout;
        Ok(self)
    }

    /// Sets how long a caller may wait for another process to initialize the
    /// same template. This is deliberately separate from the shorter timeout
    /// used by ordinary administrative operations.
    pub fn with_template_wait_timeout(mut self, timeout: Duration) -> Result<Self> {
        validate_postgres_timeout(timeout)?;
        self.template_wait_timeout = timeout;
        Ok(self)
    }

    pub fn with_stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = stale_after;
        self
    }

    /// Overrides this harness's connection budget. Repeated calls replace
    /// the previous override. If no per-database override is set, its default
    /// is clamped to this budget when the harness starts.
    pub fn with_connection_budget(mut self, permits: usize) -> Result<Self> {
        let Some(permits) = u32::try_from(permits).ok().and_then(NonZeroU32::new) else {
            return Err(Error::InvalidConfiguration {
                reason: "the connection budget must fit in a positive u32",
            });
        };
        self.connection_budget_override = Some(permits);
        Ok(self)
    }

    /// Overrides the permits held by each live database. Repeated calls
    /// replace the previous override. Its relationship to the final budget is
    /// order-independent and validated when the harness starts.
    pub fn with_connections_per_database(mut self, permits: u32) -> Result<Self> {
        let Some(permits) = NonZeroU32::new(permits) else {
            return Err(Error::InvalidConfiguration {
                reason: "per-database permits must be positive and no greater than the connection budget",
            });
        };
        self.connections_per_database_override = Some(permits);
        Ok(self)
    }

    pub fn with_cleanup_on_start(mut self, cleanup_on_start: bool) -> Self {
        self.cleanup_on_start = cleanup_on_start;
        self
    }

    pub fn project(&self) -> &ProjectName {
        &self.project
    }

    /// Returns the configured owned-container profile.
    pub fn owned_container_profile(&self) -> &OwnedContainerProfile {
        &self.owned_container_profile
    }

    /// Resolves this configuration's database connection-permit policy.
    ///
    /// Resolution is order-independent. An explicit per-database value greater
    /// than the final budget is reported here (and by
    /// [`crate::PostgresHarness::start`]) because either builder may be called
    /// first. When the per-database value is not overridden, its default is
    /// clamped to a smaller budget.
    pub fn connection_limits(&self) -> Result<ConnectionLimits> {
        let connection_budget = self
            .connection_budget_override
            .map_or(DEFAULT_CONNECTION_BUDGET, NonZeroU32::get);
        let connections_per_database = self.connections_per_database_override.map_or(
            DEFAULT_CONNECTIONS_PER_DATABASE.min(connection_budget),
            NonZeroU32::get,
        );
        if connections_per_database > connection_budget {
            return Err(Error::InvalidConfiguration {
                reason: "per-database permits must be no greater than the connection budget",
            });
        }
        Ok(ConnectionLimits {
            connection_budget: connection_budget as usize,
            connections_per_database,
        })
    }

    pub(crate) fn resolved_admin_database_url(&self) -> Option<String> {
        self.admin_database_url
            .clone()
            .or_else(|| std::env::var(POSTGRES_TEST_ADMIN_URL_ENV).ok())
    }

    #[cfg(feature = "containers")]
    pub(crate) fn resolved_image(&self) -> Result<ImageReference> {
        let image = self
            .image
            .clone()
            .or_else(|| std::env::var(POSTGRES_TEST_IMAGE_ENV).ok())
            .unwrap_or_else(|| DEFAULT_IMAGE.to_owned());
        ImageReference::parse(&image)
    }
}

impl fmt::Debug for HarnessConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HarnessConfig")
            .field("project", &self.project)
            .field(
                "admin_database_url",
                &self.admin_database_url.as_ref().map(|_| "[REDACTED]"),
            )
            .field("image", &self.image)
            .field("owned_container_profile", &self.owned_container_profile)
            .field("startup_timeout", &self.startup_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("template_wait_timeout", &self.template_wait_timeout)
            .field("stale_after", &self.stale_after)
            .field(
                "connection_budget_override",
                &self.connection_budget_override.map(NonZeroU32::get),
            )
            .field(
                "connections_per_database_override",
                &self.connections_per_database_override.map(NonZeroU32::get),
            )
            .field("cleanup_on_start", &self.cleanup_on_start)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ImageReference {
    pub(crate) repository: String,
    pub(crate) tag: String,
}

impl ImageReference {
    pub(crate) fn parse(image: &str) -> Result<Self> {
        if image.is_empty() || image.chars().any(char::is_whitespace) {
            return Err(Error::InvalidImageReference {
                reason: "the image must be non-empty and contain no whitespace",
            });
        }

        let (name_and_tag, digest) = image
            .split_once('@')
            .map_or((image, None), |(name, digest)| (name, Some(digest)));
        if name_and_tag.is_empty() || digest.is_some_and(str::is_empty) {
            return Err(Error::InvalidImageReference {
                reason: "the repository and optional digest must be non-empty",
            });
        }

        let last_slash = name_and_tag.rfind('/');
        let tag_separator = name_and_tag
            .rfind(':')
            .filter(|index| last_slash.is_none_or(|slash| *index > slash));
        let (repository, mut tag) = tag_separator.map_or_else(
            || (name_and_tag.to_owned(), "latest".to_owned()),
            |index| {
                (
                    name_and_tag[..index].to_owned(),
                    name_and_tag[index + 1..].to_owned(),
                )
            },
        );
        if repository.is_empty() || tag.is_empty() {
            return Err(Error::InvalidImageReference {
                reason: "the repository and tag must be non-empty",
            });
        }
        if let Some(digest) = digest {
            tag.push('@');
            tag.push_str(digest);
        }
        Ok(Self { repository, tag })
    }
}

fn validate_millisecond_timeout(timeout: Duration) -> Result<()> {
    if timeout.as_millis() == 0 {
        return Err(Error::TimeoutOutOfRange { duration: timeout });
    }
    Ok(())
}

fn validate_postgres_timeout(timeout: Duration) -> Result<()> {
    validate_millisecond_timeout(timeout)?;
    if timeout.as_millis() > i32::MAX as u128 {
        return Err(Error::TimeoutOutOfRange { duration: timeout });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionLimits, DEFAULT_OWNED_CONTAINER_TMPFS_SIZE_BYTES, HarnessConfig, ImageReference,
        OwnedContainerProfile, ProjectName,
    };

    #[test]
    fn project_names_are_deliberately_narrow() {
        assert!(ProjectName::new("creditkit").is_ok());
        assert!(ProjectName::new("credit_kit2").is_ok());
        assert!(ProjectName::new("").is_err());
        assert!(ProjectName::new("CreditKit").is_err());
        assert!(ProjectName::new("2creditkit").is_err());
        assert!(ProjectName::new("credit-kit").is_err());
        assert!(ProjectName::new("a_name_that_is_too_long").is_err());
    }

    #[test]
    fn image_reference_handles_registry_ports_and_digests() {
        assert_eq!(
            ImageReference::parse("postgres:18").unwrap(),
            ImageReference {
                repository: "postgres".to_owned(),
                tag: "18".to_owned(),
            }
        );
        assert_eq!(
            ImageReference::parse("registry.example:5000/postgres:18@sha256:abc").unwrap(),
            ImageReference {
                repository: "registry.example:5000/postgres".to_owned(),
                tag: "18@sha256:abc".to_owned(),
            }
        );
    }

    #[test]
    fn config_debug_redacts_an_explicit_admin_url() {
        let config = HarnessConfig::new("creditkit")
            .unwrap()
            .with_admin_database_url("postgres://user:secret@localhost/postgres");
        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn owned_container_profile_defaults_are_explicit_and_replaceable() {
        let profile = OwnedContainerProfile::default();
        assert!(profile.initdb_no_sync());
        assert_eq!(
            profile.tmpfs_size_bytes(),
            Some(DEFAULT_OWNED_CONTAINER_TMPFS_SIZE_BYTES)
        );

        let compatible = profile.with_initdb_no_sync(false).without_tmpfs();
        let config = HarnessConfig::new("creditkit")
            .unwrap()
            .with_owned_container_profile(compatible);
        assert_eq!(config.owned_container_profile(), &compatible);
        assert!(format!("{config:?}").contains("tmpfs_size_bytes: None"));
    }

    #[test]
    fn owned_container_tmpfs_cap_is_positive_and_fits_the_engine_api() {
        assert!(
            OwnedContainerProfile::default()
                .with_tmpfs_size_bytes(0)
                .is_err()
        );
        assert!(
            OwnedContainerProfile::default()
                .with_tmpfs_size_bytes(i64::MAX as u64 + 1)
                .is_err()
        );
        let profile = OwnedContainerProfile::default()
            .with_tmpfs_size_bytes(2 * 1024 * 1024 * 1024)
            .unwrap();
        assert_eq!(profile.tmpfs_size_bytes(), Some(2 * 1024 * 1024 * 1024));
    }

    #[test]
    fn connection_limit_values_are_validated_at_their_setters() {
        let error = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connection_budget(0)
            .unwrap_err();
        assert!(matches!(error, crate::Error::InvalidConfiguration { .. }));

        let error = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connections_per_database(0)
            .unwrap_err();
        assert!(matches!(error, crate::Error::InvalidConfiguration { .. }));

        if let Ok(too_large) = usize::try_from(u64::from(u32::MAX) + 1) {
            let error = HarnessConfig::new("creditkit")
                .unwrap()
                .with_connection_budget(too_large)
                .unwrap_err();
            assert!(matches!(error, crate::Error::InvalidConfiguration { .. }));
        }
    }

    #[test]
    fn connection_limit_defaults_and_implicit_clamping_are_resolved() {
        assert_eq!(
            HarnessConfig::new("creditkit")
                .unwrap()
                .connection_limits()
                .unwrap(),
            ConnectionLimits {
                connection_budget: 120,
                connections_per_database: 11,
            }
        );

        let small_budget = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connection_budget(5)
            .unwrap();
        assert_eq!(
            small_budget.connection_limits().unwrap(),
            ConnectionLimits {
                connection_budget: 5,
                connections_per_database: 5,
            }
        );

        let raised_budget = small_budget.with_connection_budget(20).unwrap();
        assert_eq!(
            raised_budget.connection_limits().unwrap(),
            ConnectionLimits {
                connection_budget: 20,
                connections_per_database: 11,
            }
        );
    }

    #[test]
    fn connection_limit_overrides_are_order_independent_and_last_wins() {
        let budget_then_per_database = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connection_budget(30)
            .unwrap()
            .with_connections_per_database(20)
            .unwrap();
        let per_database_then_budget = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connections_per_database(20)
            .unwrap()
            .with_connection_budget(30)
            .unwrap();
        assert_eq!(
            budget_then_per_database.connection_limits().unwrap(),
            per_database_then_budget.connection_limits().unwrap()
        );

        let repeated = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connection_budget(5)
            .unwrap()
            .with_connections_per_database(20)
            .unwrap()
            .with_connection_budget(30)
            .unwrap()
            .with_connections_per_database(15)
            .unwrap();
        assert_eq!(
            repeated.connection_limits().unwrap(),
            ConnectionLimits {
                connection_budget: 30,
                connections_per_database: 15,
            }
        );
    }

    #[test]
    fn incompatible_connection_limit_overrides_fail_during_resolution() {
        let standalone_per_database = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connections_per_database(121)
            .unwrap();
        assert!(matches!(
            standalone_per_database.connection_limits(),
            Err(crate::Error::InvalidConfiguration { .. })
        ));

        let per_database_then_budget = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connections_per_database(20)
            .unwrap()
            .with_connection_budget(10)
            .unwrap();
        let budget_then_per_database = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connection_budget(10)
            .unwrap()
            .with_connections_per_database(20)
            .unwrap();
        assert!(per_database_then_budget.connection_limits().is_err());
        assert!(budget_then_per_database.connection_limits().is_err());
    }

    #[test]
    fn resolved_connection_limits_expose_effective_floor_division() {
        let limits = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connection_budget(120)
            .unwrap()
            .with_connections_per_database(11)
            .unwrap()
            .connection_limits()
            .unwrap();

        assert_eq!(limits.connection_budget(), 120);
        assert_eq!(limits.connections_per_database(), 11);
        assert_eq!(limits.max_simultaneous_leases(), 10);

        let exact = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connection_budget(33)
            .unwrap()
            .with_connections_per_database(11)
            .unwrap()
            .connection_limits()
            .unwrap();
        assert_eq!(exact.max_simultaneous_leases(), 3);
    }

    #[test]
    fn config_debug_reports_raw_connection_limit_overrides() {
        let defaults = format!("{:?}", HarnessConfig::new("creditkit").unwrap());
        assert!(defaults.contains("connection_budget_override: None"));
        assert!(defaults.contains("connections_per_database_override: None"));

        let configured = HarnessConfig::new("creditkit")
            .unwrap()
            .with_connection_budget(30)
            .unwrap()
            .with_connections_per_database(20)
            .unwrap();
        let configured = format!("{configured:?}");
        assert!(configured.contains("connection_budget_override: Some(30)"));
        assert!(configured.contains("connections_per_database_override: Some(20)"));
    }

    #[test]
    fn postgres_timeouts_have_millisecond_precision() {
        let too_short = HarnessConfig::new("creditkit")
            .unwrap()
            .with_operation_timeout(std::time::Duration::from_nanos(1));
        assert!(matches!(
            too_short,
            Err(crate::Error::TimeoutOutOfRange { .. })
        ));

        let one_millisecond = HarnessConfig::new("creditkit")
            .unwrap()
            .with_operation_timeout(std::time::Duration::from_millis(1));
        assert!(one_millisecond.is_ok());
    }
}
