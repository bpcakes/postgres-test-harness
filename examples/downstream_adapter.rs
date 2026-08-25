//! Reference downstream adapter for one harness and template per test process.
//!
//! Run with the default owned container, or set `POSTGRES_TEST_ADMIN_URL` and
//! pass `--no-default-features` to use an external PostgreSQL 18 server.

use std::{error::Error as StdError, fmt, io};

use postgres_test_harness::{
    BoxError, DatabaseLease, DatabaseTemplate, FingerprintBuilder, HarnessConfig, PostgresHarness,
    Result, TemplateFingerprint, TemplateSpec,
};
use tokio::sync::OnceCell;
use tokio_postgres::NoTls;

const PROJECT: &str = "adapter_example";

// Size this from actual simultaneous application capacity per test. This
// example models one pool capped at eight connections plus one standalone
// connection, with at most eight database-owning tests in flight.
const APPLICATION_POOL_MAX: u32 = 8;
const STANDALONE_CONNECTIONS: u32 = 1;
const CONNECTIONS_PER_DATABASE: u32 = APPLICATION_POOL_MAX + STANDALONE_CONNECTIONS;
const MAX_PARALLEL_DATABASES: usize = 8;
const CONNECTION_BUDGET: usize = MAX_PARALLEL_DATABASES * CONNECTIONS_PER_DATABASE as usize;

const CREATE_WIDGETS: &str = include_str!("downstream_adapter/0001_create_widgets.sql");
const INDEX_WIDGETS: &str = include_str!("downstream_adapter/0002_index_widgets.sql");

static HARNESS: OnceCell<PostgresHarness> = OnceCell::const_new();
static TEMPLATE: OnceCell<DatabaseTemplate> = OnceCell::const_new();

#[derive(Debug)]
struct OperationAndCleanupError {
    operation: BoxError,
    cleanup: BoxError,
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

fn harness_config() -> Result<HarnessConfig> {
    HarnessConfig::new(PROJECT)?
        .with_connection_budget(CONNECTION_BUDGET)?
        .with_connections_per_database(CONNECTIONS_PER_DATABASE)
}

async fn harness() -> Result<&'static PostgresHarness> {
    HARNESS
        .get_or_try_init(|| async { PostgresHarness::start(harness_config()?).await })
        .await
}

fn schema_fingerprint() -> TemplateFingerprint {
    // Include every ordered input that shapes the migrated schema, including
    // migration bundles supplied by dependent crates in a real adapter.
    FingerprintBuilder::new("adapter-example-schema-v1")
        .add("0001_create_widgets.sql", CREATE_WIDGETS)
        .add("0002_index_widgets.sql", INDEX_WIDGETS)
        .finish()
}

async fn template() -> Result<&'static DatabaseTemplate> {
    TEMPLATE
        .get_or_try_init(|| async {
            harness()
                .await?
                .template(TemplateSpec::new(schema_fingerprint()), apply_migrations)
                .await
        })
        .await
}

/// Returns a fresh clone of the process-cached migrated template.
pub async fn database() -> Result<DatabaseLease> {
    template().await?.database().await
}

/// Use this only for tests that must exercise migrations from an empty cluster.
pub async fn empty_database() -> Result<DatabaseLease> {
    harness().await?.empty_database().await
}

async fn apply_migrations(database_url: String) -> std::result::Result<(), BoxError> {
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let connection = tokio::spawn(connection);
    let migration = client
        .batch_execute(&format!("{CREATE_WIDGETS}\n{INDEX_WIDGETS}"))
        .await
        .map_err(|error| Box::new(error) as BoxError);
    drop(client);
    combine_operation_and_cleanup(migration, finish_connection(connection).await)
}

async fn run_test_body(database_url: &str) -> std::result::Result<(), BoxError> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection = tokio::spawn(connection);
    let assertion = client
        .query_one("SELECT count(*) FROM widgets", &[])
        .await
        .map_err(|error| Box::new(error) as BoxError)
        .and_then(|row| {
            let count: i64 = row.get(0);
            ensure_widget_table_is_empty(count)
        });

    // A real adapter closes its complete application pool here, before it
    // returns the exclusive database lease to the harness.
    drop(client);
    combine_operation_and_cleanup(assertion, finish_connection(connection).await)
}

async fn finish_connection(
    connection: tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
) -> std::result::Result<(), BoxError> {
    connection
        .await
        .map_err(|error| Box::new(error) as BoxError)?
        .map_err(|error| Box::new(error) as BoxError)
}

fn ensure_widget_table_is_empty(count: i64) -> std::result::Result<(), BoxError> {
    if count == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "expected an empty widgets table, found {count} row(s)"
        ))
        .into())
    }
}

fn combine_operation_and_cleanup(
    operation: std::result::Result<(), BoxError>,
    cleanup: std::result::Result<(), BoxError>,
) -> std::result::Result<(), BoxError> {
    match (operation, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(operation), Ok(())) => Err(operation),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(operation), Err(cleanup)) => {
            Err(Box::new(OperationAndCleanupError { operation, cleanup }))
        }
    }
}

async fn run_isolated_test() -> std::result::Result<(), BoxError> {
    let database = database().await?;
    let test_result = run_test_body(database.database_url()).await;
    let cleanup_result = database
        .cleanup()
        .await
        .map_err(|error| Box::new(error) as BoxError);
    combine_operation_and_cleanup(test_result, cleanup_result)
}

#[tokio::main]
async fn main() -> std::result::Result<(), BoxError> {
    let harness = harness().await?;
    let test_and_database_cleanup = run_isolated_test().await;

    // A suite-level teardown should run even when the test or its awaited
    // lease cleanup failed. Owned shutdown includes another drain before
    // container removal; external shutdown is the documented no-op.
    let drain = harness
        .drain_deferred_cleanup()
        .await
        .map_err(|error| Box::new(error) as BoxError);
    let shutdown = harness
        .shutdown()
        .await
        .map_err(|error| Box::new(error) as BoxError);
    let teardown = combine_operation_and_cleanup(drain, shutdown);

    combine_operation_and_cleanup(test_and_database_cleanup, teardown)
}

#[cfg(test)]
mod tests {
    use super::{
        CONNECTION_BUDGET, CONNECTIONS_PER_DATABASE, MAX_PARALLEL_DATABASES,
        combine_operation_and_cleanup, ensure_widget_table_is_empty, harness_config,
        schema_fingerprint,
    };

    #[test]
    fn schema_fingerprint_is_stable_and_complete() {
        assert_eq!(
            schema_fingerprint().to_hex(),
            "7232f27f45ea0667740c7342c70bbcabc79c48e9738024e46b8c4f60fe1df1b0"
        );
    }

    #[test]
    fn connection_geometry_matches_the_pool_capacity_worksheet() {
        let limits = harness_config().unwrap().connection_limits().unwrap();
        assert_eq!(limits.connection_budget(), CONNECTION_BUDGET);
        assert_eq!(limits.connections_per_database(), CONNECTIONS_PER_DATABASE);
        assert_eq!(limits.max_simultaneous_leases(), MAX_PARALLEL_DATABASES);
        assert_eq!(
            limits.max_simultaneous_leases()
                * usize::try_from(limits.connections_per_database()).unwrap(),
            limits.connection_budget()
        );
    }

    #[test]
    fn test_and_cleanup_failures_are_both_preserved() {
        let error = combine_operation_and_cleanup(
            Err(std::io::Error::other("test failed").into()),
            Err(std::io::Error::other("cleanup failed").into()),
        )
        .expect_err("both failures must be reported");

        assert_eq!(
            error.to_string(),
            "test failed; cleanup also failed: cleanup failed"
        );
    }

    #[test]
    fn widget_assertion_is_a_fallible_result() {
        assert!(ensure_widget_table_is_empty(0).is_ok());
        assert_eq!(
            ensure_widget_table_is_empty(2)
                .expect_err("non-empty template clone must fail")
                .to_string(),
            "expected an empty widgets table, found 2 row(s)"
        );
    }
}
