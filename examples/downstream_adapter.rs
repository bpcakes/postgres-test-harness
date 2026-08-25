//! Reference downstream adapter for one harness and template per test process.
//!
//! Run with the default owned container, or set `POSTGRES_TEST_ADMIN_URL` and
//! pass `--no-default-features` to use an external PostgreSQL 18 server.

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
        .await;
    drop(client);
    let connection = connection.await?;
    migration?;
    connection?;
    Ok(())
}

async fn run_test_body(database_url: &str) -> std::result::Result<(), BoxError> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection = tokio::spawn(connection);
    let assertion = client
        .query_one("SELECT count(*) FROM widgets", &[])
        .await
        .map(|row| {
            let count: i64 = row.get(0);
            assert_eq!(count, 0);
        });

    // A real adapter closes its complete application pool here, before it
    // returns the exclusive database lease to the harness.
    drop(client);
    let connection = connection.await?;
    assertion?;
    connection?;
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), BoxError> {
    let database = database().await?;
    let test_result = run_test_body(database.database_url()).await;
    let cleanup_result = database.cleanup().await;

    // Preserve the test failure while still awaiting database cleanup.
    test_result?;
    cleanup_result?;
    harness().await?.drain_deferred_cleanup().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CONNECTION_BUDGET, CONNECTIONS_PER_DATABASE, MAX_PARALLEL_DATABASES, harness_config,
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
}
