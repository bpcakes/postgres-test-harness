//! Branching fixtures with immutable, cached database scenarios.
//!
//! Run `cargo run --example derived_scenarios`, or configure
//! `POSTGRES_TEST_ADMIN_URL` and add `--no-default-features`.

use std::{error::Error as StdError, fmt, io};

use postgres_test_harness::{
    BoxError, DatabaseTemplate, FingerprintBuilder, HarnessConfig, PostgresHarness,
};
use tokio_postgres::NoTls;

type Result<T> = std::result::Result<T, BoxError>;

const SCHEMA: &str = "
    CREATE TABLE organizations (id integer PRIMARY KEY, name text NOT NULL);
    CREATE TABLE users (
        id integer PRIMARY KEY,
        organization_id integer NOT NULL REFERENCES organizations,
        email text NOT NULL
    );
    CREATE TABLE subscriptions (
        id integer PRIMARY KEY,
        user_id integer NOT NULL REFERENCES users,
        state text NOT NULL CHECK (state IN ('active', 'cancelled')),
        due_on date NOT NULL
    );
";
const ORGANIZATION: &str = "
    INSERT INTO organizations VALUES (1, 'Example Co');
    INSERT INTO users VALUES (10, 1, 'alice@example.test');
";
const ACTIVE: &str = "INSERT INTO subscriptions VALUES (100, 10, 'active', DATE '2030-01-01');";
const CANCELLED: &str =
    "INSERT INTO subscriptions VALUES (100, 10, 'cancelled', DATE '2030-01-01');";
const OVERDUE: &str = "UPDATE subscriptions SET due_on = DATE '2000-01-01' WHERE id = 100;";

fn setup(sql: &str) -> FingerprintBuilder {
    // These exact SQL bytes fully describe this example's setup. If Rust code
    // starts shaping the fixture too, also hash its inputs and a setup revision.
    FingerprintBuilder::new("derived-scenarios-sql-v1").add("setup.sql", sql)
}

async fn execute(url: String, sql: &str) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;
    let connection = tokio::spawn(connection);
    let operation = client.batch_execute(sql).await.map_err(Into::into);
    drop(client);
    combine(operation, finish_connection(connection).await)
}

async fn scenario(parent: &DatabaseTemplate, sql: &str) -> Result<DatabaseTemplate> {
    // The step describes only this setup. derive includes the complete parent's
    // identity automatically, so changed migrations invalidate all descendants.
    Ok(parent
        .derive(setup(sql).finish_step(), |url| execute(url, sql))
        .await?)
}

async fn run_scenarios(harness: &PostgresHarness) -> Result<()> {
    let root = harness
        .template(setup(SCHEMA).finish_root(), |url| execute(url, SCHEMA))
        .await?;
    let organization = scenario(&root, ORGANIZATION).await?;
    let active = scenario(&organization, ACTIVE).await?;
    let cancelled = scenario(&organization, CANCELLED).await?;
    let overdue = scenario(&active, OVERDUE).await?;

    // Keep this small set alive for the suite. All callbacks can be skipped on
    // cache hits; never put an assertion or a per-test side effect in setup.
    check_clone(&active, "active", "2030-01-01", true).await?;
    check_clone(&active, "active", "2030-01-01", false).await?;
    check_clone(&cancelled, "cancelled", "2030-01-01", false).await?;
    check_clone(&overdue, "active", "2000-01-01", false).await?;
    Ok(())
}

async fn check_clone(
    template: &DatabaseTemplate,
    state: &str,
    due_on: &str,
    mutate: bool,
) -> Result<()> {
    let database = template.database().await?;
    let operation = inspect(database.database_url(), state, due_on, mutate).await;
    // This still runs if connecting, querying, or checking the data failed.
    let cleanup = database.cleanup().await.map_err(Into::into);
    combine(operation, cleanup)
}

async fn inspect(url: &str, state: &str, due_on: &str, mutate: bool) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
    let connection = tokio::spawn(connection);
    let operation = async {
        let rows = client
            .query(
                "SELECT o.id, o.name, u.id, u.email, s.id, s.state, s.due_on::text \
                 FROM organizations o JOIN users u ON u.organization_id = o.id \
                 JOIN subscriptions s ON s.user_id = u.id ORDER BY s.id",
                &[],
            )
            .await?;
        let actual: Vec<(i32, String, i32, String, i32, String, String)> = rows
            .iter()
            .map(|row| {
                (
                    row.get(0),
                    row.get(1),
                    row.get(2),
                    row.get(3),
                    row.get(4),
                    row.get(5),
                    row.get(6),
                )
            })
            .collect();
        let expected = vec![(
            1,
            "Example Co".to_owned(),
            10,
            "alice@example.test".to_owned(),
            100,
            state.to_owned(),
            due_on.to_owned(),
        )];
        if actual != expected {
            return Err(io::Error::other(format!(
                "scenario mismatch: expected {expected:?}, found {actual:?}"
            ))
            .into());
        }
        if mutate {
            client.batch_execute("DELETE FROM subscriptions").await?;
            let remaining: i64 = client
                .query_one("SELECT count(*) FROM subscriptions", &[])
                .await?
                .get(0);
            if remaining != 0 {
                return Err(
                    io::Error::other("clone mutation did not remove the subscription").into(),
                );
            }
        }
        Ok(())
    }
    .await;
    drop(client);
    combine(operation, finish_connection(connection).await)
}

async fn finish_connection(
    connection: tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
) -> Result<()> {
    connection.await??;
    Ok(())
}

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

fn combine<T>(operation: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (operation, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(operation), Err(cleanup)) => {
            Err(Box::new(OperationAndCleanupError { operation, cleanup }))
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let harness = PostgresHarness::start(HarnessConfig::new("scenario_example")?).await?;
    let operation = run_scenarios(&harness).await;
    let drain = harness.drain_deferred_cleanup().await.map_err(Into::into);
    // Owned shutdown removes the container. External shutdown is a no-op;
    // immutable tagged templates remain available for subsequent runs.
    let shutdown = harness.shutdown().await.map_err(Into::into);
    combine(operation, combine(drain, shutdown))?;
    println!("Active, cancelled, and overdue scenarios passed; the mutated clone was isolated.");
    Ok(())
}
