# postgres-test-harness

`postgres-test-harness` starts or connects to one PostgreSQL 18 server, caches
immutable migrated templates by fingerprint, and clones a fresh isolated
database for each Rust test. The public API is independent of SQLx, Diesel, and
`tokio-postgres`, so applications use their normal database client and
migration entry points.

[crates.io](https://crates.io/crates/postgres-test-harness/0.2.0) ·
[API documentation](https://docs.rs/postgres-test-harness/0.2.0/postgres_test_harness/) ·
[CI](https://github.com/bpcakes/postgres-test-harness/actions/workflows/ci.yml) ·
[reference adapter](examples/downstream_adapter.rs) ·
[contributing](CONTRIBUTING.md)

The harness is designed for integration suites that need real PostgreSQL
semantics without rerunning every migration for every test:

- initialize each distinct schema template once;
- give concurrent tests separate databases cloned from that template;
- bound application connections and database lifecycle work;
- clean up owned containers and tagged disposable databases safely; and
- use an existing PostgreSQL server in environments where containers are
  unavailable.

## Requirements

- Rust 1.88 or newer;
- PostgreSQL 18; and
- either a Testcontainers-compatible Docker daemon or an externally managed
  PostgreSQL admin database.

The external admin role must be allowed to create and drop databases. The
current admin client is intended for local and CI endpoints that do not require
TLS.

## Installation

Add the harness as a development dependency. The extra dependencies below are
used only by the runnable example:

```toml
[dev-dependencies]
postgres-test-harness = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tokio-postgres = "0.7"
```

The default `containers` feature starts `postgres:18`. To compile without
Testcontainers and always use an external server:

```toml
[dev-dependencies]
postgres-test-harness = { version = "0.2", default-features = false }
```

## Quick start

This test creates one content-addressed template, applies a real migration, and
leases a fresh clone for the test body:

```rust,no_run
use postgres_test_harness::{
    BoxError, FingerprintBuilder, HarnessConfig, PostgresHarness, TemplateSpec,
};
use tokio_postgres::NoTls;

const MIGRATION: &str = r#"
    CREATE TABLE widgets (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        name text NOT NULL
    );
"#;

#[tokio::test]
async fn widgets_use_a_migrated_isolated_database() -> Result<(), BoxError> {
    let harness = PostgresHarness::start(HarnessConfig::new("readme_example")?).await?;
    let fingerprint = FingerprintBuilder::new("readme-schema-v1")
        .add("0001_create_widgets.sql", MIGRATION)
        .finish();

    let template = harness
        .template(TemplateSpec::new(fingerprint), |database_url| async move {
            let (client, connection) =
                tokio_postgres::connect(&database_url, NoTls).await?;
            let connection = tokio::spawn(connection);

            let migration_result = client.batch_execute(MIGRATION).await;
            drop(client);
            let connection_result = connection.await?;
            migration_result?;
            connection_result?;
            Ok(())
        })
        .await?;

    let database = template.database().await?;
    let (client, connection) =
        tokio_postgres::connect(database.database_url(), NoTls).await?;
    let connection = tokio::spawn(connection);

    let test_result = async {
        client
            .execute("INSERT INTO widgets (name) VALUES ($1)", &[&"first"])
            .await?;
        let row = client.query_one("SELECT count(*) FROM widgets", &[]).await?;
        Ok::<i64, tokio_postgres::Error>(row.get(0))
    }
    .await;

    // Close every application connection or pool before returning the lease.
    drop(client);
    let connection_result = connection.await?;
    let count = test_result?;
    connection_result?;
    assert_eq!(count, 1);

    database.cleanup().await?;
    harness.shutdown().await?;
    Ok(())
}
```

In a real suite, cache the harness and template once per test process rather
than starting them in every test. The
[reference adapter](examples/downstream_adapter.rs) shows that complete shape,
including error-preserving teardown and an empty-database path for migration
tests.

## How it works

1. `PostgresHarness::start` starts an owned PostgreSQL container, unless an
   external admin URL is configured.
2. `template` identifies a migrated schema from every ordered input added to
   its fingerprint. Calls with the same fingerprint are single-flighted within
   a harness and coordinated across processes with PostgreSQL advisory locks.
3. `DatabaseTemplate::database` clones a uniquely named database from the
   immutable template.
4. The test uses its normal client and pool against
   `DatabaseLease::database_url`.
5. After all application connections close, `cleanup` or `defer_cleanup`
   drops the disposable database. `drain_deferred_cleanup` provides a
   suite-level barrier.

One harness can hold multiple templates with different fingerprints. Preserve a
separate `PostgresHarness::empty_database` path for tests that need to exercise
migration order from PostgreSQL's built-in `template0`.

## Test-suite adapter

Most consumers should keep a small test-support module that owns only
application-specific policy:

1. Choose one stable lowercase project namespace of at most 16 ASCII
   characters.
2. Fingerprint every ordered migration bundle used by the initializer,
   including migrations embedded by dependent crates.
3. Apply migrations through the application's normal client and migration
   entry points.
4. Close initializer and test-body connections or pools before returning a
   lease.
5. Cache one harness and each commonly used template per test process.
6. Size the harness connection limits from actual application pool capacity.

Server startup, version validation, template coordination, database names,
connection admission, stale cleanup, and container ownership belong to the
harness. Consumer adapters should not enable Testcontainers reuse, issue Docker
CLI cleanup commands, or delete databases by a name prefix.

The [downstream performance guide](docs/downstream-performance.md) contains a
connection-budget worksheet, process-caching patterns, prewarming policy, and a
one-service-per-CI-job example.

## Server modes

### Owned container

With default features, `PostgresHarness::start` launches `postgres:18`.
`POSTGRES_TEST_IMAGE` selects an official-compatible PostgreSQL 18 image.

Owned containers use a disposable-data performance profile by default:
`initdb --no-sync`, a 1 GiB tmpfs mount beneath `/var/lib/postgresql`, and
non-durable PostgreSQL runtime settings. Increase the tmpfs cap for larger
templates or select image-default storage when memory-backed storage is
inappropriate.

The harness does not enable reusable-container mode. Explicit shutdown,
ordinary process exit, Rust drop, and catchable termination signals remove an
owned container. No process can react to `SIGKILL` or a host crash; identifying
labels remain on exceptional leftovers.

### External PostgreSQL

Set an admin URL in configuration or the environment:

```console
export POSTGRES_TEST_ADMIN_URL=\
'postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable'
cargo test
```

External startup validates PostgreSQL 18 and performs an owner-aware,
age-bounded stale-database sweep by default. Active, fresh, untagged, and foreign
databases are skipped. External `shutdown` is intentionally a no-op; call
`drain_deferred_cleanup` before tearing down the server.

Without the `containers` feature, an absent admin URL returns
`Error::ExternalAdminUrlRequired`. Configuration types shared with container
mode remain available, while container-only error variants and dependencies are
omitted.

## Connection limits and cleanup

The default application budget is 120 connection permits, with 11 reserved by
each live database lease. That admits ten simultaneous leases. Override both
values when one test can open a different maximum number of pooled and
standalone connections. Harness administration uses a separate bounded,
lazy session pool.

Choose cleanup based on when the caller needs completion:

- `DatabaseLease::cleanup` waits for the database drop to finish.
- `DatabaseLease::defer_cleanup` applies bounded queue backpressure, then
  returns after the cleanup is accepted.
- `Drop` submits a non-blocking fallback and retains the lease permit until
  cleanup finishes.
- `PostgresHarness::drain_deferred_cleanup` waits for previously accepted work
  and reports retained failures.

If test bodies are shorter than a template clone, an opt-in
`DatabaseTemplate::prewarm` pool can keep a bounded number of pristine clones
ready. Dirty databases are always dropped and replaced; they are never reset or
reused.

See [operations and resource lifecycle](docs/operations.md) for capacity
formulas, cleanup failure behavior, template coordination, prewarming state, and
owned-container compatibility details.

## Performance characterization

The checked-in performance example measures owned or external server startup,
template acquisition, disposable database throughput, cleanup drain time, and
administrative connection churn. It emits versioned JSON observations and does
not apply machine-specific latency thresholds. See the
[performance workflow](docs/performance.md) for local commands, CI usage,
measurement boundaries, and the recorded baseline.

## Development and contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for prerequisites, the default and
external-server test commands, and the format and lint checks used by CI.

## License

Licensed under the [MIT License](LICENSE-MIT).
