# postgres-test-harness

`postgres-test-harness` gives Rust integration tests one PostgreSQL 18 server,
one immutable migrated template, and one isolated database per test. It starts
PostgreSQL with Testcontainers by default or uses an externally managed admin
database when `POSTGRES_TEST_ADMIN_URL` is set.

The public API is deliberately independent of SQLx, Diesel, or
`tokio-postgres`. A project supplies an async initializer that receives a
database URL and may use its own database client and migration version.

```rust,no_run
use postgres_test_harness::{
    BoxError, FingerprintBuilder, HarnessConfig, PostgresHarness, TemplateSpec,
};

# async fn example() -> Result<(), BoxError> {
let harness = PostgresHarness::start(HarnessConfig::new("example")?).await?;
let fingerprint = FingerprintBuilder::new("example-schema")
    .add(
        "migration-1",
        include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md")),
    )
    .finish();
let template = harness
    .template(TemplateSpec::new(fingerprint), |database_url| async move {
        // Connect with the application's own database client and migrate here.
        // Close every initializer connection or pool before returning. The
        // harness also terminates stragglers before making the template ready.
        let _ = database_url;
        Ok(())
    })
    .await?;

let database = template.database().await?;
let database_url = database.database_url();
// Run the test with database_url, close the application pool, then clean up.
let _ = database_url;
database.cleanup().await?;
# Ok(())
# }
```

## Consumer adapter contract

Each project should keep a small test-support adapter that owns only
project-specific policy:

1. Choose one stable, lowercase project namespace.
2. Build a `TemplateSpec` from every migration bundle applied by the
   initializer, including embedded migrations from dependent crates.
3. Initialize the template with the project's normal database client and
   migration entry points.
4. Close every initializer connection or pool before returning from the
   initializer. The harness defensively terminates stragglers before cloning.
5. Preserve a separate empty-database constructor for migration-order tests.
6. Close application pools before calling `DatabaseLease::cleanup`.

Server startup, PostgreSQL version validation, template coordination, database
names, connection permits, container ownership, and stale cleanup belong in
this crate. Consumer adapters should not enable Testcontainers reuse, issue
Docker CLI cleanup commands, or delete databases by a name prefix.

The default connection budget is 120 permits and each live database lease
holds 11. Projects with different pool geometry can override both values on
`HarnessConfig`; the per-database value must cover every connection pool a
single test may open. Connection-limit setters are order-independent and the
last override for each value wins. With no explicit per-database override, its
default is clamped to a smaller budget and returns to 11 if that budget is
raised again. Zero and out-of-range values fail at their setter; an explicit
per-database value larger than the final budget is rejected when
`PostgresHarness::start` resolves the complete configuration.

Template coordination has a separate 15-minute wait timeout so a short
administrative-operation timeout does not make concurrent callers fail while a
real migration suite is still running. Override it with
`HarnessConfig::with_template_wait_timeout` when needed.

## Environment

- `POSTGRES_TEST_ADMIN_URL` uses an existing PostgreSQL 18 server instead of
  starting a container. The URL must identify an administrative database and a
  role allowed to create and drop disposable databases. The current admin
  client expects a local or CI endpoint that does not require TLS.
- `POSTGRES_TEST_IMAGE` overrides the default `postgres:18` image.

For an owned container, the harness requests Testcontainers' IPv4 port mapping
and therefore uses an IPv4 loopback literal when Testcontainers reports
`localhost`; this prevents the operating system from selecting an unrelated
IPv6 listener for an IPv4-mapped port. Every administrative connection has a
ten-second deadline covering TCP connection, PostgreSQL startup, and
authentication. Query and lock deadlines remain governed separately by the
configured administrative-operation and template-wait timeouts.

The harness never enables Testcontainers' reusable-container mode. Reuse is
bounded by the owner process, and ordinary process exit removes an owned
container. Tagged database metadata and PostgreSQL advisory locks allow a later
process to clean resources left by an abnormal exit without deleting active or
unrecognized databases.

Owned containers are removed on explicit shutdown, Rust drop, ordinary process
exit, and catchable termination signals. No in-process implementation can react
to `SIGKILL` or a host crash; containers carry
`org.postgres-test-harness.managed=true`, project, run, and creation-time labels
so operators can identify those exceptional leftovers without relying on a
name prefix. The harness deliberately does not auto-delete an old running
container because Docker labels alone cannot prove that its owning process is
dead.

On a shared external server, startup performs an age-bounded stale-database
sweep by default. Cleanup requires valid harness metadata and an available
owner or template advisory lock, so active, fresh, untagged, and foreign
databases are skipped.

## License

Licensed under MIT.
