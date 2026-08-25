# Downstream test-suite performance guide

The fastest safe setup is usually one PostgreSQL server, one process-local
`PostgresHarness`, one process-local migrated `DatabaseTemplate`, and one
exclusive database lease per running test. The checked-in
[reference adapter](../examples/downstream_adapter.rs) implements that shape
and is compiled in both feature modes. CI also runs it against a real external
PostgreSQL 18 service.

## Know the process boundary

Tokio `OnceCell` statics guarantee one successful harness and template
initialization per executable process. They do not cross process boundaries.
Each Cargo integration-test target under `tests/` is normally a separate test
binary, so ten files can start ten owned containers even when every file uses
an identical adapter.

Choose deliberately:

- Consolidate related integration tests into fewer binaries when the default
  owned-container isolation is desirable.
- For a CI job with several test binaries, start one PostgreSQL 18 service for
  the job and give every binary the same `POSTGRES_TEST_ADMIN_URL`.
- Keep the project name and complete template fingerprint stable. Separate
  processes on the same external server then coordinate through PostgreSQL
  advisory locks and can acquire the same content-addressed template instead
  of migrating another one.

Do not create a harness in every test function. `PostgresHarness` and
`DatabaseTemplate` are cheap to clone after the adapter has initialized them,
but repeated process-local setup still adds work and obscures ownership.

## Reference adapter

The reference uses `tokio::sync::OnceCell`:

```rust,ignore
static HARNESS: OnceCell<PostgresHarness> = OnceCell::const_new();
static TEMPLATE: OnceCell<DatabaseTemplate> = OnceCell::const_new();

async fn harness() -> Result<&'static PostgresHarness> {
    HARNESS
        .get_or_try_init(|| async { PostgresHarness::start(harness_config()?).await })
        .await
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
```

An initialization error is returned and a later call may retry; one successful
value is retained for the process. The full example also closes its
`tokio-postgres` client and connection task before returning from template
initialization or before cleaning a lease.

Use `template().await?.database().await?` for ordinary tests. Keep a separate
`harness().await?.empty_database().await?` constructor only for tests that must
prove migration ordering or behavior from PostgreSQL `template0`. Migrating an
empty database for every ordinary test discards the main reuse boundary.

## Make the fingerprint complete and stable

`TemplateFingerprint` is the schema identity, not a cache-busting timestamp.
Build it in a fixed order from every byte sequence that can change the
initialized database:

- all application migrations;
- embedded migrations from dependent crates;
- seed/reference data required by the schema;
- feature or configuration inputs that change initialization.

Use stable descriptive labels and `include_bytes!`/`include_str!` for checked-in
inputs. Do not use the current time, a random value, an absolute path, or a Git
commit when identical schema inputs should reuse a template. The reference
adapter has a golden fingerprint test so reordering or changing a listed input
is visible. Completeness review must also add every newly introduced migration
or schema-shaping input to that explicit list.

## Size permits from real application capacity

Let:

- `A_i` be the maximum size of application pool `i` opened by one test;
- `S` be the maximum standalone connections opened by that test;
- `P = sum(A_i) + S` be `connections_per_database`;
- `B` be the process-local `connection_budget`;
- `L = floor(B / P)` be the maximum simultaneous database leases.

For one pool capped at 8, one standalone connection, and eight database-owning
tests in flight, the reference adapter uses `P=9`, `B=72`, and `L=8`. Pool
maximums count even when clients connect lazily: the permit policy protects the
simultaneous checkout spike, not merely the usual idle count.

Check the resolved policy before and after startup with
`HarnessConfig::connection_limits()` and `PostgresHarness::connection_limits()`.
Then compare `L * P` with the target server's non-reserved connection capacity.
The harness's owner, live templates, lifecycle-administration pool, benchmark
observer, other processes, and ambient external clients are outside the
application permit budget. External harnesses do not coordinate their budgets,
so sum them at the CI job or server level.

Increasing `B` without enough PostgreSQL capacity only lets more tests reach a
server-side failure. Reducing a third-party pool's maximum often permits more
parallel tests without changing database isolation.

## Return leases only after closing application clients

Close every application connection and pool before returning its exclusive
lease. Choose the completion contract explicitly:

- `DatabaseLease::cleanup()` waits for the database drop and reports its
  result to that caller.
- `DatabaseLease::defer_cleanup()` returns after the bounded server queue
  accepts the drop. Call `PostgresHarness::drain_deferred_cleanup()` at suite
  teardown to wait for completion and receive retained failures.
- `Drop` is a safety fallback. It queues cleanup without blocking, but it is
  not a substitute for an explicit error-observing drain on an external server.

Awaited cleanup is the clearest default. Deferred cleanup is useful when test
latency matters and a suite-level barrier already exists. If a test operation
fails, still close its application pool and attempt cleanup; preserve the test
error while recording or combining any cleanup failure.

Owned `shutdown()` closes admission, closes prewarmed queues, drains accepted
cleanup, and removes the container. External `shutdown()` is intentionally a
no-op, so external jobs must drain before their service is stopped.

## Prewarm only when clone latency matters

`DatabaseTemplate::prewarm(N)` creates `N` clean clones up front and returns a
bounded `PrewarmedDatabasePool`. A lease is always a never-used database. Its
dirty return is dropped and replaced with a distinct clone; the pool never
truncates or resets a used database.

Choose `N <= L`. Idle clones use storage but no application permits. Each pool
adds exactly `N` slots across ready, leased, deleting, and creating states, and
multiple template pools add their capacities. Use `status()` for diagnostics,
drain deferred cleanup before teardown, and call `shutdown()` when the pool's
lifetime is shorter than the harness's.

Prewarming trades initial work and roughly `N` clone sizes of storage for a
near-zero-round-trip ready lease. Measure it with representative schemas; it is
usually unnecessary when test bodies are much longer than `CREATE DATABASE`.

## Owned and external server choices

The default `containers` feature owns a PostgreSQL server per harness process.
`OwnedContainerProfile::performance()` uses `initdb --no-sync`, disposable
PostgreSQL durability settings, and a bounded 1 GiB tmpfs. This is fast and
appropriate for disposable data, but consumes host memory and may be too small
for large templates. Increase the explicit cap or use `without_tmpfs()` when
the daemon, image, or workload requires image-default storage.

External mode amortizes server startup across test binaries and lets stable
fingerprints reuse templates across those processes. For consumers that always
provide the server, disable default features to omit the Testcontainers,
container-engine, and container TLS graph:

```toml
[dev-dependencies]
postgres-test-harness = { version = "0.1.1", default-features = false }
```

An external-only build requires `HarnessConfig::with_admin_database_url` or
`POSTGRES_TEST_ADMIN_URL`. `with_image` and `OwnedContainerProfile` remain
source-compatible but are ignored, `container_id()` returns `None`, and
`shutdown()` remains a no-op.

## One external PostgreSQL service per CI job

The repository's CI runs this pattern against the executable reference
adapter:

```yaml
external-postgres:
  runs-on: ubuntu-latest
  services:
    postgres:
      image: postgres:18
      env:
        POSTGRES_PASSWORD: postgres
      ports:
        - 5432:5432
      options: >-
        --health-cmd "pg_isready -U postgres -d postgres"
        --health-interval 1s
        --health-timeout 5s
        --health-retries 30
  env:
    POSTGRES_TEST_ADMIN_URL: postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable
  steps:
    - uses: actions/checkout@v4
    - uses: dtolnay/rust-toolchain@stable
    - run: cargo run --locked --no-default-features --example downstream_adapter
```

Use one service at the job boundary and pass its URL to every test command in
that job. Do not start another service in each test binary.

`with_cleanup_on_start(false)` is appropriate only when the external server is
fresh, exclusive to this job, and destroyed with the job. It avoids a startup
catalog sweep that cannot find older runs. Keep startup cleanup enabled for a
persistent or shared server, where a previous abnormal process may have left
tagged resources. Either way, explicitly clean leases and drain this run's
deferred work.

An adapter dedicated to that ephemeral job can set the policy explicitly:

```rust,ignore
let config = HarnessConfig::new("app_tests")?.with_cleanup_on_start(false);
```

## Measure the suite, not a universal promise

Start with three or more samples and preserve the JSON observations:

```console
PTH_PERF_SAMPLES=3 cargo run --locked --release --example performance
POSTGRES_TEST_ADMIN_URL=postgres://... \
  PTH_PERF_SAMPLES=3 \
  cargo run --locked --no-default-features --release --example performance
```

Compare server startup/attach time, cold and warm template acquisition,
sequential and concurrent clone-cleanup, downstream connection spikes,
prewarm fill/lease/refill/storage, and final cleanup drain. Record image ID,
PostgreSQL settings, fixture sizes, pool geometry, storage driver, and machine
details. Use an empty `CARGO_TARGET_DIR` to compare clean compile cost. Treat
the results as directional for that workload and environment, not as latency
thresholds for correctness tests.

## Unsafe shortcuts not recommended

The harness deliberately does not recommend:

- Testcontainers reusable-container mode;
- Docker CLI container deletion or SQL database deletion based only on a name
  prefix;
- default `trust` authentication;
- PostgreSQL `FILE_COPY` without representative workload evidence;
- transaction rollback or per-schema isolation as equivalent to a fresh
  database per test;
- truncating or resetting a dirty prewarmed database for reuse.

Those shortcuts change ownership, security, or isolation semantics. Optimize
at the documented server, template, connection, cleanup, and prewarm boundaries
instead.
