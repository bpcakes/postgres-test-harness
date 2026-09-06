# Operations and resource lifecycle

This guide records the resource limits, cleanup guarantees, template
coordination, and owned-container behavior of `postgres-test-harness`. Start
with the [README](../README.md) for installation and a runnable example. For
application-specific adapter design and CI sizing, see the
[downstream performance guide](downstream-performance.md).

## Connection capacity

Each harness's default application connection budget (`B`) is 120 permits and
each live database lease reserves 11 (`P`). The effective simultaneous lease
limit is `L = floor(B / P)`, so the defaults admit ten leases and leave ten
permits unused. `HarnessConfig::connection_limits()` exposes the resolved
values before startup, and `PostgresHarness::connection_limits()` returns the
same read-only `ConnectionLimits` used by admission control after startup.

Projects with different pool geometry can override both inputs on
`HarnessConfig`. `P` must cover the sum of the maximum sizes of every
application pool plus standalone connections that one test can open. For
example, separate pools capped at ten and five connections plus one standalone
client require at least 16 permits. Pool maximums are capacity, not an eager
connection count: many pools start empty and connect lazily, while a minimum or
idle limit describes how many already-open sessions they establish or retain.
Budgeting the maximum still prevents a simultaneous checkout spike from
overcommitting the harness.

Connection-limit setters are order-independent and the last override for each
value wins. With no explicit per-database override, its default is clamped to a
smaller budget and returns to 11 if that budget is raised again. Zero and
out-of-range values fail at their setter; an explicit per-database value larger
than the final budget is rejected by `connection_limits()` and by
`PostgresHarness::start` after the complete configuration is known.

### Administrative sessions

Disposable `CREATE`/metadata and `DROP` work reuses a lazy pool of
administrative sessions owned by each harness. Each pool's limit is the smaller
of the effective lease concurrency (`connection_budget /
connections_per_database`) and one quarter of PostgreSQL's non-reserved
connection slots, with a minimum of one. The quarter-share cap preserves
headroom when one harness targets a server. Separate harnesses and processes do
not coordinate this limit, so users of a shared external server must budget
their aggregate connection capacity.

If `R` is PostgreSQL's non-reserved capacity (`max_connections -
reserved_connections - superuser_reserved_connections`), the lifecycle pool
limit is `A = max(1, min(L, floor(R / 4)))`. Sessions are created lazily, so `A`
is not an eager connection count. Sessions are checked out exclusively;
independent lifecycle operations can progress concurrently without holding the
pool lock during SQL. A reused session is reset and has the configured operation
and lock timeouts restored before work. Waiting for a session is also bounded by
the configured operation timeout. Failed or uncertain sessions are evicted and
reconnected lazily.

Managed creation failures that may have taken effect without a response close
new database admission before returning, so repeated ambiguous attempts cannot
grow an untracked residual set. A PostgreSQL error that proves creation did not
occur is returned without closing admission.

The application permit budget does not include harness administration. One
owner-lock session lives for the server, each distinct live template retains one
shared-lock session, and lifecycle create/cleanup work uses up to `A` pooled
sessions. The owned server keeps `max_connections=300`; the default PostgreSQL
18 reservation settings leave 297 regular slots. With the default policy, at
most `L * P = 110` application sessions, ten lifecycle sessions, and one owner
session leave 176 regular slots for live templates, observers, short overlap,
and safety headroom. Actual use is normally lower because the application and
lifecycle pools are lazy.

`max_connections` remains fixed rather than being derived from `B`: the
characterization has not shown a robust benefit from changing it, live-template
count is intentionally not capped, and pool limits do not imply eager sessions.
Configurations whose potential application spike exceeds the server's capacity
remain observable through `ConnectionLimits`, but cannot make PostgreSQL accept
that spike. Size them against the whole formula. On external servers, include
ambient sessions and the limits of every harness and process; those budgets are
not coordinated globally.

## Database cleanup

Database cleanup has two explicit completion contracts:

- `DatabaseLease::cleanup` uses the server's bounded workers, retains the
  lease's connection-budget permit, and returns only after `DROP DATABASE ...
  WITH (FORCE)` succeeds or fails.
- `DatabaseLease::defer_cleanup` returns after a bounded server-scoped queue
  accepts the database. It applies backpressure when that queue is full and
  releases the permit only after acceptance.

The ordinary `Drop` implementation never waits for queue capacity: it enqueues
a fallback and retains the lease's permit until cleanup finishes. This keeps
destructor latency independent of database I/O and transfers saturation
backpressure to the next database acquisition. Code that wants to release
capacity after an explicit async backpressure point should call
`defer_cleanup`.

Each server reserves at least half of a multi-session lifecycle pool for creates
and other lifecycle work, and caps cleanup at four worker threads. A one-session
pool necessarily shares that session with its single cleanup worker. The
explicit waiting queue has one slot per worker. Cleanup is therefore concurrent
across servers without allowing a cleanup burst to occupy the entire lifecycle
pool or multiplying the default thread count by the full pool size.

If `L` is the effective live lease limit, `W` the cleanup worker count, and `Q`
the waiting capacity, at most `L + W + Q` disposable databases can be live,
running cleanup, or waiting for cleanup in one server. Awaited and fallback jobs
retain their lease permits, so this formula is a conservative mixed-workload
bound. Every later lease is created under a fresh unique name from `template0`
or the immutable project template; a returned database is never reset or
reused.

A cleanup failure that may leave a residual database closes new database
admission for that harness before the worker releases its slot. Already-live
leases remain cleanable, but callers must observe the drain error and recover or
replace the harness; repeated ambiguous failures cannot accumulate an unbounded
residual set.

Call `PostgresHarness::drain_deferred_cleanup` to wait for everything accepted
before the call. The barrier returns retained failures from explicit deferred
returns, fallback drops, and cancelled awaited callers exactly once.

Owned `shutdown` closes admission, closes every registered prewarmed pool,
closes the cleanup queue, drains it, reports any failure, closes the lifecycle
pool, and removes the container. It is safe to cancel the async shutdown caller
after the terminal sequence starts because the sequence continues in its
blocking worker. External `shutdown` remains a no-op so an external server stays
usable; external users must call the drain barrier before teardown. If the
process is terminated before a drain, owned container cleanup removes the whole
server and tagged external databases remain eligible for the next owner-aware
stale sweep.

## Template coordination

Calls for the same live template on clones of one `PostgresHarness` are
single-flighted and cached by fingerprint. They return handles backed by one
template allocation and one retained shared-lock session; waiting callers do
not run their initializers. The cache keeps only a weak reference, so dropping
the final `DatabaseTemplate` releases that session. An initializer error or
cancelled caller clears its flight and lets a waiting or later caller retry with
its own initializer.

Distinct `PostgresHarness::start` calls and separate processes deliberately do
not share this in-memory cache. They coordinate through PostgreSQL advisory
locks. That coordination has a separate 15-minute wait timeout so a short
administrative-operation timeout does not make a caller fail while a real
migration suite is still running. Override it with
`HarnessConfig::with_template_wait_timeout` when needed.

### Derived scenario templates

`DatabaseTemplate::derive(step_spec, initializer)` copies the parent's sealed
database, applies the local setup step, then seals and caches the child. The
step spec describes only the local inputs. The returned `fingerprint()` is the
complete composed identity, including every ancestor. Changing a parent's
inputs therefore changes its descendants without callers restating ancestry.

Schema, table data, sequences, and other database-local objects are copied.
Database-level grants and settings are not inherited as scenario payload;
cluster-wide roles and external side effects are outside this mechanism. Every
child consumes full database storage. Count intermediate templates as well as
leaves when sizing disk or tmpfs capacity. Each live template adds one retained
shared-lock session outside the application connection budget; cold setup also
needs temporary administration and initializer connections.

The parent stays protected while child preparation copies and tags its source,
even if the awaiting caller is cancelled and its blocking worker continues.
Ordinary leases and initial prewarm copies have the same source protection.
After creation, the child, lease, or pool needs no ancestor chain: a completed
child remains usable after stale cleanup removes an unused parent. A prewarm
pool retains its own child source and refills dirty slots from that scenario.

A cache hit skips the initializer. An error, panic, or cancellation can require
a later attempt to run it again from a fresh parent copy; setup must tolerate
retries and must close its application connections before returning. Failed
abort cleanup preserves both the initializer error and the cleanup error.
Interrupted work may leave a tagged initializing template for a later retry or
stale sweep. `drain_deferred_cleanup` covers disposable lifecycle work, not
these interrupted templates. A child already published as ready in PostgreSQL
can be reused even if the caller never received the completed handle.

Terminal admission closure rejects cold derived creation and new disposable
leases. A warm cached template handle can still be returned under the existing
cache contract. External `shutdown` remains a no-op; drain disposables before
tearing down an externally managed server.

## Prewarmed disposable databases

For suites whose test bodies are shorter than a template clone, call
`DatabaseTemplate::prewarm` once and share the returned
`PrewarmedDatabasePool` inside the process:

```rust,no_run
# use postgres_test_harness::{
#     BoxError, FingerprintBuilder, HarnessConfig, PostgresHarness, TemplateSpec,
# };
# async fn example() -> Result<(), BoxError> {
# let harness = PostgresHarness::start(HarnessConfig::new("example")?).await?;
# let template = harness.template(
#     TemplateSpec::new(FingerprintBuilder::new("prewarm-example").finish()),
#     |_| async { Ok(()) },
# ).await?;
let pool = template.prewarm(4).await?;
let database = pool.database().await?;
let database_url = database.database_url();
// Run the test, then close every application connection or pool.
let _ = database_url;
database.defer_cleanup().await?;

// Wait for the dirty DROP and distinct background replacement before teardown.
harness.drain_deferred_cleanup().await?;
assert_eq!(pool.status().ready(), 4);
pool.shutdown().await?;
# Ok(())
# }
```

An opt-in prewarmed pool adds its declared capacity `N` to the storage bound.
Each slot is exactly one of ready, leased, deleting, or creating, so ready and
background work cannot multiply the requested storage footprint. Multiple
pools add their capacities independently.

Capacity must be positive and no greater than the harness's effective lease
limit `L`. Initial fill is awaited. Idle databases hold no application permits;
`database()` reserves a ready token and the full per-database permit allocation
before atomically removing an idle database. Cancellation or closed application
admission therefore leaves the ready database untouched. When the queue is
empty, callers wait for a returned database to be dropped and for a new clone
with a fresh unique name to become ready.

The pool never truncates or resets a dirty database. `cleanup()` waits for its
dirty drop and replacement; `defer_cleanup()` and `Drop` hand that work to the
bounded server lifecycle queue. Refill failures close the pool and are reported
by the awaited cleanup or the harness drain barrier. They close harness-wide
database admission only when the failure may have left a replacement database
behind.

`status()` exposes the ready, leased, deleting, and creating counts for
diagnostics. A pool keeps its source template and shared advisory-lock session
alive. Explicit `shutdown()` closes the pool, removes all idle databases, and
drains accepted lifecycle work; it does not revoke leases still held by callers.
Dropping the final pool handle queues the same idle cleanup. Owned harness
shutdown closes every registered pool before fixing its cleanup barrier and
removing the container.

## Cargo feature behavior

The default `containers` feature provides owned-container startup. Without it,
starting without an explicit or environment-provided admin URL returns
`Error::ExternalAdminUrlRequired`. `HarnessConfig::with_image` and
`OwnedContainerProfile` remain available so shared configuration code stays
source-compatible; their settings are validated but ignored in external mode.
`PostgresHarness::container_id` always returns `None`, and `shutdown` retains its
external-server no-op behavior.

`Error` is non-exhaustive. Its owned-container variants, including variants
whose sources use Testcontainers types, exist only with `containers` enabled;
`ExternalAdminUrlRequired` exists only when that feature is disabled. Default
builds therefore retain the public Rust API without exposing an optional
dependency from external-only builds.

## Owned-container performance profile

Owned containers use `OwnedContainerProfile::performance()` by default. The
profile passes exactly `--no-sync` through `POSTGRES_INITDB_ARGS` and mounts
`/var/lib/postgresql` on tmpfs with a 1 GiB size cap. `--no-sync` shortens
disposable cluster initialization; tmpfs improves database clone and cleanup
I/O. Neither setting makes test data persistent or crash-safe. The harness's
`fsync=off`, `synchronous_commit=off`, and `full_page_writes=off` runtime
settings have the same disposable-data premise.

Increase the cap for larger templates, or opt out when the Docker daemon does
not support tmpfs or memory-backed storage is inappropriate:

```rust
use postgres_test_harness::{HarnessConfig, OwnedContainerProfile};

# fn config() -> postgres_test_harness::Result<HarnessConfig> {
let larger = OwnedContainerProfile::performance()
    .with_tmpfs_size_bytes(2 * 1024 * 1024 * 1024)?;
let config = HarnessConfig::new("example")?.with_owned_container_profile(larger);

let image_default_storage = OwnedContainerProfile::performance().without_tmpfs();
let config = config.with_owned_container_profile(image_default_storage);
# Ok(config)
# }
```

A full compatibility opt-out for an official-compatible custom image is
`OwnedContainerProfile::performance().with_initdb_no_sync(false).without_tmpfs()`.
Custom images used with the optimizations must honor the Docker Official
PostgreSQL 18 contracts: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`,
and `POSTGRES_INITDB_ARGS`; PostgreSQL data beneath `/var/lib/postgresql`; port
5432; and the `postgres -c name=value` command shape. All images must still
provide PostgreSQL 18 with `uuidv7()` and accept password authentication over
the mapped TCP port.

An unsupported tmpfs mount retains the Docker daemon's error and names the
opt-out. If the cap is exhausted during startup, the error reports the cap and
the recognized storage-exhaustion evidence. Recognized allocation failures and
exit status 137 are reported separately as possible Docker or host memory
exhaustion, with guidance to reduce pressure, increase the daemon allowance, or
disable tmpfs.

The profile deliberately does not add `--no-data-checksums`,
`wal_level=minimal`, reusable containers, or host `trust` authentication.
Settings that change database semantics belong in separate, explicit profiles
backed by representative compatibility measurements.

For an owned container, the harness requests Testcontainers' IPv4 port mapping
and therefore uses an IPv4 loopback literal when Testcontainers reports
`localhost`; this prevents the operating system from selecting an unrelated
IPv6 listener for an IPv4-mapped port. The Docker Official image's temporary
initdb server listens only on a Unix socket, so log output is not treated as
readiness. Owned startup instead retries authenticated connections through the
mapped TCP port within `with_startup_timeout`; the successful connection is then
used for PostgreSQL 18 validation and the owner lock.

Cleanup after an expired startup deadline is awaited before the error returns,
so the deadline does not abandon a partially created container. If that cleanup
also fails, the returned error preserves both the startup failure and the
cleanup failure. Other administrative connections have a ten-second connection
deadline. Query and lock deadlines remain governed separately by the configured
administrative-operation and template-wait timeouts.

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
