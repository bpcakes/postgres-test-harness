# Performance characterization

`examples/performance.rs` is the reproducible end-to-end performance workload
for `postgres-test-harness`. It uses only the crate's public lifecycle API and
emits a versioned JSON document to stdout by default. Set `PTH_PERF_OUTPUT` to
write it directly to a file instead. Progress and a compact min/median/max
summary always go to stderr. File destinations are validated before PostgreSQL
work begins, after source provenance is captured. The completed JSON replaces
the destination atomically; a final write failure emits the report to stdout
and still returns an error.

This is characterization, not a correctness test. Durations are observations;
the example and CI workflow do not contain pass/fail latency thresholds.

## Owned-container run

Pre-pull the configured image so startup measures a cached-image container
start rather than registry or network time, then use a release build:

```bash
docker pull postgres:18
PTH_PERF_OUTPUT=target/performance-owned.json \
cargo run --locked --release --example performance
```

The example refuses to start owned mode if `docker image inspect` cannot find
the image locally. It passes that resolved reference explicitly to the harness,
then verifies every started container's actual image ID against the cached
content ID. It also records the Docker daemon storage driver.
`POSTGRES_TEST_IMAGE` selects a different image. This value is Docker's local
image configuration/content ID (`docker image inspect .Id`), not a registry
manifest from `RepoDigests`.

The default run records and uses the library's owned performance profile:
`initdb --no-sync` plus a 1 GiB tmpfs cap. To isolate either contribution on
the same host, set `PTH_PERF_OWNED_INITDB_NO_SYNC=false` and/or
`PTH_PERF_OWNED_TMPFS_SIZE_BYTES=off`. A positive byte count selects a
different explicit tmpfs cap. Keep these values identical across samples in a
single report; compare separate JSON reports rather than changing storage
mid-run. Docker's storage-driver field remains daemon provenance when tmpfs is
active—it is not a claim that PostgreSQL data used that driver.

## External-server run

Set the same administrative URL accepted by the library:

```bash
POSTGRES_TEST_ADMIN_URL='postgres://postgres:secret@127.0.0.1/postgres' \
PTH_PERF_EXTERNAL_IMAGE_CONTENT_ID='sha256:provisioned-image-id' \
PTH_PERF_EXTERNAL_STORAGE_DRIVER='provisioner-or-filesystem' \
PTH_PERF_OUTPUT=target/performance-external.json \
cargo run --locked --release --example performance
```

The URL and credentials are never written to the report. Image and storage
metadata cannot be discovered portably from PostgreSQL, so the two external
metadata variables are optional provenance supplied by the operator. Supply
the same Docker content-ID kind used by owned mode, rather than a registry
manifest digest. Their fields remain present with a
`not_reported_for_external_server` source when the values are unavailable.
The benchmark discovers `fsync`, `synchronous_commit`, `full_page_writes`,
`data_checksums`, `wal_level`, `max_connections`, `reserved_connections`, and
`superuser_reserved_connections` from PostgreSQL itself.
Treat reports with different values as different environments rather than
attributing the difference to harness mode.

An external run disables the startup stale sweep so that unrelated cleanup is
not folded into startup. Each invocation uses a random, valid project namespace
to isolate concurrent local runs and prints it before starting database work.
After each sample releases its owner and template locks, it runs a zero-age,
metadata- and lock-aware sweep. Ordinary measurement errors still close the
observer and run this sweep; if both operations fail, the primary and cleanup
errors are reported together. Successful samples additionally require the exact
expected cleanup counts. If asynchronous connection shutdown leaves a lock
briefly active, the benchmark starts more sweeps only within a 120-second retry
window, backing off exponentially from 25 ms to a one-second cap. Each mutating
sweep is allowed to finish under the library's own operation timeout rather
than being canceled mid-drop, so the retry window is not an end-to-end cleanup
deadline. The JSON preserves every cleanup attempt and all dropped and skipped
counters; unexpected leftovers fail the run. A forced process termination
cannot run asynchronous cleanup, so use the logged project with
`cleanup_stale_databases` to recover that exceptional case.

## Workload and timing boundaries

Each invocation runs three independent samples by default. Each sample starts
or attaches one harness and measures both fixtures:

- `small`: one table and one row;
- `representative`: 50,000 rows containing a primary key, indexed lookup key,
  and repeated payload. The report records the actual `pg_database_size` of
  each initialized template, rather than assuming a byte size from the row
  count.

The JSON records these boundaries for every sample and fixture:

- `server_startup`: immediately around `PostgresHarness::start`. In owned mode
  the image-cache check happens before the timer. The timed call cannot finish
  on the Docker entrypoint's temporary initdb server: harness startup must
  authenticate through the mapped TCP port within its startup deadline and
  validate PostgreSQL 18. Timeout cleanup is included because startup does not
  return while a partially created owned container still needs removal. The
  example then retains an observer TCP session across a second postmaster-start
  probe and fails if the postmaster changes.
- `cold_template_acquisition`: a run-unique fingerprint whose initializer must
  execute. This includes the fixture migration and template finalization.
- `warm_template_acquisition`: the same fingerprint while the cold handle is
  live. Its initializer deliberately returns an error if called, proving the
  ready path was used.
- `sequential_clone_cleanup`: repeated `DatabaseTemplate::database` plus
  awaited `DatabaseLease::cleanup`, one at a time.
- `bounded_concurrent_clone_cleanup`: the same complete lifecycle with a
  caller-side semaphore. Total elapsed time, throughput, and each operation's
  elapsed time are recorded.
- `downstream_pool_checkout_spike`: creates `PTH_PERF_CONCURRENCY` leases in
  parallel and eagerly opens `PTH_PERF_DOWNSTREAM_POOL_SIZE` independent
  application connections to every database. The timer covers lease creation
  and connection checkout. The phase retains every client, probes it, and
  requires `pg_stat_activity` to report exactly the configured product before
  closing all clients and cleaning every lease concurrently. The JSON records
  checkout throughput, the observed peak, cleanup time, and total time. This is
  a deliberately full synthetic pool: real pools often establish connections
  lazily, so a configured maximum or idle limit must not be mistaken for an
  eager connection count.
- `explicit_cleanup_drain`: creates the configured leases before timing, then
  awaits their explicit cleanups concurrently. Its `method.execution` is
  `caller_bounded` and records the configured drain count as its concurrency.
- `deferred_cleanup`: creates the configured leases before timing, explicitly
  awaits `DatabaseLease::defer_cleanup` for each lease, and then awaits
  `PostgresHarness::drain_deferred_cleanup`. The report separates aggregate and
  per-lease caller-return latency from final drain-barrier latency and total
  elapsed time. The 120-second guard prevents a broken barrier from hanging; it
  is not a benchmark threshold. Execution is reported as
  `implementation_managed` and completion as `awaited_drain_barrier`. A single
  untimed post-barrier catalog query verifies every exact database name is
  absent; polling is no longer the completion mechanism. The sequential caller
  returns intentionally expose bounded-queue backpressure and are not an
  apples-to-apples comparison with the concurrent explicit drain.

One persistent observer connection reads `pg_stat_database.sessions` before
and after every phase. The report includes the cumulative values, delta, and
active admin-database sessions. That delta characterizes harness connection
churn without adding instrumentation to the production path. It is exact on
the single-purpose owned server. On a shared external server it is intentionally
labeled server-wide and can include ambient clients, so run under controlled
load or interpret it as an upper bound. A server-side statistics reset during a
phase can also invalidate a delta; retain the raw before/after counters.
Disposable lifecycle sessions are pooled lazily, so a phase delta now reports
only connections added while the pool grows or replaces a failed backend. The
`active_after` snapshot includes already-idle pooled sessions. The workload
orders sequential work before bounded-concurrent work intentionally: this
shows one-session warm-up, growth to the caller's concurrency, and zero-churn
reuse in later phases. Correctness tests separately identify these sessions by
their `postgres-test-harness lifecycle:<project>` application name and prove
the configured bound with `pg_stat_activity`.
The startup field is explicitly named
`admin_sessions_after_observer_connect`: it is an untimed post-start snapshot
and includes the observer itself, while each phase delta keeps the same
observer at both endpoints.
Observer connection, query, and shutdown awaits use the same 90-second
operation timeout recorded for the harness, so a silent external server cannot
leave a local characterization waiting indefinitely.

The pool-spike peak uses a different, database-scoped observation:
`pg_stat_activity` is filtered to the exact disposable database names held by
that phase. It therefore excludes the owner lock, template locks, lifecycle
pool, and observer, all of which connect to the administrative database. A
successful phase proves that the requested eager downstream connections were
simultaneously established; it does not claim that a third-party pool with the
same maximum normally opens them all.

The top-level report also records the source commit and worktree state,
PostgreSQL version and critical settings, postmaster start, logical CPU count,
OS, architecture, server mode, image metadata, storage driver, the effective
owned initdb/storage profile, fixture rows, execution and completion methods,
concurrency where caller-controlled, sample counts, resolved connection budget,
per-database permits, effective maximum leases, downstream pool size, timeouts,
the cleanup barrier guard, and external cleanup retry policy. Schema version 7
adds the downstream pool-spike measurement, resolved lease capacity, and both
reserved-connection settings. Schema version 6 replaced catalog-polled deferred
completion with explicit caller-return and awaited-drain timings. Consumers
should branch on `schema_version`.

## Configuration

All numeric overrides must be positive. The example resolves the same
`ConnectionLimits` API used by the running harness and rejects concurrency or
drain counts above `floor(connection_budget / connections_per_database)`. It
also rejects a downstream pool size above the per-database reservation.
When `PTH_PERF_CONNECTIONS_PER_DATABASE` is unset, the benchmark leaves that
builder override unset too, so the library's default clamps to a smaller budget
exactly as it does for downstream callers.

| Variable | Default | Meaning |
| --- | ---: | --- |
| `PTH_PERF_SAMPLES` | 3 | Independent server/harness samples |
| `PTH_PERF_SEQUENTIAL_OPERATIONS` | 4 | Sequential clone-cleanup lifecycles per fixture |
| `PTH_PERF_CONCURRENCY` | 4 | Maximum concurrent clone-cleanup lifecycles |
| `PTH_PERF_CONCURRENT_OPERATIONS` | 8 | Total bounded-concurrent lifecycles per fixture |
| `PTH_PERF_DRAIN_DATABASES` | 4 | Pre-created leases in each cleanup-drain measurement |
| `PTH_PERF_REPRESENTATIVE_ROWS` | 50000 | Rows migrated into the representative fixture |
| `PTH_PERF_CONNECTION_BUDGET` | 120 | Total downstream-connection permits (`B`) |
| `PTH_PERF_CONNECTIONS_PER_DATABASE` | 11 | Permits reserved by each live lease (`P`) |
| `PTH_PERF_DOWNSTREAM_POOL_SIZE` | 10 | Connections eagerly opened on every lease in the pool spike; must be at most `P` |
| `PTH_PERF_OUTPUT` | stdout | JSON output path; use ignored `target/` for clean provenance |
| `PTH_PERF_OWNED_INITDB_NO_SYNC` | `true` | Whether owned initdb uses `--no-sync` |
| `PTH_PERF_OWNED_TMPFS_SIZE_BYTES` | `1073741824` | Owned tmpfs byte cap, or `off` for image-default storage |
| `PTH_PERF_EXTERNAL_IMAGE_CONTENT_ID` | unset | External-server Docker image content ID |
| `PTH_PERF_EXTERNAL_STORAGE_DRIVER` | unset | External-server storage provenance |

Keep the workload configuration equal when comparing reports. Multiple raw
samples remain in `samples`; `summary` provides min, median, mean, and max for
each timing. Preserve raw JSON artifacts because a single aggregate hides
variance and connection-count changes.

To characterize lease and downstream-pool geometry without changing the
library defaults, run a matrix of separate reports. For example, these points
exercise one small pool, four half-sized pools, and all ten default leases with
ten eager connections each:

```bash
PTH_PERF_CONCURRENCY=1 PTH_PERF_DOWNSTREAM_POOL_SIZE=1 \
PTH_PERF_OUTPUT=target/performance-c1-p1.json \
cargo run --locked --release --example performance

PTH_PERF_CONCURRENCY=4 PTH_PERF_DOWNSTREAM_POOL_SIZE=5 \
PTH_PERF_OUTPUT=target/performance-c4-p5.json \
cargo run --locked --release --example performance

PTH_PERF_CONCURRENCY=10 PTH_PERF_CONCURRENT_OPERATIONS=10 \
PTH_PERF_DRAIN_DATABASES=10 PTH_PERF_DOWNSTREAM_POOL_SIZE=10 \
PTH_PERF_OUTPUT=target/performance-c10-p10.json \
cargo run --locked --release --example performance
```

The permit formula describes potential application use, not total PostgreSQL
sessions. For `B=120` and `P=11`, `L=floor(B/P)=10` and the maximum represented
application spike is `L*P=110`. Separately, one owner session, one session per
live template, the lazy lifecycle pool, the observer, and ambient external
traffic consume server slots. Compare the report's three PostgreSQL capacity
settings with the observed pool spike before increasing either dimension.

## CI comparison

The manual `Performance characterization` Actions workflow pre-pulls the owned
image, runs the same release example, writes a Markdown summary, and uploads
the complete JSON artifact even if summary rendering fails. Its dispatch inputs
expose lease concurrency, downstream pool size, budget, per-database permits,
operation count, and drain count so matrix points do not require source edits.
Its `external` option scopes `POSTGRES_TEST_ADMIN_URL` from an Actions secret to
the benchmark step. Dispatches are serialized by mode and the job has a
one-hour ceiling. The workflow is deliberately not a required push or
pull-request check.

The current crate always compiles owned-container support. When an external-only
feature is introduced, add `cargo check --no-default-features --example
performance` (or the feature's final equivalent) to this workflow. The example
itself does not call Testcontainers and is already expressed through the public
harness boundary.

## Baseline

The current checked-in baseline and its machine/configuration provenance are in
[performance-baseline.md](performance-baseline.md). Regenerate it after a
structural performance change and attach both owned and external JSON artifacts
when those environments are available.
