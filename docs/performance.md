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
`POSTGRES_TEST_IMAGE` selects a different image.

## External-server run

Set the same administrative URL accepted by the library:

```bash
POSTGRES_TEST_ADMIN_URL='postgres://postgres:secret@127.0.0.1/postgres' \
PTH_PERF_EXTERNAL_IMAGE_DIGEST='sha256:provisioned-image-id' \
PTH_PERF_EXTERNAL_STORAGE_DRIVER='provisioner-or-filesystem' \
PTH_PERF_OUTPUT=target/performance-external.json \
cargo run --locked --release --example performance
```

The URL and credentials are never written to the report. Image and storage
metadata cannot be discovered portably from PostgreSQL, so the two external
metadata variables are optional provenance supplied by the operator. Their
fields remain present with a `not_reported_for_external_server` source when the
values are unavailable.

An external run disables the startup stale sweep so that unrelated cleanup is
not folded into startup. Each invocation uses a random, valid project namespace
to isolate concurrent local runs and prints it before starting database work.
After each sample releases its owner and template locks, it runs a zero-age,
metadata- and lock-aware sweep. Ordinary measurement errors still close the
observer and run this sweep; if both operations fail, the primary and cleanup
errors are reported together. Successful samples additionally require the exact
expected cleanup counts. If asynchronous connection shutdown leaves a lock
briefly active, the benchmark starts more sweeps only within a 120-second retry
window. Each mutating sweep is allowed to finish under the library's own
operation timeout rather than being canceled mid-drop, so the retry window is
not an end-to-end cleanup deadline. The JSON preserves every cleanup attempt
and all dropped and skipped counters; unexpected leftovers fail the run. A
forced process termination cannot run asynchronous cleanup, so use the logged
project with `cleanup_stale_databases` to recover that exceptional case.

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
  on the Docker entrypoint's temporary initdb server: harness startup must open
  the mapped TCP admin connection and validate PostgreSQL 18. The example then
  retains an observer TCP session across a second postmaster-start probe and
  fails if the postmaster changes.
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
- `explicit_cleanup_drain`: creates the configured leases before timing, then
  awaits their explicit cleanups concurrently. Its `concurrency` field equals
  the configured drain count.
- `deferred_cleanup_drain`: creates the configured leases before timing, drops
  all of them, and ends only after a persistent observer confirms every exact
  database name is absent from `pg_database`. The 120-second guard prevents a
  broken run from hanging; it is not a benchmark threshold. On the characterized
  implementation this path uses one serial fallback worker and 10 ms polling,
  so it is not an apples-to-apples latency comparison with the concurrent
  explicit drain. Its purpose is to record caller-to-final-drain behavior for
  PERF04.

One persistent observer connection reads `pg_stat_database.sessions` before
and after every phase. The report includes the cumulative values, delta, and
active admin-database sessions. That delta characterizes harness connection
churn without adding instrumentation to the production path. It is exact on
the single-purpose owned server. On a shared external server it is intentionally
labeled server-wide and can include ambient clients, so run under controlled
load or interpret it as an upper bound. A server-side statistics reset during a
phase can also invalidate a delta; retain the raw before/after counters.

The top-level report also records the source commit and worktree state,
PostgreSQL version, postmaster start, logical CPU count, OS, architecture,
server mode, image metadata, storage driver, fixture rows, concurrency, sample
counts, connection budget, per-database permits, timeouts, and cleanup policy.

## Configuration

All numeric overrides must be positive. The example rejects concurrency or
drain counts that cannot fit in its explicit 120-permit, 11-per-database
connection policy.

| Variable | Default | Meaning |
| --- | ---: | --- |
| `PTH_PERF_SAMPLES` | 3 | Independent server/harness samples |
| `PTH_PERF_SEQUENTIAL_OPERATIONS` | 4 | Sequential clone-cleanup lifecycles per fixture |
| `PTH_PERF_CONCURRENCY` | 4 | Maximum concurrent clone-cleanup lifecycles |
| `PTH_PERF_CONCURRENT_OPERATIONS` | 8 | Total bounded-concurrent lifecycles per fixture |
| `PTH_PERF_DRAIN_DATABASES` | 4 | Pre-created leases in each cleanup-drain measurement |
| `PTH_PERF_REPRESENTATIVE_ROWS` | 50000 | Rows migrated into the representative fixture |
| `PTH_PERF_OUTPUT` | stdout | JSON output path; use ignored `target/` for clean provenance |
| `PTH_PERF_EXTERNAL_IMAGE_DIGEST` | unset | External-server image provenance |
| `PTH_PERF_EXTERNAL_STORAGE_DRIVER` | unset | External-server storage provenance |

Keep the workload configuration equal when comparing reports. Multiple raw
samples remain in `samples`; `summary` provides min, median, mean, and max for
each timing. Preserve raw JSON artifacts because a single aggregate hides
variance and connection-count changes.

## CI comparison

The manual `Performance characterization` Actions workflow pre-pulls the owned
image, runs the same release example, writes a Markdown summary, and uploads
the complete JSON artifact even if summary rendering fails. Its `external`
option scopes `POSTGRES_TEST_ADMIN_URL` from an Actions secret to the benchmark
step. Dispatches are serialized by mode and the job has a one-hour ceiling. The
workflow is deliberately not a required push or pull-request check.

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
