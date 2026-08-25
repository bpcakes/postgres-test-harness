# Performance baseline

This baseline characterizes the production library at commit
`667557a5bd7ba58dd51a05535552bd3c6a961fc7`. The schema-v3 naming and
documentation changes were uncommitted when it ran, so the JSON correctly
reported `git_worktree_dirty: true`; no `src/` file differed from that commit.
The reports were written beneath ignored `target/`, which did not add any
worktree change of its own.

Both runs completed on 2026-08-24 with the same workload. These numbers are
directional observations from one machine, not project thresholds.

## Provenance and configuration

| Property | Value |
| --- | --- |
| PostgreSQL | 18.1 (`180001`), Debian image build |
| Image | `postgres:18` |
| Image content ID | `sha256:80c0891f5de95d70f2fd664165fa6b8aa246b805a5c13c97824c4bd0970f4c8e` |
| Docker storage driver | `overlay2` |
| Host | Linux x86-64, 64 logical CPUs |
| Samples | 3 independent harness starts per mode |
| Benchmark projects | `pghp_684b94ae920` owned; `pghp_30d72d80b04` external |
| Sequential work | 4 clone-cleanup lifecycles per fixture |
| Concurrent work | 8 clone-cleanup lifecycles, bounded at 4 |
| Cleanup drains | 4 pre-created databases |
| Connection policy | 120 total permits, 11 per live database |
| Small template size | 7,894,719 bytes |
| Representative template | 50,000 rows, 29,095,615 bytes |

The owned run used a previously cached image and verified every started
container against its recorded image ID. The external run used a
separately started container with the same image, storage driver, durability
settings, and connection limit. All six samples retained their mapped TCP
observer session across the post-start probe and reported a stable postmaster
start time, confirming that startup did not stop at the temporary initdb
server.

## Owned-container results

Elapsed milliseconds across three samples:

| Metric | Fixture | Min | Median | Mean | Max |
| --- | --- | ---: | ---: | ---: | ---: |
| Server startup | all | 2560.538 | 2718.199 | 2751.948 | 2977.107 |
| Cold template acquisition | small | 82.234 | 83.651 | 84.320 | 87.074 |
| Cold template acquisition | representative | 458.438 | 468.013 | 474.686 | 497.607 |
| Warm template acquisition | small | 6.263 | 7.279 | 7.100 | 7.759 |
| Warm template acquisition | representative | 6.867 | 7.301 | 7.528 | 8.416 |
| Sequential clone-cleanup (4) | small | 203.312 | 213.823 | 212.756 | 221.132 |
| Sequential clone-cleanup (4) | representative | 343.458 | 364.284 | 359.004 | 369.269 |
| Bounded concurrent clone-cleanup (8 at 4) | small | 142.716 | 149.360 | 158.030 | 182.013 |
| Bounded concurrent clone-cleanup (8 at 4) | representative | 394.330 | 398.280 | 425.215 | 483.034 |
| Explicit cleanup drain (4-way) | small | 23.014 | 23.386 | 24.366 | 26.699 |
| Explicit cleanup drain (4-way) | representative | 31.198 | 32.882 | 46.689 | 75.987 |
| Deferred cleanup drain (4 queued to serial worker) | small | 105.182 | 142.671 | 131.162 | 145.633 |
| Deferred cleanup drain (4 queued to serial worker) | representative | 167.036 | 199.573 | 192.154 | 209.854 |

Startup's 2.56–2.98 second range is material variance even with a cached image;
preserving raw observations is therefore more informative than a single
headline value.

The epic's earlier 1.829-second directional probe measured a separate ad hoc
final-TCP-readiness experiment. This workflow times the complete
`PostgresHarness::start`, including Testcontainers startup, mapped-port
resolution, admin authentication, PostgreSQL-version validation, and owner-lock
acquisition. PERF06 comparisons must rerun this workflow before and after a
change on the same host rather than compare directly with the audit probe.

## External-server results

Elapsed milliseconds across three samples:

| Metric | Fixture | Min | Median | Mean | Max |
| --- | --- | ---: | ---: | ---: | ---: |
| Harness attach/startup | all | 6.444 | 6.855 | 6.970 | 7.612 |
| Cold template acquisition | small | 69.768 | 70.025 | 73.924 | 81.980 |
| Cold template acquisition | representative | 464.746 | 470.607 | 469.718 | 473.800 |
| Warm template acquisition | small | 6.816 | 6.893 | 6.966 | 7.188 |
| Warm template acquisition | representative | 7.084 | 7.958 | 7.986 | 8.916 |
| Sequential clone-cleanup (4) | small | 199.359 | 202.395 | 203.685 | 209.301 |
| Sequential clone-cleanup (4) | representative | 365.680 | 402.629 | 394.186 | 414.251 |
| Bounded concurrent clone-cleanup (8 at 4) | small | 139.297 | 146.542 | 146.993 | 155.138 |
| Bounded concurrent clone-cleanup (8 at 4) | representative | 465.725 | 471.586 | 515.216 | 608.337 |
| Explicit cleanup drain (4-way) | small | 25.657 | 28.214 | 28.317 | 31.079 |
| Explicit cleanup drain (4-way) | representative | 49.732 | 91.358 | 79.115 | 96.254 |
| Deferred cleanup drain (4 queued to serial worker) | small | 83.819 | 95.757 | 106.935 | 141.230 |
| Deferred cleanup drain (4 queued to serial worker) | representative | 153.471 | 155.939 | 177.273 | 222.407 |

Explicit drain used four concurrent cleanup calls. Deferred drain submitted
four leases to the current process-global serial worker and detected completion
with 10 ms catalog polling. The rows characterize different public behaviors;
their ratio is not an estimate of intrinsic explicit-versus-deferred cleanup
overhead.

The external cleanup pass took 32.9–39.1 ms per sample and dropped exactly two
run-unique templates each time. Every pass completed in one attempt with all
three skipped counters at zero. It found no residual test database, and a final
catalog query confirmed that the unique project had no database remaining.

## Administrative connection churn

The owned server had no ambient clients, and both modes produced the same
per-sample session deltas:

| Phase | Work | Admin-database session delta |
| --- | ---: | ---: |
| Cold template acquisition | 1 | 1 |
| Warm template acquisition | 1 | 1 |
| Sequential clone-cleanup | 4 | 8 |
| Bounded concurrent clone-cleanup | 8 | 16 |
| Explicit cleanup drain | 4 | 4 |
| Deferred cleanup drain | 4 | 4 |

The values expose the current fresh-session-per-operation behavior without
instrumenting the production code: a complete clone plus explicit cleanup uses
two administrative connections, while template acquisition retains one new
shared-lock session per call. Future performance changes should compare these
deltas alongside latency and throughput rather than optimizing only one axis.

## PERF06 owned-container profile follow-up

On 2026-08-24, the schema-v5 workload was rerun three times with the PERF06
implementation at source commit
`845925b0f5247fcc0eb831837f48dda9e91b3467`. The reports correctly record a
dirty worktree because the profile implementation and this documentation were
not yet committed. All three owned comparisons used the same cached image,
host, workload, PostgreSQL settings, and template sizes as the baseline above:
7,894,719 bytes for `small` and 29,095,615 bytes for `representative`.

Median elapsed milliseconds across three samples per profile:

| Metric | Fixture | Compatibility: synced initdb, image storage | `--no-sync`, image storage | Default: `--no-sync`, 1 GiB tmpfs |
| --- | --- | ---: | ---: | ---: |
| Server startup | all | 1710.431 | 1309.803 | 1236.474 |
| Cold template acquisition/migration | small | 75.563 | 77.490 | 61.983 |
| Cold template acquisition/migration | representative | 443.346 | 464.827 | 425.552 |
| Sequential clone-cleanup (4) | small | 200.445 | 205.435 | 154.001 |
| Sequential clone-cleanup (4) | representative | 322.970 | 323.550 | 257.206 |
| Bounded concurrent clone-cleanup (8 at 4) | small | 143.175 | 142.797 | 99.835 |
| Bounded concurrent clone-cleanup (8 at 4) | representative | 413.168 | 432.675 | 263.934 |
| Explicit cleanup drain (4-way) | small | 29.886 | 29.476 | 12.669 |
| Explicit cleanup drain (4-way) | representative | 70.577 | 29.610 | 15.476 |
| Deferred cleanup drain (4 queued) | small | 83.193 | 83.400 | 70.354 |
| Deferred cleanup drain (4 queued) | representative | 164.039 | 163.593 | 129.677 |

On this run, `--no-sync` reduced median final-TCP-ready startup by 23.4%
relative to the compatibility profile. Adding tmpfs reduced representative
sequential clone-cleanup by 20.5% and bounded-concurrent clone-cleanup by 39.0%
relative to `--no-sync` on image-default storage. The explicit representative
cleanup measurement was noisy in the compatibility run, so it should not be
used as a standalone effect estimate. These remain machine-specific
observations, not thresholds.

The default-profile report recorded `data_checksums=on`, `wal_level=replica`,
`fsync=off`, `synchronous_commit=off`, and `full_page_writes=off`. This proves
the profile did not obtain speed by disabling checksums or reducing WAL level.
Each sample retained the same mapped-TCP postmaster across its readiness probe,
and explicit shutdown left no managed container.

An external-mode schema-v5 run against the same image and runtime PostgreSQL
settings also completed three samples. Its configuration recorded
`owned_container_profile: null`, median attach time was 6.501 ms, and every
sample's metadata-aware cleanup dropped exactly its two templates with no test
database left. This is evidence that owned storage configuration is not
applied to external servers.

The raw local reports are:

- `target/performance-owned-perf06-compatibility.json`
- `target/performance-owned-perf06-image-storage.json`
- `target/performance-owned-perf06.json`
- `target/performance-external-perf06.json`

## PERF02 lifecycle admin-session pool follow-up

On 2026-08-24, the default owned schema-v5 workload was rerun three times after
adding the lazy bounded lifecycle admin-session pool. The run used the same
host, cached image content ID, 1 GiB tmpfs profile, 50,000-row representative
fixture, operation counts, and four-way caller concurrency as the PERF06
default-profile comparison. The JSON records base commit
`2d6c30f7d433cb5cab1fa138ca6158b37c368f56` and a dirty worktree containing the
PERF02 implementation. Its project was `pghp_fdc593bc995`.

Median elapsed milliseconds, compared with the PERF06 default-profile medians:

| Metric | Fixture | PERF06 fresh sessions | PERF02 pooled sessions | Directional reduction |
| --- | --- | ---: | ---: | ---: |
| Sequential clone-cleanup (4) | small | 154.001 | 110.150 | 28.5% |
| Sequential clone-cleanup (4) | representative | 257.206 | 187.691 | 27.0% |
| Bounded concurrent clone-cleanup (8 at 4) | small | 99.835 | 81.440 | 18.4% |
| Bounded concurrent clone-cleanup (8 at 4) | representative | 263.934 | 238.148 | 9.8% |
| Explicit cleanup drain (4-way) | small | 12.669 | 9.383 | 25.9% |
| Explicit cleanup drain (4-way) | representative | 15.476 | 11.690 | 24.5% |
| Deferred cleanup drain (4 queued) | small | 70.354 | 47.352 | 32.7% |
| Deferred cleanup drain (4 queued) | representative | 129.677 | 117.607 | 9.3% |

These are cross-run observations rather than latency thresholds. Startup was
again noisy and is not attributed to the pool. Every measured lifecycle median
improved, while the connection counter gives the more direct mechanism check.

All three PERF02 samples reported identical lifecycle session deltas:

| Fixture | Sequential clone-cleanup | Bounded concurrent clone-cleanup | Explicit drain | Deferred drain |
| --- | ---: | ---: | ---: | ---: |
| Small (pool cold) | 1 | 3 | 0 | 0 |
| Representative (pool warm) | 0 | 0 | 0 | 0 |

The comparable pre-pool workload opened 8, 16, 4, and 4 sessions in those four
phases for each fixture: 64 new lifecycle sessions per sample. PERF02 opened
four, a 93.75% reduction. After the first bounded-concurrent phase,
`active_after=8` exactly accounted for one owner lock, one observer, two live
template locks, and four lazy lifecycle-pool sessions. This shows that the
pool grew to actual caller concurrency rather than eagerly filling its default
limit of ten. The ignored PostgreSQL regression suite separately configures a
four-session limit, blocks four simultaneous metadata operations, and observes
exactly four project-labeled lifecycle backends through `pg_stat_activity`.

The raw local report is `target/performance-owned-perf02.json`.

## PERF04 bounded deferred-cleanup follow-up

On 2026-08-24, the default owned workload was rerun three times after replacing
the process-global fallback worker with bounded per-server cleanup workers and
an awaited sequence barrier. The schema-v6 run used the same cached image
content ID, 1 GiB tmpfs profile, 50,000-row representative fixture, operation
counts, and four-database drain as the PERF02 sample. It records base commit
`10b7f5a59f7b35eceb8bf6389aa82f1cb1cb6853` and a dirty worktree containing the
PERF04 implementation. Its project was `pghp_b9be2276762`.

Schema v6 separates the time required to hand four leases to the bounded queue
from the subsequent final drain:

| Fixture | Caller return median | Final drain median | Caller-to-final median |
| --- | ---: | ---: | ---: |
| Small | 0.155 ms | 6.740 ms | 6.899 ms |
| Representative | 0.167 ms | 12.085 ms | 12.257 ms |

The former PERF02 `deferred_cleanup_drain` measured Drop-to-catalog-poll
completion and had medians of 47.352 ms and 117.607 ms respectively. Those
values are useful directional context, not an apples-to-apples speedup claim:
the old observation included 10 ms polling granularity, while PERF04 ends on
the explicit drain barrier and performs one untimed catalog assertion after it
returns. All six PERF04 deferred phases opened zero new lifecycle sessions,
showing reuse of the already-warm per-server pool.

The raw local report is `target/performance-owned-perf04.json`.

## PERF03 live-template cache follow-up

On 2026-08-25, the default owned schema-v6 workload was rerun three times after
adding fingerprint-keyed single-flight and weak live-template caching to each
server. It used the same host, cached PostgreSQL image content ID, 1 GiB tmpfs
profile, fixture sizes, operation counts, and four-way concurrency as the
earlier owned measurements. The report records base commit
`9a25695b6162079b7a17805c700a44c603d909bb` and a dirty worktree containing the
PERF03 implementation. Its project was `pghp_300a1280627`.

Median acquisition time compared with the original PERF01 owned baseline:

| Metric | Fixture | PERF01 | PERF03 | Directional reduction |
| --- | --- | ---: | ---: | ---: |
| Cold template acquisition | Small | 83.651 ms | 66.114 ms | 21.0% |
| Cold template acquisition | Representative | 468.013 ms | 461.711 ms | 1.3% |
| Warm template acquisition | Small | 7.279 ms | 0.004261 ms | 99.94% |
| Warm template acquisition | Representative | 7.301 ms | 0.002864 ms | 99.96% |

The cold differences are cross-run observations that also include intervening
changes and ordinary machine variance; they are not attributed to the cache.
The warm mechanism is directly supported by the session counter: all six cold
phases opened exactly one administrative session and all six warm phases
opened zero. The original PERF01 workload opened one session on both paths.
Thus the warm call reused the cold handle's retained shared-lock session rather
than creating a second PostgreSQL coordination client.

The ignored PostgreSQL regression independently starts eight same-server cold
callers and observes one initializer plus one retained shared-lock session. It
also completes a warm lookup while the shared-description catalog is locked
and an exclusive advisory-lock waiter is queued, then verifies that a distinct
server still queues behind that waiter. Those synchronization conditions prove
the live warm path performs neither catalog nor advisory-lock round trips while
preserving cross-server fairness.

The raw local report is `target/performance-owned-perf03.json`.

## PERF07 effective-concurrency follow-up

On 2026-08-25, the schema-v7 owned workload was run at three lease/pool
geometry points after lifecycle-session pooling, bounded deferred cleanup, and
live-template caching were all present. Each point contains three independent
server samples and both the small and 50,000-row representative fixture. All
runs used base commit `9a25695b6162079b7a17805c700a44c603d909bb`
with a dirty worktree containing the PERF03 and PERF07 implementations, the
same cached image content ID and 1 GiB tmpfs profile, PostgreSQL 18.1, and the
same 64-logical-CPU Linux host as the PERF03 follow-up.

The harness policy remained `B=120` total permits and `P=11` permits per
database, giving the newly exposed `L=floor(B/P)=10` effective simultaneous
leases. PostgreSQL reported `max_connections=300`, `reserved_connections=0`,
and `superuser_reserved_connections=3`, or 297 non-reserved slots. No defaults
or server settings were changed for these runs.

The new phase creates all measured leases concurrently, eagerly opens and
probes the configured number of application clients on each, requires the
exact total to be visible in `pg_stat_activity`, then closes those clients and
cleans every lease. Median observations were:

| Leases | Eager clients per lease | Total clients | Peak observed in each fixture/sample | Small checkout | Small total | Representative checkout | Representative total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 1 | 1 | 22.077 ms | 27.994 ms | 38.324 ms | 45.134 ms |
| 4 | 5 | 20 | 20 | 39.406 ms | 50.168 ms | 107.743 ms | 139.339 ms |
| 10 | 10 | 100 | 100 | 117.990 ms | 209.639 ms | 290.931 ms | 476.347 ms |

At the three points, median eager-checkout throughput was respectively 45.3,
507.5, and 847.5 connections/second for the small fixture and 26.1, 185.6, and
343.7 connections/second for the representative fixture. These are geometry
observations rather than a claim that a third-party pool normally connects
eagerly. The exact peak checks are the important capacity evidence: the owned
server sustained every requested spike through all ten effective leases.

The largest point used 100 application sessions. Adding the one owner, one
live measured template, persistent benchmark observer, and at most ten lazy
lifecycle sessions remains far below the 300-session server limit and its 297
non-reserved slots. Throughput still scaled at that point, while the report
contains no comparison showing a robust benefit from a different permit policy
or `max_connections`. PERF07 therefore keeps the 120/11 defaults and owned
`max_connections=300`. Live-template count is intentionally not bounded and
pool maxima are not eager counts, so deriving the server setting from the
permit budget would imply a guarantee the API does not make.

The raw local reports are
`target/performance-owned-perf07-c1-p1.json`,
`target/performance-owned-perf07-c4-p5.json`, and
`target/performance-owned-perf07-c10-p10.json`.

A separate three-sample external-mode safety run targeted a temporary
PostgreSQL 18.1 server configured with `max_connections=20`,
`reserved_connections=0`, and `superuser_reserved_connections=3`. It used
`B=10`, `P=5`, `L=2`, and five eager clients on each of two leases. Every one
of the six fixture/sample observations reported the exact ten application
sessions at peak. The lazy lifecycle pool was capped at two by `L`; together
with one owner, one live template, and the observer, the measured workload fit
within the server's 17 non-reserved slots. This directly exercises low-capacity
external sizing and records both reserved-slot settings. Its timings are not
compared with owned mode because the temporary external server retained
PostgreSQL's durable defaults (`fsync=on`, `synchronous_commit=on`, and
`full_page_writes=on`). The raw report is
`target/performance-external-perf07-low-capacity.json`; the temporary container
was removed after the run.

## PERF05 prewarmed-queue follow-up

On 2026-08-25, the default owned workload was rerun three times after adding a
four-slot prewarmed disposable-database queue. The schema-v8 report used base
commit `025cf6d8d7f54978c23cc02b7737a0b633067c0a` with a dirty worktree
containing the PERF05 implementation. PostgreSQL 18.1, the cached image content
ID, 1 GiB tmpfs profile, 64-logical-CPU Linux host, fixture definitions, and
`B=120`, `P=11`, `L=10` permit policy matched the immediately preceding owned
characterization.

The phase synchronously fills four clean clones, leases all four without a
PostgreSQL round trip, measures their exact combined database size, defers all
dirty returns, awaits the drop-and-replacement barrier, requires four new ready
clones, and finally shuts the pool down. Median observations were:

| Fixture | Initial fill | First ready lease | Steady ready lease | Initial four-clone storage | Refill | Refill throughput | Final idle cleanup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Small | 76.513 ms | 3.283 us | 1.397 us | 30,935,100 bytes | 34.305 ms | 116.6 databases/s | 6.149 ms |
| Representative | 141.103 ms | 3.352 us | 1.467 us | 115,738,684 bytes | 105.178 ms | 38.0 databases/s | 11.582 ms |

All six fixture/sample refills ended with exactly four ready databases. The
reported storage is the sum of `pg_database_size` for the exact initial clone
names, not a whole-cluster estimate. It quantifies the intentional latency for
storage trade: ready-queue lease latency is in-process and near zero, while
initial fill retains roughly four clone sizes and the background refill still
pays a complete dirty `DROP` plus fresh `CREATE DATABASE` per slot. The phase's
administrative-session delta was zero in all six observations because the
earlier phases had already warmed the bounded lifecycle pool.

The ignored PostgreSQL lifecycle regression independently proves that idle
slots consume no application permits, exhausted callers wait, cancellation
does not consume a slot, dirty schema state never appears in a replacement,
distinct templates have independent queues, a pool retains its template lock,
and an injected missing-template refill failure reaches the drain barrier and
closes the queue. The raw local report is
`target/performance-owned-perf05.json`.

## PERF08 external-only compile follow-up

On 2026-08-25, the feature-gated implementation was measured with Rust/Cargo
1.97.1 on the same 64-logical-CPU Linux host. Both commands used the warm Cargo
source cache but a distinct empty target directory, `--locked`, and `--lib`, so
neither build reused compiled artifacts from the other:

```console
cargo check --locked --lib
cargo check --locked --no-default-features --lib
```

One directional clean-target observation was:

| Build | Elapsed | User CPU | System CPU | Peak RSS | Normal/build tree entries |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default `containers` | 19.70 s | 78.89 s | 21.07 s | 594,256 KiB | 277 |
| External only | 5.20 s | 20.26 s | 6.34 s | 326,896 KiB | 110 |

The external-only observation reduced elapsed check time by 73.6%, peak RSS by
45.0%, and unique `cargo tree --edges normal,build` lines by 60.3% on this
machine. These are compile-cost observations, not stable thresholds.

`cargo tree --no-default-features` contained no Testcontainers, Bollard,
Rustls, or Ring node, and the harness root had no direct `libc` edge. `libc`
remained transitively necessary through Tokio/Postgres networking, UUID random
generation, and related platform crates, so making the container cleanup use
optional removed the direct edge but could not remove that platform package.
The ignored external-only lifecycle test also attached to a separately started
PostgreSQL 18.1 container, created a template and disposable databases, cleaned
them, and confirmed external `shutdown` remained a no-op; the temporary
container was removed after validation.
