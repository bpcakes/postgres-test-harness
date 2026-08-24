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
