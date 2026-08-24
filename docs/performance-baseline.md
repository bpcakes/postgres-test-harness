# Performance baseline

This baseline characterizes the production library at commit
`6edc72c936b909438900274d5f3968878875d15e`. The benchmark and documentation
changes were uncommitted when it ran, so the JSON correctly reported
`git_worktree_dirty: true`; no `src/` file differed from that commit. The
reports used schema version 2 and were written beneath ignored `target/`, which
did not add any worktree change of its own.

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
| Benchmark projects | `pghp_01a0347fb7` owned; `pghp_01a0347f4f` external |
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
| Server startup | all | 3401.962 | 4178.939 | 3937.837 | 4232.611 |
| Cold template acquisition | small | 74.645 | 75.876 | 75.513 | 76.019 |
| Cold template acquisition | representative | 443.698 | 448.534 | 450.918 | 460.521 |
| Warm template acquisition | small | 6.438 | 6.818 | 6.786 | 7.101 |
| Warm template acquisition | representative | 6.396 | 7.042 | 6.857 | 7.134 |
| Sequential clone-cleanup (4) | small | 205.439 | 211.329 | 211.090 | 216.501 |
| Sequential clone-cleanup (4) | representative | 327.126 | 332.100 | 338.476 | 356.201 |
| Bounded concurrent clone-cleanup (8 at 4) | small | 133.174 | 150.196 | 144.747 | 150.871 |
| Bounded concurrent clone-cleanup (8 at 4) | representative | 391.839 | 526.143 | 559.801 | 761.421 |
| Explicit cleanup drain (4-way) | small | 23.474 | 25.129 | 26.630 | 31.289 |
| Explicit cleanup drain (4-way) | representative | 26.266 | 41.782 | 103.687 | 243.013 |
| Deferred cleanup drain (4 queued to serial worker) | small | 93.974 | 94.080 | 163.598 | 302.740 |
| Deferred cleanup drain (4 queued to serial worker) | representative | 150.261 | 176.835 | 201.058 | 276.079 |

Startup's 3.40–4.23 second range is material variance even with a cached image;
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
| Harness attach/startup | all | 6.413 | 7.218 | 6.953 | 7.228 |
| Cold template acquisition | small | 65.253 | 70.256 | 71.956 | 80.359 |
| Cold template acquisition | representative | 473.397 | 474.803 | 475.927 | 479.581 |
| Warm template acquisition | small | 6.200 | 6.860 | 7.105 | 8.255 |
| Warm template acquisition | representative | 6.544 | 6.910 | 6.889 | 7.213 |
| Sequential clone-cleanup (4) | small | 195.414 | 203.554 | 206.416 | 220.280 |
| Sequential clone-cleanup (4) | representative | 333.633 | 410.186 | 422.321 | 523.143 |
| Bounded concurrent clone-cleanup (8 at 4) | small | 140.805 | 152.712 | 154.482 | 169.928 |
| Bounded concurrent clone-cleanup (8 at 4) | representative | 523.182 | 736.240 | 683.688 | 791.641 |
| Explicit cleanup drain (4-way) | small | 24.240 | 24.652 | 24.767 | 25.411 |
| Explicit cleanup drain (4-way) | representative | 135.964 | 190.120 | 177.462 | 206.302 |
| Deferred cleanup drain (4 queued to serial worker) | small | 81.773 | 93.937 | 90.166 | 94.787 |
| Deferred cleanup drain (4 queued to serial worker) | representative | 176.744 | 267.590 | 237.501 | 268.168 |

Explicit drain used four concurrent cleanup calls. Deferred drain submitted
four leases to the current process-global serial worker and detected completion
with 10 ms catalog polling. The rows characterize different public behaviors;
their ratio is not an estimate of intrinsic explicit-versus-deferred cleanup
overhead.

The external cleanup pass took 35.0–38.5 ms per sample and dropped exactly two
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
