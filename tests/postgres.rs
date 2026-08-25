#![cfg(feature = "containers")]

use std::{
    fs,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Sender},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use postgres_test_harness::{
    BoxError, DEFAULT_OWNED_CONTAINER_TMPFS_SIZE_BYTES, Error, FingerprintBuilder, HarnessConfig,
    OwnedContainerProfile, PostgresHarness, TemplateSpec, cleanup_stale_databases,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn postgres_lifecycle_regressions_work_end_to_end() {
    let harness = PostgresHarness::start(
        HarnessConfig::new("harness_it")
            .unwrap()
            .with_cleanup_on_start(false),
    )
    .await
    .expect("start owned PostgreSQL 18 harness");
    assert!(!harness.is_external());
    let limits = harness.connection_limits();
    assert_eq!(limits.connection_budget(), 120);
    assert_eq!(limits.connections_per_database(), 11);
    assert_eq!(limits.max_simultaneous_leases(), 10);
    let container_id = harness
        .container_id()
        .expect("owned harness exposes its container ID")
        .to_owned();
    let admin_url = harness.admin_database_url().to_owned();
    assert_default_owned_profile(&container_id);
    assert_eq!(
        scalar_string(admin_url.clone(), "SHOW data_checksums")
            .await
            .expect("read data-checksum setting"),
        "on"
    );
    assert_eq!(
        scalar_string(admin_url.clone(), "SHOW wal_level")
            .await
            .expect("read WAL level"),
        "replica"
    );
    let postmaster_started_at =
        scalar_string(admin_url.clone(), "SELECT pg_postmaster_start_time()::text")
            .await
            .expect("read final postmaster start time");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        scalar_string(admin_url.clone(), "SELECT pg_postmaster_start_time()::text")
            .await
            .expect("re-read final postmaster start time"),
        postmaster_started_at,
        "owned startup must retain the same final TCP postmaster"
    );

    {
        let first = harness
            .empty_database()
            .await
            .expect("create database through a cold lifecycle admin pool");
        first
            .cleanup()
            .await
            .expect("return the first lifecycle admin session");
        let first_pids = lifecycle_admin_pids(admin_url.clone(), "harness_it")
            .await
            .expect("inspect the warmed lifecycle admin pool");
        assert_eq!(first_pids.len(), 1);

        let second = harness
            .empty_database()
            .await
            .expect("create database through a warm lifecycle admin pool");
        second
            .cleanup()
            .await
            .expect("reuse the lifecycle admin session for cleanup");
        assert_eq!(
            lifecycle_admin_pids(admin_url.clone(), "harness_it")
                .await
                .expect("inspect the reused lifecycle admin pool"),
            first_pids,
            "sequential create and cleanup operations should reuse one backend"
        );

        assert!(
            terminate_backend(admin_url.clone(), first_pids[0])
                .await
                .expect("terminate the idle lifecycle admin backend")
        );
        wait_until_lifecycle_admin_session_count(&admin_url, "harness_it", 0).await;

        let reconnected = harness
            .empty_database()
            .await
            .expect("replace a terminated lifecycle admin backend");
        reconnected
            .cleanup()
            .await
            .expect("reuse the replacement backend for cleanup");
        let replacement_pids = lifecycle_admin_pids(admin_url.clone(), "harness_it")
            .await
            .expect("inspect the replacement lifecycle admin backend");
        assert_eq!(replacement_pids.len(), 1);
        assert_ne!(replacement_pids, first_pids);
    }

    {
        let cleanup_spec = TemplateSpec::new(FingerprintBuilder::new("cleanup-schema").finish());
        let cleanup_template = harness
            .template(cleanup_spec, |_| async { Ok(()) })
            .await
            .expect("initialize cleanup template");
        let active_database = cleanup_template
            .database()
            .await
            .expect("create active cleanup database");
        const UNTAGGED_DATABASE: &str = "pgh_harness_it_test_00000000000000000000000000000000";
        let untagged_database =
            TemporaryDatabase::create(admin_url.clone(), UNTAGGED_DATABASE.to_owned()).await;

        let report = cleanup_stale_databases(&admin_url, "harness_it", Duration::ZERO)
            .await
            .expect("run owner-aware cleanup");
        assert_eq!(report.dropped_test_databases, 0);
        assert_eq!(report.dropped_templates, 0);
        assert_eq!(report.skipped_active, 2);
        assert_eq!(report.skipped_fresh, 0);
        assert_eq!(report.skipped_unrecognized, 1);
        assert!(
            database_exists(admin_url.clone(), UNTAGGED_DATABASE.to_owned())
                .await
                .expect("check untagged database")
        );

        const RACING_DATABASE: &str = "pgh_harness_it_test_11111111111111111111111111111111";
        let racing_database =
            TemporaryDatabase::create(admin_url.clone(), RACING_DATABASE.to_owned()).await;
        execute(
            admin_url.clone(),
            format!(
                "COMMENT ON DATABASE \"{RACING_DATABASE}\" IS \
                 'postgres-test-harness:v1;kind=test;project=harness_it;owner=123456789;created=0'"
            ),
        )
        .await
        .expect("tag stale database for concurrent cleanup");
        let (left_report, right_report) = tokio::join!(
            cleanup_stale_databases(&admin_url, "harness_it", Duration::ZERO),
            cleanup_stale_databases(&admin_url, "harness_it", Duration::ZERO),
        );
        let left_report = left_report.expect("run first concurrent cleanup");
        let right_report = right_report.expect("run second concurrent cleanup");
        assert_eq!(
            left_report.dropped_test_databases + right_report.dropped_test_databases,
            1
        );
        assert_eq!(left_report.dropped_templates, 0);
        assert_eq!(right_report.dropped_templates, 0);
        assert!(
            !database_exists(admin_url.clone(), RACING_DATABASE.to_owned())
                .await
                .expect("verify concurrent cleanup removed the stale database once")
        );

        racing_database.remove().await;
        untagged_database.remove().await;
        active_database
            .cleanup()
            .await
            .expect("clean active cleanup database");
    }

    {
        let fingerprint = FingerprintBuilder::new("drop-lock-regression").finish();
        let lock_key = advisory_key("template", &format!("harness_it:{}", fingerprint.to_hex()));
        let template = harness
            .template(TemplateSpec::new(fingerprint), |_| async { Ok(()) })
            .await
            .expect("initialize template for retained-lock drop regression");
        assert!(
            !advisory_lock_is_acquirable(admin_url.clone(), lock_key)
                .await
                .expect("check retained template lock"),
            "template should retain its shared advisory lock"
        );

        drop(template);

        wait_until_advisory_lock_is_acquirable(&admin_url, lock_key).await;
    }

    {
        const LIVE_TEMPLATES: usize = 16;
        let shared_locks_before = advisory_lock_count(admin_url.clone(), "ShareLock")
            .await
            .expect("count shared locks before many-template capacity regression");
        let mut templates = Vec::with_capacity(LIVE_TEMPLATES);
        for index in 0..LIVE_TEMPLATES {
            let fingerprint = FingerprintBuilder::new("many-live-templates")
                .add("index", index.to_string())
                .finish();
            templates.push(
                harness
                    .template(TemplateSpec::new(fingerprint), |_| async { Ok(()) })
                    .await
                    .unwrap_or_else(|error| panic!("initialize live template {index}: {error}")),
            );
        }
        assert_eq!(
            advisory_lock_count(admin_url.clone(), "ShareLock")
                .await
                .expect("count shared locks with many live templates"),
            shared_locks_before + LIVE_TEMPLATES as i64,
            "every distinct live template retains one separate PostgreSQL session"
        );
        drop(templates);
        wait_until_advisory_lock_count(&admin_url, "ShareLock", shared_locks_before).await;
    }

    {
        let limited_harness = PostgresHarness::start(
            HarnessConfig::new("limits_it")
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_connection_budget(2)
                .unwrap()
                .with_connections_per_database(2)
                .unwrap()
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start connection-limited harness");
        let first = limited_harness
            .empty_database()
            .await
            .expect("acquire the full connection budget");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), limited_harness.empty_database())
                .await
                .is_err(),
            "a second lease should wait while the resolved budget is exhausted"
        );

        first
            .cleanup()
            .await
            .expect("release the full connection budget");
        let second = tokio::time::timeout(Duration::from_secs(5), limited_harness.empty_database())
            .await
            .expect("a released connection budget should become available")
            .expect("create a database after releasing the budget");
        second
            .cleanup()
            .await
            .expect("clean the connection-limit regression database");
    }

    {
        const PROJECT: &str = "prewarm_it";
        let prewarm_harness = PostgresHarness::start(
            HarnessConfig::new(PROJECT)
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_connection_budget(2)
                .unwrap()
                .with_connections_per_database(1)
                .unwrap()
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start prewarm regression harness");
        let fingerprint = FingerprintBuilder::new("prewarm-main").finish();
        let lock_key = advisory_key("template", &format!("{PROJECT}:{}", fingerprint.to_hex()));
        let template = prewarm_harness
            .template(TemplateSpec::new(fingerprint), |database_url| async move {
                execute(database_url, "CREATE TABLE base_marker (value integer)").await
            })
            .await
            .expect("initialize prewarm template");
        assert!(matches!(
            template.prewarm(0).await,
            Err(Error::InvalidPrewarmCapacity {
                capacity: 0,
                max_capacity: 2,
            })
        ));
        assert!(matches!(
            template.prewarm(3).await,
            Err(Error::InvalidPrewarmCapacity {
                capacity: 3,
                max_capacity: 2,
            })
        ));
        let pool = template
            .prewarm(2)
            .await
            .expect("fill two prewarmed database slots");
        assert_eq!(pool.capacity(), 2);
        let initial = pool.status();
        assert_eq!(initial.ready(), 2);
        assert_eq!(initial.leased(), 0);
        assert_eq!(initial.occupied_slots(), 2);

        // Idle prewarmed databases do not reserve application permits: the
        // same two-permit harness can still admit two ordinary databases.
        let direct_first = prewarm_harness
            .empty_database()
            .await
            .expect("lease first ordinary database beside idle prewarm slots");
        let direct_second =
            tokio::time::timeout(Duration::from_secs(5), prewarm_harness.empty_database())
                .await
                .expect("idle prewarm slots must not exhaust application admission")
                .expect("lease second ordinary database beside idle prewarm slots");
        direct_first.cleanup().await.expect("clean direct database");
        direct_second
            .cleanup()
            .await
            .expect("clean direct database");
        assert_eq!(pool.status().ready(), 2);

        let first = pool
            .database()
            .await
            .expect("lease first prewarmed database");
        let first_name = first.database_name().to_owned();
        let second = pool
            .database()
            .await
            .expect("lease second prewarmed database");
        let second_name = second.database_name().to_owned();
        assert_ne!(first_name, second_name);
        execute(
            first.database_url().to_owned(),
            "CREATE TABLE contaminated (value integer)",
        )
        .await
        .expect("dirty one prewarmed lease");

        let cancelled_pool = pool.clone();
        let cancelled_waiter = tokio::spawn(async move { cancelled_pool.database().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !cancelled_waiter.is_finished(),
            "an exhausted queue must wait"
        );
        cancelled_waiter.abort();
        assert!(cancelled_waiter.await.unwrap_err().is_cancelled());
        assert_eq!(pool.status().leased(), 2);
        assert_eq!(pool.status().ready(), 0);

        let waiting_pool = pool.clone();
        let waiting_lease = tokio::spawn(async move { waiting_pool.database().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !waiting_lease.is_finished(),
            "queue exhaustion must backpressure"
        );
        first
            .defer_cleanup()
            .await
            .expect("return dirty lease to background refill");
        prewarm_harness
            .drain_deferred_cleanup()
            .await
            .expect("drain dirty drop and distinct replacement creation");
        let replacement = tokio::time::timeout(Duration::from_secs(5), waiting_lease)
            .await
            .expect("a completed refill must wake one queue waiter")
            .expect("queue waiter must not panic")
            .expect("lease the freshly refilled database");
        assert_ne!(replacement.database_name(), first_name);
        assert_ne!(replacement.database_name(), second_name);
        assert!(
            !relation_exists(replacement.database_url().to_owned(), "contaminated")
                .await
                .expect("inspect replacement database"),
            "a dirty database must never be reset and reused"
        );
        assert!(
            relation_exists(replacement.database_url().to_owned(), "base_marker")
                .await
                .expect("inspect replacement template contents")
        );

        second
            .defer_cleanup()
            .await
            .expect("return second prewarmed lease");
        replacement
            .defer_cleanup()
            .await
            .expect("return replacement lease");
        prewarm_harness
            .drain_deferred_cleanup()
            .await
            .expect("refill both returned slots");
        let refilled = pool.status();
        assert_eq!(refilled.ready(), 2);
        assert_eq!(refilled.leased(), 0);
        assert_eq!(refilled.creating(), 0);
        assert_eq!(refilled.deleting(), 0);
        assert_eq!(refilled.occupied_slots(), 2);

        let other_template = prewarm_harness
            .template(
                TemplateSpec::new(FingerprintBuilder::new("prewarm-other").finish()),
                |database_url| async move {
                    execute(database_url, "CREATE TABLE other_marker (value integer)").await
                },
            )
            .await
            .expect("initialize a second prewarm template");
        let other_pool = other_template
            .prewarm(1)
            .await
            .expect("fill an independent template queue");
        let other = other_pool
            .database()
            .await
            .expect("lease from second template queue");
        assert!(
            relation_exists(other.database_url().to_owned(), "other_marker")
                .await
                .expect("inspect second-template lease")
        );
        assert!(
            !relation_exists(other.database_url().to_owned(), "base_marker")
                .await
                .expect("inspect second-template isolation")
        );
        other
            .defer_cleanup()
            .await
            .expect("return second-template lease");
        prewarm_harness
            .drain_deferred_cleanup()
            .await
            .expect("refill second-template queue");
        other_pool
            .shutdown()
            .await
            .expect("drop and drain second-template idle database");

        drop(template);
        assert!(
            !advisory_lock_is_acquirable(admin_url.clone(), lock_key)
                .await
                .expect("inspect template lock retained by prewarm pool"),
            "the pool must retain its source template shared lock"
        );
        pool.shutdown()
            .await
            .expect("drop and drain all idle prewarmed databases");
        assert_eq!(
            scalar_i64(
                admin_url.clone(),
                "SELECT count(*) FROM pg_database WHERE datname LIKE 'pgh_prewarm_it_test_%'",
            )
            .await
            .expect("count residual prewarm databases"),
            0
        );
        wait_until_advisory_lock_is_acquirable(&admin_url, lock_key).await;
    }

    {
        const PROJECT: &str = "prewarmfail";
        let failure_harness = PostgresHarness::start(
            HarnessConfig::new(PROJECT)
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_connection_budget(1)
                .unwrap()
                .with_connections_per_database(1)
                .unwrap()
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start prewarm refill-failure harness");
        let template = failure_harness
            .template(
                TemplateSpec::new(FingerprintBuilder::new("prewarm-failure").finish()),
                |_| async { Ok(()) },
            )
            .await
            .expect("initialize refill-failure template");
        let template_name = template.database_name().to_owned();
        let pool = template
            .prewarm(1)
            .await
            .expect("fill refill-failure queue");
        let lease = pool
            .database()
            .await
            .expect("lease the only refill-failure slot");
        execute(
            admin_url.clone(),
            format!("DROP DATABASE \"{template_name}\" WITH (FORCE)"),
        )
        .await
        .expect("remove source template to inject a refill failure");
        lease
            .defer_cleanup()
            .await
            .expect("queue dirty deletion and failing replacement");
        let error = failure_harness
            .drain_deferred_cleanup()
            .await
            .expect_err("the refill failure must reach the cleanup barrier");
        assert!(matches!(error, Error::DeferredCleanup { .. }));
        let failed = pool.status();
        assert!(failed.is_closed());
        assert_eq!(failed.ready(), 0);
        assert_eq!(failed.occupied_slots(), 0);
        pool.shutdown()
            .await
            .expect("a second barrier should find no hidden refill work");
    }

    {
        const PROJECT: &str = "admin_pool_it";
        const POOL_LIMIT: usize = 4;
        const OPERATIONS: usize = 8;
        let pooled_harness = PostgresHarness::start(
            HarnessConfig::new(PROJECT)
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_connection_budget(POOL_LIMIT)
                .unwrap()
                .with_connections_per_database(1)
                .unwrap()
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start bounded admin-pool harness");
        let catalog_lock = CatalogLock::acquire(admin_url.clone()).await;
        let mut lifecycles = tokio::task::JoinSet::new();
        for _ in 0..OPERATIONS {
            let pooled_harness = pooled_harness.clone();
            lifecycles.spawn(async move {
                let database = pooled_harness.empty_database().await?;
                database.cleanup().await
            });
        }

        wait_until_lifecycle_admin_session_count(&admin_url, PROJECT, POOL_LIMIT).await;
        assert_eq!(
            lifecycle_admin_pids(admin_url.clone(), PROJECT)
                .await
                .expect("inspect concurrent lifecycle admin sessions")
                .len(),
            POOL_LIMIT,
            "lifecycle SQL should progress concurrently up to the configured bound"
        );
        drop(catalog_lock);

        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(result) = lifecycles.join_next().await {
                result
                    .expect("bounded lifecycle task should not panic")
                    .expect("bounded lifecycle should create and clean its database");
            }
        })
        .await
        .expect("bounded lifecycle work should complete after catalog unlock");
        assert_eq!(
            lifecycle_admin_pids(admin_url.clone(), PROJECT)
                .await
                .expect("inspect the idle bounded admin pool")
                .len(),
            POOL_LIMIT,
            "the lazy pool must retain no more sessions than its bound"
        );
    }

    {
        let spec = TemplateSpec::new(
            FingerprintBuilder::new("integration-schema")
                .add(
                    "0001-create-marker",
                    "CREATE TABLE harness_marker (value text NOT NULL)",
                )
                .finish(),
        );
        let template = harness
            .template(spec, |database_url| async move {
                execute(
                    database_url,
                    "CREATE TABLE harness_marker (value text NOT NULL)",
                )
                .await
            })
            .await
            .expect("initialize template");
        let reused = harness
            .template(spec, |_| async move {
                Err(std::io::Error::other("ready template initializer must not run").into())
            })
            .await
            .expect("reuse ready template");
        assert_eq!(template.database_name(), reused.database_name());

        for iteration in 0..16 {
            let database = template.database().await.unwrap_or_else(|error| {
                panic!("create repeated disposable database {iteration}: {error}")
            });
            assert!(
                relation_exists(database.database_url().to_owned(), "harness_marker")
                    .await
                    .unwrap_or_else(|error| {
                        panic!("query repeated disposable database {iteration}: {error}")
                    })
            );
            database.cleanup().await.unwrap_or_else(|error| {
                panic!("clean repeated disposable database {iteration}: {error}")
            });
        }

        let first = template.database().await.expect("clone first database");
        let second = template.database().await.expect("clone second database");
        execute(
            first.database_url().to_owned(),
            "INSERT INTO harness_marker (value) VALUES ('first')",
        )
        .await
        .expect("insert isolated row");
        assert_eq!(
            scalar_i64(
                second.database_url().to_owned(),
                "SELECT count(*) FROM harness_marker",
            )
            .await
            .expect("count rows in second database"),
            0
        );

        let empty = harness
            .empty_database()
            .await
            .expect("create empty database");
        assert!(
            !relation_exists(empty.database_url().to_owned(), "harness_marker")
                .await
                .expect("check marker in empty database")
        );

        let first_name = first.database_name().to_owned();
        first
            .cleanup()
            .await
            .expect("explicitly clean first database");
        assert!(
            !database_exists(admin_url.clone(), first_name)
                .await
                .expect("check explicit cleanup")
        );

        let second_name = second.database_name().to_owned();
        drop(second);
        harness
            .drain_deferred_cleanup()
            .await
            .expect("drain Drop fallback cleanup");
        assert!(
            !database_exists(admin_url.clone(), second_name)
                .await
                .expect("check Drop fallback cleanup")
        );
        empty.cleanup().await.expect("clean empty database");
    }

    {
        let deferred = harness
            .empty_database()
            .await
            .expect("create explicitly deferred database");
        let deferred_name = deferred.database_name().to_owned();
        let held_application_client = HeldClient::connect(deferred.database_url().to_owned()).await;
        deferred
            .defer_cleanup()
            .await
            .expect("admit explicit deferred cleanup");
        harness
            .drain_deferred_cleanup()
            .await
            .expect("drain explicit deferred cleanup with an active client");
        assert!(
            !database_exists(admin_url.clone(), deferred_name.clone())
                .await
                .expect("check explicitly deferred cleanup")
        );
        drop(held_application_client);

        let recreated = harness
            .empty_database()
            .await
            .expect("create a fresh database after deferred cleanup");
        assert_ne!(
            recreated.database_name(),
            deferred_name,
            "a returned database must never be reused"
        );
        recreated.cleanup().await.expect("clean fresh database");
    }

    {
        let concurrent_spec = TemplateSpec::new(
            FingerprintBuilder::new("concurrent-schema")
                .add(
                    "0001-create-concurrent-marker",
                    "CREATE TABLE concurrent_marker (value text NOT NULL)",
                )
                .finish(),
        );
        const CONCURRENT_CALLERS: usize = 8;
        let shared_locks_before = advisory_lock_count(admin_url.clone(), "ShareLock")
            .await
            .expect("count shared advisory locks before cold start");
        let initialization_count = Arc::new(AtomicUsize::new(0));
        let mut calls = tokio::task::JoinSet::new();
        for _ in 0..CONCURRENT_CALLERS {
            let concurrent_harness = harness.clone();
            let initialization_count = initialization_count.clone();
            calls.spawn(async move {
                concurrent_harness
                    .template(concurrent_spec, move |database_url| async move {
                        initialization_count.fetch_add(1, Ordering::SeqCst);
                        execute(
                            database_url,
                            "CREATE TABLE concurrent_marker (value text NOT NULL)",
                        )
                        .await
                    })
                    .await
            });
        }
        let mut templates = Vec::with_capacity(CONCURRENT_CALLERS);
        let cold_start = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(result) = calls.join_next().await {
                templates.push(
                    result
                        .expect("concurrent template task should not panic")
                        .expect("initialize or reuse concurrent template"),
                );
            }
        })
        .await;
        if cold_start.is_err() {
            let activity = postgres_activity(admin_url.clone())
                .await
                .expect("inspect stalled cold-start coordination");
            panic!("N-way cold-start coordination stalled:\n{activity:#?}");
        }
        assert!(
            templates
                .iter()
                .all(|template| template.database_name() == templates[0].database_name())
        );
        assert_eq!(initialization_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            advisory_lock_count(admin_url.clone(), "ShareLock")
                .await
                .expect("count shared advisory locks after cold start"),
            shared_locks_before + 1,
            "same-server cold-start callers must retain one shared-lock session"
        );

        drop(templates);
        wait_until_advisory_lock_count(&admin_url, "ShareLock", shared_locks_before).await;
    }

    {
        let clone_fingerprint = FingerprintBuilder::new("template-clone-lock").finish();
        let clone_lock_key = advisory_key(
            "template",
            &format!("harness_it:{}", clone_fingerprint.to_hex()),
        );
        let template = harness
            .template(TemplateSpec::new(clone_fingerprint), |_| async { Ok(()) })
            .await
            .expect("initialize clone-held-lock template");
        let template_clone = template.clone();

        drop(template);
        assert!(
            !advisory_lock_is_acquirable(admin_url.clone(), clone_lock_key)
                .await
                .expect("check lock retained by template clone")
        );
        drop(template_clone);
        wait_until_advisory_lock_is_acquirable(&admin_url, clone_lock_key).await;

        let reacquired = tokio::time::timeout(
            Duration::from_secs(5),
            harness.template(TemplateSpec::new(clone_fingerprint), |_| async {
                Err(std::io::Error::other("ready template initializer must not rerun").into())
            }),
        )
        .await
        .expect("a stale weak cache entry must start a new acquisition flight")
        .expect("reacquire the catalog-ready template after its final handle drops");
        assert!(
            !advisory_lock_is_acquirable(admin_url.clone(), clone_lock_key)
                .await
                .expect("check lock retained by the reacquired template")
        );
        drop(reacquired);
        wait_until_advisory_lock_is_acquirable(&admin_url, clone_lock_key).await;
    }

    {
        let queued_fingerprint = FingerprintBuilder::new("queued-exclusive").finish();
        let queued_spec = TemplateSpec::new(queued_fingerprint);
        let queued_lock_key = advisory_key(
            "template",
            &format!("harness_it:{}", queued_fingerprint.to_hex()),
        );
        let shared_locks_before = advisory_lock_count(admin_url.clone(), "ShareLock")
            .await
            .expect("count shared advisory locks before warm-path regression");
        let peer_harness = PostgresHarness::start(
            HarnessConfig::new("harness_it")
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start distinct server cache for queued-exclusive regression");
        let first_template = harness
            .template(queued_spec, |_| async { Ok(()) })
            .await
            .expect("initialize queued-exclusive template");
        let mut exclusive = QueuedAdvisoryLock::queue(admin_url.clone(), queued_lock_key).await;
        let catalog_lock = CatalogLock::acquire(admin_url.clone()).await;
        let warm_template = tokio::time::timeout(
            Duration::from_secs(1),
            harness.template(queued_spec, |_| async {
                Err(std::io::Error::other("warm template initializer must not run").into())
            }),
        )
        .await
        .expect("warm cache path must not wait on PostgreSQL catalog or advisory locks")
        .expect("reuse live same-server template from cache");
        assert_eq!(
            advisory_lock_count(admin_url.clone(), "ShareLock")
                .await
                .expect("count shared advisory locks after warm acquisition"),
            shared_locks_before + 1,
            "warm acquisition must reuse the retained shared-lock session"
        );
        drop(catalog_lock);

        let waiting_harness = peer_harness.clone();
        let waiting_template = tokio::spawn(async move {
            waiting_harness
                .template(queued_spec, |_| async {
                    Err(std::io::Error::other(
                        "ready template initializer must not run behind queued exclusive lock",
                    )
                    .into())
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiting_template.is_finished(),
            "a distinct server's shared template acquisition must queue behind an exclusive waiter"
        );

        drop(first_template);
        drop(warm_template);
        exclusive.wait_until_acquired().await;
        assert!(
            !waiting_template.is_finished(),
            "a distinct server's shared template acquisition must wait while the exclusive lock is held"
        );
        exclusive.release();
        let waiting_template = tokio::time::timeout(Duration::from_secs(5), waiting_template)
            .await
            .expect("shared template acquisition should resume after exclusive unlock")
            .expect("queued shared template task should not panic")
            .expect("reuse template after queued exclusive lock");
        drop(waiting_template);
    }

    {
        let error_fingerprint = FingerprintBuilder::new("initializer-error-recovery").finish();
        let error_spec = TemplateSpec::new(error_fingerprint);
        let failing_harness = harness.clone();
        let (initializer_started_sender, initializer_started_receiver) =
            tokio::sync::oneshot::channel();
        let (fail_sender, fail_receiver) = tokio::sync::oneshot::channel();
        let failing_initialization = tokio::spawn(async move {
            failing_harness
                .template(error_spec, move |_| async move {
                    let _ = initializer_started_sender.send(());
                    let _ = fail_receiver.await;
                    Err(std::io::Error::other("forced initializer failure").into())
                })
                .await
        });
        initializer_started_receiver
            .await
            .expect("failing initializer should start");

        let waiting_harness = harness.clone();
        let (waiter_started_sender, waiter_started_receiver) = tokio::sync::oneshot::channel();
        let recovery_initializations = Arc::new(AtomicUsize::new(0));
        let waiter_initializations = recovery_initializations.clone();
        let waiting_recovery = tokio::spawn(async move {
            let _ = waiter_started_sender.send(());
            waiting_harness
                .template(error_spec, move |_| async move {
                    waiter_initializations.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
        });
        waiter_started_receiver
            .await
            .expect("recovery caller should reach the in-flight template");
        assert!(!waiting_recovery.is_finished());
        let _ = fail_sender.send(());

        let error = match failing_initialization
            .await
            .expect("failing template task should not panic")
        {
            Ok(_) => panic!("initializer failure should be reported"),
            Err(error) => error,
        };
        assert!(matches!(error, Error::TemplateInitializer { .. }));

        let recovered = tokio::time::timeout(Duration::from_secs(5), waiting_recovery)
            .await
            .expect("initializer error must wake its same-server waiter")
            .expect("recovery template task should not panic")
            .expect("recover after initializer failure");
        assert_eq!(recovery_initializations.load(Ordering::SeqCst), 1);
        drop(recovered);
    }

    {
        let cancellation_fingerprint =
            FingerprintBuilder::new("initializer-cancellation-recovery").finish();
        let cancellation_spec = TemplateSpec::new(cancellation_fingerprint);
        let cancellation_harness = harness.clone();
        let (initializer_started_sender, initializer_started_receiver) =
            tokio::sync::oneshot::channel();
        let initialization = tokio::spawn(async move {
            cancellation_harness
                .template(cancellation_spec, move |_| async move {
                    let _ = initializer_started_sender.send(());
                    std::future::pending::<()>().await;
                    Ok(())
                })
                .await
        });
        initializer_started_receiver
            .await
            .expect("initializer should start before cancellation");

        let waiting_harness = harness.clone();
        let (waiter_started_sender, waiter_started_receiver) = tokio::sync::oneshot::channel();
        let waiting_recovery = tokio::spawn(async move {
            let _ = waiter_started_sender.send(());
            waiting_harness
                .template(cancellation_spec, |_| async { Ok(()) })
                .await
        });
        waiter_started_receiver
            .await
            .expect("recovery caller should reach the cancellable flight");
        assert!(!waiting_recovery.is_finished());
        initialization.abort();
        let join_error = match initialization.await {
            Ok(_) => panic!("cancelled initialization should not return"),
            Err(error) => error,
        };
        assert!(join_error.is_cancelled());

        let recovered = tokio::time::timeout(Duration::from_secs(5), waiting_recovery)
            .await
            .expect("cancelled initializer must wake its same-server waiter")
            .expect("recovery template task should not panic")
            .expect("recover initializing template after cancellation");
        drop(recovered);
    }

    {
        let slow_harness = PostgresHarness::start(
            HarnessConfig::new("slow_it")
                .unwrap()
                .with_admin_database_url(harness.admin_database_url())
                .with_operation_timeout(Duration::from_secs(1))
                .unwrap()
                .with_template_wait_timeout(Duration::from_secs(4))
                .unwrap()
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start short-operation-timeout harness");
        let slow_peer_harness = PostgresHarness::start(
            HarnessConfig::new("slow_it")
                .unwrap()
                .with_admin_database_url(harness.admin_database_url())
                .with_operation_timeout(Duration::from_secs(1))
                .unwrap()
                .with_template_wait_timeout(Duration::from_secs(4))
                .unwrap()
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start distinct short-operation-timeout server cache");
        let slow_spec = TemplateSpec::new(FingerprintBuilder::new("slow-schema").finish());
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let slow_initializations = Arc::new(AtomicUsize::new(0));
        let first_barrier = barrier.clone();
        let first_count = slow_initializations.clone();
        let first_slow_harness = slow_harness.clone();
        let second_barrier = barrier.clone();
        let second_count = slow_initializations.clone();
        let second_slow_harness = slow_peer_harness.clone();
        let (first_slow, second_slow) = tokio::time::timeout(Duration::from_secs(8), async move {
            tokio::join!(
                first_slow_harness.template(slow_spec, move |database_url| async move {
                    first_count.fetch_add(1, Ordering::SeqCst);
                    first_barrier.wait().await;
                    tokio::time::sleep(Duration::from_millis(1_500)).await;
                    execute(database_url, "CREATE TABLE slow_marker (value integer)").await
                }),
                async move {
                    second_barrier.wait().await;
                    second_slow_harness
                        .template(slow_spec, move |_| {
                            let second_count = second_count.clone();
                            async move {
                                second_count.fetch_add(1, Ordering::SeqCst);
                                Err(std::io::Error::other(
                                    "waiting caller must reuse initialized template",
                                )
                                .into())
                            }
                        })
                        .await
                }
            )
        })
        .await
        .expect("template coordination should honor its separate wait timeout");
        let first_slow = first_slow.expect("initialize slow template");
        let second_slow = second_slow.expect("wait for and reuse slow template");
        assert_eq!(first_slow.database_name(), second_slow.database_name());
        assert_eq!(slow_initializations.load(Ordering::SeqCst), 1);
    }

    {
        let held_client = Arc::new(Mutex::new(None));
        let held_client_for_initializer = held_client.clone();
        let connection_spec =
            TemplateSpec::new(FingerprintBuilder::new("held-connection").finish());
        let connection_template = harness
            .template(connection_spec, move |database_url| async move {
                let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
                let (release_sender, release_receiver) = mpsc::channel();
                let worker = std::thread::spawn(move || {
                    let result = (|| -> Result<Client, postgres::Error> {
                        let mut client = Client::connect(&database_url, NoTls)?;
                        client
                            .batch_execute("CREATE TABLE held_connection_marker (value integer)")?;
                        Ok(client)
                    })();
                    match result {
                        Ok(client) => {
                            let _ = ready_sender.send(Ok(()));
                            let _ = release_receiver.recv();
                            drop(client);
                        }
                        Err(error) => {
                            let _ = ready_sender.send(Err(error.to_string()));
                        }
                    }
                });
                *held_client_for_initializer.lock().unwrap() = Some(HeldClient {
                    release: Some(release_sender),
                    worker: Some(worker),
                });
                ready_receiver
                    .await
                    .map_err(|_| std::io::Error::other("initializer client stopped before ready"))?
                    .map_err(std::io::Error::other)?;
                Ok(())
            })
            .await
            .expect("finalize template despite a lingering initializer connection");
        let connection_clone = connection_template
            .database()
            .await
            .expect("clone template after terminating lingering connections");
        assert!(
            relation_exists(
                connection_clone.database_url().to_owned(),
                "held_connection_marker"
            )
            .await
            .expect("query clone made from connection-safe template")
        );
        connection_clone.cleanup().await.unwrap();
    }

    {
        let unknown_fingerprint = FingerprintBuilder::new("unknown-template").finish();
        let unknown_name = format!(
            "pgh_harness_it_template_{}",
            &unknown_fingerprint.to_hex()[..24]
        );
        let unknown_database =
            TemporaryDatabase::create(admin_url.clone(), unknown_name.clone()).await;
        let error = match harness
            .template(TemplateSpec::new(unknown_fingerprint), |_| async { Ok(()) })
            .await
        {
            Ok(_) => panic!("unrecognized deterministic template must not be replaced"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Error::InconsistentMetadata { database_name } if database_name == unknown_name
        ));
        assert!(
            database_exists(admin_url.clone(), unknown_name)
                .await
                .expect("verify unrecognized template was preserved")
        );
        unknown_database.remove().await;
    }

    {
        let compensation_harness = PostgresHarness::start(
            HarnessConfig::new("tag_fail_it")
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_operation_timeout(Duration::from_secs(1))
                .unwrap()
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start metadata-compensation harness");
        let warm_database = compensation_harness
            .empty_database()
            .await
            .expect("warm the short-timeout lifecycle admin pool");
        warm_database
            .cleanup()
            .await
            .expect("return the short-timeout lifecycle admin session");
        let warm_session = lifecycle_admin_pids(admin_url.clone(), "tag_fail_it")
            .await
            .expect("inspect the warm short-timeout lifecycle admin pool");
        assert_eq!(warm_session.len(), 1);
        let catalog_lock = CatalogLock::acquire(admin_url.clone()).await;

        let creation_harness = compensation_harness.clone();
        let creation = tokio::spawn(async move { creation_harness.empty_database().await });
        let database_name =
            wait_until_database_with_prefix(&admin_url, "pgh_tag_fail_it_test_").await;
        let failed_session = lifecycle_admin_pids(admin_url.clone(), "tag_fail_it")
            .await
            .expect("inspect the session running the forced metadata failure");
        assert_eq!(
            failed_session, warm_session,
            "the forced timeout should run on the reused session"
        );
        wait_until_database_statement(&admin_url, &database_name, "DROP DATABASE").await;
        drop(catalog_lock);

        let error = creation
            .await
            .expect("metadata-compensation task should not panic")
            .expect_err("metadata write should time out");
        assert!(matches!(
            error,
            Error::Postgres {
                operation: "write disposable PostgreSQL database metadata",
                ..
            }
        ));
        assert!(
            !database_exists(admin_url.clone(), database_name)
                .await
                .expect("verify metadata compensation removed the database")
        );
        wait_until_lifecycle_admin_session_count(&admin_url, "tag_fail_it", 0).await;
        let recovered = compensation_harness
            .empty_database()
            .await
            .expect("replace the session after an uncertain operation failure");
        recovered
            .cleanup()
            .await
            .expect("clean a database through the replacement session");
        let replacement_session = lifecycle_admin_pids(admin_url.clone(), "tag_fail_it")
            .await
            .expect("inspect replacement after poisoned-session eviction");
        assert_eq!(replacement_session.len(), 1);
        assert_ne!(replacement_session, failed_session);
    }

    {
        let cancellation_harness = PostgresHarness::start(
            HarnessConfig::new("cancel_lease_it")
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start lease-cancellation harness");
        let catalog_lock = CatalogLock::acquire(admin_url.clone()).await;
        let creation_harness = cancellation_harness.clone();
        let creation = tokio::spawn(async move { creation_harness.empty_database().await });
        let database_name =
            wait_until_database_with_prefix(&admin_url, "pgh_cancel_lease_it_test_").await;

        creation.abort();
        let join_error = creation
            .await
            .expect_err("cancelled lease creation should not return a result");
        assert!(join_error.is_cancelled());
        drop(catalog_lock);
        wait_until_database_is_absent(&admin_url, &database_name).await;
    }

    {
        let failure_harness = PostgresHarness::start(
            HarnessConfig::new("cleanup_fail_it")
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_operation_timeout(Duration::from_millis(100))
                .unwrap()
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start deferred-cleanup failure harness");

        let awaited_database = failure_harness
            .empty_database()
            .await
            .expect("create awaited-cleanup failure database");
        let awaited_database_name = awaited_database.database_name().to_owned();
        let database = failure_harness
            .empty_database()
            .await
            .expect("create deferred-cleanup failure database before admission closes");
        let database_name = database.database_name().to_owned();
        let catalog_lock = CatalogLock::acquire(admin_url.clone()).await;
        let error = awaited_database
            .cleanup()
            .await
            .expect_err("the catalog lock should make awaited DROP time out");
        assert!(matches!(
            error,
            Error::Postgres {
                operation: "drop disposable PostgreSQL database",
                ..
            }
        ));
        failure_harness
            .drain_deferred_cleanup()
            .await
            .expect("a delivered awaited failure must not be repeated by drain");
        assert!(matches!(
            failure_harness.empty_database().await,
            Err(Error::ConnectionBudgetClosed)
        ));
        drop(catalog_lock);
        execute(
            admin_url.clone(),
            format!("DROP DATABASE IF EXISTS \"{awaited_database_name}\" WITH (FORCE)"),
        )
        .await
        .expect("remove injected awaited-cleanup residual");

        let catalog_lock = CatalogLock::acquire(admin_url.clone()).await;
        database
            .defer_cleanup()
            .await
            .expect("admit cleanup before its injected PostgreSQL failure");
        // The held catalog lock is the synchronization condition: the drain
        // cannot complete successfully before PostgreSQL's statement timeout.
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            failure_harness.drain_deferred_cleanup(),
        )
        .await
        .expect("deferred failure drain should remain bounded")
        .expect_err("the catalog lock should make deferred DROP time out");
        let Error::DeferredCleanup {
            failure_count,
            failures,
        } = error
        else {
            panic!("unexpected deferred failure: {error:?}");
        };
        assert_eq!(failure_count, 1);
        assert_eq!(failures[0].database_name(), database_name);
        assert!(matches!(
            failures[0].source_error(),
            Error::Postgres {
                operation: "drop disposable PostgreSQL database",
                ..
            }
        ));
        failure_harness
            .drain_deferred_cleanup()
            .await
            .expect("a deferred failure is reported exactly once");
        drop(catalog_lock);
        execute(
            admin_url.clone(),
            format!("DROP DATABASE IF EXISTS \"{database_name}\" WITH (FORCE)"),
        )
        .await
        .expect("remove injected deferred-cleanup residual");
    }

    {
        let cancellation_harness = PostgresHarness::start(
            HarnessConfig::new("cancel_clean_it")
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start cleanup-cancellation harness");
        let database = cancellation_harness
            .empty_database()
            .await
            .expect("create cleanup-cancellation database");
        let database_name = database.database_name().to_owned();
        let catalog_lock = CatalogLock::acquire(admin_url.clone()).await;
        let cleanup = tokio::spawn(async move { database.cleanup().await });
        wait_until_database_statement(&admin_url, &database_name, "DROP DATABASE").await;
        cleanup.abort();
        assert!(
            cleanup
                .await
                .expect_err("cancelled awaited cleanup should not return")
                .is_cancelled()
        );
        drop(catalog_lock);
        tokio::time::timeout(
            Duration::from_secs(5),
            cancellation_harness.drain_deferred_cleanup(),
        )
        .await
        .expect("cancelled awaited cleanup should finish in its worker")
        .expect("successful cancelled cleanup should not report a failure");
        assert!(
            !database_exists(admin_url.clone(), database_name)
                .await
                .expect("check cancelled awaited cleanup")
        );
    }

    {
        let external = PostgresHarness::start(
            HarnessConfig::new("external_it")
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_connection_budget(1_000)
                .unwrap()
                .with_connections_per_database(100)
                .unwrap()
                .with_image("does-not-exist.invalid/postgres:18")
                .unwrap()
                .with_owned_container_profile(
                    OwnedContainerProfile::default()
                        .with_tmpfs_size_bytes(1)
                        .unwrap(),
                )
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start external-server harness");
        assert!(external.is_external());
        let limits = external.connection_limits();
        assert_eq!(limits.connection_budget(), 1_000);
        assert_eq!(limits.connections_per_database(), 100);
        assert_eq!(limits.max_simultaneous_leases(), 10);
        external
            .shutdown()
            .await
            .expect("external shutdown should be a no-op");
        let external_database = external
            .empty_database()
            .await
            .expect("create external-mode database after no-op shutdown");
        external_database
            .cleanup()
            .await
            .expect("clean external-mode database");
        let lifetime_database = external
            .empty_database()
            .await
            .expect("create external lifetime database");
        let lifetime_database_name = lifetime_database.database_name().to_owned();
        lifetime_database
            .defer_cleanup()
            .await
            .expect("defer external lifetime cleanup");
        drop(external);
        wait_until_database_is_absent(&admin_url, &lifetime_database_name).await;
    }

    {
        let shutdown_harness = PostgresHarness::start(
            HarnessConfig::new("shutdown_it")
                .unwrap()
                .with_connection_budget(1)
                .unwrap()
                .with_connections_per_database(1)
                .unwrap()
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start owned-shutdown harness");
        let shutdown_template = shutdown_harness
            .template(
                TemplateSpec::new(FingerprintBuilder::new("shutdown-prewarm").finish()),
                |_| async { Ok(()) },
            )
            .await
            .expect("initialize owned-shutdown prewarm template");
        let shutdown_pool = shutdown_template
            .prewarm(1)
            .await
            .expect("fill owned-shutdown prewarm queue");
        shutdown_harness
            .empty_database()
            .await
            .expect("create cleanup accepted before owned shutdown")
            .defer_cleanup()
            .await
            .expect("queue cleanup before owned shutdown");
        let active_database = shutdown_pool
            .database()
            .await
            .expect("saturate owned-shutdown admission");
        let waiting_harness = shutdown_harness.clone();
        let waiter = tokio::spawn(async move { waiting_harness.empty_database().await });
        let waiting_pool = shutdown_pool.clone();
        let pool_waiter = tokio::spawn(async move { waiting_pool.database().await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let concurrent_harness = shutdown_harness.clone();
        let (first_shutdown, second_shutdown) =
            tokio::join!(shutdown_harness.shutdown(), concurrent_harness.shutdown());
        first_shutdown.expect("first owned shutdown");
        second_shutdown.expect("concurrent owned shutdown");
        let waiter_error = match tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("owned shutdown should wake queued admission")
            .expect("queued database task should not panic")
        {
            Ok(_) => panic!("queued database admission must not outlive owned shutdown"),
            Err(error) => error,
        };
        assert!(matches!(waiter_error, Error::ConnectionBudgetClosed));
        let pool_waiter_error = match tokio::time::timeout(Duration::from_secs(1), pool_waiter)
            .await
            .expect("owned shutdown should wake an exhausted prewarm queue")
            .expect("queued prewarm task should not panic")
        {
            Ok(_) => panic!("prewarm admission must not outlive owned shutdown"),
            Err(error) => error,
        };
        assert!(matches!(pool_waiter_error, Error::PrewarmPoolClosed));
        assert!(shutdown_pool.status().is_closed());
        shutdown_harness
            .shutdown()
            .await
            .expect("repeated owned shutdown");
        assert!(matches!(
            active_database.cleanup().await,
            Err(Error::CleanupQueueClosed)
        ));
    }

    harness
        .shutdown()
        .await
        .expect("remove owned PostgreSQL container");
    wait_until_container_is_absent(&container_id).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn process_exit_removes_an_owned_container_with_a_live_lease() {
    const CHILD_ENV: &str = "PTH_PROCESS_TEARDOWN_CHILD";
    const MARKER_ENV: &str = "PTH_PROCESS_TEARDOWN_MARKER";
    if std::env::var_os(CHILD_ENV).is_some() {
        let marker = std::env::var_os(MARKER_ENV).expect("child marker path is configured");
        let harness = PostgresHarness::start(
            HarnessConfig::new("exit_clean_it")
                .unwrap()
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start process-teardown harness");
        let container_id = harness
            .container_id()
            .expect("process-teardown harness owns a container");
        fs::write(&marker, container_id).expect("publish process-teardown container ID");
        let _live_database = harness
            .empty_database()
            .await
            .expect("create a live lease before process exit");
        std::process::exit(0);
    }

    let marker = std::env::temp_dir().join(format!(
        "postgres-test-harness-process-teardown-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let output = Command::new(std::env::current_exe().expect("resolve integration-test binary"))
        .args([
            "--ignored",
            "--exact",
            "process_exit_removes_an_owned_container_with_a_live_lease",
            "--nocapture",
        ])
        .env(CHILD_ENV, "1")
        .env(MARKER_ENV, &marker)
        .output()
        .expect("run process-teardown child");
    assert!(
        output.status.success(),
        "process-teardown child failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let container_id = fs::read_to_string(&marker).expect("read process-teardown container ID");
    let _ = fs::remove_file(&marker);
    wait_until_container_is_absent(container_id.trim()).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon and cached postgres:18 image"]
async fn official_compatible_custom_image_supports_default_and_compatibility_profiles() {
    let custom_image = format!("pth-perf06-custom:{}", uuid::Uuid::new_v4().simple());
    docker_command(&["image", "tag", "postgres:18", &custom_image]);
    let _image = TemporaryImageTag(custom_image.clone());

    let fast_harness = PostgresHarness::start(
        HarnessConfig::new("custom_fast_it")
            .unwrap()
            .with_image(custom_image.clone())
            .unwrap()
            .with_cleanup_on_start(false),
    )
    .await
    .expect("start an official-compatible custom image with the default profile");
    let fast_container_id = fast_harness.container_id().unwrap().to_owned();
    assert_default_owned_profile(&fast_container_id);
    fast_harness
        .shutdown()
        .await
        .expect("remove default-profile custom-image container");
    wait_until_container_is_absent(&fast_container_id).await;

    let profile = OwnedContainerProfile::default()
        .with_initdb_no_sync(false)
        .without_tmpfs();

    let harness = PostgresHarness::start(
        HarnessConfig::new("custom_it")
            .unwrap()
            .with_image(custom_image)
            .unwrap()
            .with_owned_container_profile(profile)
            .with_cleanup_on_start(false),
    )
    .await
    .expect("start an official-compatible custom image without storage extensions");
    let container_id = harness.container_id().unwrap().to_owned();
    let environment = docker_inspect_json(&container_id, "{{json .Config.Env}}");
    assert!(
        environment
            .as_array()
            .expect("container environment array")
            .iter()
            .all(|entry| !entry
                .as_str()
                .unwrap_or_default()
                .starts_with("POSTGRES_INITDB_ARGS="))
    );
    let mounts = docker_inspect_json(&container_id, "{{json .Mounts}}");
    assert!(
        mounts
            .as_array()
            .expect("container mounts array")
            .iter()
            .all(|mount| mount["Type"] != "tmpfs")
    );
    assert_eq!(
        scalar_i64(harness.admin_database_url().to_owned(), "SELECT 18::bigint")
            .await
            .expect("query compatible custom image"),
        18
    );

    harness
        .shutdown()
        .await
        .expect("remove custom-image container");
    wait_until_container_is_absent(&container_id).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon and cached postgres:18 image"]
async fn too_small_tmpfs_reports_storage_exhaustion_and_removes_container() {
    const PROJECT: &str = "tinyfs_it";
    let profile = OwnedContainerProfile::default()
        .with_tmpfs_size_bytes(1024 * 1024)
        .unwrap();
    let error = match PostgresHarness::start(
        HarnessConfig::new(PROJECT)
            .unwrap()
            .with_owned_container_profile(profile)
            .with_startup_timeout(Duration::from_secs(10))
            .unwrap()
            .with_cleanup_on_start(false),
    )
    .await
    {
        Ok(_) => panic!("a one-MiB PostgreSQL tmpfs must fail initialization"),
        Err(error) => error,
    };

    assert!(
        matches!(error, Error::ContainerStorageExhausted { .. }),
        "unexpected tiny-tmpfs startup error: {error:?}"
    );
    assert!(error.to_string().contains("no space left on device"));
    wait_until_project_containers_are_absent(PROJECT).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon and cached postgres:18 image"]
async fn startup_timeout_eventually_removes_a_partially_started_container() {
    const PROJECT: &str = "timeout_it";
    let error = match PostgresHarness::start(
        HarnessConfig::new(PROJECT)
            .unwrap()
            .with_startup_timeout(Duration::from_millis(1))
            .unwrap()
            .with_cleanup_on_start(false),
    )
    .await
    {
        Ok(_) => panic!("one millisecond must not be enough for owned startup"),
        Err(error) => error,
    };

    assert!(
        matches!(
            error,
            Error::ContainerStartupTimeout { .. }
                | Error::ContainerReadinessTimeout { .. }
                | Error::ContainerExitedBeforeReady { .. }
        ),
        "unexpected startup-timeout error: {error:?}"
    );
    wait_until_project_containers_are_absent(PROJECT).await;
}

struct TemporaryImageTag(String);

impl Drop for TemporaryImageTag {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["image", "rm", &self.0])
            .output();
    }
}

fn assert_default_owned_profile(container_id: &str) {
    let environment = docker_inspect_json(container_id, "{{json .Config.Env}}");
    let environment = environment.as_array().expect("container environment array");
    assert!(
        environment
            .iter()
            .any(|entry| entry == "POSTGRES_INITDB_ARGS=--no-sync")
    );
    assert!(
        environment.iter().all(|entry| {
            !entry
                .as_str()
                .unwrap_or_default()
                .starts_with("POSTGRES_HOST_AUTH_METHOD=")
        }),
        "owned containers must not enable trust authentication"
    );

    let mounts = docker_inspect_json(container_id, "{{json .HostConfig.Mounts}}");
    let mount = mounts
        .as_array()
        .expect("container host mounts array")
        .iter()
        .find(|mount| mount["Target"] == "/var/lib/postgresql")
        .expect("PostgreSQL storage tmpfs mount");
    assert_eq!(mount["Type"], "tmpfs");
    assert_eq!(
        mount["TmpfsOptions"]["SizeBytes"],
        DEFAULT_OWNED_CONTAINER_TMPFS_SIZE_BYTES
    );
    assert_eq!(mount["TmpfsOptions"]["Mode"], 0o1777);
}

fn docker_inspect_json(container_id: &str, format: &str) -> Value {
    serde_json::from_str(&docker_command(&[
        "container",
        "inspect",
        "--format",
        format,
        container_id,
    ]))
    .expect("parse Docker inspection JSON")
}

fn docker_command(arguments: &[&str]) -> String {
    let output = Command::new("docker")
        .args(arguments)
        .output()
        .unwrap_or_else(|error| panic!("run `docker {}`: {error}", arguments.join(" ")));
    assert!(
        output.status.success(),
        "`docker {}` failed: {}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("Docker output is UTF-8")
        .trim()
        .to_owned()
}

async fn wait_until_container_is_absent(container_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let exists = Command::new("docker")
            .args(["container", "inspect", container_id])
            .output()
            .expect("inspect Docker container")
            .status
            .success();
        if !exists {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "owned container '{container_id}' was not removed"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_until_project_containers_are_absent(project: &str) {
    let label = format!("org.postgres-test-harness.project={project}");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut absent_since = None;
    loop {
        let containers = docker_command(&["ps", "-aq", "--filter", &format!("label={label}")]);
        if containers.is_empty() {
            let first_absent = absent_since.get_or_insert_with(Instant::now);
            if first_absent.elapsed() >= Duration::from_millis(250) {
                return;
            }
        } else {
            absent_since = None;
        }
        assert!(
            Instant::now() < deadline,
            "owned containers for project '{project}' were not removed: {containers}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn execute(database_url: String, sql: impl Into<String>) -> Result<(), BoxError> {
    let sql = sql.into();
    with_client(database_url, move |client| {
        client.batch_execute(&sql)?;
        Ok(())
    })
    .await
}

struct HeldClient {
    release: Option<Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl HeldClient {
    async fn connect(database_url: String) -> Self {
        let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || match Client::connect(&database_url, NoTls) {
            Ok(client) => {
                let _ = ready_sender.send(Ok(()));
                let _ = release_receiver.recv();
                drop(client);
            }
            Err(error) => {
                let _ = ready_sender.send(Err(error.to_string()));
            }
        });
        ready_receiver
            .await
            .expect("held application client stopped before reporting readiness")
            .expect("connect held application client");
        Self {
            release: Some(release_sender),
            worker: Some(worker),
        }
    }
}

struct CatalogLock {
    release: Option<Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

struct QueuedAdvisoryLock {
    acquired: Option<tokio::sync::oneshot::Receiver<Result<(), String>>>,
    release: Option<Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl QueuedAdvisoryLock {
    async fn queue(admin_url: String, key: i64) -> Self {
        let (pid_sender, pid_receiver) = tokio::sync::oneshot::channel();
        let (acquired_sender, acquired_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let query_url = admin_url.clone();
        let worker = std::thread::spawn(move || {
            let mut client = match Client::connect(&query_url, NoTls) {
                Ok(client) => client,
                Err(error) => {
                    let message = error.to_string();
                    let _ = pid_sender.send(Err(message.clone()));
                    let _ = acquired_sender.send(Err(message));
                    return;
                }
            };
            let pid: i32 = match client.query_one("SELECT pg_backend_pid()", &[]) {
                Ok(row) => row.get(0),
                Err(error) => {
                    let message = error.to_string();
                    let _ = pid_sender.send(Err(message.clone()));
                    let _ = acquired_sender.send(Err(message));
                    return;
                }
            };
            let _ = pid_sender.send(Ok(pid));
            match client.query_one("SELECT pg_advisory_lock($1)", &[&key]) {
                Ok(_) => {
                    let _ = acquired_sender.send(Ok(()));
                    let _ = release_receiver.recv();
                    let _ = client.query_one("SELECT pg_advisory_unlock($1)", &[&key]);
                }
                Err(error) => {
                    let _ = acquired_sender.send(Err(error.to_string()));
                }
            }
        });
        let pid = pid_receiver
            .await
            .expect("queued advisory-lock worker stopped before reporting its PID")
            .expect("connect queued advisory-lock worker");
        wait_until_advisory_waiter(&admin_url, pid).await;
        Self {
            acquired: Some(acquired_receiver),
            release: Some(release_sender),
            worker: Some(worker),
        }
    }

    async fn wait_until_acquired(&mut self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.acquired
                .take()
                .expect("queued advisory lock is awaited once"),
        )
        .await
        .expect("queued exclusive advisory lock should become available")
        .expect("queued advisory-lock worker stopped before acquisition")
        .expect("acquire queued exclusive advisory lock");
    }

    fn release(mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}

impl Drop for QueuedAdvisoryLock {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl CatalogLock {
    async fn acquire(admin_url: String) -> Self {
        let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = (|| -> Result<Client, postgres::Error> {
                let mut client = Client::connect(&admin_url, NoTls)?;
                client.batch_execute(
                    "BEGIN; \
                     LOCK TABLE pg_catalog.pg_shdescription IN ACCESS EXCLUSIVE MODE",
                )?;
                Ok(client)
            })();
            match result {
                Ok(mut client) => {
                    let _ = ready_sender.send(Ok(()));
                    let _ = release_receiver.recv();
                    let _ = client.batch_execute("ROLLBACK");
                }
                Err(error) => {
                    let _ = ready_sender.send(Err(error.to_string()));
                }
            }
        });
        ready_receiver
            .await
            .expect("catalog-lock worker stopped before ready")
            .expect("lock shared-description catalog");
        Self {
            release: Some(release_sender),
            worker: Some(worker),
        }
    }
}

impl Drop for CatalogLock {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for HeldClient {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct TemporaryDatabase {
    admin_url: String,
    name: String,
    armed: bool,
}

impl TemporaryDatabase {
    async fn create(admin_url: String, name: String) -> Self {
        execute(admin_url.clone(), format!("CREATE DATABASE \"{name}\""))
            .await
            .expect("create temporary lookalike database");
        Self {
            admin_url,
            name,
            armed: true,
        }
    }

    async fn remove(mut self) {
        execute(
            self.admin_url.clone(),
            format!("DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)", self.name),
        )
        .await
        .expect("remove temporary lookalike database");
        self.armed = false;
    }
}

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let admin_url = self.admin_url.clone();
        let name = self.name.clone();
        let _ = std::thread::spawn(move || {
            if let Ok(mut client) = Client::connect(&admin_url, NoTls) {
                let _ = client
                    .batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"));
            }
        })
        .join();
    }
}

async fn scalar_i64(database_url: String, sql: &'static str) -> Result<i64, BoxError> {
    with_client(database_url, move |client| {
        Ok(client.query_one(sql, &[])?.get(0))
    })
    .await
}

async fn scalar_string(database_url: String, sql: &'static str) -> Result<String, BoxError> {
    with_client(database_url, move |client| {
        Ok(client.query_one(sql, &[])?.get(0))
    })
    .await
}

async fn relation_exists(database_url: String, relation: &'static str) -> Result<bool, BoxError> {
    with_client(database_url, move |client| {
        let relation: Option<String> = client
            .query_one("SELECT to_regclass($1)::text", &[&relation])?
            .get(0);
        Ok(relation.is_some())
    })
    .await
}

async fn database_exists(database_url: String, database_name: String) -> Result<bool, BoxError> {
    with_client(database_url, move |client| {
        Ok(client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
                &[&database_name],
            )?
            .get(0))
    })
    .await
}

async fn lifecycle_admin_pids(database_url: String, project: &str) -> Result<Vec<i32>, BoxError> {
    let application_name = format!("postgres-test-harness lifecycle:{project}");
    with_client(database_url, move |client| {
        Ok(client
            .query(
                "SELECT pid FROM pg_stat_activity \
                 WHERE datname = current_database() AND application_name = $1 \
                 ORDER BY pid",
                &[&application_name],
            )?
            .into_iter()
            .map(|row| row.get(0))
            .collect())
    })
    .await
}

async fn terminate_backend(database_url: String, pid: i32) -> Result<bool, BoxError> {
    with_client(database_url, move |client| {
        Ok(client
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])?
            .get(0))
    })
    .await
}

async fn postgres_activity(database_url: String) -> Result<Vec<String>, BoxError> {
    with_client(database_url, move |client| {
        Ok(client
            .query(
                "SELECT a.pid, a.state, coalesce(a.wait_event_type, ''), \
                        coalesce(a.wait_event, ''), a.query, \
                        coalesce(string_agg(l.mode || ':' || l.granted::text, ','), '') \
                 FROM pg_stat_activity a \
                 LEFT JOIN pg_locks l ON l.pid = a.pid AND l.locktype = 'advisory' \
                 WHERE a.datname = current_database() AND a.pid <> pg_backend_pid() \
                 GROUP BY a.pid, a.state, a.wait_event_type, a.wait_event, a.query \
                 ORDER BY a.pid",
                &[],
            )?
            .into_iter()
            .map(|row| {
                format!(
                    "pid={} state={} wait={}/{} locks={} query={}",
                    row.get::<_, i32>(0),
                    row.get::<_, String>(1),
                    row.get::<_, String>(2),
                    row.get::<_, String>(3),
                    row.get::<_, String>(5),
                    row.get::<_, String>(4),
                )
            })
            .collect())
    })
    .await
}

fn advisory_key(domain: &str, identity: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(b"postgres-test-harness-advisory-v1");
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain.as_bytes());
    hasher.update((identity.len() as u64).to_be_bytes());
    hasher.update(identity.as_bytes());
    let digest = hasher.finalize();
    i64::from_be_bytes(digest[..8].try_into().unwrap())
}

async fn advisory_lock_is_acquirable(database_url: String, key: i64) -> Result<bool, BoxError> {
    with_client(database_url, move |client| {
        let acquired: bool = client
            .query_one("SELECT pg_try_advisory_lock($1)", &[&key])?
            .get(0);
        if acquired {
            client.query_one("SELECT pg_advisory_unlock($1)", &[&key])?;
        }
        Ok(acquired)
    })
    .await
}

async fn advisory_lock_count(database_url: String, mode: &'static str) -> Result<i64, BoxError> {
    with_client(database_url, move |client| {
        Ok(client
            .query_one(
                "SELECT count(*) FROM pg_locks \
                 WHERE locktype = 'advisory' AND mode = $1 AND granted",
                &[&mode],
            )?
            .get(0))
    })
    .await
}

async fn with_client<T, F>(database_url: String, operation: F) -> Result<T, BoxError>
where
    T: Send + 'static,
    F: FnOnce(&mut Client) -> Result<T, postgres::Error> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut client = Client::connect(&database_url, NoTls)?;
        operation(&mut client)
    })
    .await
    .map_err(|source| -> BoxError { Box::new(source) })?
    .map_err(|source| -> BoxError { Box::new(source) })
}

async fn wait_until_database_is_absent(admin_url: &str, database_name: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if !database_exists(admin_url.to_owned(), database_name.to_owned())
            .await
            .expect("poll fallback cleanup")
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "fallback cleanup did not drop database '{database_name}'"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_until_lifecycle_admin_session_count(admin_url: &str, project: &str, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let count = lifecycle_admin_pids(admin_url.to_owned(), project)
            .await
            .expect("poll lifecycle admin sessions")
            .len();
        if count == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected} lifecycle admin sessions for '{project}', observed {count}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until_advisory_lock_is_acquirable(admin_url: &str, key: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if advisory_lock_is_acquirable(admin_url.to_owned(), key)
            .await
            .expect("poll retained advisory lock")
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "retained advisory lock was not released after client drop"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until_advisory_lock_count(admin_url: &str, mode: &'static str, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let count = advisory_lock_count(admin_url.to_owned(), mode)
            .await
            .expect("poll advisory lock count");
        if count == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected} granted {mode} advisory locks, observed {count}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until_advisory_waiter(admin_url: &str, pid: i32) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let waiting = with_client(admin_url.to_owned(), move |client| {
            Ok(client
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks \
                     WHERE pid = $1 AND locktype = 'advisory' AND NOT granted)",
                    &[&pid],
                )?
                .get::<_, bool>(0))
        })
        .await
        .expect("poll queued advisory lock");
        if waiting {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "backend {pid} did not queue for its advisory lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until_database_with_prefix(admin_url: &str, prefix: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let query_prefix = prefix.to_owned();
        let database_name = with_client(admin_url.to_owned(), move |client| {
            Ok(client
                .query("SELECT datname FROM pg_database", &[])?
                .into_iter()
                .map(|row| row.get::<_, String>(0))
                .find(|name| name.starts_with(&query_prefix)))
        })
        .await
        .expect("poll managed database creation");
        if let Some(database_name) = database_name {
            return database_name;
        }
        assert!(
            Instant::now() < deadline,
            "managed database with prefix '{prefix}' was not created"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until_database_statement(
    admin_url: &str,
    database_name: &str,
    statement_prefix: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let query_database_name = database_name.to_owned();
        let query_statement_prefix = statement_prefix.to_owned();
        let statement_is_running = with_client(admin_url.to_owned(), move |client| {
            Ok(client
                .query("SELECT query FROM pg_stat_activity", &[])?
                .into_iter()
                .map(|row| row.get::<_, String>(0))
                .any(|query| {
                    query.starts_with(&query_statement_prefix)
                        && query.contains(&query_database_name)
                }))
        })
        .await
        .expect("poll managed database statement");
        if statement_is_running {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "'{statement_prefix}' did not run for database '{database_name}'"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
