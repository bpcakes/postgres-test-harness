use std::{
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
    BoxError, Error, FingerprintBuilder, HarnessConfig, PostgresHarness, TemplateSpec,
    cleanup_stale_databases,
};
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
    assert!(harness.container_id().is_some());
    let admin_url = harness.admin_database_url().to_owned();

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
        wait_until_database_is_absent(&admin_url, &second_name).await;
        empty.cleanup().await.expect("clean empty database");
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
        let initialization_count = Arc::new(AtomicUsize::new(0));
        let first_harness = harness.clone();
        let first_count = initialization_count.clone();
        let second_harness = harness.clone();
        let second_count = initialization_count.clone();
        let (first_template, second_template) = tokio::join!(
            first_harness.template(concurrent_spec, move |database_url| {
                let initialization_count = first_count.clone();
                async move {
                    initialization_count.fetch_add(1, Ordering::SeqCst);
                    execute(
                        database_url,
                        "CREATE TABLE concurrent_marker (value text NOT NULL)",
                    )
                    .await
                }
            }),
            second_harness.template(concurrent_spec, move |database_url| {
                let initialization_count = second_count.clone();
                async move {
                    initialization_count.fetch_add(1, Ordering::SeqCst);
                    execute(
                        database_url,
                        "CREATE TABLE concurrent_marker (value text NOT NULL)",
                    )
                    .await
                }
            })
        );
        let first_template = first_template.expect("initialize concurrent template");
        let second_template = second_template.expect("reuse concurrent template");
        assert_eq!(
            first_template.database_name(),
            second_template.database_name()
        );
        assert_eq!(initialization_count.load(Ordering::SeqCst), 1);
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
        let slow_spec = TemplateSpec::new(FingerprintBuilder::new("slow-schema").finish());
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let slow_initializations = Arc::new(AtomicUsize::new(0));
        let first_barrier = barrier.clone();
        let first_count = slow_initializations.clone();
        let first_slow_harness = slow_harness.clone();
        let second_barrier = barrier.clone();
        let second_count = slow_initializations.clone();
        let second_slow_harness = slow_harness.clone();
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
        let catalog_lock = CatalogLock::acquire(admin_url.clone()).await;

        let creation_harness = compensation_harness.clone();
        let creation = tokio::spawn(async move { creation_harness.empty_database().await });
        let database_name =
            wait_until_database_with_prefix(&admin_url, "pgh_tag_fail_it_test_").await;
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
        let external = PostgresHarness::start(
            HarnessConfig::new("external_it")
                .unwrap()
                .with_admin_database_url(admin_url.clone())
                .with_cleanup_on_start(false),
        )
        .await
        .expect("start external-server harness");
        assert!(external.is_external());
        let external_database = external
            .empty_database()
            .await
            .expect("create external-mode database");
        external_database
            .cleanup()
            .await
            .expect("clean external-mode database");
    }

    harness
        .shutdown()
        .await
        .expect("remove owned PostgreSQL container");
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

struct CatalogLock {
    release: Option<Sender<()>>,
    worker: Option<JoinHandle<()>>,
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
