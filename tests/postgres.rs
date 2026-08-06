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

    let held_client = Arc::new(Mutex::new(None));
    let held_client_for_initializer = held_client.clone();
    let connection_spec = TemplateSpec::new(FingerprintBuilder::new("held-connection").finish());
    let connection_template = harness
        .template(connection_spec, move |database_url| async move {
            let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
            let (release_sender, release_receiver) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                let result = (|| -> Result<Client, postgres::Error> {
                    let mut client = Client::connect(&database_url, NoTls)?;
                    client.batch_execute("CREATE TABLE held_connection_marker (value integer)")?;
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
    drop(held_client.lock().unwrap().take());

    let unknown_fingerprint = FingerprintBuilder::new("unknown-template").finish();
    let unknown_name = format!(
        "pgh_harness_it_template_{}",
        &unknown_fingerprint.to_hex()[..24]
    );
    let unknown_database = TemporaryDatabase::create(
        harness.admin_database_url().to_owned(),
        unknown_name.clone(),
    )
    .await;
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
        database_exists(harness.admin_database_url().to_owned(), unknown_name)
            .await
            .expect("verify unrecognized template was preserved")
    );
    unknown_database.remove().await;

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

    let admin_url = harness.admin_database_url().to_owned();
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

    let active = template.database().await.expect("create active database");
    const UNTAGGED_DATABASE: &str = "pgh_harness_it_test_00000000000000000000000000000000";
    let untagged_database =
        TemporaryDatabase::create(admin_url.clone(), UNTAGGED_DATABASE.to_owned()).await;
    let report = cleanup_stale_databases(&admin_url, "harness_it", Duration::ZERO)
        .await
        .expect("run owner-aware cleanup");
    assert_eq!(report.total_dropped(), 0);
    assert!(
        report.skipped_active >= 2,
        "active test and template are leased"
    );
    assert!(
        report.skipped_unrecognized >= 1,
        "untagged lookalike database must be preserved"
    );
    assert!(
        database_exists(admin_url.clone(), UNTAGGED_DATABASE.to_owned())
            .await
            .expect("check untagged database")
    );
    untagged_database.remove().await;
    active.cleanup().await.expect("clean active database");
    empty.cleanup().await.expect("clean empty database");

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

    drop(reused);
    drop(template);
    drop(first_template);
    drop(second_template);
    drop(first_slow);
    drop(second_slow);
    drop(slow_harness);
    drop(connection_template);
    drop(external);
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
