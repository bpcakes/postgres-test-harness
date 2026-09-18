use std::future::{Future, poll_fn};

use super::*;
use tokio::{sync::oneshot, task::JoinHandle as Task};

fn registered_waiter<F>(future: F) -> (Task<F::Output>, oneshot::Receiver<()>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut future = Box::pin(future);
        let mut sender = Some(sender);
        poll_fn(|context| {
            let result = future.as_mut().poll(context);
            if result.is_pending()
                && let Some(sender) = sender.take()
            {
                let _ = sender.send(());
            }
            result
        })
        .await
    });
    (task, receiver)
}

enum BuildAction {
    Complete,
    Error,
    Panic,
}

async fn gated_build(
    parent: DatabaseTemplate,
    spec: StepSpec,
) -> (
    Task<postgres_test_harness::Result<DatabaseTemplate>>,
    oneshot::Sender<BuildAction>,
) {
    let (started, ready) = oneshot::channel();
    let (release, action) = oneshot::channel();
    let task = tokio::spawn(async move {
        parent
            .derive(spec, move |url| async move {
                execute(url.clone(), "INSERT INTO scenario VALUES (9, 'partial')").await?;
                let _ = started.send(());
                match action.await.expect("controller retains initializer gate") {
                    BuildAction::Complete => {
                        execute(
                            url,
                            format!("DELETE FROM scenario WHERE id = 9; {CHILD_SQL}"),
                        )
                        .await
                    }
                    BuildAction::Error => {
                        Err(std::io::Error::other("injected child initializer failure").into())
                    }
                    BuildAction::Panic => panic!("injected child initializer panic"),
                }
            })
            .await
    });
    bounded(ready)
        .await
        .expect("initializer reached partial write");
    (task, release)
}

async fn assert_interrupted_build_recovers(action: Option<BuildAction>) {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-recovery").await;
    let spec = sql_step("interrupted-child", CHILD_SQL);
    let (winner, release) = gated_build(parent.clone(), spec).await;
    let retry_parent = parent.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let retry_calls = calls.clone();
    let (waiter, registered) = registered_waiter(async move {
        retry_parent
            .derive(spec, move |url| async move {
                retry_calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    scenario_rows(&url).await?,
                    ["organization", "active", "invoice"]
                );
                execute(url, CHILD_SQL).await
            })
            .await
    });
    bounded(registered).await.unwrap();
    let error_return = matches!(action, Some(BuildAction::Error));
    let cancelled = action.is_none();
    if let Some(action) = action {
        release.send(action).ok().unwrap();
    } else {
        winner.abort();
    }
    match bounded(winner).await {
        Ok(Err(Error::TemplateInitializer { source })) if error_return => {
            assert_eq!(source.to_string(), "injected child initializer failure");
        }
        Err(error) if cancelled => assert!(error.is_cancelled()),
        Err(error) => assert!(error.is_panic()),
        _ => panic!("interrupted winner must report the requested failure"),
    }
    let recovered = bounded(waiter)
        .await
        .unwrap()
        .expect("waiter recovered a fresh child");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_template_rows(&recovered, &["organization", "cancelled", "child"]).await;
    assert_template_rows(&parent, &["organization", "active", "invoice"]).await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_initializer_error_restarts_registered_waiter_from_parent() {
    assert_interrupted_build_recovers(Some(BuildAction::Error)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_initializer_cancellation_restarts_registered_waiter_from_parent() {
    assert_interrupted_build_recovers(None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_initializer_panic_restarts_registered_waiter_from_parent() {
    assert_interrupted_build_recovers(Some(BuildAction::Panic)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_initializer_and_abort_errors_are_preserved_for_retry() {
    let fixture = OwnedHarnessFixture::start().await;
    let harness = PostgresHarness::start(
        HarnessConfig::new("derive_abort")
            .unwrap()
            .with_admin_database_url(fixture.admin_url.clone())
            .with_operation_timeout(Duration::from_millis(300))
            .unwrap()
            .with_cleanup_on_start(false),
    )
    .await
    .unwrap();
    let parent = scenario_root(&harness, "derived-abort-errors").await;
    let spec = sql_step("abort-errors", CHILD_SQL);
    let (task, release) = gated_build(parent.clone(), spec).await;
    let catalog = CatalogLock::acquire(fixture.admin_url.clone()).await;
    release.send(BuildAction::Error).ok().unwrap();
    match bounded(task).await.unwrap() {
        Err(Error::TemplateInitializerAndCleanup {
            initializer,
            cleanup,
        }) => {
            assert_eq!(
                initializer.to_string(),
                "injected child initializer failure"
            );
            assert!(matches!(
                *cleanup,
                Error::Postgres {
                    operation: "drop disposable PostgreSQL database",
                    ..
                }
            ));
        }
        _ => panic!("callback and abort failures must both be preserved"),
    }
    drop(catalog);
    let child = parent
        .derive(spec, |url| async move {
            assert_eq!(
                scenario_rows(&url).await?,
                ["organization", "active", "invoice"]
            );
            execute(url, CHILD_SQL).await
        })
        .await
        .expect("retry recognized child after abort failure");
    assert_template_rows(&child, &["organization", "cancelled", "child"]).await;
    harness
        .drain_deferred_cleanup()
        .await
        .expect("abort is outside disposable drain");
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_concurrent_cold_start_publishes_one_child_lock_and_setup() {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-concurrent").await;
    let locks_before = advisory_lock_count(fixture.admin_url.clone(), "ShareLock")
        .await
        .unwrap();
    let spec = sql_step("concurrent-child", CHILD_SQL);
    let (winner, release) = gated_build(parent.clone(), spec).await;
    let unexpected_calls = Arc::new(AtomicUsize::new(0));
    let mut waiters = Vec::new();
    for _ in 0..7 {
        let parent = parent.clone();
        let calls = unexpected_calls.clone();
        let (waiter, registered) = registered_waiter(async move {
            parent
                .derive(spec, |_| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(std::io::Error::other("cold-start waiter must reuse winner").into())
                })
                .await
        });
        bounded(registered).await.unwrap();
        waiters.push(waiter);
    }
    release.send(BuildAction::Complete).ok().unwrap();
    let winner = bounded(winner).await.unwrap().unwrap();
    let mut children = vec![winner];
    for waiter in waiters {
        children.push(bounded(waiter).await.unwrap().unwrap());
    }
    assert!(
        children
            .iter()
            .all(|child| child.database_name() == children[0].database_name())
    );
    assert_eq!(unexpected_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        advisory_lock_count(fixture.admin_url.clone(), "ShareLock")
            .await
            .unwrap(),
        locks_before + 1
    );
    assert_template_rows(&children[0], &["organization", "cancelled", "child"]).await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_cancelled_waiter_leaves_winning_initializer_running() {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-waiter-cancel").await;
    let spec = sql_step("waiter-cancel", CHILD_SQL);
    let (winner, release) = gated_build(parent.clone(), spec).await;
    let waiting_parent = parent.clone();
    let (waiter, registered) = registered_waiter(async move {
        waiting_parent
            .derive(spec, |_| async {
                panic!("waiter initializer must not run")
            })
            .await
    });
    bounded(registered).await.unwrap();
    waiter.abort();
    assert!(bounded(waiter).await.err().unwrap().is_cancelled());
    release.send(BuildAction::Complete).ok().unwrap();
    let child = bounded(winner).await.unwrap().unwrap();
    let warm = parent
        .derive(spec, |_| async { panic!("warm initializer must not run") })
        .await
        .unwrap();
    assert_eq!(child.database_name(), warm.database_name());
    assert_template_rows(&warm, &["organization", "cancelled", "child"]).await;
    fixture.shutdown().await;
}

async fn child_metadata(admin_url: &str, name: &str) -> Option<(bool, String)> {
    let name = name.to_owned();
    with_client(admin_url.to_owned(), move |client| {
        Ok(client.query_opt(
            "SELECT datallowconn, shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = $1",
            &[&name],
        )?.map(|row| (row.get(0), row.get(1))))
    }).await.unwrap()
}

async fn remove_cached_child(
    fixture: &OwnedHarnessFixture,
    parent: &DatabaseTemplate,
    spec: StepSpec,
) -> (String, i64) {
    let child = parent
        .derive(spec, |url| execute(url, CHILD_SQL))
        .await
        .unwrap();
    let name = child.database_name().to_owned();
    let key = advisory_key(
        "template",
        &format!("harness_it:{}", child.fingerprint().to_hex()),
    );
    drop(child);
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, key).await;
    cleanup_stale_databases(&fixture.admin_url, "harness_it", Duration::ZERO)
        .await
        .unwrap();
    assert!(
        !database_exists(fixture.admin_url.clone(), name.clone())
            .await
            .unwrap()
    );
    (name, key)
}

struct AdvisoryGate {
    pid: i32,
    release: Option<Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl AdvisoryGate {
    async fn hold(url: String, key: i64) -> Self {
        let (started, ready) = oneshot::channel();
        let (release, released) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut client = Client::connect(&url, NoTls).unwrap();
            let pid = client
                .query_one("SELECT pg_backend_pid()", &[])
                .unwrap()
                .get(0);
            client
                .query_one("SELECT pg_advisory_lock($1)", &[&key])
                .unwrap();
            started.send(pid).unwrap();
            let _ = released.recv();
            client
                .query_one("SELECT pg_advisory_unlock($1)", &[&key])
                .unwrap();
        });
        Self {
            pid: bounded(ready).await.unwrap(),
            release: Some(release),
            worker: Some(worker),
        }
    }
}

impl Drop for AdvisoryGate {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_detached_preparation_retains_and_releases_parent() {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-detached").await;
    let parent_key = advisory_key(
        "template",
        &format!("harness_it:{}", parent.fingerprint().to_hex()),
    );
    let spec = sql_step("detached-child", CHILD_SQL);
    let (name, key) = remove_cached_child(&fixture, &parent, spec).await;
    let gate = AdvisoryGate::hold(fixture.admin_url.clone(), key).await;
    let task = tokio::spawn(async move {
        parent
            .derive(spec, |_| async {
                panic!("cancelled caller must not initialize")
            })
            .await
    });
    wait_for_lock_blocker(&fixture.admin_url, gate.pid, None).await;
    task.abort();
    assert!(bounded(task).await.err().unwrap().is_cancelled());
    assert!(
        !advisory_lock_is_acquirable(fixture.admin_url.clone(), parent_key)
            .await
            .unwrap()
    );
    drop(gate);
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, parent_key).await;
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, key).await;
    let (_, metadata) = child_metadata(&fixture.admin_url, &name)
        .await
        .expect("abandoned copy stays tagged");
    assert!(metadata.contains("state=initializing"));
    let parent = scenario_root(&fixture.harness, "derived-detached").await;
    let child = parent
        .derive(spec, |url| async move {
            assert_eq!(
                scenario_rows(&url).await?,
                ["organization", "active", "invoice"]
            );
            execute(url, CHILD_SQL).await
        })
        .await
        .unwrap();
    assert_template_rows(&child, &["organization", "cancelled", "child"]).await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_abandoned_finalization_reuses_catalog_ready_child() {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-finalize").await;
    let spec = sql_step("finalize-child", CHILD_SQL);
    let (name, key) = remove_cached_child(&fixture, &parent, spec).await;
    let (task, release) = gated_build(parent.clone(), spec).await;
    let catalog = CatalogLock::acquire(fixture.admin_url.clone()).await;
    release.send(BuildAction::Complete).ok().unwrap();
    wait_for_lock_blocker(
        &fixture.admin_url,
        catalog.pid,
        Some((&name, "state=ready")),
    )
    .await;
    task.abort();
    assert!(bounded(task).await.err().unwrap().is_cancelled());
    drop(catalog);
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, key).await;
    let (allow_connections, metadata) = child_metadata(&fixture.admin_url, &name).await.unwrap();
    assert!(!allow_connections);
    assert!(metadata.contains("state=ready"));
    let child = parent
        .derive(spec, |_| async {
            panic!("catalog-ready child must skip initializer")
        })
        .await
        .unwrap();
    assert_template_rows(&child, &["organization", "cancelled", "child"]).await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_independent_harnesses_coordinate_through_postgres() {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-peers").await;
    let peer = PostgresHarness::start(
        HarnessConfig::new("harness_it")
            .unwrap()
            .with_admin_database_url(fixture.admin_url.clone())
            .with_cleanup_on_start(false),
    )
    .await
    .unwrap();
    let peer_parent = scenario_root(&peer, "derived-peers").await;
    let spec = sql_step("peer-child", CHILD_SQL);
    let (winner, release) = gated_build(parent.clone(), spec).await;
    // The winning callback holds an exclusive target advisory lock on its
    // admin session; identify that blocker without relying on a local counter.
    let blocker = with_client(fixture.admin_url.clone(), |client| {
        Ok(client
            .query_one(
                "SELECT pid FROM pg_locks WHERE locktype = 'advisory' \
            AND mode = 'ExclusiveLock' AND granted AND pid IN \
            (SELECT pid FROM pg_stat_activity WHERE query LIKE 'COMMENT ON DATABASE %' \
             AND strpos(query, 'state=initializing') > 0) LIMIT 1",
                &[],
            )?
            .get(0))
    })
    .await
    .unwrap();
    let waiter = tokio::spawn(async move {
        peer_parent
            .derive(spec, |_| async {
                panic!("peer must reuse successful initializer")
            })
            .await
    });
    wait_for_lock_blocker(&fixture.admin_url, blocker, None).await;
    release.send(BuildAction::Complete).ok().unwrap();
    let first = bounded(winner).await.unwrap().unwrap();
    let second = bounded(waiter).await.unwrap().unwrap();
    assert_eq!(first.database_name(), second.database_name());
    assert_template_rows(&second, &["organization", "cancelled", "child"]).await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_child_survives_parent_stale_cleanup() {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-parent-gc").await;
    let name = parent.database_name().to_owned();
    let key = advisory_key(
        "template",
        &format!("harness_it:{}", parent.fingerprint().to_hex()),
    );
    let child = parent
        .derive(sql_step("child", CHILD_SQL), |url| execute(url, CHILD_SQL))
        .await
        .unwrap();
    drop(parent);
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, key).await;
    let report = cleanup_stale_databases(&fixture.admin_url, "harness_it", Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(report.dropped_templates, 1);
    assert!(
        !database_exists(fixture.admin_url.clone(), name)
            .await
            .unwrap()
    );
    assert!(
        database_exists(fixture.admin_url.clone(), child.database_name().to_owned())
            .await
            .unwrap()
    );
    assert_template_rows(&child, &["organization", "cancelled", "child"]).await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_unrecognized_target_is_preserved() {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-collision").await;
    let spec = sql_step("collision-child", CHILD_SQL);
    let (name, _) = remove_cached_child(&fixture, &parent, spec).await;
    let lookalike = TemporaryDatabase::create(fixture.admin_url.clone(), name.clone()).await;
    let mut url = url::Url::parse(&fixture.admin_url).unwrap();
    url.set_path(&format!("/{name}"));
    execute(url.to_string(), PARENT_SQL).await.unwrap();
    let result = parent
        .derive(spec, |_| async {
            panic!("unknown target must not be initialized")
        })
        .await;
    assert!(
        matches!(result, Err(Error::InconsistentMetadata { database_name }) if database_name == name)
    );
    assert_eq!(
        scenario_rows(url.as_str()).await.unwrap(),
        ["organization", "active", "invoice"]
    );
    lookalike.remove().await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_ready_target_without_matching_lineage_is_preserved() {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-lineage").await;
    let spec = sql_step("lineage-child", CHILD_SQL);
    let child = parent
        .derive(spec, |url| execute(url, CHILD_SQL))
        .await
        .unwrap();
    let name = child.database_name().to_owned();
    let key = advisory_key(
        "template",
        &format!("harness_it:{}", child.fingerprint().to_hex()),
    );
    drop(child);
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, key).await;
    // Earlier builds could seal a template0 build under a derived fingerprint;
    // its metadata matched a real child's except for the missing parent.
    let (_, metadata) = child_metadata(&fixture.admin_url, &name).await.unwrap();
    let lineage = format!(";parent={}", parent.fingerprint().to_hex());
    assert!(metadata.contains(&lineage));
    execute(
        fixture.admin_url.clone(),
        format!(
            "COMMENT ON DATABASE \"{name}\" IS '{}'",
            metadata.replace(&lineage, "")
        ),
    )
    .await
    .unwrap();
    let result = parent
        .derive(spec, |_| async {
            panic!("a target with another lineage must not be initialized")
        })
        .await;
    assert!(
        matches!(result, Err(Error::InconsistentMetadata { database_name }) if database_name == name)
    );
    assert!(
        database_exists(fixture.admin_url.clone(), name)
            .await
            .unwrap()
    );
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_sealing_terminates_lingering_child_connection() {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-seal").await;
    let held = Arc::new(Mutex::new(None));
    let callback_held = held.clone();
    let child = parent
        .derive(sql_step("seal-child", CHILD_SQL), move |url| async move {
            execute(url.clone(), CHILD_SQL).await?;
            let connection = HeldClient::connect(url).await;
            // The held client's backend belongs only to this initializing child.
            *callback_held.lock().unwrap() = Some(connection);
            Ok(())
        })
        .await
        .unwrap();
    let name = child.database_name().to_owned();
    let sessions = with_client(fixture.admin_url.clone(), move |client| {
        Ok(client
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = $1",
                &[&name],
            )?
            .get::<_, i64>(0))
    })
    .await
    .unwrap();
    assert_eq!(sessions, 0);
    assert_template_rows(&child, &["organization", "cancelled", "child"]).await;
    assert_template_rows(&parent, &["organization", "active", "invoice"]).await;
    drop(held);
    fixture.shutdown().await;
}
