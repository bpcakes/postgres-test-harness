use super::*;

fn template_key(template: &DatabaseTemplate) -> i64 {
    advisory_key(
        "template",
        &format!("harness_it:{}", template.fingerprint().to_hex()),
    )
}

async fn assert_cancelled_copy_retains_source(prewarm: bool) {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-copy-source").await;
    let parent_key = template_key(&parent);
    let child = parent
        .derive(sql_step("child", CHILD_SQL), |url| execute(url, CHILD_SQL))
        .await
        .unwrap();
    let key = template_key(&child);
    drop(parent);
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, parent_key).await;
    let catalog = CatalogLock::acquire(fixture.admin_url.clone()).await;
    let task = tokio::spawn(async move {
        if prewarm {
            child.prewarm(1).await.map(drop)
        } else {
            child.database().await.map(drop)
        }
    });
    let name = wait_until_database_with_prefix(&fixture.admin_url, "pgh_harness_it_test_").await;
    wait_for_lock_blocker(&fixture.admin_url, catalog.pid, Some((&name, "kind=test"))).await;
    task.abort();
    assert!(bounded(task).await.err().unwrap().is_cancelled());
    // Only the first/only copy is in flight; no other source handle or queued
    // prior prewarm deletion can supply the protection asserted here.
    assert!(
        !advisory_lock_is_acquirable(fixture.admin_url.clone(), key)
            .await
            .unwrap()
    );
    drop(catalog);
    wait_until_database_is_absent(&fixture.admin_url, &name).await;
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, key).await;
    fixture.harness.drain_deferred_cleanup().await.unwrap();
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_cancelled_lease_worker_retains_source_until_copy_finishes() {
    assert_cancelled_copy_retains_source(false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_cancelled_initial_prewarm_worker_retains_source_until_copy_finishes() {
    assert_cancelled_copy_retains_source(true).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_prewarm_refills_pristine_child_without_retaining_ancestors() {
    let fixture = OwnedHarnessFixture::start().await;
    let root = scenario_root(&fixture.harness, "derived-prewarm").await;
    let root_key = template_key(&root);
    let child = root
        .derive(sql_step("child", CHILD_SQL), |url| execute(url, CHILD_SQL))
        .await
        .unwrap();
    let child_key = template_key(&child);
    let leaf = child
        .derive(sql_step("grandchild", GRANDCHILD_SQL), |url| {
            execute(url, GRANDCHILD_SQL)
        })
        .await
        .unwrap();
    let leaf_key = template_key(&leaf);
    let pool = leaf.prewarm(1).await.unwrap();
    drop(root);
    drop(child);
    drop(leaf);
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, root_key).await;
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, child_key).await;
    assert!(
        !advisory_lock_is_acquirable(fixture.admin_url.clone(), leaf_key)
            .await
            .unwrap()
    );
    let first = pool.database().await.unwrap();
    let name = first.database_name().to_owned();
    assert_eq!(
        scenario_rows(first.database_url()).await.unwrap(),
        ["organization", "cancelled", "child", "grandchild"]
    );
    execute(first.database_url().to_owned(), "DELETE FROM scenario")
        .await
        .unwrap();
    first.cleanup().await.unwrap();
    fixture.harness.drain_deferred_cleanup().await.unwrap();
    assert_eq!(pool.status().ready(), 1);
    assert_eq!(pool.status().leased(), 0);
    assert_eq!(pool.status().creating(), 0);
    assert_eq!(pool.status().deleting(), 0);
    let replacement = pool.database().await.unwrap();
    assert_ne!(replacement.database_name(), name);
    assert_eq!(
        scenario_rows(replacement.database_url()).await.unwrap(),
        ["organization", "cancelled", "child", "grandchild"]
    );
    replacement.cleanup().await.unwrap();
    pool.shutdown().await.unwrap();
    fixture.harness.drain_deferred_cleanup().await.unwrap();
    wait_until_advisory_lock_is_acquirable(&fixture.admin_url, leaf_key).await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_cold_creation_obeys_closed_admission_on_a_live_server() {
    let fixture = OwnedHarnessFixture::start().await;
    let harness = PostgresHarness::start(
        HarnessConfig::new("derive_gate")
            .unwrap()
            .with_admin_database_url(fixture.admin_url.clone())
            .with_operation_timeout(Duration::from_millis(300))
            .unwrap()
            .with_cleanup_on_start(false),
    )
    .await
    .unwrap();
    let parent = scenario_root(&harness, "derived-admission").await;
    let child = parent
        .derive(sql_step("child", CHILD_SQL), |url| execute(url, CHILD_SQL))
        .await
        .unwrap();
    let uncached_spec = sql_step("uncached", SIBLING_SQL);
    let target_name = remove_admission_target(&fixture.admin_url, &parent, uncached_spec).await;
    let disposable = harness.empty_database().await.unwrap();
    let residual_name = disposable.database_name().to_owned();
    let catalog = CatalogLock::acquire(fixture.admin_url.clone()).await;
    assert!(matches!(
        disposable.cleanup().await,
        Err(Error::Postgres {
            operation: "drop disposable PostgreSQL database",
            ..
        })
    ));
    drop(catalog);
    assert_eq!(
        scalar_i64(fixture.admin_url.clone(), "SELECT 1::bigint")
            .await
            .unwrap(),
        1
    );
    let before = managed_names(&fixture.admin_url, "derive_gate").await;
    let calls = AtomicUsize::new(0);
    let result = parent
        .derive(uncached_spec, |_| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;
    assert!(matches!(result, Err(Error::ConnectionBudgetClosed)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        !database_exists(fixture.admin_url.clone(), target_name)
            .await
            .unwrap()
    );
    assert_eq!(
        managed_names(&fixture.admin_url, "derive_gate").await,
        before
    );
    assert!(matches!(
        child.database().await,
        Err(Error::ConnectionBudgetClosed)
    ));
    let cached = parent
        .derive(sql_step("child", CHILD_SQL), |_| async {
            panic!("cached handle must skip callback")
        })
        .await
        .unwrap();
    assert_eq!(cached.database_name(), child.database_name());
    execute(
        fixture.admin_url.clone(),
        format!("DROP DATABASE \"{residual_name}\" WITH (FORCE)"),
    )
    .await
    .unwrap();
    harness.drain_deferred_cleanup().await.unwrap();
    fixture.shutdown().await;
}

async fn remove_admission_target(
    admin_url: &str,
    parent: &DatabaseTemplate,
    spec: StepSpec,
) -> String {
    let target = parent
        .derive(spec, |url| execute(url, SIBLING_SQL))
        .await
        .unwrap();
    let name = target.database_name().to_owned();
    let key = advisory_key(
        "template",
        &format!("derive_gate:{}", target.fingerprint().to_hex()),
    );
    drop(target);
    wait_until_advisory_lock_is_acquirable(admin_url, key).await;
    cleanup_stale_databases(admin_url, "derive_gate", Duration::ZERO)
        .await
        .unwrap();
    assert!(
        !database_exists(admin_url.to_owned(), name.clone())
            .await
            .unwrap()
    );
    name
}

async fn managed_names(admin_url: &str, project: &str) -> Vec<String> {
    let prefix = format!("pgh_{project}_");
    with_client(admin_url.to_owned(), move |client| {
        Ok(client
            .query(
                "SELECT datname FROM pg_database WHERE starts_with(datname, $1) ORDER BY datname",
                &[&prefix],
            )?
            .into_iter()
            .map(|row| row.get(0))
            .collect())
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_creation_and_leases_fail_after_owned_shutdown() {
    let fixture = OwnedHarnessFixture::start().await;
    let parent = scenario_root(&fixture.harness, "derived-shutdown").await;
    let child = parent
        .derive(sql_step("child", CHILD_SQL), |url| execute(url, CHILD_SQL))
        .await
        .unwrap();
    fixture.shutdown().await;
    assert!(matches!(
        child.database().await,
        Err(Error::ConnectionBudgetClosed)
    ));
    let calls = AtomicUsize::new(0);
    assert!(
        parent
            .derive(sql_step("new-child", SIBLING_SQL), |_| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
