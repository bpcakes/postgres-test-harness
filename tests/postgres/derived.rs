use std::future::Future;

use super::*;
use postgres_test_harness::{DatabaseTemplate, RootSpec, StepSpec};

#[path = "derived/lifetime.rs"]
mod lifetime;
#[path = "derived/recovery.rs"]
mod recovery;

const PARENT_SQL: &str = "CREATE TABLE scenario (id integer PRIMARY KEY, value text NOT NULL); \
    INSERT INTO scenario VALUES (1, 'organization'), (2, 'active'), (3, 'invoice')";
const CHILD_SQL: &str = "UPDATE scenario SET value = 'cancelled' WHERE id = 2; \
    DELETE FROM scenario WHERE id = 3; INSERT INTO scenario VALUES (4, 'child')";
const SIBLING_SQL: &str = "INSERT INTO scenario VALUES (4, 'sibling')";
const GRANDCHILD_SQL: &str = "INSERT INTO scenario VALUES (5, 'grandchild')";

fn sql_root(domain: &str, sql: &str) -> RootSpec {
    FingerprintBuilder::new(domain)
        .add("setup.sql", sql)
        .finish_root()
}

fn sql_step(domain: &str, sql: &str) -> StepSpec {
    FingerprintBuilder::new(domain)
        .add("setup.sql", sql)
        .finish_step()
}

async fn scenario_root(harness: &PostgresHarness, domain: &str) -> DatabaseTemplate {
    harness
        .template(sql_root(domain, PARENT_SQL), |url| execute(url, PARENT_SQL))
        .await
        .expect("initialize populated scenario root")
}

async fn scenario_rows(url: &str) -> Result<Vec<String>, BoxError> {
    with_client(url.to_owned(), |client| {
        Ok(client
            .query("SELECT value FROM scenario ORDER BY id", &[])?
            .into_iter()
            .map(|row| row.get(0))
            .collect())
    })
    .await
}

async fn assert_template_rows(template: &DatabaseTemplate, expected: &[&str]) {
    let database = template.database().await.expect("clone scenario");
    let rows = scenario_rows(database.database_url())
        .await
        .expect("read scenario");
    database.cleanup().await.expect("clean scenario clone");
    assert_eq!(rows, expected);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_branches_inherit_data_and_isolate_updates_deletes_and_test_writes() {
    let fixture = OwnedHarnessFixture::start().await;
    let root = scenario_root(&fixture.harness, "derived-isolation").await;
    let child = root
        .derive(sql_step("child", CHILD_SQL), |url| async move {
            assert_eq!(
                scenario_rows(&url).await?,
                ["organization", "active", "invoice"]
            );
            execute(url, CHILD_SQL).await
        })
        .await
        .expect("derive child with inherited data");
    let sibling = root
        .derive(sql_step("sibling", SIBLING_SQL), |url| {
            execute(url, SIBLING_SQL)
        })
        .await
        .expect("derive independent sibling");
    assert_ne!(child.fingerprint(), root.fingerprint());
    // The step's own inputs, claimed as a root, never name the child.
    assert_ne!(
        child.fingerprint(),
        sql_root("child", CHILD_SQL).fingerprint()
    );
    assert_ne!(child.fingerprint(), sibling.fingerprint());
    let dirty = child.database().await.expect("clone child for mutation");
    execute(
        dirty.database_url().to_owned(),
        "DELETE FROM scenario; INSERT INTO scenario VALUES (9, 'dirty')",
    )
    .await
    .expect("mutate isolated test database");
    dirty.cleanup().await.expect("drop dirty clone");

    assert_template_rows(&root, &["organization", "active", "invoice"]).await;
    assert_template_rows(&child, &["organization", "cancelled", "child"]).await;
    assert_template_rows(&sibling, &["organization", "active", "invoice", "sibling"]).await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_grandchildren_track_changed_ancestor_inputs() {
    let fixture = OwnedHarnessFixture::start().await;
    let root = scenario_root(&fixture.harness, "derived-ancestry").await;
    let updated_sql =
        format!("{PARENT_SQL}; UPDATE scenario SET value = 'organization-v2' WHERE id = 1");
    let changed_root = fixture
        .harness
        .template(sql_root("derived-ancestry", &updated_sql), |url| {
            execute(url, updated_sql)
        })
        .await
        .expect("initialize changed ancestor");
    let mut descendants = Vec::new();
    for parent in [&root, &changed_root] {
        let child = parent
            .derive(sql_step("child", CHILD_SQL), |url| execute(url, CHILD_SQL))
            .await
            .expect("derive child under ancestor");
        let grandchild = child
            .derive(sql_step("grandchild", GRANDCHILD_SQL), |url| {
                execute(url, GRANDCHILD_SQL)
            })
            .await
            .expect("derive grandchild");
        descendants.push((child, grandchild));
    }
    assert_ne!(
        descendants[0].0.fingerprint(),
        descendants[1].0.fingerprint()
    );
    assert_ne!(
        descendants[0].1.fingerprint(),
        descendants[1].1.fingerprint()
    );
    assert_ne!(
        descendants[0].1.database_name(),
        descendants[1].1.database_name()
    );
    assert_template_rows(
        &descendants[0].1,
        &["organization", "cancelled", "child", "grandchild"],
    )
    .await;
    assert_template_rows(
        &descendants[1].1,
        &["organization-v2", "cancelled", "child", "grandchild"],
    )
    .await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Docker-compatible daemon"]
async fn derived_warm_acquisition_skips_database_work_and_initializer() {
    let fixture = OwnedHarnessFixture::start().await;
    let root = scenario_root(&fixture.harness, "derived-warm").await;
    let spec = sql_step("child", CHILD_SQL);
    let child = root
        .derive(spec, |url| execute(url, CHILD_SQL))
        .await
        .expect("derive cold child");
    let locks = advisory_lock_count(fixture.admin_url.clone(), "ShareLock")
        .await
        .unwrap();
    let child_key = advisory_key(
        "template",
        &format!("harness_it:{}", child.fingerprint().to_hex()),
    );
    let mut exclusive = QueuedAdvisoryLock::queue(fixture.admin_url.clone(), child_key).await;
    let catalog = CatalogLock::acquire(fixture.admin_url.clone()).await;
    let warm = tokio::time::timeout(
        Duration::from_secs(1),
        root.derive(spec, |_| async {
            Err(std::io::Error::other("warm initializer must not run").into())
        }),
    )
    .await
    .expect("warm child bypasses catalog and queued exclusive lock")
    .expect("reuse child");
    assert_eq!(child.database_name(), warm.database_name());
    assert_eq!(
        advisory_lock_count(fixture.admin_url.clone(), "ShareLock")
            .await
            .unwrap(),
        locks
    );
    drop(catalog);
    drop(child);
    drop(warm);
    exclusive.wait_until_acquired().await;
    exclusive.release();
    fixture.shutdown().await;
}

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("derived lifecycle operation should finish within its test deadline")
}

async fn wait_for_lock_blocker(admin_url: &str, blocker: i32, comment: Option<(&str, &str)>) {
    let child = comment.map(|(name, _)| name.to_owned());
    let marker = comment.map(|(_, marker)| marker.to_owned());
    bounded(async {
        loop {
            let child = child.clone();
            let marker = marker.clone();
            let waiting = with_client(admin_url.to_owned(), move |client| {
                Ok(client
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                     WHERE state = 'active' AND wait_event_type = 'Lock' \
                     AND $1 = ANY(pg_blocking_pids(pid)) \
                     AND ($2::text IS NULL OR (query LIKE 'COMMENT ON DATABASE %' \
                     AND strpos(query, $2) > 0 AND strpos(query, $3) > 0)))",
                        &[&blocker, &child, &marker],
                    )?
                    .get::<_, bool>(0))
            })
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}
