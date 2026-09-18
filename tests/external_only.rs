#![cfg(not(feature = "containers"))]

use postgres_test_harness::{
    BoxError, Error, FingerprintBuilder, HarnessConfig, POSTGRES_TEST_ADMIN_URL_ENV,
    PostgresHarness,
};
use std::process::Command;

#[tokio::test]
async fn missing_external_admin_url_reports_the_disabled_container_feature() {
    const CHILD_ENV: &str = "PTH_EXTERNAL_ONLY_MISSING_URL_CHILD";
    if std::env::var_os(CHILD_ENV).is_none() {
        let output =
            Command::new(std::env::current_exe().expect("resolve integration-test binary"))
                .args([
                    "--exact",
                    "missing_external_admin_url_reports_the_disabled_container_feature",
                    "--nocapture",
                ])
                .env(CHILD_ENV, "1")
                .env_remove(POSTGRES_TEST_ADMIN_URL_ENV)
                .output()
                .expect("run missing-admin-URL child");
        assert!(
            output.status.success(),
            "missing-admin-URL child failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let error = match PostgresHarness::start(HarnessConfig::new("external_only").unwrap()).await {
        Ok(_) => panic!("external-only startup must require an admin URL"),
        Err(error) => error,
    };

    assert!(matches!(error, Error::ExternalAdminUrlRequired));
    assert!(error.to_string().contains("with_admin_database_url"));
    assert!(error.to_string().contains(POSTGRES_TEST_ADMIN_URL_ENV));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires POSTGRES_TEST_ADMIN_URL for a PostgreSQL 18 admin database"]
async fn external_only_lifecycle_works_end_to_end() {
    let admin_url = std::env::var(POSTGRES_TEST_ADMIN_URL_ENV)
        .expect("external-only lifecycle requires POSTGRES_TEST_ADMIN_URL");
    let harness = PostgresHarness::start(
        HarnessConfig::new("external_only")
            .unwrap()
            .with_admin_database_url(admin_url)
            .with_image("unused.invalid/postgres:18")
            .unwrap()
            .with_cleanup_on_start(false),
    )
    .await
    .expect("attach an external PostgreSQL 18 server");

    assert!(harness.is_external());
    assert_eq!(harness.container_id(), None);

    let template = harness
        .template(
            FingerprintBuilder::new("external-only-lifecycle").finish_root(),
            |_| async { Ok::<_, BoxError>(()) },
        )
        .await
        .expect("create an external-server template");
    template
        .database()
        .await
        .expect("clone an external-server database")
        .cleanup()
        .await
        .expect("drop the external-server database");

    harness
        .shutdown()
        .await
        .expect("external-only shutdown is a no-op");
    harness
        .empty_database()
        .await
        .expect("external server remains usable after shutdown")
        .cleanup()
        .await
        .expect("drop a database created after external shutdown");
    harness
        .drain_deferred_cleanup()
        .await
        .expect("drain external-server cleanup");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires POSTGRES_TEST_ADMIN_URL for a PostgreSQL 18 admin database"]
async fn external_only_derived_scenario_survives_no_op_shutdown() {
    let harness = PostgresHarness::start(
        HarnessConfig::new("external_derived")
            .unwrap()
            .with_cleanup_on_start(false),
    )
    .await
    .expect("attach the configured external PostgreSQL server");
    let root_sql = "CREATE TABLE scenario (id integer PRIMARY KEY, state text NOT NULL); \
                    INSERT INTO scenario VALUES (1, 'active')";
    let child_sql = "UPDATE scenario SET state = 'cancelled' WHERE id = 1";
    let fingerprint = |sql| FingerprintBuilder::new("external-derived-v1").add("setup.sql", sql);
    let root = harness
        .template(fingerprint(root_sql).finish_root(), |url| {
            external_execute(url, root_sql)
        })
        .await
        .unwrap();
    let child = root
        .derive(fingerprint(child_sql).finish_step(), |url| {
            external_execute(url, child_sql)
        })
        .await
        .unwrap();
    for after_shutdown in [false, true] {
        if after_shutdown {
            harness.shutdown().await.unwrap();
        }
        let database = child.database().await.unwrap();
        let url = database.database_url().to_owned();
        let rows = tokio::task::spawn_blocking(move || {
            let mut client = postgres::Client::connect(&url, postgres::NoTls)?;
            client.query("SELECT id, state FROM scenario ORDER BY id", &[])
        })
        .await
        .unwrap();
        database.cleanup().await.unwrap();
        let rows = rows.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), 1);
        assert_eq!(rows[0].get::<_, String>(1), "cancelled");
    }
    harness.drain_deferred_cleanup().await.unwrap();
}

async fn external_execute(url: String, sql: &'static str) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || {
        postgres::Client::connect(&url, postgres::NoTls)?.batch_execute(sql)
    })
    .await??;
    Ok(())
}
