#![cfg(not(feature = "containers"))]

use postgres_test_harness::{
    BoxError, Error, FingerprintBuilder, HarnessConfig, POSTGRES_TEST_ADMIN_URL_ENV,
    PostgresHarness, TemplateSpec,
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
            TemplateSpec::new(FingerprintBuilder::new("external-only-lifecycle").finish()),
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
