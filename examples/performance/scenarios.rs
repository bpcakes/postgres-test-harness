//! Equivalent-state scenario comparison; all construction is inside the total.

use super::*;
use postgres_test_harness::DatabaseTemplate;

const MIGRATION: &str = "CREATE TABLE scenario_rows (
    id bigint PRIMARY KEY, payload text NOT NULL, state text NOT NULL
); CREATE INDEX scenario_state_idx ON scenario_rows (state);";
const BRANCHES: [&str; 2] = ["active", "cancelled"];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Strategy {
    PerTest,
    FlatCached,
    Derived,
}

impl Strategy {
    fn name(self) -> &'static str {
        match self {
            Self::PerTest => "per_test",
            Self::FlatCached => "flat_cached",
            Self::Derived => "derived",
        }
    }
}

#[derive(Serialize)]
pub(super) struct ScenarioReuseReport {
    branch_count: usize,
    tests_per_branch: usize,
    shared_rows: usize,
    test_lifecycles_per_strategy: usize,
    pub(super) strategies: Vec<StrategyReport>,
}

#[derive(Serialize)]
pub(super) struct StrategyReport {
    strategy: Strategy,
    construction_ns: u128,
    root_acquisition_ns: Option<u128>,
    warm_acquisition_ns: u128,
    lease_acquisition_ns: u128,
    repeated_test_setup_ns: u128,
    test_lifecycles_ns: u128,
    cleanup_ns: u128,
    drain_ns: u128,
    total_ns: u128,
    initializer_counts: InitializerCounts,
    pub(super) templates: Vec<TemplateStorage>,
    template_storage_bytes: i64,
}

impl StrategyReport {
    pub(super) fn metrics(&self) -> Vec<(String, u128)> {
        let mut timings = vec![
            ("construction", self.construction_ns),
            ("warm_acquisition", self.warm_acquisition_ns),
            ("lease_acquisition", self.lease_acquisition_ns),
            ("repeated_test_setup", self.repeated_test_setup_ns),
            ("test_lifecycles", self.test_lifecycles_ns),
            ("cleanup", self.cleanup_ns),
            ("drain", self.drain_ns),
            ("total", self.total_ns),
        ];
        if let Some(root) = self.root_acquisition_ns {
            timings.push(("root_acquisition", root));
        }
        timings
            .into_iter()
            .map(|(metric, value)| (format!("scenario_{}_{metric}", self.strategy.name()), value))
            .collect()
    }
}

#[derive(Serialize)]
pub(super) struct TemplateStorage {
    pub(super) database_name: String,
    role: &'static str,
    size_bytes: i64,
}

#[derive(Debug, Default, Eq, PartialEq, Serialize)]
struct InitializerCounts {
    root_migrations: usize,
    shared_fixtures: usize,
    branch_steps: [usize; 2],
}

impl InitializerCounts {
    fn validate(&self, strategy: Strategy, tests: usize) -> AnyResult<()> {
        let expected = match strategy {
            Strategy::PerTest => Self {
                root_migrations: 1,
                shared_fixtures: 2 * tests,
                branch_steps: [tests; 2],
            },
            Strategy::FlatCached => Self {
                root_migrations: 2,
                shared_fixtures: 2,
                branch_steps: [1; 2],
            },
            Strategy::Derived => Self {
                root_migrations: 1,
                shared_fixtures: 1,
                branch_steps: [1; 2],
            },
        };
        if *self != expected {
            return Err(io::Error::other(format!(
                "{} setup counts: expected {expected:?}, observed {self:?}",
                strategy.name()
            ))
            .into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Step {
    Migration,
    Shared,
    Branch(usize),
}

struct Workload {
    domain: String,
    shared_sql: String,
    branch_sql: [String; 2],
    rows: i64,
    tests: usize,
}

impl Workload {
    fn new(config: &BenchmarkConfig, invocation: &str, sample: usize) -> AnyResult<Self> {
        Ok(Self {
            domain: format!("scenario-reuse-v1:{invocation}:{sample}"),
            shared_sql: format!(
                "INSERT INTO scenario_rows SELECT id, repeat('payload-' || id::text, 8), \
                 'base' FROM generate_series(1, {}) AS id;",
                config.representative_rows
            ),
            branch_sql: BRANCHES
                .map(|state| format!("UPDATE scenario_rows SET state = '{state}' WHERE id = 1;")),
            rows: i64::try_from(config.representative_rows)?,
            tests: config.sequential_operations,
        })
    }

    fn sql(&self, step: Step) -> &str {
        match step {
            Step::Migration => MIGRATION,
            Step::Shared => &self.shared_sql,
            Step::Branch(branch) => &self.branch_sql[branch],
        }
    }

    fn spec(&self, strategy: Strategy, steps: &[Step]) -> TemplateSpec {
        let mut builder = FingerprintBuilder::new(&self.domain).add("strategy", strategy.name());
        for step in steps {
            builder = builder.add("setup.sql", self.sql(*step));
        }
        TemplateSpec::new(builder.finish())
    }

    async fn setup(
        &self,
        client: &Client,
        steps: &[Step],
        counts: &mut InitializerCounts,
    ) -> AnyResult<()> {
        for step in steps {
            match step {
                Step::Migration => counts.root_migrations += 1,
                Step::Shared => counts.shared_fixtures += 1,
                Step::Branch(branch) => counts.branch_steps[*branch] += 1,
            }
            run_observer_operation(
                "apply scenario setup",
                OPERATION_TIMEOUT,
                client.batch_execute(self.sql(*step)),
            )
            .await?;
        }
        Ok(())
    }

    async fn initialize(
        &self,
        url: &str,
        steps: &[Step],
        counts: &mut InitializerCounts,
    ) -> AnyResult<()> {
        let connection = Observer::connect(url, OPERATION_TIMEOUT).await?;
        let setup = self.setup(connection.client(), steps, counts).await;
        combine_operation_and_cleanup(setup, connection.close().await).map(|_| ())
    }

    async fn validate(&self, client: &Client, branch: usize) -> AnyResult<()> {
        // PK uniqueness, exact count, the complete ID range, and every payload
        // and state are checked. A count alone would miss the wrong branch.
        let row = run_observer_operation(
            "validate scenario rows",
            OPERATION_TIMEOUT,
            client.query_one(
                "SELECT count(*), COALESCE(bool_and(
                    id BETWEEN 1 AND $1 AND payload = repeat('payload-' || id::text, 8)
                    AND state = CASE WHEN id = 1 THEN $2 ELSE 'base' END
                 ), false) FROM scenario_rows",
                &[&self.rows, &BRANCHES[branch]],
            ),
        )
        .await?;
        let count: i64 = row.get(0);
        let contents_match: bool = row.get(1);
        if count != self.rows || !contents_match {
            return Err(io::Error::other(format!(
                "{} scenario has incorrect data: {count} rows, expected {}",
                BRANCHES[branch], self.rows
            ))
            .into());
        }
        Ok(())
    }
}

struct Node {
    template: DatabaseTemplate,
    spec: TemplateSpec,
    source: Option<usize>,
    role: &'static str,
}

struct Constructed {
    nodes: Vec<Node>,
    branches: [usize; 2],
    root_acquisition_ns: Option<u128>,
}

impl Constructed {
    async fn add(
        &mut self,
        harness: &PostgresHarness,
        workload: &Workload,
        strategy: Strategy,
        source: Option<usize>,
        steps: &[Step],
        counts: &mut InitializerCounts,
    ) -> AnyResult<()> {
        let role = match steps.last().expect("template setup has at least one step") {
            Step::Migration => "root",
            Step::Shared => "shared",
            Step::Branch(branch) => BRANCHES[*branch],
        };
        let spec = workload.spec(strategy, steps);
        let setup = |url: String| async move { workload.initialize(&url, steps, counts).await };
        let template = match source {
            Some(parent) => self.nodes[parent].template.derive(spec, setup).await?,
            None => harness.template(spec, setup).await?,
        };
        self.nodes.push(Node {
            template,
            spec,
            source,
            role,
        });
        Ok(())
    }

    async fn warm(&self, harness: &PostgresHarness) -> AnyResult<u128> {
        let started = Instant::now();
        // Reacquire every constructed handle, including root/intermediates.
        for node in &self.nodes {
            let skipped = |_| async {
                Err(io::Error::other("scenario warm initializer unexpectedly ran").into())
            };
            let cached = match node.source {
                Some(parent) => {
                    self.nodes[parent]
                        .template
                        .derive(node.spec, skipped)
                        .await?
                }
                None => harness.template(node.spec, skipped).await?,
            };
            if cached.database_name() != node.template.database_name() {
                return Err(io::Error::other("warm scenario changed its database identity").into());
            }
        }
        Ok(started.elapsed().as_nanos())
    }

    async fn storage(&self, observer: &Observer) -> AnyResult<Vec<TemplateStorage>> {
        let mut templates = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            templates.push(TemplateStorage {
                database_name: node.template.database_name().to_owned(),
                role: node.role,
                size_bytes: observer
                    .database_size(node.template.database_name())
                    .await?,
            });
        }
        Ok(templates)
    }
}

async fn construct(
    harness: &PostgresHarness,
    workload: &Workload,
    strategy: Strategy,
    counts: &mut InitializerCounts,
) -> AnyResult<Constructed> {
    let mut built = Constructed {
        nodes: Vec::new(),
        branches: [0, 0],
        root_acquisition_ns: None,
    };
    if strategy != Strategy::FlatCached {
        let started = Instant::now();
        built
            .add(
                harness,
                workload,
                strategy,
                None,
                &[Step::Migration],
                counts,
            )
            .await?;
        built.root_acquisition_ns = Some(started.elapsed().as_nanos());
    }
    match strategy {
        Strategy::PerTest => (),
        Strategy::FlatCached => {
            for branch in 0..BRANCHES.len() {
                built
                    .add(
                        harness,
                        workload,
                        strategy,
                        None,
                        &[Step::Migration, Step::Shared, Step::Branch(branch)],
                        counts,
                    )
                    .await?;
            }
            built.branches = [0, 1];
        }
        Strategy::Derived => {
            built
                .add(
                    harness,
                    workload,
                    strategy,
                    Some(0),
                    &[Step::Shared],
                    counts,
                )
                .await?;
            for branch in 0..BRANCHES.len() {
                built
                    .add(
                        harness,
                        workload,
                        strategy,
                        Some(1),
                        &[Step::Branch(branch)],
                        counts,
                    )
                    .await?;
            }
            built.branches = [2, 3];
        }
    }
    Ok(built)
}

#[derive(Default)]
struct LifecycleTimings {
    lease_acquisition_ns: u128,
    repeated_test_setup_ns: u128,
    cleanup_ns: u128,
    names: Vec<String>,
}

async fn use_database(
    url: &str,
    workload: &Workload,
    branch: usize,
    strategy: Strategy,
    counts: &mut InitializerCounts,
    timings: &mut LifecycleTimings,
) -> AnyResult<()> {
    let connection = Observer::connect(url, OPERATION_TIMEOUT).await?;
    let operation = async {
        if strategy == Strategy::PerTest {
            let started = Instant::now();
            workload
                .setup(
                    connection.client(),
                    &[Step::Shared, Step::Branch(branch)],
                    counts,
                )
                .await?;
            timings.repeated_test_setup_ns += started.elapsed().as_nanos();
        }
        workload.validate(connection.client(), branch).await
    }
    .await;
    combine_operation_and_cleanup(operation, connection.close().await).map(|_| ())
}

async fn test_lifecycles(
    built: &Constructed,
    workload: &Workload,
    strategy: Strategy,
    counts: &mut InitializerCounts,
) -> AnyResult<LifecycleTimings> {
    let mut timings = LifecycleTimings::default();
    for branch in 0..BRANCHES.len() {
        for _ in 0..workload.tests {
            let started = Instant::now();
            let lease = built.nodes[built.branches[branch]]
                .template
                .database()
                .await?;
            timings.lease_acquisition_ns += started.elapsed().as_nanos();
            timings.names.push(lease.database_name().to_owned());
            let operation = use_database(
                lease.database_url(),
                workload,
                branch,
                strategy,
                counts,
                &mut timings,
            )
            .await;
            let cleanup_started = Instant::now();
            let cleanup = lease
                .cleanup()
                .await
                .map_err(|error| Box::new(error) as AnyError);
            timings.cleanup_ns += cleanup_started.elapsed().as_nanos();
            combine_operation_and_cleanup(operation, cleanup)?;
        }
    }
    Ok(timings)
}

async fn run_strategy(
    harness: &PostgresHarness,
    observer: &Observer,
    workload: &Workload,
    strategy: Strategy,
) -> AnyResult<StrategyReport> {
    eprintln!("  scenario strategy {}", strategy.name());
    let mut counts = InitializerCounts::default();
    let total_started = Instant::now();
    let built = construct(harness, workload, strategy, &mut counts).await?;
    let construction_ns = total_started.elapsed().as_nanos();
    let lifecycles_started = Instant::now();
    let lifecycles = test_lifecycles(&built, workload, strategy, &mut counts).await;
    let test_lifecycles_ns = lifecycles_started.elapsed().as_nanos();
    let drain_started = Instant::now();
    let drain = harness
        .drain_deferred_cleanup()
        .await
        .map_err(|error| Box::new(error) as AnyError);
    let drain_ns = drain_started.elapsed().as_nanos();
    let total_ns = total_started.elapsed().as_nanos();
    let (timings, ()) = combine_operation_and_cleanup(lifecycles, drain)?;
    counts.validate(strategy, workload.tests)?;
    observer.ensure_databases_are_absent(&timings.names).await?;
    // Diagnostics deliberately excluded from the cold-to-finished workload.
    let warm_acquisition_ns = built.warm(harness).await?;
    let templates = built.storage(observer).await?;
    let template_storage_bytes = templates.iter().map(|template| template.size_bytes).sum();
    Ok(StrategyReport {
        strategy,
        construction_ns,
        root_acquisition_ns: built.root_acquisition_ns,
        warm_acquisition_ns,
        lease_acquisition_ns: timings.lease_acquisition_ns,
        repeated_test_setup_ns: timings.repeated_test_setup_ns,
        test_lifecycles_ns,
        cleanup_ns: timings.cleanup_ns,
        drain_ns,
        total_ns,
        initializer_counts: counts,
        templates,
        template_storage_bytes,
    })
}

fn strategy_order(sample: usize) -> [Strategy; 3] {
    let mut order = [Strategy::PerTest, Strategy::FlatCached, Strategy::Derived];
    order.rotate_left(sample.saturating_sub(1) % 3);
    order
}

pub(super) async fn run(
    config: &BenchmarkConfig,
    harness: &PostgresHarness,
    observer: &Observer,
    invocation: &str,
    sample: usize,
) -> AnyResult<ScenarioReuseReport> {
    let workload = Workload::new(config, invocation, sample)?;
    let mut strategies = Vec::new();
    for strategy in strategy_order(sample) {
        strategies.push(run_strategy(harness, observer, &workload, strategy).await?);
    }
    Ok(ScenarioReuseReport {
        branch_count: BRANCHES.len(),
        tests_per_branch: workload.tests,
        shared_rows: config.representative_rows,
        test_lifecycles_per_strategy: BRANCHES.len() * workload.tests,
        strategies,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_rotation_balances_every_position() {
        for position in 0..3 {
            let mut observed: Vec<_> = (1..=3)
                .map(|sample| strategy_order(sample)[position].name())
                .collect();
            observed.sort_unstable();
            assert_eq!(observed, ["derived", "flat_cached", "per_test"]);
        }
        assert_eq!(strategy_order(4), strategy_order(1));
    }

    #[test]
    fn cached_strategies_reject_repeated_shared_setup() {
        let repeated = InitializerCounts {
            root_migrations: 1,
            shared_fixtures: 8,
            branch_steps: [4, 4],
        };
        assert!(repeated.validate(Strategy::PerTest, 4).is_ok());
        assert!(repeated.validate(Strategy::Derived, 4).is_err());
        assert!(repeated.validate(Strategy::FlatCached, 4).is_err());
        assert!(
            InitializerCounts::default()
                .validate(Strategy::Derived, 4)
                .is_err()
        );
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL 18 via POSTGRES_TEST_ADMIN_URL or default container support"]
    async fn scenario_oracle_rejects_wrong_branch_payload_and_count() {
        let harness = PostgresHarness::start(HarnessConfig::new("scenario_oracle").unwrap())
            .await
            .unwrap();
        let database = harness.empty_database().await.unwrap();
        let workload = Workload {
            domain: "oracle".to_owned(),
            rows: 3,
            tests: 1,
            shared_sql: "INSERT INTO scenario_rows VALUES (1, repeat('payload-1', 8), 'base'), \
                (2, repeat('payload-2', 8), 'base'), (3, repeat('payload-3', 8), 'base')"
                .to_owned(),
            branch_sql: [
                "UPDATE scenario_rows SET state = 'active' WHERE id = 1".to_owned(),
                String::new(),
            ],
        };
        let mut counts = InitializerCounts::default();
        workload
            .initialize(
                database.database_url(),
                &[Step::Migration, Step::Shared, Step::Branch(0)],
                &mut counts,
            )
            .await
            .unwrap();
        let connection = Observer::connect(database.database_url(), OPERATION_TIMEOUT)
            .await
            .unwrap();
        let client = connection.client();
        workload.validate(client, 0).await.unwrap();
        assert!(workload.validate(client, 1).await.is_err());
        client
            .batch_execute("UPDATE scenario_rows SET payload = 'wrong' WHERE id = 2")
            .await
            .unwrap();
        assert!(workload.validate(client, 0).await.is_err());
        client
            .batch_execute("UPDATE scenario_rows SET payload = repeat('payload-2', 8) WHERE id = 2")
            .await
            .unwrap();
        workload.validate(client, 0).await.unwrap();
        client
            .batch_execute("DELETE FROM scenario_rows WHERE id = 3")
            .await
            .unwrap();
        assert!(workload.validate(client, 0).await.is_err());
        connection.close().await.unwrap();
        database.cleanup().await.unwrap();
        harness.drain_deferred_cleanup().await.unwrap();
        harness.shutdown().await.unwrap();
    }
}
