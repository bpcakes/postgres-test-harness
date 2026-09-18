use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::{Duration, SystemTime},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    BoxError, ConnectionLimits, Error, HarnessConfig, ProjectName, Result, RootSpec, StepSpec,
    TemplateFingerprint,
    admin::{
        AdminClient, AdminDatabaseUrl, AdminSessionDisposition, DatabaseRecord, PersistentClient,
        acquire_shared_template_advisory_lock, acquire_template_advisory_lock, advisory_key,
        connect_admin, disable_database_connections, drop_database, find_database, list_databases,
        release_advisory_lock, release_shared_advisory_lock, set_database_metadata,
        terminate_database_connections, try_acquire_advisory_lock, validate_postgres_18,
    },
    admission::ManagedDatabaseCreationFailure,
    cleanup::{CleanupOutcome, log_cleanup},
    fingerprint::TemplateIdentity,
    metadata::{ResourceMetadata, TemplateState},
    name::{DatabaseKind, DatabaseName},
    server::{ServerInner, TemplateCacheAction, TemplateInner, run_blocking},
};

const DEFAULT_CLEANUP_OPERATION_TIMEOUT: Duration = Duration::from_secs(90);

/// Process-local owner of a PostgreSQL 18 server and its database capacity.
#[derive(Clone)]
pub struct PostgresHarness {
    server: Arc<ServerInner>,
}

impl PostgresHarness {
    pub async fn start(config: HarnessConfig) -> Result<Self> {
        let server = ServerInner::start(config).await?;
        let harness = Self { server };
        if harness.server.is_external() && harness.server.cleanup_on_start {
            cleanup_stale_with(
                harness.server.admin_url.clone(),
                harness.server.project.clone(),
                harness.server.stale_after,
                harness.server.operation_timeout,
            )
            .await?;
        }
        Ok(harness)
    }

    pub fn project(&self) -> &ProjectName {
        &self.server.project
    }

    pub fn admin_database_url(&self) -> &str {
        self.server.admin_url.as_str()
    }

    pub fn is_external(&self) -> bool {
        self.server.is_external()
    }

    /// Returns the owned container ID, or `None` in external-server mode.
    ///
    /// Builds without the `containers` feature always return `None`.
    pub fn container_id(&self) -> Option<&str> {
        self.server.container_id()
    }

    /// Returns the resolved downstream connection-permit policy used by this
    /// harness's database admission control.
    pub fn connection_limits(&self) -> ConnectionLimits {
        self.server.connection_limits
    }

    /// Closes database admission, drains accepted cleanup, and removes an owned
    /// container.
    ///
    /// Admission remains closed after the first owned shutdown attempt, even
    /// if container removal fails. External servers and their admission remain
    /// untouched. Repeated and concurrent calls do not retry removal; the call
    /// that completes the terminal worker reports any removal error, while
    /// later calls observe completed shutdown. Active leases remain owned but
    /// can no longer contact a successfully removed server. Cleanup submissions
    /// already accepted by the bounded server queue finish before its pooled
    /// admin sessions and container are closed. Deferred failures are returned
    /// from this barrier. External-server shutdown remains a no-op, including
    /// in builds without the `containers` feature; call
    /// [`Self::drain_deferred_cleanup`] explicitly for an external server.
    pub async fn shutdown(&self) -> Result<()> {
        self.server.shutdown_container().await
    }

    /// Waits for every database cleanup accepted before this call.
    ///
    /// Failures from [`DatabaseLease::defer_cleanup`], the `Drop` fallback, or
    /// an awaited cleanup whose caller was cancelled are retained and returned
    /// exactly once. Cleanup submitted concurrently after the barrier begins is
    /// covered by a later drain. A failure that may leave a residual database
    /// closes new admission so storage cannot accumulate without bound.
    pub async fn drain_deferred_cleanup(&self) -> Result<()> {
        let database_cleanup = self.server.database_cleanup.clone();
        let outcome = run_blocking(move || Ok(database_cleanup.begin_drain())).await?;
        outcome.finish()
    }

    /// Creates a pristine database from PostgreSQL's built-in `template0`.
    pub async fn empty_database(&self) -> Result<DatabaseLease> {
        create_test_database(self.server.clone(), TemplateSource::Empty).await
    }

    /// Gets or initializes one immutable, content-addressed template database
    /// from PostgreSQL's built-in `template0`.
    ///
    /// Calls with the same spec on clones of this harness single-flight
    /// initialization and share one live template handle. The cache is weak:
    /// dropping the final [`DatabaseTemplate`] releases its retained PostgreSQL
    /// shared-lock session. Separate harness starts continue to coordinate through
    /// PostgreSQL advisory locks.
    pub async fn template<F, Fut>(&self, spec: RootSpec, initializer: F) -> Result<DatabaseTemplate>
    where
        F: FnOnce(String) -> Fut,
        Fut: Future<Output = std::result::Result<(), BoxError>>,
    {
        get_or_initialize_template(
            self.server.clone(),
            TemplateIdentity::root(spec),
            TemplateSource::Empty,
            initializer,
        )
        .await
    }
}

/// Immutable initialized database that can be cloned for individual tests.
#[derive(Clone)]
pub struct DatabaseTemplate {
    inner: Arc<TemplateInner>,
}

impl DatabaseTemplate {
    pub fn database_name(&self) -> &str {
        self.inner.name().as_str()
    }

    /// Complete template identity, including every ancestor for derived templates.
    ///
    /// The fingerprint is output-only. To reopen a derived scenario, repeat its
    /// [`PostgresHarness::template`] and [`Self::derive`] calls; cache hits skip
    /// each initializer.
    pub fn fingerprint(&self) -> TemplateFingerprint {
        self.inner.fingerprint()
    }

    /// Gets or initializes an immutable child by copying this template and
    /// applying one local setup step to the copy.
    ///
    /// `step` fingerprints the local step only. The harness combines it with
    /// this template's complete identity; the returned [`Self::fingerprint`]
    /// is that composed identity. Include all SQL, fixture data,
    /// configuration, seeds, and setup-code revisions that can change the
    /// step's output. Closures and their captures are not hashed.
    ///
    /// The initializer receives a writable child URL with inherited schema
    /// and rows. It is skipped on cache hits, and runs on the caller's task
    /// without `Send` or `'static` requirements. Close application connections
    /// and tasks before returning. Successful initialization seals the child;
    /// its leases and descendants are independent of the parent's lifetime.
    ///
    /// Concurrent callers coordinate as for [`PostgresHarness::template`].
    /// Failed or cancelled attempts can run again from a fresh parent copy.
    /// Cancellation can leave a tagged initializing database for retry or
    /// stale cleanup; [`PostgresHarness::drain_deferred_cleanup`] does not
    /// cover those abandoned templates. Returned initializer errors preserve
    /// an additional abort-cleanup error when present.
    ///
    /// Each node is a full PostgreSQL database copy and each live template
    /// retains an administrative lock session. Initialization connections are
    /// outside the downstream lease-permit budget. Inheritance covers
    /// database-local state: database-level grants/settings, cluster-wide
    /// roles, and external side effects are not copied as scenario state.
    pub async fn derive<F, Fut>(&self, step: StepSpec, initializer: F) -> Result<DatabaseTemplate>
    where
        F: FnOnce(String) -> Fut,
        Fut: Future<Output = std::result::Result<(), BoxError>>,
    {
        get_or_initialize_template(
            self.inner.server().clone(),
            TemplateIdentity::derived(self.fingerprint(), step),
            TemplateSource::Template(self.inner.clone()),
            initializer,
        )
        .await
    }

    pub async fn database(&self) -> Result<DatabaseLease> {
        create_test_database(
            self.inner.server().clone(),
            TemplateSource::Template(self.inner.clone()),
        )
        .await
    }

    /// Creates and fills an opt-in bounded queue of pristine disposable databases.
    ///
    /// Idle databases retain this template's shared lock but consume no
    /// downstream connection permits. The capacity may not exceed the
    /// harness's effective simultaneous-lease limit. Every leased database is
    /// dropped after use; the background lifecycle queue creates a distinct
    /// replacement rather than reusing dirty state.
    pub async fn prewarm(&self, capacity: usize) -> Result<PrewarmedDatabasePool> {
        let max_capacity = self
            .inner
            .server()
            .connection_limits
            .max_simultaneous_leases();
        if capacity == 0 || capacity > max_capacity {
            return Err(Error::InvalidPrewarmCapacity {
                capacity,
                max_capacity,
            });
        }

        let pool = PrewarmedDatabasePool::new(self.inner.clone(), capacity)?;
        for _ in 0..capacity {
            let creation = pool.inner.begin_creation()?;
            let prepared = create_unpublished_database(
                self.inner.server().clone(),
                TemplateSource::Template(self.inner.clone()),
            )
            .await;
            match prepared {
                Ok(prepared) => creation.publish(prepared),
                Err(error) => {
                    creation.fail();
                    return Err(error);
                }
            }
        }
        Ok(pool)
    }
}

/// Current resource state of a bounded prewarmed database queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PrewarmPoolStatus {
    capacity: usize,
    ready: usize,
    leased: usize,
    creating: usize,
    deleting: usize,
    closed: bool,
}

impl PrewarmPoolStatus {
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    pub const fn ready(&self) -> usize {
        self.ready
    }

    pub const fn leased(&self) -> usize {
        self.leased
    }

    pub const fn creating(&self) -> usize {
        self.creating
    }

    pub const fn deleting(&self) -> usize {
        self.deleting
    }

    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Returns the number of capacity slots currently represented by ready,
    /// leased, creating, or deleting work.
    pub const fn occupied_slots(&self) -> usize {
        self.ready + self.leased + self.creating + self.deleting
    }
}

/// Opt-in bounded queue of never-used disposable databases cloned from one template.
pub struct PrewarmedDatabasePool {
    inner: Arc<PrewarmPoolInner>,
}

impl PrewarmedDatabasePool {
    fn new(template: Arc<TemplateInner>, capacity: usize) -> Result<Self> {
        let inner = Arc::new(PrewarmPoolInner {
            template,
            capacity,
            public_handles: AtomicUsize::new(1),
            ready_admission: Arc::new(Semaphore::new(0)),
            closed: Arc::new(Semaphore::new(0)),
            state: Mutex::new(PrewarmPoolState::new()),
        });
        inner.template.server().register_prewarm_pool(&inner)?;
        Ok(Self { inner })
    }

    /// Returns the configured database-slot bound.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Returns a point-in-time view of the bounded queue state.
    pub fn status(&self) -> PrewarmPoolStatus {
        self.inner.status()
    }

    /// Leases one never-used database from the ready queue.
    ///
    /// A ready token is reserved before downstream application capacity. If
    /// application admission or this pool is closed, or if this future is
    /// cancelled, the token is returned and the idle database remains in the
    /// queue until pool cleanup consumes it.
    pub async fn database(&self) -> Result<DatabaseLease> {
        let ready = self
            .inner
            .ready_admission
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::PrewarmPoolClosed)?;
        let permit = acquire_database_permit_or_pool_close(
            self.inner.template.server().acquire_database_permit(),
            self.inner.closed.clone(),
        )
        .await?;
        let Some(prepared) = self.inner.take_ready() else {
            return Err(Error::PrewarmPoolClosed);
        };
        ready.forget();
        Ok(prepared.into_lease(permit, Arc::downgrade(&self.inner)))
    }

    /// Closes this queue, drops all idle databases, and waits for lifecycle
    /// work accepted before the resulting server-scoped cleanup barrier.
    ///
    /// Leases still owned by callers are not revoked. They remain responsible
    /// for their own cleanup and will not trigger a refill after this call.
    pub async fn shutdown(self) -> Result<()> {
        self.inner.close_and_queue_ready();
        let database_cleanup = self.inner.template.server().database_cleanup.clone();
        let outcome = run_blocking(move || Ok(database_cleanup.begin_drain())).await?;
        outcome.finish()
    }
}

impl Clone for PrewarmedDatabasePool {
    fn clone(&self) -> Self {
        self.inner.public_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl std::fmt::Debug for PrewarmedDatabasePool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PrewarmedDatabasePool")
            .field("template", &self.inner.template.name().as_str())
            .field("status", &self.status())
            .finish()
    }
}

impl Drop for PrewarmedDatabasePool {
    fn drop(&mut self) {
        if self.inner.public_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.close_and_queue_ready();
        }
    }
}

pub(crate) struct PrewarmPoolInner {
    template: Arc<TemplateInner>,
    capacity: usize,
    public_handles: AtomicUsize,
    ready_admission: Arc<Semaphore>,
    closed: Arc<Semaphore>,
    state: Mutex<PrewarmPoolState>,
}

struct PrewarmPoolState {
    ready: VecDeque<PreparedDatabase>,
    leased: usize,
    creating: usize,
    deleting: usize,
    accepting: bool,
    cleanup_started: bool,
}

impl PrewarmPoolState {
    fn new() -> Self {
        Self {
            ready: VecDeque::new(),
            leased: 0,
            creating: 0,
            deleting: 0,
            accepting: true,
            cleanup_started: false,
        }
    }

    fn occupied_slots(&self) -> usize {
        self.ready.len() + self.leased + self.creating + self.deleting
    }

    fn fail_and_begin_cleanup(
        &mut self,
        phase: PrewarmReturnPhase,
        capacity: usize,
    ) -> Option<VecDeque<PreparedDatabase>> {
        match phase {
            PrewarmReturnPhase::Deleting => {
                debug_assert!(self.deleting > 0);
                self.deleting = self.deleting.saturating_sub(1);
            }
            PrewarmReturnPhase::Creating => {
                debug_assert!(self.creating > 0);
                self.creating = self.creating.saturating_sub(1);
            }
            PrewarmReturnPhase::Complete => return None,
        }
        self.begin_cleanup(capacity)
    }

    fn begin_cleanup(&mut self, capacity: usize) -> Option<VecDeque<PreparedDatabase>> {
        self.accepting = false;
        if self.cleanup_started {
            return None;
        }
        self.cleanup_started = true;
        let ready = std::mem::take(&mut self.ready);
        self.deleting += ready.len();
        debug_assert!(self.occupied_slots() <= capacity);
        Some(ready)
    }
}

impl PrewarmPoolInner {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, PrewarmPoolState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn status(&self) -> PrewarmPoolStatus {
        let state = self.lock_state();
        PrewarmPoolStatus {
            capacity: self.capacity,
            ready: state.ready.len(),
            leased: state.leased,
            creating: state.creating,
            deleting: state.deleting,
            closed: !state.accepting,
        }
    }

    fn begin_creation(self: &Arc<Self>) -> Result<PrewarmCreation> {
        let mut state = self.lock_state();
        if !state.accepting {
            return Err(Error::PrewarmPoolClosed);
        }
        debug_assert!(state.occupied_slots() < self.capacity);
        if state.occupied_slots() >= self.capacity {
            return Err(Error::PrewarmPoolClosed);
        }
        state.creating += 1;
        drop(state);
        Ok(PrewarmCreation {
            pool: self.clone(),
            active: true,
        })
    }

    fn take_ready(&self) -> Option<PreparedDatabase> {
        let mut state = self.lock_state();
        if !state.accepting {
            return None;
        }
        let prepared = state.ready.pop_front()?;
        state.leased += 1;
        debug_assert!(state.occupied_slots() <= self.capacity);
        Some(prepared)
    }

    fn begin_return(self: &Arc<Self>) -> PrewarmReturn {
        let mut state = self.lock_state();
        debug_assert!(state.leased > 0);
        state.leased = state.leased.saturating_sub(1);
        state.deleting += 1;
        debug_assert!(state.occupied_slots() <= self.capacity);
        drop(state);
        PrewarmReturn {
            pool: self.clone(),
            phase: PrewarmReturnPhase::Deleting,
        }
    }

    fn finish_creation(
        self: &Arc<Self>,
        prepared: PreparedDatabase,
    ) -> Option<(PreparedDatabase, ClosedPrewarmDeletion)> {
        let mut state = self.lock_state();
        debug_assert!(state.creating > 0);
        state.creating = state.creating.saturating_sub(1);
        if state.accepting && !state.cleanup_started {
            state.ready.push_back(prepared);
            debug_assert!(state.occupied_slots() <= self.capacity);
            drop(state);
            self.ready_admission.add_permits(1);
            None
        } else {
            state.deleting += 1;
            debug_assert!(state.occupied_slots() <= self.capacity);
            Some((
                prepared,
                ClosedPrewarmDeletion {
                    pool: self.clone(),
                    active: true,
                },
            ))
        }
    }

    fn fail_phase(self: &Arc<Self>, phase: PrewarmReturnPhase) {
        let ready = self
            .lock_state()
            .fail_and_begin_cleanup(phase, self.capacity);
        self.finish_close(ready);
    }

    pub(crate) fn close_and_queue_ready(self: &Arc<Self>) {
        let ready = self.lock_state().begin_cleanup(self.capacity);
        self.finish_close(ready);
    }

    fn finish_close(self: &Arc<Self>, ready: Option<VecDeque<PreparedDatabase>>) {
        self.ready_admission.close();
        self.closed.close();
        let Some(ready) = ready else {
            return;
        };
        for prepared in ready {
            queue_closed_prepared_database(self.clone(), prepared);
        }
    }
}

async fn acquire_database_permit_or_pool_close<F>(
    database_admission: F,
    pool_closed: Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit>
where
    F: Future<Output = Result<OwnedSemaphorePermit>>,
{
    let mut database_admission = std::pin::pin!(database_admission);
    let mut pool_closed = std::pin::pin!(pool_closed.acquire_owned());
    std::future::poll_fn(|context| {
        if let Poll::Ready(result) = database_admission.as_mut().poll(context) {
            return Poll::Ready(result);
        }
        if pool_closed.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(Error::PrewarmPoolClosed));
        }
        Poll::Pending
    })
    .await
}

struct PreparedDatabase {
    name: DatabaseName,
    database_url: String,
}

impl PreparedDatabase {
    fn into_lease(
        self,
        permit: OwnedSemaphorePermit,
        prewarm_pool: Weak<PrewarmPoolInner>,
    ) -> DatabaseLease {
        DatabaseLease {
            inner: Some(DatabaseLeaseInner {
                server: prewarm_pool
                    .upgrade()
                    .expect("a leased prewarmed database retains its public pool")
                    .template
                    .server()
                    .clone(),
                name: self.name,
                permit,
                prewarm_pool: Some(prewarm_pool),
            }),
            database_url: self.database_url,
        }
    }
}

struct UnpublishedDatabase {
    server: Arc<ServerInner>,
    prepared: Option<PreparedDatabase>,
}

impl UnpublishedDatabase {
    fn publish(mut self) -> PreparedDatabase {
        self.prepared
            .take()
            .expect("an unpublished database is published at most once")
    }
}

impl Drop for UnpublishedDatabase {
    fn drop(&mut self) {
        if let Some(prepared) = self.prepared.take() {
            queue_unpublished_database(self.server.clone(), prepared);
        }
    }
}

struct PrewarmCreation {
    pool: Arc<PrewarmPoolInner>,
    active: bool,
}

impl PrewarmCreation {
    fn publish(mut self, unpublished: UnpublishedDatabase) {
        let prepared = unpublished.publish();
        if let Some((prepared, deletion)) = self.pool.finish_creation(prepared) {
            queue_counted_prepared_database(
                self.pool.template.server().clone(),
                prepared,
                deletion,
            );
        }
        self.active = false;
    }

    fn fail(mut self) {
        self.pool.fail_phase(PrewarmReturnPhase::Creating);
        self.active = false;
    }
}

impl Drop for PrewarmCreation {
    fn drop(&mut self) {
        if self.active {
            self.pool.fail_phase(PrewarmReturnPhase::Creating);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrewarmReturnPhase {
    Deleting,
    Creating,
    Complete,
}

struct PrewarmReturn {
    pool: Arc<PrewarmPoolInner>,
    phase: PrewarmReturnPhase,
}

impl PrewarmReturn {
    fn after_drop(&mut self) -> bool {
        let mut state = self.pool.lock_state();
        debug_assert_eq!(self.phase, PrewarmReturnPhase::Deleting);
        debug_assert!(state.deleting > 0);
        state.deleting = state.deleting.saturating_sub(1);
        if state.accepting && !state.cleanup_started {
            state.creating += 1;
            self.phase = PrewarmReturnPhase::Creating;
            debug_assert!(state.occupied_slots() <= self.pool.capacity);
            true
        } else {
            self.phase = PrewarmReturnPhase::Complete;
            false
        }
    }

    fn publish(
        &mut self,
        prepared: PreparedDatabase,
    ) -> Option<(PreparedDatabase, ClosedPrewarmDeletion)> {
        debug_assert_eq!(self.phase, PrewarmReturnPhase::Creating);
        let unpublished = self.pool.finish_creation(prepared);
        self.phase = PrewarmReturnPhase::Complete;
        unpublished
    }
}

impl Drop for PrewarmReturn {
    fn drop(&mut self) {
        if self.phase != PrewarmReturnPhase::Complete {
            self.pool.fail_phase(self.phase);
            self.phase = PrewarmReturnPhase::Complete;
        }
    }
}

struct ClosedPrewarmDeletion {
    pool: Arc<PrewarmPoolInner>,
    active: bool,
}

impl ClosedPrewarmDeletion {
    fn finish(mut self) {
        let mut state = self.pool.lock_state();
        debug_assert!(state.deleting > 0);
        state.deleting = state.deleting.saturating_sub(1);
        self.active = false;
    }
}

impl Drop for ClosedPrewarmDeletion {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.pool.lock_state();
            debug_assert!(state.deleting > 0);
            state.deleting = state.deleting.saturating_sub(1);
        }
    }
}

/// Exclusive ownership of one disposable test database.
///
/// Dropping a lease enqueues cleanup without waiting for queue capacity. The
/// fallback retains its connection-budget permit until cleanup finishes, so
/// saturation delays later database acquisition instead of blocking `Drop`.
pub struct DatabaseLease {
    inner: Option<DatabaseLeaseInner>,
    database_url: String,
}

impl DatabaseLease {
    pub fn database_name(&self) -> &str {
        self.inner
            .as_ref()
            .expect("database lease is present until cleanup consumes it")
            .name
            .as_str()
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Drops this database and waits for confirmation.
    ///
    /// The database's connection-budget permit remains held until cleanup
    /// finishes. A failed database drop closes new database admission for this
    /// harness so residual storage remains bounded. Close all application
    /// connections and pools before calling.
    pub async fn cleanup(mut self) -> Result<()> {
        let inner = self
            .inner
            .take()
            .expect("database lease cleanup runs at most once");
        let completion = run_blocking(move || inner.queue_awaited_cleanup()).await?;
        completion
            .await
            .map_err(|_| Error::CleanupWorkerStopped)?
            .finish()
    }

    /// Returns this database to the bounded server-scoped cleanup queue.
    ///
    /// This future completes once the queue accepts the database, not when its
    /// `DROP` finishes. It applies backpressure while the bounded queue is full,
    /// then releases the database's connection-budget permit. Call
    /// [`PostgresHarness::drain_deferred_cleanup`] before process or external
    /// server teardown to await final completion and observe cleanup failures.
    /// A failure that may leave a residual database closes new database
    /// admission for this harness. Close all application connections and pools
    /// before calling.
    pub async fn defer_cleanup(mut self) -> Result<()> {
        let inner = self
            .inner
            .take()
            .expect("database lease cleanup runs at most once");
        run_blocking(move || inner.queue_deferred_cleanup()).await
    }
}

impl std::fmt::Debug for DatabaseLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DatabaseLease")
            .field(
                "database_name",
                &self.inner.as_ref().map(|inner| inner.name.as_str()),
            )
            .field("database_url", &"[REDACTED]")
            .finish()
    }
}

impl Drop for DatabaseLease {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take()
            && let Err(error) = inner.queue_fallback_cleanup()
        {
            // Owned shutdown removes the containing server. On an external
            // server the tagged residual is left for the next stale sweep.
            log_cleanup(format_args!(
                "fallback database cleanup could not be queued: {error}"
            ));
        }
    }
}

struct DatabaseLeaseInner {
    server: Arc<ServerInner>,
    name: DatabaseName,
    permit: OwnedSemaphorePermit,
    prewarm_pool: Option<Weak<PrewarmPoolInner>>,
}

impl DatabaseLeaseInner {
    fn queue_awaited_cleanup(
        self,
    ) -> Result<tokio::sync::oneshot::Receiver<crate::cleanup::AwaitedCleanupOutcome>> {
        let Self {
            server,
            name,
            permit,
            prewarm_pool,
        } = self;
        let database_name = name.as_str().to_owned();
        let database_cleanup = server.database_cleanup.clone();
        let prewarm_return = prewarm_pool
            .and_then(|pool| pool.upgrade())
            .map(|pool| pool.begin_return());
        database_cleanup.submit_awaited_outcome(
            database_name,
            permit,
            database_cleanup_operation(server, name, prewarm_return),
        )
    }

    fn queue_deferred_cleanup(self) -> Result<()> {
        let Self {
            server,
            name,
            permit,
            prewarm_pool,
        } = self;
        let database_name = name.as_str().to_owned();
        let database_cleanup = server.database_cleanup.clone();
        let prewarm_return = prewarm_pool
            .and_then(|pool| pool.upgrade())
            .map(|pool| pool.begin_return());
        database_cleanup.submit_deferred_outcome(
            database_name,
            Some(permit),
            database_cleanup_operation(server, name, prewarm_return),
        )
    }

    fn queue_fallback_cleanup(self) -> Result<()> {
        let Self {
            server,
            name,
            permit,
            prewarm_pool,
        } = self;
        let database_name = name.as_str().to_owned();
        let database_cleanup = server.database_cleanup.clone();
        let prewarm_return = prewarm_pool
            .and_then(|pool| pool.upgrade())
            .map(|pool| pool.begin_return());
        database_cleanup.submit_fallback_outcome(
            database_name,
            permit,
            database_cleanup_operation(server, name, prewarm_return),
        )
    }
}

fn database_cleanup_operation(
    server: Arc<ServerInner>,
    name: DatabaseName,
    mut prewarm_return: Option<PrewarmReturn>,
) -> impl FnOnce() -> CleanupOutcome + Send + 'static {
    move || {
        let operation_server = server.clone();
        let outcome = server.with_lifecycle_admin_disposition(
            "connect for disposable database cleanup",
            move |client| {
                if let Err(error) = drop_database(client, &name) {
                    return AdminSessionDisposition::Evict(CleanupOutcome::residual_possible(
                        error,
                    ));
                }
                let Some(ref mut refill) = prewarm_return else {
                    return AdminSessionDisposition::Reuse(CleanupOutcome::succeeded());
                };
                if !refill.after_drop() {
                    return AdminSessionDisposition::Reuse(CleanupOutcome::succeeded());
                }

                let fresh_name = DatabaseName::test(&operation_server.project);
                if let Err(error) = operation_server.create_managed_database_classified(
                    client,
                    &fresh_name,
                    refill.pool.template.name().as_str(),
                    &ResourceMetadata::test(
                        operation_server.project.clone(),
                        operation_server.owner_key,
                    ),
                ) {
                    return match error {
                        ManagedDatabaseCreationFailure::NoResidual(error) => {
                            AdminSessionDisposition::Reuse(CleanupOutcome::no_residual(error))
                        }
                        ManagedDatabaseCreationFailure::ResidualPossible(error) => {
                            AdminSessionDisposition::Evict(CleanupOutcome::residual_possible(error))
                        }
                    };
                }
                let prepared = PreparedDatabase {
                    database_url: operation_server.admin_url.database_url(&fresh_name),
                    name: fresh_name,
                };
                if let Some((prepared, deletion)) = refill.publish(prepared) {
                    // The pool closed after reserving this replacement. Remove it
                    // on the same admin session so the enclosing cleanup barrier
                    // still covers the complete slot transition.
                    let result = drop_database(client, &prepared.name);
                    deletion.finish();
                    if let Err(error) = result {
                        return AdminSessionDisposition::Evict(CleanupOutcome::residual_possible(
                            error,
                        ));
                    }
                }
                AdminSessionDisposition::Reuse(CleanupOutcome::succeeded())
            },
        );
        match outcome {
            Ok(outcome) => outcome,
            Err(error) => CleanupOutcome::residual_possible(error),
        }
    }
}

/// Retains a managed source through blocking copy and metadata work.
#[derive(Clone)]
enum TemplateSource {
    Empty,
    Template(Arc<TemplateInner>),
}

impl TemplateSource {
    fn name(&self) -> &str {
        match self {
            Self::Empty => "template0",
            Self::Template(template) => template.name().as_str(),
        }
    }
}

async fn get_or_initialize_template<F, Fut>(
    server: Arc<ServerInner>,
    identity: TemplateIdentity,
    source: TemplateSource,
    initializer: F,
) -> Result<DatabaseTemplate>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = std::result::Result<(), BoxError>>,
{
    let fingerprint = identity.fingerprint();
    let mut initializer = Some(initializer);
    loop {
        match server.template_cache_action(fingerprint) {
            TemplateCacheAction::Ready(inner) => return Ok(DatabaseTemplate { inner }),
            TemplateCacheAction::Wait(waiter) => {
                if let Some(inner) = waiter.wait().await {
                    return Ok(DatabaseTemplate { inner });
                }
            }
            TemplateCacheAction::Initialize(cache_initializer) => {
                let name = DatabaseName::template(&server.project, fingerprint);
                let preparation_server = server.clone();
                let preparation_source = source.clone();
                let preparation = run_blocking(move || {
                    begin_template(preparation_server, name, identity, preparation_source)
                })
                .await?;

                let template_lock = match preparation {
                    TemplatePreparation::Ready(template_lock) => template_lock,
                    TemplatePreparation::Initialize(initialization) => {
                        let database_url = initialization
                            .server
                            .admin_url
                            .database_url(&initialization.name);
                        let initialize = initializer
                            .take()
                            .expect("a template caller initializes at most once");
                        match initialize(database_url).await {
                            Ok(()) => run_blocking(move || initialization.finish()).await?,
                            Err(initializer) => {
                                let cleanup = run_blocking(move || initialization.abort()).await;
                                return match cleanup {
                                    Ok(()) => Err(Error::TemplateInitializer {
                                        source: initializer,
                                    }),
                                    Err(cleanup) => Err(Error::TemplateInitializerAndCleanup {
                                        initializer,
                                        cleanup: Box::new(cleanup),
                                    }),
                                };
                            }
                        }
                    }
                };

                let inner = Arc::new(TemplateInner::new(
                    server.clone(),
                    DatabaseName::template(&server.project, fingerprint),
                    fingerprint,
                    template_lock,
                ));
                return Ok(DatabaseTemplate {
                    inner: cache_initializer.publish(inner),
                });
            }
        }
    }
}

enum TemplatePreparation {
    Ready(PersistentClient),
    Initialize(TemplateInitialization),
}

struct TemplateInitialization {
    server: Arc<ServerInner>,
    name: DatabaseName,
    identity: TemplateIdentity,
    lock_key: i64,
    coordination_key: i64,
    exclusive_lock: PersistentClient,
}

impl TemplateInitialization {
    fn finish(self) -> Result<PersistentClient> {
        let Self {
            server,
            name,
            identity,
            lock_key,
            coordination_key,
            mut exclusive_lock,
        } = self;
        let client = exclusive_lock.client_mut();
        disable_database_connections(client, &name)?;
        terminate_database_connections(client, &name, server.operation_timeout)?;
        set_database_metadata(
            client,
            &name,
            &ResourceMetadata::template(
                server.project.clone(),
                lock_key,
                identity,
                TemplateState::Ready,
            ),
        )?;
        release_advisory_lock(client, lock_key)?;
        let ready =
            acquire_and_inspect_shared_template_lock(client, &server, &name, identity, lock_key)?;
        release_advisory_lock(client, coordination_key)?;
        if ready {
            Ok(exclusive_lock)
        } else {
            Err(Error::InconsistentMetadata {
                database_name: name.as_str().to_owned(),
            })
        }
    }

    fn abort(self) -> Result<()> {
        let Self {
            name,
            lock_key,
            coordination_key,
            mut exclusive_lock,
            ..
        } = self;
        let client = exclusive_lock.client_mut();
        let cleanup = drop_database(client, &name);
        let unlock_template = release_advisory_lock(client, lock_key);
        let unlock_coordination = release_advisory_lock(client, coordination_key);
        cleanup.and(unlock_template).and(unlock_coordination)
    }
}

fn begin_template(
    server: Arc<ServerInner>,
    name: DatabaseName,
    identity: TemplateIdentity,
    source: TemplateSource,
) -> Result<TemplatePreparation> {
    let lock_key = template_lock_key(&server.project, identity.fingerprint());
    let coordination_key = template_coordination_key(&server.project, identity.fingerprint());
    let mut persistent_client = PersistentClient::new(connect_admin(
        &server.admin_url,
        server.operation_timeout,
        "connect for PostgreSQL template coordination",
    )?);
    loop {
        let client = persistent_client.client_mut();
        if acquire_and_inspect_shared_template_lock(client, &server, &name, identity, lock_key)? {
            return Ok(TemplatePreparation::Ready(persistent_client));
        }

        // Serialize upgrades so a retained ready shared lock cannot strand an
        // earlier coordination attempt waiting for the exclusive lock.
        acquire_template_advisory_lock(
            client,
            coordination_key,
            server.template_wait_timeout,
            server.operation_timeout,
        )?;
        if acquire_and_inspect_shared_template_lock(client, &server, &name, identity, lock_key)? {
            release_advisory_lock(client, coordination_key)?;
            return Ok(TemplatePreparation::Ready(persistent_client));
        }

        acquire_template_advisory_lock(
            client,
            lock_key,
            server.template_wait_timeout,
            server.operation_timeout,
        )?;
        if template_is_ready(client, &server, &name, identity, lock_key)? {
            release_advisory_lock(client, lock_key)?;
            let ready = acquire_and_inspect_shared_template_lock(
                client, &server, &name, identity, lock_key,
            )?;
            release_advisory_lock(client, coordination_key)?;
            if ready {
                return Ok(TemplatePreparation::Ready(persistent_client));
            }
            continue;
        }
        if let Some(record) = find_database(client, &name)? {
            let recoverable = record
                .comment
                .as_deref()
                .and_then(ResourceMetadata::parse)
                .is_some_and(|metadata| {
                    recoverable_template_initialization(
                        &metadata,
                        &server.project,
                        identity,
                        lock_key,
                    )
                });
            if !recoverable {
                return Err(Error::InconsistentMetadata {
                    database_name: name.as_str().to_owned(),
                });
            }
            drop_database(client, &name)?;
        }
        server
            .create_managed_database_classified(
                client,
                &name,
                source.name(),
                &ResourceMetadata::template(
                    server.project.clone(),
                    lock_key,
                    identity,
                    TemplateState::Initializing,
                ),
            )
            .map_err(ManagedDatabaseCreationFailure::into_error)?;
        // An already-running worker can outlive its async caller. Release its
        // source only after CREATE and metadata tagging have finished.
        drop(source);
        return Ok(TemplatePreparation::Initialize(TemplateInitialization {
            server,
            name,
            identity,
            lock_key,
            coordination_key,
            exclusive_lock: persistent_client,
        }));
    }
}

fn template_lock_key(project: &ProjectName, fingerprint: TemplateFingerprint) -> i64 {
    advisory_key(
        "template",
        &format!("{}:{}", project.as_str(), fingerprint.to_hex()),
    )
}

fn template_coordination_key(project: &ProjectName, fingerprint: TemplateFingerprint) -> i64 {
    advisory_key(
        "template-coordination",
        &format!("{}:{}", project.as_str(), fingerprint.to_hex()),
    )
}

fn acquire_and_inspect_shared_template_lock(
    client: &mut AdminClient,
    server: &ServerInner,
    name: &DatabaseName,
    identity: TemplateIdentity,
    lock_key: i64,
) -> Result<bool> {
    acquire_shared_template_advisory_lock(
        client,
        lock_key,
        server.template_wait_timeout,
        server.operation_timeout,
    )?;
    if template_is_ready(client, server, name, identity, lock_key)? {
        Ok(true)
    } else {
        release_shared_advisory_lock(client, lock_key)?;
        Ok(false)
    }
}

fn recoverable_template_initialization(
    metadata: &ResourceMetadata,
    project: &ProjectName,
    identity: TemplateIdentity,
    lock_key: i64,
) -> bool {
    template_metadata_matches(
        metadata,
        project,
        identity,
        lock_key,
        TemplateState::Initializing,
    )
}

fn template_is_ready(
    client: &mut AdminClient,
    server: &ServerInner,
    name: &DatabaseName,
    identity: TemplateIdentity,
    lock_key: i64,
) -> Result<bool> {
    let record = find_database(client, name)?;
    Ok(template_record_is_ready(
        record.as_ref(),
        &server.project,
        identity,
        lock_key,
    ))
}

fn template_record_is_ready(
    record: Option<&DatabaseRecord>,
    project: &ProjectName,
    identity: TemplateIdentity,
    lock_key: i64,
) -> bool {
    record
        .and_then(|record| record.comment.as_deref())
        .and_then(ResourceMetadata::parse)
        .is_some_and(|metadata| {
            template_metadata_matches(&metadata, project, identity, lock_key, TemplateState::Ready)
        })
}

/// Requires the recorded parent too: a database built from a different
/// source lacks the inherited data its fingerprint promises.
fn template_metadata_matches(
    metadata: &ResourceMetadata,
    project: &ProjectName,
    identity: TemplateIdentity,
    lock_key: i64,
    state: TemplateState,
) -> bool {
    matches!(
        metadata,
        ResourceMetadata::Template {
            project: metadata_project,
            lock_key: metadata_lock_key,
            fingerprint,
            parent,
            state: metadata_state,
            ..
        } if metadata_project == project
            && *metadata_lock_key == lock_key
            && *fingerprint == identity.fingerprint()
            && *parent == identity.parent()
            && *metadata_state == state
    )
}

async fn create_test_database(
    server: Arc<ServerInner>,
    source: TemplateSource,
) -> Result<DatabaseLease> {
    let permit = server.acquire_database_permit().await?;
    let name = DatabaseName::test(&server.project);
    let database_url = server.admin_url.database_url(&name);
    run_blocking(move || {
        server.with_lifecycle_admin("connect for disposable database creation", |client| {
            server
                .create_managed_database_classified(
                    client,
                    &name,
                    source.name(),
                    &ResourceMetadata::test(server.project.clone(), server.owner_key),
                )
                .map_err(ManagedDatabaseCreationFailure::into_error)
        })?;
        drop(source);
        Ok(DatabaseLease {
            inner: Some(DatabaseLeaseInner {
                server,
                name,
                permit,
                prewarm_pool: None,
            }),
            database_url,
        })
    })
    .await
}

async fn create_unpublished_database(
    server: Arc<ServerInner>,
    source: TemplateSource,
) -> Result<UnpublishedDatabase> {
    run_blocking(move || {
        let name = DatabaseName::test(&server.project);
        let database_url = server.admin_url.database_url(&name);
        server.with_lifecycle_admin("connect for prewarmed database creation", |client| {
            server
                .create_managed_database_classified(
                    client,
                    &name,
                    source.name(),
                    &ResourceMetadata::test(server.project.clone(), server.owner_key),
                )
                .map_err(ManagedDatabaseCreationFailure::into_error)
        })?;
        drop(source);
        Ok(UnpublishedDatabase {
            server,
            prepared: Some(PreparedDatabase { name, database_url }),
        })
    })
    .await
}

fn queue_unpublished_database(server: Arc<ServerInner>, prepared: PreparedDatabase) {
    let database_name = prepared.name.as_str().to_owned();
    let database_cleanup = server.database_cleanup.clone();
    if let Err(error) = database_cleanup.submit_prewarmer(database_name, move || {
        server.with_lifecycle_admin("connect for unpublished prewarm cleanup", |client| {
            drop_database(client, &prepared.name)
        })
    }) {
        // Owned shutdown removes the containing server. On an external server,
        // the tagged residual remains eligible for owner-aware stale cleanup.
        log_cleanup(format_args!(
            "unpublished prewarmed database cleanup could not be queued: {error}"
        ));
    }
}

fn queue_closed_prepared_database(pool: Arc<PrewarmPoolInner>, prepared: PreparedDatabase) {
    let server = pool.template.server().clone();
    let deletion = ClosedPrewarmDeletion { pool, active: true };
    queue_counted_prepared_database(server, prepared, deletion);
}

fn queue_counted_prepared_database(
    server: Arc<ServerInner>,
    prepared: PreparedDatabase,
    deletion: ClosedPrewarmDeletion,
) {
    let database_name = prepared.name.as_str().to_owned();
    let database_cleanup = server.database_cleanup.clone();
    if let Err(error) = database_cleanup.submit_prewarmer(database_name, move || {
        let result = server.with_lifecycle_admin("connect for prewarm pool shutdown", |client| {
            drop_database(client, &prepared.name)
        });
        deletion.finish();
        result
    }) {
        log_cleanup(format_args!(
            "idle prewarmed database cleanup could not be queued: {error}"
        ));
    }
}

/// Counts from an owner-aware stale database cleanup pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct CleanupReport {
    pub dropped_test_databases: usize,
    pub dropped_templates: usize,
    pub skipped_active: usize,
    pub skipped_fresh: usize,
    pub skipped_unrecognized: usize,
}

impl CleanupReport {
    pub fn total_dropped(self) -> usize {
        self.dropped_test_databases + self.dropped_templates
    }
}

/// Drops only old, tagged resources whose PostgreSQL owner lock is not held.
pub async fn cleanup_stale_databases(
    admin_database_url: &str,
    project: impl Into<String>,
    stale_after: Duration,
) -> Result<CleanupReport> {
    let admin_url = AdminDatabaseUrl::parse(admin_database_url)?;
    let project = ProjectName::new(project)?;
    cleanup_stale_with(
        admin_url,
        project,
        stale_after,
        DEFAULT_CLEANUP_OPERATION_TIMEOUT,
    )
    .await
}

async fn cleanup_stale_with(
    admin_url: AdminDatabaseUrl,
    project: ProjectName,
    stale_after: Duration,
    operation_timeout: Duration,
) -> Result<CleanupReport> {
    run_blocking(move || {
        cleanup_stale_blocking(&admin_url, &project, stale_after, operation_timeout)
    })
    .await
}

fn cleanup_stale_blocking(
    admin_url: &AdminDatabaseUrl,
    project: &ProjectName,
    stale_after: Duration,
    operation_timeout: Duration,
) -> Result<CleanupReport> {
    let mut client = connect_admin(
        admin_url,
        operation_timeout,
        "connect for stale PostgreSQL database cleanup",
    )?;
    validate_postgres_18(&mut client)?;
    let records = list_databases(&mut client)?;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut report = CleanupReport::default();

    for record in records {
        let lock_key = match classify_cleanup_record(project, &record, now, stale_after) {
            CleanupClassification::Ignore => continue,
            CleanupClassification::Unrecognized => {
                report.skipped_unrecognized += 1;
                continue;
            }
            CleanupClassification::Fresh => {
                report.skipped_fresh += 1;
                continue;
            }
            CleanupClassification::Candidate { lock_key, .. } => lock_key,
        };
        if !try_acquire_advisory_lock(&mut client, lock_key)? {
            report.skipped_active += 1;
            continue;
        }
        let cleanup_result = cleanup_candidate_after_lock(
            &mut client,
            project,
            &record,
            lock_key,
            now,
            stale_after,
            &mut report,
        );
        let Some(dropped_kind) = finish_locked_cleanup(cleanup_result, || {
            release_advisory_lock(&mut client, lock_key)
        })?
        else {
            continue;
        };
        match dropped_kind {
            DatabaseKind::Test => report.dropped_test_databases += 1,
            DatabaseKind::Template => report.dropped_templates += 1,
        }
    }
    Ok(report)
}

fn cleanup_candidate_after_lock(
    client: &mut AdminClient,
    project: &ProjectName,
    snapshot: &DatabaseRecord,
    held_key: i64,
    now: u64,
    stale_after: Duration,
    report: &mut CleanupReport,
) -> Result<Option<DatabaseKind>> {
    let snapshot_name = DatabaseName::from_existing(project, snapshot.name.clone())
        .expect("classified cleanup candidate has a valid managed database name");
    let current = find_database(client, &snapshot_name)?;
    let Some(name) = revalidate_cleanup_candidate(
        project,
        snapshot,
        current.as_ref(),
        held_key,
        now,
        stale_after,
        report,
    ) else {
        return Ok(None);
    };
    drop_database(client, &name)?;
    Ok(Some(name.kind()))
}

fn finish_locked_cleanup<T>(
    operation: Result<T>,
    unlock: impl FnOnce() -> Result<()>,
) -> Result<T> {
    let unlock = unlock();
    match operation {
        Ok(value) => {
            unlock?;
            Ok(value)
        }
        Err(error) => Err(error),
    }
}

enum CleanupClassification {
    Ignore,
    Unrecognized,
    Fresh,
    Candidate { name: DatabaseName, lock_key: i64 },
}

fn classify_cleanup_record(
    project: &ProjectName,
    record: &DatabaseRecord,
    now: u64,
    stale_after: Duration,
) -> CleanupClassification {
    let Some(name) = DatabaseName::from_existing(project, record.name.clone()) else {
        return CleanupClassification::Ignore;
    };
    let Some(metadata) = record.comment.as_deref().and_then(ResourceMetadata::parse) else {
        return CleanupClassification::Unrecognized;
    };
    if metadata.project() != project || !metadata_matches_name(&metadata, &name) {
        return CleanupClassification::Unrecognized;
    }
    if now.saturating_sub(metadata.created_at()) < stale_after.as_secs() {
        return CleanupClassification::Fresh;
    }
    CleanupClassification::Candidate {
        name,
        lock_key: metadata.lock_key(),
    }
}

fn revalidate_cleanup_candidate(
    project: &ProjectName,
    snapshot: &DatabaseRecord,
    current: Option<&DatabaseRecord>,
    held_key: i64,
    now: u64,
    stale_after: Duration,
    report: &mut CleanupReport,
) -> Option<DatabaseName> {
    let current = current?;
    match classify_cleanup_record(project, current, now, stale_after) {
        CleanupClassification::Ignore => None,
        CleanupClassification::Unrecognized => {
            report.skipped_unrecognized += 1;
            None
        }
        CleanupClassification::Fresh => {
            report.skipped_fresh += 1;
            None
        }
        CleanupClassification::Candidate { name, lock_key }
            if current == snapshot && lock_key == held_key =>
        {
            Some(name)
        }
        CleanupClassification::Candidate { .. } => None,
    }
}

fn metadata_matches_name(metadata: &ResourceMetadata, name: &DatabaseName) -> bool {
    match (metadata, name.kind()) {
        (ResourceMetadata::Test { .. }, DatabaseKind::Test) => true,
        (
            ResourceMetadata::Template {
                project,
                fingerprint,
                ..
            },
            DatabaseKind::Template,
        ) => DatabaseName::template(project, *fingerprint) == *name,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };

    use tokio::sync::{Semaphore, oneshot};

    use super::{
        CleanupClassification, CleanupReport, PreparedDatabase, PrewarmPoolState,
        PrewarmReturnPhase, acquire_database_permit_or_pool_close, classify_cleanup_record,
        finish_locked_cleanup, recoverable_template_initialization, revalidate_cleanup_candidate,
        run_blocking, template_coordination_key, template_lock_key, template_record_is_ready,
    };
    use crate::{
        Error, FingerprintBuilder, ProjectName, TemplateFingerprint,
        admin::DatabaseRecord,
        fingerprint::TemplateIdentity,
        metadata::{ResourceMetadata, TemplateState},
        name::{DatabaseKind, DatabaseName},
    };

    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn prewarm_close_wakes_database_capacity_wait() {
        let database_admission = Arc::new(Semaphore::new(0));
        let pool_closed = Arc::new(Semaphore::new(0));
        let (started_sender, started_receiver) = oneshot::channel();
        let waiting_admission = database_admission.clone();
        let waiting_close = pool_closed.clone();
        let waiter = tokio::spawn(async move {
            acquire_database_permit_or_pool_close(
                async move {
                    let _ = started_sender.send(());
                    waiting_admission
                        .acquire_owned()
                        .await
                        .map_err(|_| Error::ConnectionBudgetClosed)
                },
                waiting_close,
            )
            .await
        });
        started_receiver
            .await
            .expect("database-capacity wait should start");

        pool_closed.close();
        let error = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("pool closure should wake the database-capacity wait")
            .expect("capacity waiter should not panic")
            .expect_err("a closed prewarm pool must reject the checkout");
        assert!(matches!(error, Error::PrewarmPoolClosed));
    }

    #[test]
    fn failed_prewarm_phase_begins_cleanup_for_every_ready_database() {
        let project = ProjectName::new("prewarm_failure").unwrap();
        let mut state = PrewarmPoolState::new();
        state.ready.extend((0..2).map(|_| {
            let name = DatabaseName::test(&project);
            PreparedDatabase {
                database_url: format!("postgres://localhost/{}", name.as_str()),
                name,
            }
        }));
        state.creating = 1;

        let ready = state
            .fail_and_begin_cleanup(PrewarmReturnPhase::Creating, 3)
            .expect("the first failure starts terminal cleanup");

        assert_eq!(ready.len(), 2);
        assert!(state.ready.is_empty());
        assert_eq!(state.creating, 0);
        assert_eq!(state.deleting, 2);
        assert_eq!(state.occupied_slots(), 2);
        assert!(!state.accepting);
        assert!(state.cleanup_started);
        assert!(state.begin_cleanup(3).is_none());
    }

    fn test_record(
        project: &ProjectName,
        name: &DatabaseName,
        owner_key: i64,
        created_at: u64,
    ) -> DatabaseRecord {
        DatabaseRecord {
            name: name.as_str().to_owned(),
            comment: Some(
                ResourceMetadata::Test {
                    project: project.clone(),
                    owner_key,
                    created_at,
                }
                .encode(),
            ),
        }
    }

    fn template_record(
        project: &ProjectName,
        fingerprint: TemplateFingerprint,
        parent: Option<TemplateFingerprint>,
        lock_key: i64,
        state: TemplateState,
        created_at: u64,
    ) -> DatabaseRecord {
        DatabaseRecord {
            name: DatabaseName::template(project, fingerprint)
                .as_str()
                .to_owned(),
            comment: Some(
                ResourceMetadata::Template {
                    project: project.clone(),
                    lock_key,
                    fingerprint,
                    parent,
                    state,
                    created_at,
                }
                .encode(),
            ),
        }
    }

    fn root_identity(domain: &str) -> TemplateIdentity {
        TemplateIdentity::root(FingerprintBuilder::new(domain).finish_root())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn detached_blocking_results_are_dropped() {
        let cleaned = Arc::new(AtomicBool::new(false));
        let cleaned_for_task = cleaned.clone();
        let (started_sender, started_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = mpsc::channel();

        let task = tokio::spawn(async move {
            run_blocking(move || {
                let _ = started_sender.send(());
                release_receiver.recv().unwrap();
                Ok(DropProbe(cleaned_for_task))
            })
            .await
        });

        started_receiver.await.unwrap();
        task.abort();
        release_sender.send(()).unwrap();
        let join_error = match task.await {
            Ok(_) => panic!("cancelled task unexpectedly completed"),
            Err(error) => error,
        };
        assert!(join_error.is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cleaned.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached blocking result should be cleaned");
    }

    #[test]
    fn template_lock_domains_are_distinct_and_namespaced_by_project() {
        let fingerprint = root_identity("schema").fingerprint();
        let left = template_lock_key(&ProjectName::new("left").unwrap(), fingerprint);
        let right = template_lock_key(&ProjectName::new("right").unwrap(), fingerprint);
        let coordination =
            template_coordination_key(&ProjectName::new("left").unwrap(), fingerprint);
        assert_ne!(left, right);
        assert_ne!(left, coordination);
    }

    #[test]
    fn only_matching_initializing_templates_are_recoverable() {
        let project = ProjectName::new("creditkit").unwrap();
        let identity = root_identity("schema");
        let lock_key = template_lock_key(&project, identity.fingerprint());
        let initializing = ResourceMetadata::template(
            project.clone(),
            lock_key,
            identity,
            TemplateState::Initializing,
        );
        let ready =
            ResourceMetadata::template(project.clone(), lock_key, identity, TemplateState::Ready);
        assert!(recoverable_template_initialization(
            &initializing,
            &project,
            identity,
            lock_key
        ));
        assert!(!recoverable_template_initialization(
            &ready, &project, identity, lock_key
        ));
        assert!(!recoverable_template_initialization(
            &initializing,
            &ProjectName::new("another").unwrap(),
            identity,
            lock_key
        ));
    }

    #[test]
    fn exact_template_records_preserve_catalog_states() {
        let project = ProjectName::new("creditkit").unwrap();
        let identity = root_identity("schema");
        let lock_key = template_lock_key(&project, identity.fingerprint());
        let name = DatabaseName::template(&project, identity.fingerprint())
            .as_str()
            .to_owned();
        let untagged = DatabaseRecord {
            name: name.clone(),
            comment: None,
        };
        let initializing = DatabaseRecord {
            name: name.clone(),
            comment: Some(
                ResourceMetadata::template(
                    project.clone(),
                    lock_key,
                    identity,
                    TemplateState::Initializing,
                )
                .encode(),
            ),
        };
        let ready = DatabaseRecord {
            name,
            comment: Some(
                ResourceMetadata::template(
                    project.clone(),
                    lock_key,
                    identity,
                    TemplateState::Ready,
                )
                .encode(),
            ),
        };

        assert!(!template_record_is_ready(
            None, &project, identity, lock_key
        ));
        assert!(!template_record_is_ready(
            Some(&untagged),
            &project,
            identity,
            lock_key
        ));
        assert!(!template_record_is_ready(
            Some(&initializing),
            &project,
            identity,
            lock_key
        ));
        assert!(template_record_is_ready(
            Some(&ready),
            &project,
            identity,
            lock_key
        ));
    }

    #[test]
    fn template_records_must_match_the_requested_lineage() {
        let project = ProjectName::new("creditkit").unwrap();
        let root = root_identity("schema");
        let other_root = root_identity("other-schema");
        let child = TemplateIdentity::derived(
            root.fingerprint(),
            FingerprintBuilder::new("fixture").finish_step(),
        );
        let child_key = template_lock_key(&project, child.fingerprint());
        let accepts_child = |parent, state| {
            let record =
                template_record(&project, child.fingerprint(), parent, child_key, state, 10);
            let metadata = ResourceMetadata::parse(record.comment.as_deref().unwrap()).unwrap();
            match state {
                TemplateState::Ready => {
                    template_record_is_ready(Some(&record), &project, child, child_key)
                }
                TemplateState::Initializing => {
                    recoverable_template_initialization(&metadata, &project, child, child_key)
                }
            }
        };

        for state in [TemplateState::Ready, TemplateState::Initializing] {
            assert!(accepts_child(Some(root.fingerprint()), state));
            // A template0 build tagged with the child's fingerprint lacks the
            // parent's data, as does a copy of an unrelated parent.
            assert!(!accepts_child(None, state));
            assert!(!accepts_child(Some(other_root.fingerprint()), state));
        }

        let root_key = template_lock_key(&project, root.fingerprint());
        let parented_root = template_record(
            &project,
            root.fingerprint(),
            Some(other_root.fingerprint()),
            root_key,
            TemplateState::Ready,
            10,
        );
        assert!(!template_record_is_ready(
            Some(&parented_root),
            &project,
            root,
            root_key
        ));
    }

    #[test]
    fn cleanup_classification_is_conservative_without_postgres() {
        let project = ProjectName::new("creditkit").unwrap();
        let test_name = DatabaseName::test(&project);
        let metadata = ResourceMetadata::test(project.clone(), 42);
        let created_at = metadata.created_at();

        let unrecognized = classify_cleanup_record(
            &project,
            &DatabaseRecord {
                name: test_name.as_str().to_owned(),
                comment: None,
            },
            created_at + 100,
            Duration::from_secs(10),
        );
        assert!(matches!(unrecognized, CleanupClassification::Unrecognized));

        let fresh = classify_cleanup_record(
            &project,
            &DatabaseRecord {
                name: test_name.as_str().to_owned(),
                comment: Some(metadata.encode()),
            },
            created_at + 5,
            Duration::from_secs(10),
        );
        assert!(matches!(fresh, CleanupClassification::Fresh));

        let candidate = classify_cleanup_record(
            &project,
            &DatabaseRecord {
                name: test_name.as_str().to_owned(),
                comment: Some(metadata.encode()),
            },
            created_at + 10,
            Duration::from_secs(10),
        );
        assert!(matches!(
            candidate,
            CleanupClassification::Candidate { lock_key: 42, .. }
        ));

        let malformed_name = classify_cleanup_record(
            &project,
            &DatabaseRecord {
                name: "pgh_creditkit_test_backup".to_owned(),
                comment: Some(metadata.encode()),
            },
            created_at + 10,
            Duration::from_secs(10),
        );
        assert!(matches!(malformed_name, CleanupClassification::Ignore));
    }

    #[test]
    fn locked_cleanup_requires_an_unchanged_candidate() {
        let project = ProjectName::new("creditkit").unwrap();
        let test_name = DatabaseName::test(&project);
        let snapshot = test_record(&project, &test_name, 42, 10);
        let mut report = CleanupReport::default();

        let unchanged = revalidate_cleanup_candidate(
            &project,
            &snapshot,
            Some(&snapshot),
            42,
            100,
            Duration::from_secs(10),
            &mut report,
        );
        assert_eq!(
            unchanged.as_ref().map(DatabaseName::kind),
            Some(DatabaseKind::Test)
        );

        let refreshed = test_record(&project, &test_name, 42, 95);
        assert!(
            revalidate_cleanup_candidate(
                &project,
                &snapshot,
                Some(&refreshed),
                42,
                100,
                Duration::from_secs(10),
                &mut report,
            )
            .is_none()
        );
        assert_eq!(report.skipped_fresh, 1);

        let changed_key = test_record(&project, &test_name, 84, 10);
        assert!(
            revalidate_cleanup_candidate(
                &project,
                &snapshot,
                Some(&changed_key),
                42,
                100,
                Duration::from_secs(10),
                &mut report,
            )
            .is_none()
        );
        assert!(
            revalidate_cleanup_candidate(
                &project,
                &snapshot,
                None,
                42,
                100,
                Duration::from_secs(10),
                &mut report,
            )
            .is_none()
        );
    }

    #[test]
    fn locked_cleanup_rejects_a_finalized_template_snapshot() {
        let project = ProjectName::new("creditkit").unwrap();
        let fingerprint = root_identity("cleanup-race").fingerprint();
        let initializing = template_record(
            &project,
            fingerprint,
            None,
            42,
            TemplateState::Initializing,
            10,
        );
        let ready = template_record(&project, fingerprint, None, 42, TemplateState::Ready, 10);
        let mut report = CleanupReport::default();

        assert!(
            revalidate_cleanup_candidate(
                &project,
                &initializing,
                Some(&ready),
                42,
                100,
                Duration::from_secs(10),
                &mut report,
            )
            .is_none()
        );
        assert_eq!(report, CleanupReport::default());
    }

    #[test]
    fn locked_cleanup_keeps_the_operation_error_primary() {
        let unlocked = AtomicBool::new(false);
        let error = finish_locked_cleanup::<()>(
            Err(Error::InvalidConfiguration {
                reason: "locked reread",
            }),
            || {
                unlocked.store(true, Ordering::SeqCst);
                Err(Error::InvalidConfiguration { reason: "unlock" })
            },
        )
        .unwrap_err();
        assert!(unlocked.load(Ordering::SeqCst));
        assert!(matches!(
            error,
            Error::InvalidConfiguration {
                reason: "locked reread"
            }
        ));

        let error = finish_locked_cleanup(Ok(()), || {
            Err(Error::InvalidConfiguration { reason: "unlock" })
        })
        .unwrap_err();
        assert!(matches!(
            error,
            Error::InvalidConfiguration { reason: "unlock" }
        ));
    }
}
