use std::{
    collections::{BTreeSet, VecDeque},
    io::Write as _,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use crate::{DeferredCleanupFailure, Error, Result};

type CleanupOperation = Box<dyn FnOnce() -> Result<()> + Send + 'static>;

/// Server-scoped execution and completion tracking for disposable database
/// cleanup.
///
/// Explicit submissions backpressure at `queue_capacity` waiting operations,
/// while destructor fallbacks remain nonblocking and retain their database
/// permits until completion. Exactly `worker_count` workers can execute cleanup
/// concurrently. A submitted job retains the queue until its completion
/// sequence has been recorded, which lets an external-server harness outlive
/// all public handles while cleanup is still running.
pub(crate) struct DatabaseCleanupQueue {
    submission: Arc<SubmissionQueue>,
    completion: Arc<CompletionState>,
    drain: Mutex<()>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

struct SubmissionQueue {
    capacity: usize,
    state: Mutex<SubmissionState>,
    changed: Condvar,
}

struct SubmissionState {
    jobs: VecDeque<CleanupJob>,
    next_sequence: u64,
    closed: bool,
}

struct CompletionState {
    state: Mutex<CompletionStateInner>,
    changed: Condvar,
    database_admission: Arc<Semaphore>,
}

struct CompletionStateInner {
    completed_through: u64,
    completed_out_of_order: BTreeSet<u64>,
    failures: VecDeque<SequencedFailure>,
    failure_delivery_in_flight: bool,
}

struct SequencedFailure {
    sequence: u64,
    failure: DeferredCleanupFailure,
}

/// Failure delivery from a completed sequence barrier.
///
/// Dropping this value before `finish` restores its failures. This makes a
/// drain cancellation-safe even though the blocking wait continues after its
/// async caller is cancelled.
pub(crate) struct CleanupDrainOutcome {
    completion: Option<Arc<CompletionState>>,
    failures: Option<Vec<SequencedFailure>>,
    queue_guard: Option<Arc<DatabaseCleanupQueue>>,
}

pub(crate) struct AwaitedCleanupOutcome {
    result: Option<Result<()>>,
    sequence: u64,
    database_name: Option<String>,
    queue_guard: Option<Arc<DatabaseCleanupQueue>>,
}

struct CleanupJob {
    sequence: u64,
    database_name: String,
    operation: CleanupOperation,
    completion: CleanupCompletion,
    permit: Option<OwnedSemaphorePermit>,
    queue_guard: Arc<DatabaseCleanupQueue>,
}

enum CleanupCompletion {
    Awaited(oneshot::Sender<AwaitedCleanupOutcome>),
    Deferred,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CleanupAdmission {
    Backpressured,
    NonblockingFallback,
}

impl DatabaseCleanupQueue {
    pub(crate) fn new(
        project: &str,
        worker_count: usize,
        queue_capacity: usize,
        database_admission: Arc<Semaphore>,
    ) -> Result<Arc<Self>> {
        debug_assert!(worker_count > 0);
        debug_assert!(queue_capacity > 0);
        let submission = Arc::new(SubmissionQueue {
            capacity: queue_capacity.max(1),
            state: Mutex::new(SubmissionState {
                jobs: VecDeque::new(),
                next_sequence: 0,
                closed: false,
            }),
            changed: Condvar::new(),
        });
        let completion = Arc::new(CompletionState {
            state: Mutex::new(CompletionStateInner {
                completed_through: 0,
                completed_out_of_order: BTreeSet::new(),
                failures: VecDeque::new(),
                failure_delivery_in_flight: false,
            }),
            changed: Condvar::new(),
            database_admission,
        });
        let mut workers = Vec::with_capacity(worker_count.max(1));
        for index in 0..worker_count.max(1) {
            let worker_submission = submission.clone();
            let worker_completion = completion.clone();
            let thread_name = format!("postgres-test-harness-cleanup-{project}-{index}");
            match std::thread::Builder::new()
                .name(thread_name)
                .spawn(move || cleanup_worker(worker_submission, worker_completion))
            {
                Ok(worker) => workers.push(worker),
                Err(source) => {
                    submission.close();
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(Error::CleanupWorkerStart { source });
                }
            }
        }

        Ok(Arc::new(Self {
            submission,
            completion,
            drain: Mutex::new(()),
            workers: Mutex::new(workers),
        }))
    }

    pub(crate) fn submit_awaited<F>(
        self: &Arc<Self>,
        database_name: String,
        permit: OwnedSemaphorePermit,
        operation: F,
    ) -> Result<oneshot::Receiver<AwaitedCleanupOutcome>>
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        self.enqueue(
            database_name,
            Box::new(operation),
            CleanupCompletion::Awaited(sender),
            Some(permit),
            CleanupAdmission::Backpressured,
        )?;
        Ok(receiver)
    }

    pub(crate) fn submit_deferred<F>(
        self: &Arc<Self>,
        database_name: String,
        permit: Option<OwnedSemaphorePermit>,
        operation: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        self.enqueue(
            database_name,
            Box::new(operation),
            CleanupCompletion::Deferred,
            None,
            CleanupAdmission::Backpressured,
        )?;
        // Deferred submission hands application capacity back only after the
        // bounded queue has accepted responsibility for the residual database.
        drop(permit);
        Ok(())
    }

    /// Enqueues destructor fallback without waiting for queue capacity.
    ///
    /// The retained permit bounds these extra waiting jobs by the live database
    /// limit and transfers any backpressure to the next database acquisition.
    pub(crate) fn submit_fallback<F>(
        self: &Arc<Self>,
        database_name: String,
        permit: OwnedSemaphorePermit,
        operation: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        self.enqueue(
            database_name,
            Box::new(operation),
            CleanupCompletion::Deferred,
            Some(permit),
            CleanupAdmission::NonblockingFallback,
        )
    }

    fn enqueue(
        self: &Arc<Self>,
        database_name: String,
        operation: CleanupOperation,
        completion: CleanupCompletion,
        permit: Option<OwnedSemaphorePermit>,
        admission: CleanupAdmission,
    ) -> Result<()> {
        let mut submission = self
            .submission
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while admission == CleanupAdmission::Backpressured
            && submission.jobs.len() >= self.submission.capacity
            && !submission.closed
        {
            submission = self
                .submission
                .changed
                .wait(submission)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if submission.closed {
            return Err(Error::CleanupQueueClosed);
        }
        submission.next_sequence = submission
            .next_sequence
            .checked_add(1)
            .expect("a process cannot submit u64::MAX database cleanups");
        let sequence = submission.next_sequence;
        let job = CleanupJob {
            sequence,
            database_name,
            operation,
            completion,
            permit,
            queue_guard: self.clone(),
        };
        submission.jobs.push_back(job);
        self.submission.changed.notify_one();
        Ok(())
    }

    /// Waits for every operation admitted before this call and reports each
    /// deferred or otherwise-undeliverable failure exactly once.
    pub(crate) fn begin_drain(self: &Arc<Self>) -> CleanupDrainOutcome {
        let _drain = self
            .drain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let target = self.current_target();
        self.completion
            .wait_and_take(target)
            .retain_queue(self.clone())
    }

    /// Stops accepting work, drains all accepted work, and joins the workers.
    /// Used by owned shutdown before the admin pool or container is closed.
    pub(crate) fn close_and_begin_drain(self: &Arc<Self>) -> CleanupDrainOutcome {
        let _drain = self
            .drain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let target = self.close_submissions();
        let outcome = self
            .completion
            .wait_and_take(target)
            .retain_queue(self.clone());
        self.join_workers();
        outcome
    }

    #[cfg(test)]
    fn drain(self: &Arc<Self>) -> Result<()> {
        self.begin_drain().finish()
    }

    #[cfg(test)]
    fn close_and_drain(self: &Arc<Self>) -> Result<()> {
        self.close_and_begin_drain().finish()
    }

    fn current_target(&self) -> u64 {
        self.submission.current_target()
    }

    fn close_submissions(&self) -> u64 {
        self.submission.close()
    }

    fn join_workers(&self) {
        let current_thread = std::thread::current().id();
        let workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for worker in workers {
            if worker.thread().id() == current_thread {
                // The last server handle may be released by its own final
                // cleanup operation. Dropping this handle detaches the current
                // worker, which exits as soon as this call returns.
                continue;
            }
            let _ = worker.join();
        }
    }
}

impl SubmissionQueue {
    fn next_job(&self) -> Option<CleanupJob> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(job) = state.jobs.pop_front() {
                self.changed.notify_all();
                return Some(job);
            }
            if state.closed {
                return None;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn current_target(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_sequence
    }

    fn close(&self) -> u64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        let target = state.next_sequence;
        self.changed.notify_all();
        target
    }
}

impl Drop for DatabaseCleanupQueue {
    fn drop(&mut self) {
        let target = self.close_submissions();
        if let Err(error) = self.completion.wait_and_take(target).finish() {
            log_cleanup(format_args!(
                "deferred database cleanup was not explicitly drained: {error}"
            ));
        }
        self.join_workers();
    }
}

impl CompletionState {
    fn close_database_admission(&self) {
        self.database_admission.close();
    }

    fn complete(&self, sequence: u64, failure: Option<DeferredCleanupFailure>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(failure) = failure {
            state
                .failures
                .push_back(SequencedFailure { sequence, failure });
        }
        if sequence == state.completed_through + 1 {
            state.completed_through = sequence;
            loop {
                let next = state.completed_through + 1;
                if !state.completed_out_of_order.remove(&next) {
                    break;
                }
                state.completed_through = next;
            }
        } else if sequence > state.completed_through {
            state.completed_out_of_order.insert(sequence);
        }
        self.changed.notify_all();
    }

    fn record_late_failure(&self, sequence: u64, failure: DeferredCleanupFailure) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .failures
            .push_back(SequencedFailure { sequence, failure });
        self.changed.notify_all();
    }

    fn wait_and_take(self: &Arc<Self>, target: u64) -> CleanupDrainOutcome {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.completed_through < target || state.failure_delivery_in_flight {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }

        let all_failures = std::mem::take(&mut state.failures);
        let (reported, retained): (VecDeque<_>, VecDeque<_>) = all_failures
            .into_iter()
            .partition(|failure| failure.sequence <= target);
        state.failures = retained;
        let mut failures = reported.into_iter().collect::<Vec<_>>();
        failures.sort_unstable_by_key(|failure| failure.sequence);
        if !failures.is_empty() {
            state.failure_delivery_in_flight = true;
        }
        drop(state);

        if failures.is_empty() {
            CleanupDrainOutcome {
                completion: None,
                failures: None,
                queue_guard: None,
            }
        } else {
            CleanupDrainOutcome {
                completion: Some(self.clone()),
                failures: Some(failures),
                queue_guard: None,
            }
        }
    }

    fn acknowledge_delivery(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(state.failure_delivery_in_flight);
        state.failure_delivery_in_flight = false;
        self.changed.notify_all();
    }

    fn restore_delivery(&self, failures: Vec<SequencedFailure>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(state.failure_delivery_in_flight);
        state.failures.extend(failures);
        state.failure_delivery_in_flight = false;
        self.changed.notify_all();
    }
}

impl CleanupDrainOutcome {
    fn retain_queue(mut self, queue: Arc<DatabaseCleanupQueue>) -> Self {
        if self.failures.is_some() {
            self.queue_guard = Some(queue);
        }
        self
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        let Some(failures) = self.failures.take() else {
            return Ok(());
        };
        self.completion
            .take()
            .expect("a failure delivery retains its completion state")
            .acknowledge_delivery();
        self.queue_guard.take();
        Err(Error::deferred_cleanup(
            failures
                .into_iter()
                .map(|failure| failure.failure)
                .collect(),
        ))
    }
}

impl Drop for CleanupDrainOutcome {
    fn drop(&mut self) {
        if let Some(failures) = self.failures.take() {
            self.completion
                .take()
                .expect("a failure delivery retains its completion state")
                .restore_delivery(failures);
            self.queue_guard.take();
        }
    }
}

impl AwaitedCleanupOutcome {
    fn new(
        result: Result<()>,
        sequence: u64,
        database_name: String,
        queue_guard: Arc<DatabaseCleanupQueue>,
    ) -> Self {
        let retains_failure = result.is_err();
        Self {
            result: Some(result),
            sequence,
            database_name: retains_failure.then_some(database_name),
            queue_guard: retains_failure.then_some(queue_guard),
        }
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        let result = self
            .result
            .take()
            .expect("an awaited cleanup result is consumed at most once");
        self.database_name.take();
        self.queue_guard.take();
        result
    }
}

impl Drop for AwaitedCleanupOutcome {
    fn drop(&mut self) {
        let Some(Err(error)) = self.result.take() else {
            return;
        };
        let database_name = self
            .database_name
            .take()
            .expect("an unconsumed cleanup error retains its database name");
        let queue = self
            .queue_guard
            .take()
            .expect("an unconsumed cleanup error retains its server queue");
        let failure = DeferredCleanupFailure::new(database_name, error);
        let failure_message = failure.to_string();
        queue.completion.record_late_failure(self.sequence, failure);
        log_cleanup(format_args!("{failure_message}"));
        drop(queue);
    }
}

fn cleanup_worker(submission: Arc<SubmissionQueue>, completion: Arc<CompletionState>) {
    while let Some(job) = submission.next_job() {
        run_cleanup_job(job, &completion);
    }
}

fn run_cleanup_job(job: CleanupJob, completion_state: &CompletionState) {
    let CleanupJob {
        sequence,
        database_name,
        operation,
        completion,
        permit,
        queue_guard,
    } = job;
    let result = match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(result) => result,
        Err(_) => Err(Error::CleanupWorkerPanicked),
    };
    if result.is_err() {
        // A failed DROP leaves a residual database outside the in-flight queue.
        // Closing admission prevents repeated failures from growing storage
        // without bound; already-live leases remain cleanable.
        completion_state.close_database_admission();
    }
    // Awaited cleanup and nonblocking destructor fallback retain application
    // capacity through the database drop.
    drop(permit);

    let failure = match completion {
        CleanupCompletion::Awaited(sender) => {
            let outcome =
                AwaitedCleanupOutcome::new(result, sequence, database_name, queue_guard.clone());
            let _ = sender.send(outcome);
            None
        }
        CleanupCompletion::Deferred => result
            .err()
            .map(|error| DeferredCleanupFailure::new(database_name, error)),
    };
    let failure_message = failure.as_ref().map(ToString::to_string);
    completion_state.complete(sequence, failure);
    if let Some(failure_message) = failure_message {
        log_cleanup(format_args!("{failure_message}"));
    }
    // Keep the queue alive until the sequence is visible to every barrier.
    drop(queue_guard);
}

pub(crate) fn log_cleanup(arguments: std::fmt::Arguments<'_>) {
    let _ = writeln!(
        std::io::stderr().lock(),
        "postgres-test-harness: {arguments}"
    );
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::Duration,
    };

    use super::DatabaseCleanupQueue;
    use crate::Error;

    struct Gate {
        open: Mutex<bool>,
        changed: Condvar,
    }

    impl Gate {
        fn closed() -> Arc<Self> {
            Arc::new(Self {
                open: Mutex::new(false),
                changed: Condvar::new(),
            })
        }

        fn wait(&self) {
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.changed.wait(open).unwrap();
            }
        }

        fn open(&self) {
            *self.open.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    fn test_queue(project: &str, workers: usize, capacity: usize) -> Arc<DatabaseCleanupQueue> {
        DatabaseCleanupQueue::new(
            project,
            workers,
            capacity,
            Arc::new(tokio::sync::Semaphore::new(64)),
        )
        .unwrap()
    }

    #[test]
    fn queue_bounds_concurrency_and_backpressures_submitters() {
        let queue = test_queue("bounded", 2, 2);
        let gate = Gate::closed();
        let running = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        for index in 0..4 {
            let gate = gate.clone();
            let running = running.clone();
            let maximum = maximum.clone();
            queue
                .submit_deferred(format!("database-{index}"), None, move || {
                    let current = running.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(current, Ordering::SeqCst);
                    gate.wait();
                    running.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap();
        }

        let blocked_queue = queue.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let blocked_budget = Arc::new(tokio::sync::Semaphore::new(1));
        let blocked_permit = runtime
            .block_on(blocked_budget.clone().acquire_owned())
            .unwrap();
        let (returned_sender, returned_receiver) = mpsc::channel();
        std::thread::spawn(move || {
            let result = blocked_queue.submit_deferred(
                "database-4".to_owned(),
                Some(blocked_permit),
                || Ok(()),
            );
            let _ = returned_sender.send(result);
        });
        assert!(
            returned_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "a fifth submission must wait behind two workers and two queued jobs"
        );
        assert_eq!(
            blocked_budget.available_permits(),
            0,
            "queue saturation must retain the returning lease's permit"
        );

        gate.open();
        returned_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(blocked_budget.available_permits(), 1);
        queue.drain().unwrap();
        assert_eq!(maximum.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn fallback_submission_does_not_wait_for_queue_capacity() {
        let queue = test_queue("fallback", 1, 1);
        let gate = Gate::closed();
        let worker_gate = gate.clone();
        let (started_sender, started_receiver) = mpsc::channel();
        queue
            .submit_deferred("running".to_owned(), None, move || {
                let _ = started_sender.send(());
                worker_gate.wait();
                Ok(())
            })
            .unwrap();
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("the cleanup worker should start the first job");
        queue
            .submit_deferred("waiting".to_owned(), None, || Ok(()))
            .unwrap();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let budget = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = runtime.block_on(budget.clone().acquire_owned()).unwrap();
        let fallback_queue = queue.clone();
        let (returned_sender, returned_receiver) = mpsc::channel();
        let submitter = std::thread::spawn(move || {
            let result = fallback_queue.submit_fallback("fallback".to_owned(), permit, || Ok(()));
            let _ = returned_sender.send(result);
        });

        returned_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("fallback submission must not wait for worker progress")
            .unwrap();
        submitter.join().unwrap();
        assert_eq!(
            budget.available_permits(),
            0,
            "fallback overflow must retain its permit as the residual bound"
        );
        assert_eq!(
            queue.submission.state.lock().unwrap().jobs.len(),
            2,
            "fallback may exceed only the explicit waiting-queue capacity"
        );

        gate.open();
        queue.drain().unwrap();
        assert_eq!(budget.available_permits(), 1);
    }

    #[test]
    fn drain_reports_deferred_failures_once() {
        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let queue = DatabaseCleanupQueue::new("failure", 2, 2, admission.clone()).unwrap();
        queue
            .submit_deferred("broken-a".to_owned(), None, || {
                Err(Error::InvalidConfiguration {
                    reason: "injected-a",
                })
            })
            .unwrap();
        queue
            .submit_deferred("broken-b".to_owned(), None, || {
                Err(Error::InvalidConfiguration {
                    reason: "injected-b",
                })
            })
            .unwrap();
        let error = queue.drain().unwrap_err();
        let Error::DeferredCleanup {
            failure_count,
            failures,
        } = error
        else {
            panic!("unexpected drain error: {error:?}");
        };
        assert_eq!(failure_count, 2);
        let mut database_names = failures
            .iter()
            .map(|failure| failure.database_name())
            .collect::<Vec<_>>();
        database_names.sort_unstable();
        assert_eq!(database_names, ["broken-a", "broken-b"]);
        assert!(
            failures
                .iter()
                .all(|failure| failure.source_error().to_string().contains("injected"))
        );
        assert!(
            admission.is_closed(),
            "a cleanup failure must stop new databases from growing the residual set"
        );
        queue.drain().unwrap();
    }

    #[test]
    fn cancelled_failure_delivery_is_restored_for_the_next_drain() {
        let queue = test_queue("cancelled-drain", 1, 1);
        queue
            .submit_deferred("broken".to_owned(), None, || {
                Err(Error::InvalidConfiguration {
                    reason: "restore me",
                })
            })
            .unwrap();
        let abandoned = queue.begin_drain();

        let retry_queue = queue.clone();
        let (retry_sender, retry_receiver) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = retry_sender.send(retry_queue.drain());
        });
        assert!(
            retry_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "another drain must not pass an unacknowledged failure delivery"
        );

        drop(abandoned);
        let error = retry_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error,
            Error::DeferredCleanup {
                failure_count: 1,
                ..
            }
        ));
        queue.drain().unwrap();
    }

    #[test]
    fn abandoned_drain_keeps_teardown_alive_until_failure_is_restored() {
        let queue = test_queue("teardown-drain", 1, 1);
        queue
            .submit_deferred("broken".to_owned(), None, || {
                Err(Error::InvalidConfiguration { reason: "teardown" })
            })
            .unwrap();
        let outcome = queue.begin_drain();
        let weak_queue = Arc::downgrade(&queue);
        drop(queue);
        assert!(
            weak_queue.upgrade().is_some(),
            "an in-flight failure delivery must retain its queue"
        );
        drop(outcome);
        assert!(
            weak_queue.upgrade().is_none(),
            "queue teardown should finish after restoring and logging the failure"
        );
    }

    #[test]
    fn awaited_failures_go_to_the_waiter_unless_it_is_cancelled() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let permit = runtime
            .block_on(async {
                Arc::new(tokio::sync::Semaphore::new(1))
                    .acquire_owned()
                    .await
            })
            .unwrap();
        let queue = test_queue("awaited", 1, 1);
        let receiver = queue
            .submit_awaited("direct".to_owned(), permit, || {
                Err(Error::InvalidConfiguration { reason: "direct" })
            })
            .unwrap();
        assert!(matches!(
            runtime.block_on(receiver).unwrap().finish(),
            Err(Error::InvalidConfiguration { reason: "direct" })
        ));
        queue.drain().unwrap();

        let permit = runtime
            .block_on(async {
                Arc::new(tokio::sync::Semaphore::new(1))
                    .acquire_owned()
                    .await
            })
            .unwrap();
        let receiver = queue
            .submit_awaited("cancelled".to_owned(), permit, || {
                Err(Error::InvalidConfiguration {
                    reason: "cancelled",
                })
            })
            .unwrap();
        queue
            .drain()
            .expect("the result is still deliverable to its awaited caller");
        drop(receiver);
        let error = queue.drain().unwrap_err();
        assert!(matches!(
            error,
            Error::DeferredCleanup {
                failure_count: 1,
                ..
            }
        ));
    }

    #[test]
    fn drain_is_a_sequence_barrier_for_out_of_order_completion() {
        let queue = test_queue("barrier", 2, 2);
        let gate = Gate::closed();
        let blocked_gate = gate.clone();
        queue
            .submit_deferred("first".to_owned(), None, move || {
                blocked_gate.wait();
                Ok(())
            })
            .unwrap();
        queue
            .submit_deferred("second".to_owned(), None, || Ok(()))
            .unwrap();

        let draining_queue = queue.clone();
        let (drained_sender, drained_receiver) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = drained_sender.send(draining_queue.drain());
        });
        assert!(
            drained_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "a later completion must not satisfy a barrier while sequence one is running"
        );
        gate.open();
        drained_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
    }

    #[test]
    fn close_drains_accepted_work_and_rejects_later_submissions() {
        let queue = test_queue("shutdown", 1, 1);
        let completed = Arc::new(AtomicUsize::new(0));
        let completed_by_job = completed.clone();
        let gate = Gate::closed();
        let worker_gate = gate.clone();
        queue
            .submit_deferred("accepted".to_owned(), None, move || {
                worker_gate.wait();
                completed_by_job.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        let closing_queue = queue.clone();
        let (closed_sender, closed_receiver) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = closed_sender.send(closing_queue.close_and_drain());
        });
        assert!(
            closed_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "shutdown must wait for work accepted before queue closure"
        );
        gate.open();
        closed_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(matches!(
            queue.submit_deferred("rejected".to_owned(), None, || Ok(())),
            Err(Error::CleanupQueueClosed)
        ));
    }

    #[test]
    fn rejected_awaited_submission_has_no_deferred_error_path() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let budget = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = runtime.block_on(budget.clone().acquire_owned()).unwrap();
        let queue = test_queue("rejected-awaited", 1, 1);
        queue.close_and_drain().unwrap();

        let error = match queue.submit_awaited("rejected".to_owned(), permit, || Ok(())) {
            Ok(_) => panic!("a closed queue must reject awaited cleanup"),
            Err(error) => error,
        };

        assert!(matches!(error, Error::CleanupQueueClosed));
        assert_eq!(budget.available_permits(), 1);
        queue
            .drain()
            .expect("a rejected cleanup must not also enter deferred failure delivery");
    }

    #[test]
    fn permit_handoff_matches_awaited_and_deferred_contracts() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let deferred_budget = Arc::new(tokio::sync::Semaphore::new(1));
        let deferred_permit = runtime
            .block_on(deferred_budget.clone().acquire_owned())
            .unwrap();
        let deferred_gate = Gate::closed();
        let deferred_worker_gate = deferred_gate.clone();
        let deferred_queue = test_queue("deferred-permit", 1, 1);
        deferred_queue
            .submit_deferred("deferred".to_owned(), Some(deferred_permit), move || {
                deferred_worker_gate.wait();
                Ok(())
            })
            .unwrap();
        assert_eq!(
            deferred_budget.available_permits(),
            1,
            "deferred cleanup releases its permit after bounded queue admission"
        );
        deferred_gate.open();
        deferred_queue.drain().unwrap();

        let awaited_budget = Arc::new(tokio::sync::Semaphore::new(1));
        let awaited_permit = runtime
            .block_on(awaited_budget.clone().acquire_owned())
            .unwrap();
        let awaited_gate = Gate::closed();
        let awaited_worker_gate = awaited_gate.clone();
        let awaited_queue = test_queue("awaited-permit", 1, 1);
        let completion = awaited_queue
            .submit_awaited("awaited".to_owned(), awaited_permit, move || {
                awaited_worker_gate.wait();
                Ok(())
            })
            .unwrap();
        assert_eq!(
            awaited_budget.available_permits(),
            0,
            "awaited cleanup retains its permit while DROP is pending"
        );
        awaited_gate.open();
        runtime.block_on(completion).unwrap().finish().unwrap();
        assert_eq!(awaited_budget.available_permits(), 1);
        awaited_queue.drain().unwrap();
    }

    #[test]
    fn separate_server_queues_do_not_head_of_line_block_each_other() {
        let blocked_queue = test_queue("blocked-server", 1, 1);
        let independent_queue = test_queue("independent-server", 1, 1);
        let gate = Gate::closed();
        let worker_gate = gate.clone();
        blocked_queue
            .submit_deferred("blocked".to_owned(), None, move || {
                worker_gate.wait();
                Ok(())
            })
            .unwrap();

        let completed = Arc::new(AtomicUsize::new(0));
        let completed_by_job = completed.clone();
        independent_queue
            .submit_deferred("independent".to_owned(), None, move || {
                completed_by_job.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        independent_queue.drain().unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);

        gate.open();
        blocked_queue.drain().unwrap();
    }
}
