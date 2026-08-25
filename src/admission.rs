use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{Error, Result};

/// Failure from managed-database creation, classified by whether the attempted
/// operation may have left the requested database behind.
pub(crate) enum ManagedDatabaseCreationFailure {
    NoResidual(Error),
    ResidualPossible(Error),
}

impl ManagedDatabaseCreationFailure {
    pub(crate) fn into_error(self) -> Error {
        match self {
            Self::NoResidual(error) | Self::ResidualPossible(error) => error,
        }
    }
}

/// Server-scoped admission for every operation that can create a managed
/// database, plus the downstream permits retained by published leases.
///
/// Creation paths that do not consume downstream permits (templates and
/// prewarm replacements) still pass through `attempt_managed_creation`, so a
/// terminal residual-risk transition cannot be bypassed accidentally.
pub(crate) struct DatabaseAdmission {
    permits: Arc<Semaphore>,
}

impl DatabaseAdmission {
    pub(crate) fn new(permits: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(permits)),
        }
    }

    pub(crate) async fn acquire(&self, permits: u32) -> Result<OwnedSemaphorePermit> {
        self.permits
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| Error::ConnectionBudgetClosed)
    }

    pub(crate) fn ensure_open(&self) -> Result<()> {
        if self.permits.is_closed() {
            Err(Error::ConnectionBudgetClosed)
        } else {
            Ok(())
        }
    }

    pub(crate) fn close(&self) {
        self.permits.close();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.permits.is_closed()
    }

    pub(crate) fn attempt_managed_creation<F>(
        &self,
        operation: F,
    ) -> std::result::Result<(), ManagedDatabaseCreationFailure>
    where
        F: FnOnce() -> std::result::Result<(), ManagedDatabaseCreationFailure>,
    {
        if let Err(error) = self.ensure_open() {
            return Err(ManagedDatabaseCreationFailure::NoResidual(error));
        }

        let result = operation();
        if matches!(
            result,
            Err(ManagedDatabaseCreationFailure::ResidualPossible(_))
        ) {
            self.close();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{DatabaseAdmission, ManagedDatabaseCreationFailure};
    use crate::Error;

    #[test]
    fn managed_creation_admission_is_terminal_only_after_residual_risk() {
        let admission = DatabaseAdmission::new(1);

        let proven = admission.attempt_managed_creation(|| {
            Err(ManagedDatabaseCreationFailure::NoResidual(
                Error::InvalidConfiguration { reason: "proven" },
            ))
        });
        assert!(matches!(
            proven,
            Err(ManagedDatabaseCreationFailure::NoResidual(
                Error::InvalidConfiguration { reason: "proven" }
            ))
        ));
        assert!(!admission.is_closed());

        let ambiguous = admission.attempt_managed_creation(|| {
            Err(ManagedDatabaseCreationFailure::ResidualPossible(
                Error::InvalidConfiguration {
                    reason: "ambiguous",
                },
            ))
        });
        assert!(matches!(
            ambiguous,
            Err(ManagedDatabaseCreationFailure::ResidualPossible(
                Error::InvalidConfiguration {
                    reason: "ambiguous"
                }
            ))
        ));
        assert!(admission.is_closed());

        let invoked = AtomicBool::new(false);
        let rejected = admission.attempt_managed_creation(|| {
            invoked.store(true, Ordering::SeqCst);
            Ok(())
        });
        assert!(matches!(
            rejected,
            Err(ManagedDatabaseCreationFailure::NoResidual(
                Error::ConnectionBudgetClosed
            ))
        ));
        assert!(!invoked.load(Ordering::SeqCst));
    }
}
