use crate::engine::engine_pragmas::EnginePragmas;

/// Actually current vrc-get-litedb crate doesn't support multi threading so
/// this service is almost no-op.
// this class should have interior mutability
pub(crate) struct LockService {
    pragma: EnginePragmas,
}

impl LockService {
    pub fn new(pragma: EnginePragmas) -> Self {
        LockService { pragma }
    }

    pub async fn enter_exclusive(&self) -> ExclusiveScope {
        // no lock
        ExclusiveScope {}
    }

    pub async fn try_enter_exclusive(&self) -> Option<ExclusiveScope> {
        // no lock
        Some(ExclusiveScope {})
    }

    pub async fn enter_lock(&self, _: &str) -> CollectionLockScope {
        // no lock
        CollectionLockScope {}
    }

    pub async fn enter_transaction(&self) -> TransactionScope {
        TransactionScope {}
    }
}

pub(crate) struct ExclusiveScope {}

pub(crate) struct CollectionLockScope {}

#[must_use]
pub(crate) struct TransactionScope {}
