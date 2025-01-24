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

    pub async fn enter_lock(&self, collection: &str) -> CollectionLockScope {
        // no lock
        CollectionLockScope {}
    }

    pub async fn enter_transaction(&self) {
    }

    pub fn exit_transaction(&self) {
    }
}

pub(crate) struct ExclusiveScope {}

pub(crate) struct CollectionLockScope {}
