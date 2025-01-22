use crate::engine::engine_pragmas::EnginePragmas;

/// Actually current vrc-get-litedb crate doesn't support multi threading so
/// this service is almost no-op.
pub(crate) struct LockService {
    pragma: EnginePragmas,
}

impl LockService {
    pub fn new(pragma: EnginePragmas) -> Self {
        LockService {
            pragma,
        }
    }

    pub fn enter_exclusive(&self) -> ExclusiveScope {
        // no lock
        ExclusiveScope{}
    }
}

pub(crate) struct ExclusiveScope {
}
