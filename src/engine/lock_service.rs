use crate::engine::engine_pragmas::EnginePragmas;
use async_lock::{Mutex, MutexGuardArc, RwLock, RwLockReadGuardArc, RwLockWriteGuardArc};
use std::collections::HashMap;
use std::sync::Arc;

// this class should have interior mutability
pub(crate) struct LockService {
    pragma: EnginePragmas,
    transaction: Arc<RwLock<()>>,
    collections: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl LockService {
    pub fn new(pragma: EnginePragmas) -> Self {
        LockService {
            pragma,
            transaction: Arc::new(RwLock::new(())),
            collections: Mutex::new(HashMap::new()),
        }
    }

    pub async fn enter_exclusive(&self) -> ExclusiveScope {
        // TODO: timeout
        let lock = self.transaction.write_arc().await;
        ExclusiveScope { lock }
    }

    pub async fn try_enter_exclusive(&self) -> Option<ExclusiveScope> {
        self.transaction
            .try_write_arc()
            .map(|lock| ExclusiveScope { lock })
    }

    pub async fn enter_lock(&self, collection: &str) -> CollectionLockScope {
        // no lock
        let lock = self
            .collections
            .lock()
            .await
            .entry(collection.to_string())
            .or_default()
            .clone()
            .lock_arc()
            .await;
        CollectionLockScope { lock }
    }

    pub async fn enter_transaction(&self) -> TransactionScope {
        // TODO: timeout
        let lock = self.transaction.read_arc().await;
        TransactionScope { lock }
    }
}

pub(crate) struct ExclusiveScope {
    lock: RwLockWriteGuardArc<()>,
}

pub(crate) struct CollectionLockScope {
    lock: MutexGuardArc<()>,
}

#[must_use]
pub(crate) struct TransactionScope {
    lock: RwLockReadGuardArc<()>,
}
