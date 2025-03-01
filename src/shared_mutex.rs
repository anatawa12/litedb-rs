//! This module provides (simplified) implementation of [Mutex in C#].
//!
//! This implementation only supports locks in `Global\\` namespace for simplicity.
//!
//! On windows, this module is based on shared windows mutex ([`CreateMutexExW`])
//! and this is machine-shared lock.
//!
//! [Mutex in C#]: https://learn.microsoft.com/en-us/dotnet/api/system.threading.mutex?view=net-8.0
//! [`CreateMutexExW`]: windows::Win32::System::Threading::CreateMutexExW

#[cfg(windows)]
use windows::*;

use std::ffi::OsStr;
use std::io;
use std::marker::PhantomData;

/// The Shared Mutex
pub struct SharedMutex {
    inner: SharedMutexImpl,
}

pub struct SharedMutexGuard<'a> {
    _inner: MutexGuardImpl,
    _phantom: PhantomData<&'a SharedMutexImpl>,
}

pub struct SharedMutexOwnedGuard {
    _inner: MutexGuardImpl,
    _owner: SharedMutex,
}

impl SharedMutex {
    pub async fn new(name: impl AsRef<OsStr>) -> io::Result<SharedMutex> {
        async fn inner(name: &OsStr) -> io::Result<SharedMutex> {
            let name_bytes = name.as_encoded_bytes();

            if !name_bytes.starts_with(b"Global\\") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Global Mutex is only supported",
                ));
            }

            let global_name = &name_bytes[b"Global\\".len()..];

            if !global_name
                .iter()
                .all(|&x| matches!(x, b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' | b'.'| b'-'| b'_'))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid character in mutex name",
                ));
            }

            Ok(SharedMutex {
                inner: SharedMutexImpl::new(name).await?,
            })
        }

        inner(name.as_ref()).await
    }

    pub async fn lock(&self) -> io::Result<SharedMutexGuard> {
        Ok(SharedMutexGuard {
            _inner: self.inner.lock().await?,
            _phantom: PhantomData,
        })
    }

    pub async fn lock_owned(self) -> io::Result<SharedMutexOwnedGuard> {
        Ok(SharedMutexOwnedGuard {
            _inner: self.inner.lock().await?,
            _owner: self,
        })
    }
}

#[allow(dead_code)]
fn _type_check() {
    use crate::utils::checker::*;

    check_sync_send(dummy::<SharedMutex>());
    check_sync_send(dummy::<SharedMutexGuard>());
    check_sync_send(dummy::<SharedMutexOwnedGuard>());
}

#[cfg(windows)]
mod windows {
    use futures::channel::oneshot;
    use std::ffi::OsStr;
    use std::io;
    use std::ops::Deref;
    use std::sync::Mutex;
    use windows::Win32::Foundation::*;
    use windows::Win32::System::SystemServices::MAXIMUM_ALLOWED;
    use windows::Win32::System::Threading::*;
    use windows::core::{Free, Owned};

    // https://github.com/dotnet/runtime/blob/2fef8277b701cfa6636d8ab55c14da6e001b9218/src/libraries/System.Private.CoreLib/src/System/Threading/EventWaitHandle.Windows.cs#L12
    const ACCESS_RIGHTS: u32 = MAXIMUM_ALLOWED | PROCESS_SYNCHRONIZE.0 | MUTEX_MODIFY_STATE.0;

    #[derive(Copy, Clone)]
    #[repr(transparent)]
    struct SendHandle(HANDLE);
    unsafe impl Send for SendHandle {}
    unsafe impl Sync for SendHandle {}

    impl Free for SendHandle {
        unsafe fn free(&mut self) {
            unsafe { self.0.free() };
        }
    }

    pub(super) struct SharedMutexImpl {
        handle: Owned<SendHandle>,
    }

    pub(super) struct MutexGuardImpl {
        wait_sender: std::sync::mpsc::SyncSender<()>,
        release_end_receiver: Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl SharedMutexImpl {
        pub async fn new(name: &OsStr) -> io::Result<Self> {
            let name = windows::core::HSTRING::from(name);

            let handle = match tokio::task::spawn_blocking(move || {
                match unsafe { CreateMutexExW(None, &name, 0, ACCESS_RIGHTS) } {
                    Ok(handle) => Ok(SendHandle(handle)),
                    Err(e) => Err(e),
                }
            })
            .await
            {
                Ok(handle) => handle?,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "background task failed",
                    ));
                }
            };

            Ok(Self {
                handle: unsafe { Owned::new(handle) },
            })
        }

        pub async fn lock(&self) -> io::Result<MutexGuardImpl> {
            let (lock_sender, result_receiver) = oneshot::channel::<io::Result<()>>();
            let (wait_sender, wait_receiver) = std::sync::mpsc::sync_channel::<()>(1);
            let (release_end_sender, release_end_receiver) = std::sync::mpsc::channel::<()>();

            let handle: SendHandle = *self.handle.deref();

            // create thread for mutex creation and free since
            // locking and release needs on single thread.
            std::thread::spawn(move || {
                let handle = handle; // move entire SendHandle to avoid sending HANDLE
                unsafe {
                    let r = WaitForSingleObject(handle.0, INFINITE);
                    match r {
                        WAIT_FAILED => {
                            lock_sender.send(Err(io::Error::last_os_error())).unwrap();
                            return;
                        }
                        WAIT_ABANDONED => {
                            lock_sender.send(Err(io::Error::new(io::ErrorKind::Deadlock, "The mutex is held by another thread and the thread exited with lock in kept."))).unwrap();
                            return;
                        }
                        _ => {}
                    }
                }

                lock_sender.send(Ok(())).unwrap();

                wait_receiver.recv().unwrap();

                unsafe {
                    ReleaseMutex(handle.0).ok();
                }

                release_end_sender.send(()).unwrap();
            });

            result_receiver.await.unwrap()?;

            Ok(MutexGuardImpl {
                wait_sender,
                release_end_receiver: Mutex::new(release_end_receiver),
            })
        }
    }

    impl Drop for MutexGuardImpl {
        fn drop(&mut self) {
            self.wait_sender.send(()).ok();
            self.release_end_receiver.get_mut().unwrap().recv().ok();
        }
    }
}
