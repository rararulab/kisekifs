// Copyright 2024 kisekifs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

use std::{
    collections::HashMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use futures::FutureExt;
use tokio::task::AbortHandle;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TaskSnapshot {
    pub active:    usize,
    pub spawned:   usize,
    pub completed: usize,
    pub panicked:  usize,
}

/// Owns every background task started by one mount.
///
/// The mutex makes task admission and registry closure one atomic decision:
/// once draining starts, no task can slip into the tracker afterward.
pub struct MountTaskRegistry {
    tracker:       TaskTracker,
    cancellation:  CancellationToken,
    accepting:     Mutex<bool>,
    abort_handles: Mutex<HashMap<usize, AbortHandle>>,
    next_task_id:  AtomicUsize,
    active:        AtomicUsize,
    spawned:       AtomicUsize,
    completed:     AtomicUsize,
    panicked:      AtomicUsize,
}

impl Default for MountTaskRegistry {
    fn default() -> Self { Self::new() }
}

impl MountTaskRegistry {
    pub fn new() -> Self {
        Self {
            tracker:       TaskTracker::new(),
            cancellation:  CancellationToken::new(),
            accepting:     Mutex::new(true),
            abort_handles: Mutex::new(HashMap::new()),
            next_task_id:  AtomicUsize::new(0),
            active:        AtomicUsize::new(0),
            spawned:       AtomicUsize::new(0),
            completed:     AtomicUsize::new(0),
            panicked:      AtomicUsize::new(0),
        }
    }

    pub fn spawn<F>(self: &Arc<Self>, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_registration_hook(future, || {})
    }

    fn spawn_with_registration_hook<F, H>(
        self: &Arc<Self>,
        future: F,
        before_abort_registration: H,
    ) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
        H: FnOnce(),
    {
        let accepting = self
            .accepting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !*accepting {
            return false;
        }

        self.active.fetch_add(1, Ordering::AcqRel);
        self.spawned.fetch_add(1, Ordering::Relaxed);
        let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        let finished = Arc::new(AtomicBool::new(false));
        let registry = self.clone();
        let completion = CompletionGuard {
            registry: registry.clone(),
            task_id,
            finished: finished.clone(),
        };
        let handle = self.tracker.spawn(async move {
            let _completion = completion;
            let panicked = AssertUnwindSafe(future).catch_unwind().await.is_err();
            if panicked {
                registry.panicked.fetch_add(1, Ordering::Relaxed);
            }
        });
        before_abort_registration();
        let mut abort_handles = self
            .abort_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !finished.load(Ordering::Acquire) {
            abort_handles.insert(task_id, handle.abort_handle());
        }
        drop(abort_handles);
        // Keep admission locked until the task has either completed or has an
        // abort handle in the registry. Otherwise draining could miss a task
        // in the gap between tracker admission and handle registration.
        debug_assert!(*accepting);
        drop(accepting);
        true
    }

    pub fn begin_draining(&self) -> bool {
        let mut accepting = self
            .accepting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !*accepting {
            return false;
        }
        *accepting = false;
        self.tracker.close();
        debug_assert!(!*accepting);
        drop(accepting);
        true
    }

    pub fn cancel(&self) { self.cancellation.cancel(); }

    pub fn cancellation_token(&self) -> CancellationToken { self.cancellation.clone() }

    pub async fn wait(&self) { self.tracker.wait().await }

    pub fn abort_all(&self) -> usize {
        let mut handles = self
            .abort_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        handles.retain(|_, handle| !handle.is_finished());
        let count = handles.len();
        for (_, handle) in handles.drain() {
            handle.abort();
        }
        drop(handles);
        count
    }

    pub fn snapshot(&self) -> TaskSnapshot {
        TaskSnapshot {
            active:    self.active.load(Ordering::Acquire),
            spawned:   self.spawned.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Acquire),
            panicked:  self.panicked.load(Ordering::Relaxed),
        }
    }
}

struct CompletionGuard {
    registry: Arc<MountTaskRegistry>,
    task_id:  usize,
    finished: Arc<AtomicBool>,
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
        self.registry
            .abort_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.task_id);
        self.registry.active.fetch_sub(1, Ordering::AcqRel);
        self.registry.completed.fetch_add(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn draining_rejects_new_tasks_and_joins_cancelled_workers() {
        let registry = Arc::new(MountTaskRegistry::new());
        let cancellation = registry.cancellation_token();
        assert!(registry.spawn(async move { cancellation.cancelled().await }));
        assert!(registry.begin_draining());
        assert!(!registry.begin_draining());
        assert!(!registry.spawn(async {}));

        registry.cancel();
        tokio::time::timeout(Duration::from_secs(1), registry.wait())
            .await
            .unwrap();
        assert_eq!(
            registry.snapshot(),
            TaskSnapshot {
                active:    0,
                spawned:   1,
                completed: 1,
                panicked:  0,
            }
        );
    }

    #[tokio::test]
    async fn panics_are_counted_without_detaching_the_task() {
        let registry = Arc::new(MountTaskRegistry::new());
        assert!(registry.spawn(async { panic!("injected task panic") }));
        registry.begin_draining();
        registry.wait().await;
        assert_eq!(registry.snapshot().panicked, 1);
    }

    #[tokio::test]
    async fn forced_abort_still_joins_and_accounts_for_a_stuck_worker() {
        let registry = Arc::new(MountTaskRegistry::new());
        assert!(registry.spawn(std::future::pending()));
        registry.begin_draining();
        assert_eq!(registry.abort_all(), 1);
        registry.wait().await;
        assert_eq!(registry.snapshot().active, 0);
        assert_eq!(registry.snapshot().completed, 1);
    }

    #[tokio::test]
    async fn completed_tasks_release_abort_handles_before_mount_shutdown() {
        let registry = Arc::new(MountTaskRegistry::new());
        for _ in 0..128 {
            assert!(registry.spawn(async {}));
        }
        registry.begin_draining();
        registry.wait().await;
        assert_eq!(
            registry
                .abort_handles
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn draining_cannot_miss_task_during_abort_handle_registration() {
        use std::{
            sync::{Condvar, mpsc::RecvTimeoutError},
            thread,
        };

        let registry = Arc::new(MountTaskRegistry::new());
        let runtime = tokio::runtime::Handle::current();
        let (registration_tx, registration_rx) = std::sync::mpsc::sync_channel(1);
        let release_registration = Arc::new((Mutex::new(false), Condvar::new()));

        let spawn_registry = registry.clone();
        let spawn_release = release_registration.clone();
        let spawn_thread = thread::spawn(move || {
            let _runtime = runtime.enter();
            spawn_registry.spawn_with_registration_hook(std::future::pending(), move || {
                registration_tx.send(()).unwrap();
                let (lock, ready) = &*spawn_release;
                let mut released = lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                while !*released {
                    released = ready
                        .wait(released)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                drop(released);
            })
        });

        registration_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        let drain_registry = registry.clone();
        let (drained_tx, drained_rx) = std::sync::mpsc::sync_channel(1);
        let drain_thread = thread::spawn(move || {
            assert!(drain_registry.begin_draining());
            drained_tx.send(drain_registry.abort_all()).unwrap();
        });

        assert!(matches!(
            drained_rx.recv_timeout(Duration::from_millis(50)),
            Err(RecvTimeoutError::Timeout)
        ));
        let (lock, ready) = &*release_registration;
        *lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        ready.notify_one();

        assert!(spawn_thread.join().unwrap());
        assert_eq!(drained_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        drain_thread.join().unwrap();
        tokio::time::timeout(Duration::from_secs(1), registry.wait())
            .await
            .unwrap();
        assert_eq!(registry.snapshot().active, 0);
    }
}
