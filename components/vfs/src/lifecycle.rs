// Copyright 2024 kisekifs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::{
    fmt::{Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};

use kiseki_storage::task_registry::TaskSnapshot;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[repr(u8)]
pub enum LifecycleState {
    Starting = 0,
    Recovering = 1,
    Ready = 2,
    Draining = 3,
    Stopped = 4,
    Failed = 5,
}

impl Display for LifecycleState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Starting => "starting",
            Self::Recovering => "recovering",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    pub state:                 LifecycleState,
    pub active_operations:     usize,
    pub writers_total:         usize,
    pub writers_flushed:       usize,
    pub staged_pending_before: usize,
    pub staged_pending_after:  usize,
    pub tasks:                 TaskSnapshot,
    pub tasks_aborted:         usize,
    pub elapsed:               Duration,
    pub timed_out:             bool,
    pub errors:                Vec<String>,
}

impl ShutdownReport {
    pub const fn is_clean(&self) -> bool {
        !self.timed_out
            && self.errors.is_empty()
            && self.tasks.panicked == 0
            && self.tasks.active == 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownError {
    pub report: Box<ShutdownReport>,
}

impl Display for ShutdownError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "mount shutdown was not clean (timed_out={}, errors={}, task_panics={}, \
             active_tasks={})",
            self.report.timed_out,
            self.report.errors.len(),
            self.report.tasks.panicked,
            self.report.tasks.active
        )
    }
}

impl std::error::Error for ShutdownError {}

pub(crate) struct MountLifecycle {
    state:             AtomicU8,
    active_operations: AtomicUsize,
    operations_done:   Notify,
}

impl MountLifecycle {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state:             AtomicU8::new(LifecycleState::Starting as u8),
            active_operations: AtomicUsize::new(0),
            operations_done:   Notify::new(),
        })
    }

    pub(crate) fn state(&self) -> LifecycleState {
        match self.state.load(Ordering::Acquire) {
            0 => LifecycleState::Starting,
            1 => LifecycleState::Recovering,
            2 => LifecycleState::Ready,
            3 => LifecycleState::Draining,
            4 => LifecycleState::Stopped,
            _ => LifecycleState::Failed,
        }
    }

    pub(crate) fn mark_recovering(&self) -> bool {
        self.transition(LifecycleState::Starting, LifecycleState::Recovering)
    }

    pub(crate) fn mark_ready(&self) -> bool {
        self.transition(LifecycleState::Recovering, LifecycleState::Ready)
    }

    pub(crate) fn mark_failed(&self) {
        let state = self.state();
        if !matches!(state, LifecycleState::Draining | LifecycleState::Stopped) {
            self.state
                .store(LifecycleState::Failed as u8, Ordering::Release);
        }
    }

    pub(crate) fn begin_draining(&self) -> bool {
        loop {
            let current = self.state();
            if matches!(current, LifecycleState::Draining | LifecycleState::Stopped) {
                return false;
            }
            if self
                .state
                .compare_exchange(
                    current as u8,
                    LifecycleState::Draining as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    pub(crate) fn mark_stopped(&self) {
        self.state
            .store(LifecycleState::Stopped as u8, Ordering::Release);
    }

    pub(crate) fn mark_shutdown_failed(&self) {
        self.state
            .store(LifecycleState::Failed as u8, Ordering::Release);
    }

    pub(crate) fn begin_operation(self: &Arc<Self>) -> Option<OperationGuard> {
        if self.state() != LifecycleState::Ready {
            return None;
        }
        self.active_operations.fetch_add(1, Ordering::AcqRel);
        if self.state() != LifecycleState::Ready {
            self.finish_operation();
            return None;
        }
        Some(OperationGuard(self.clone()))
    }

    pub(crate) fn active_operations(&self) -> usize {
        self.active_operations.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_for_operations(&self) {
        loop {
            let notified = self.operations_done.notified();
            if self.active_operations() == 0 {
                return;
            }
            notified.await;
        }
    }

    fn transition(&self, from: LifecycleState, to: LifecycleState) -> bool {
        self.state
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn finish_operation(&self) {
        if self.active_operations.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.operations_done.notify_waiters();
        }
    }
}

pub struct OperationGuard(Arc<MountLifecycle>);

impl Drop for OperationGuard {
    fn drop(&mut self) { self.0.finish_operation() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn draining_closes_operation_admission_and_waits_for_existing_work() {
        let lifecycle = MountLifecycle::new();
        assert!(lifecycle.mark_recovering());
        assert!(lifecycle.mark_ready());
        let operation = lifecycle.begin_operation().unwrap();
        assert!(lifecycle.begin_draining());
        assert!(lifecycle.begin_operation().is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), lifecycle.wait_for_operations())
                .await
                .is_err()
        );
        drop(operation);
        lifecycle.wait_for_operations().await;
    }

    #[test]
    fn active_background_tasks_make_a_shutdown_report_unclean() {
        let report = ShutdownReport {
            state:                 LifecycleState::Failed,
            active_operations:     0,
            writers_total:         0,
            writers_flushed:       0,
            staged_pending_before: 0,
            staged_pending_after:  0,
            tasks:                 TaskSnapshot {
                active: 1,
                ..TaskSnapshot::default()
            },
            tasks_aborted:         1,
            elapsed:               Duration::ZERO,
            timed_out:             true,
            errors:                vec!["worker remained active".to_string()],
        };

        assert!(!report.is_clean());
    }
}
