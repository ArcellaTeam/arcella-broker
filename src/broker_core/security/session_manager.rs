// arcella-broker/src/broker_core/security/session_manager.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Session management for the L2 protocol server.
//!
//! This module provides the [`SessionManager`], which is responsible for the lifecycle
//! of active L2 sessions. It ensures fast, lock-free lookups during the frame-processing
//! hot path while safely handling concurrent registrations and background garbage collection
//! of expired sessions.

use std::collections::HashMap;
use std::sync::{
    Arc, 
    atomic::Ordering,
    OnceLock,
    RwLock,
};
use std::time::Instant;
use tokio::time::{interval, Duration};

use crate::broker_core::l2::SessionState;
use crate::protocol::ServerLimits;

use super::{SessionContextInner, ServerSessionContext};

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Server-side active session manager.
///
/// Responsible for:
/// - Registering new sessions after a successful handshake.
/// - Fast lookup of a session by `session_id` when processing incoming frames.
/// - Cleaning up expired (timeout) sessions via a background task.
///
/// # Thread Safety
/// This struct is designed to be shared across multiple threads via `Arc<SessionManager>`.
/// Internal state is protected by an `RwLock`, allowing highly concurrent reads (hot path)
/// and exclusive, brief writes (registration, removal, and garbage collection).
pub struct SessionManager {
    sessions: RwLock<HashMap<u64, Arc<ServerSessionContext>>>,
}

impl SessionManager {
    /// Creates a new, empty session manager.
    ///
    /// # Initial State
    /// The manager starts with zero active sessions. It is ready to accept
    /// registrations immediately after creation.
    ///
    /// # Thread Safety
    /// The returned instance is safe to share across multiple threads via `Arc<SessionManager>`.
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a new session after a successful handshake.
    ///
    /// # Arguments
    /// * `session_id` - The unique identifier for the session.
    /// * `session_token` - The cryptographic token used for session resumption/validation.
    /// * `limits` - The `ServerLimits` negotiated during the handshake.
    /// * `l2_state` - The underlying L2 protocol state (sequence numbers, credits, etc.).
    ///
    /// # Idempotency
    /// If a session with the given `session_id` already exists, this method returns
    /// a clone of the existing `Arc<ServerSessionContext>` without overwriting it.
    /// This prevents race conditions during concurrent handshake completions for the same ID.
    ///
    /// # Returns
    /// An `Arc<ServerSessionContext>` representing the registered session.
    pub fn register_session(
        &self,
        session_id: u64,
        session_token: [u8; 32],
        limits: ServerLimits,
        l2_state: Arc<SessionState>,
    ) -> Arc<ServerSessionContext> {
        let mut sessions = self.sessions.write().expect("SessionManager RwLock write poisoned");
        sessions.entry(session_id).or_insert_with(|| {
            Arc::new(ServerSessionContext::new(
                session_id, 
                session_token, 
                limits, 
                l2_state, 
            ))
        }).clone()
    }

    /// Retrieves the session context by ID.
    ///
    /// # Performance (Zero-Lock Hot Path)
    /// This method acquires a read lock only for the duration of the `HashMap` lookup.
    /// It returns a clone of the `Arc`, ensuring that the lock is released immediately.
    /// The caller can then use the session context for frame processing without holding
    /// any locks, guaranteeing zero contention on the hot path.
    pub fn get_session(&self, session_id: u64) -> Option<Arc<ServerSessionContext>> {
        let sessions = self.sessions.read().expect("SessionManager RwLock read poisoned");
        sessions.get(&session_id).map(Arc::clone)
    }

    /// Removes a session (on connection drop or explicit close).
    ///
    /// # When to Call
    /// This should be invoked when a physical connection is dropped, a handshake fails,
    /// or an explicit `Close` (0x07) frame is received and processed.
    ///
    /// # Side Effects
    /// Removing the session from the registry does *not* automatically close the
    /// underlying socket or abort background tasks. The caller is responsible for
    /// ensuring the socket is dropped or calling `force_close()` on the context.
    pub fn remove_session(&self, session_id: u64) {
        let mut sessions = self.sessions.write().expect("SessionManager RwLock write poisoned");
        sessions.remove(&session_id);
    }

    /// Removes all sessions that have exceeded the inactivity timeout.
    ///
    /// The method implements a **snapshot-based background garbage collection**
    /// strategy optimized to minimize impact on the hot path.
    ///
    /// # Algorithm
    /// 1. **Scan phase (Read Lock):** Iterates over active sessions.
    /// Collects the identifiers of those with `now - last_activity_ms > timeout_ms`.
    /// At this stage only `RwLock::read` is held, which does not block
    /// concurrent `get_session` calls in frame-processing tasks.
    /// 2. **Deletion phase (Write Lock):** Briefly acquires `RwLock::write`
    /// solely to physically remove the collected identifiers from the registry.
    ///
    /// # Race Window Semantics
    /// Between the scan phase and the deletion phase there is a microsecond-scale window.
    /// If a session receives activity (via [`SessionContext::touch`]) during this moment,
    /// it will still be removed.
    ///
    /// This behavior is **intentional and safe** for the following reasons:
    /// - **Decision point:** The "expired" status is fixed at the start of the
    /// cleanup cycle ($T_1$). Subsequent activity at $T_1 + \epsilon$ does not cancel
    /// the background garbage collector's decision.
    /// - **Time scale:** The race window is measured in microseconds, whereas
    /// `timeout_ms` is typically minutes (e.g., 300,000 ms). The probability
    /// of a legitimate packet landing in this window is negligible.
    /// - **Protection against "eternally alive" sessions:** If the method re-checked
    /// `last_activity_ms` under the `write` lock, high-frequency clients could
    /// constantly "wake up" a session, preventing the garbage collector from completing
    /// cleanup and potentially causing accumulation of inactive resources.
    ///
    /// # Returns
    /// The number of sessions actually removed. The value is intended for
    /// export to metrics and logging (e.g., `session_cleanup_removed_total`).
    ///
    /// # Thread safety
    /// The method does not block the hot path for the duration of a full traversal.
    /// The `write` lock is held strictly in proportion to the number of sessions being removed.
    pub fn cleanup_expired(&self, timeout_ms: u64) -> usize {
        let now = SessionContextInner::now_ms();
        let mut expired_ids = Vec::new();

        // Phase 1: Scan with read lock (doesn't block get_session for long)
        {
            let sessions = self.sessions.read().expect("SessionManager RwLock read poisoned");
            for (&id, ctx) in sessions.iter() {
                let last = ctx.inner.last_activity_ms.load(Ordering::Relaxed);
                if now.saturating_sub(last) > timeout_ms {
                    expired_ids.push(id);
                }
            }
        }        

        // Phase 2: Delete with write lock (brief, only for removed sessions)
        if !expired_ids.is_empty() {
            let mut sessions = self.sessions.write().expect("SessionManager RwLock write poisoned");
            for id in &expired_ids {
                if let Some(ctx) = sessions.remove(id) {
                    ctx.force_close(); // Signal the task to drop the socket    
                }
            }
        }

        expired_ids.len()
    }        

    /// Returns the current number of active sessions.
    ///
    /// # Performance
    /// This provides a point-in-time snapshot. It acquires a read lock briefly.
    /// It is suitable for metrics and telemetry, but should **not** be called
    /// in the per-frame hot path.
    pub fn active_sessions_count(&self) -> usize {
        self.sessions.read().expect("SessionManager RwLock read poisoned").len()
    }

    /// Spawns a background Tokio task that periodically cleans up expired sessions.
    ///
    /// # Arguments
    /// * `timeout_ms`: The maximum allowed inactivity period for a session before it is considered expired.
    /// * `interval_ms`: How often the background task wakes up to perform the cleanup scan.
    ///   *(Recommended: `interval_ms` should be significantly smaller than `timeout_ms`, 
    ///   e.g., 10% of the timeout value, to ensure timely cleanup without excessive CPU wakeups).*
    ///
    /// # Lifecycle & Cancellation Safety
    /// The task runs indefinitely until the returned `JoinHandle` is explicitly aborted,
    /// or the Tokio runtime shuts down. The task is cancellation-safe: aborting the handle
    /// will simply stop future cleanup cycles without leaving the `SessionManager` in an 
    /// inconsistent state.
    pub fn spawn_cleanup_task(
        self: &Arc<Self>,
        timeout_ms: u64,
        interval_ms: u64,
    ) -> tokio::task::JoinHandle<()> {
        let manager = Arc::clone(self);
        let mut interval = interval(
            Duration::from_millis(interval_ms)
        );
        
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                let removed = manager.cleanup_expired(timeout_ms);
                if removed > 0 {
                    tracing::info!(
                        removed_sessions = removed,
                        active_sessions = manager.active_sessions_count(),
                        "GC: cleaned up expired sessions"
                    );
                }
            }
        })
    }    
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::SessionContext;

    fn make_test_l2_state() -> Arc<SessionState> {
        Arc::new(SessionState::new(1))
    }

    #[tokio::test]
    async fn test_register_and_get() {
        let mgr = SessionManager::new();
        let l2 = make_test_l2_state();
        
        let ctx = mgr.register_session(1, [0xAA; 32], ServerLimits::default(), l2);
        assert_eq!(ctx.session_id(), 1);
        
        let retrieved = mgr.get_session(1).unwrap();
        assert!(Arc::ptr_eq(&ctx, &retrieved));
    }
}
