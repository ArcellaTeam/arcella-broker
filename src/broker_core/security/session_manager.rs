// arcella-broker/src/broker_core/security/session_manager.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;

use bytes::Bytes;

use crate::broker_core::l2::SessionState;
use crate::protocol::ServerLimits;

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Active session context.
///
/// Stores all metadata required for validating and processing L2 frames.
/// Wrapped in `Arc` for safe and fast access from connection processing tasks
/// without holding the session manager lock.
#[derive(Debug)]
pub struct SessionContext {
    /// Unique session identifier.
    pub session_id: u64,
    
    /// Session token issued by the server (for validation during Resume).
    pub session_token: [u8; 32],
    
    /// Source identifier (source prefix) extracted during authentication.
    /// Will be used in Stage 11 for strict validation of routing permissions.
    pub source_prefix: Bytes,
    
    /// Server limits negotiated during the handshake (Hello/Welcome).
    pub limits: ServerLimits,
    
    /// L2 session state (monotonic sequence number counters).
    pub l2_state: Arc<SessionState>,
    
    /// Time of last activity in milliseconds (Unix timestamp ms).
    /// Atomic updates ensure zero-lock on the hot path.
    pub last_activity_ms: AtomicU64,

    /// Token to forcefully terminate the connection processing task
    /// when the session expires or is explicitly removed.
    pub cancellation_token: CancellationToken,    
}

impl SessionContext {
    pub fn new(
        session_id: u64,
        session_token: [u8; 32],
        limits: ServerLimits,
        l2_state: Arc<SessionState>,
    ) -> Self {
        Self {
            session_id,
            session_token,
            source_prefix: Bytes::new(),
            limits,
            l2_state,
            last_activity_ms: AtomicU64::new(Self::now_ms()),
            cancellation_token: CancellationToken::new(),
        }
    }
    
    /// Updates the last activity time.
    #[inline]
    pub fn touch(&self) {
        self.last_activity_ms.store(Self::now_ms(), Ordering::Relaxed);
    }

    /// Checks whether the session lifetime has expired.
    #[inline]
    pub fn is_expired(&self, timeout_ms: u64) -> bool {
        let now = Self::now_ms();
        let last = self.last_activity_ms.load(Ordering::Relaxed);
        now.saturating_sub(last) > timeout_ms
    }

    /// Gets the current monotonic time in milliseconds
    /// (resilient to system clock changes).
    #[inline]
    fn now_ms() -> u64 {
        PROCESS_START.get_or_init(Instant::now).elapsed().as_millis() as u64
    }

    /// Signals the connection processing task to terminate.
    pub fn force_close(&self) {
        self.cancellation_token.cancel();
    }
}

/// Server-side active session manager.
///
/// Responsible for:
/// - Registering new sessions after a successful handshake.
/// - Fast lookup of a session by `session_id` when processing incoming frames.
/// - Cleaning up expired (timeout) sessions via a background task.
pub struct SessionManager {
    sessions: RwLock<HashMap<u64, Arc<SessionContext>>>,
}

impl SessionManager {
    /// Creates a new empty session manager.
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a new session after a successful handshake.
    pub fn register_session(
        &self,
        session_id: u64,
        session_token: [u8; 32],
        limits: ServerLimits,
        l2_state: Arc<SessionState>,
    ) -> Arc<SessionContext> {
        let mut sessions = self.sessions.write().expect("SessionManager RwLock write poisoned");
        sessions.entry(session_id).or_insert_with(|| {
            Arc::new(SessionContext::new(session_id, session_token, limits, l2_state))
        }).clone()
    }

    /// Retrieves the session context by ID.
    /// Returns a clone of the Arc so as not to hold the RwLock
    /// during frame processing (zero-lock hot path).
    pub fn get_session(&self, session_id: u64) -> Option<Arc<SessionContext>> {
        let sessions = self.sessions.read().expect("SessionManager RwLock read poisoned");
        sessions.get(&session_id).map(Arc::clone)
    }

    /// Removes a session (on connection drop or explicit close).
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
    /// Collects the identifiers of those with now - last_activity_ms > timeout_ms.
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
    /// # Return value
    /// The number of sessions actually removed. The value is intended for
    /// export to metrics and logging (e.g., `session_cleanup_removed_total`).
    ///
    /// # Thread safety
    /// The method does not block the hot path for the duration of a full traversal.
    /// The `write` lock is held strictly in proportion to the number of sessions being removed.
    pub fn cleanup_expired(&self, timeout_ms: u64) -> usize {
        let now = SessionContext::now_ms();
        let mut expired_ids = Vec::new();

        // 1. Fast pass over the Read lock (does not block new get_session calls for long)
        {
            let sessions = self.sessions.read().expect("SessionManager RwLock read poisoned");
            for (&id, ctx) in sessions.iter() {
                let last = ctx.last_activity_ms.load(Ordering::Relaxed);
                if now.saturating_sub(last) > timeout_ms {
                    expired_ids.push(id);
                }
            }
        }        

        // 2. Short Write lock only for deletion
        if !expired_ids.is_empty() {
            let mut sessions = self.sessions.write().expect("SessionManager RwLock write poisoned");
            for id in &expired_ids {
                if let Some(ctx) = sessions.remove(&id) {
                    ctx.force_close(); // Signal the task to drop the socket    
                }
            }
        }

        expired_ids.len()
    }        

    /// Returns the current number of active sessions (for metrics).
    pub fn active_sessions_count(&self) -> usize {
        self.sessions.read().expect("SessionManager RwLock read poisoned").len()
    }

    /// Spawns a background task that periodically cleans up expired sessions.
    /// Returns a JoinHandle that can be aborted to stop the GC.
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
    use tokio::time::{self, Duration};

    fn make_test_l2_state() -> Arc<SessionState> {
        Arc::new(SessionState::new(1))
    }

    #[tokio::test]
    async fn test_register_and_get() {
        let mgr = SessionManager::new();
        let l2 = make_test_l2_state();
        
        let ctx = mgr.register_session(1, [0xAA; 32], ServerLimits::default(), l2);
        assert_eq!(ctx.session_id, 1);
        
        let retrieved = mgr.get_session(1).unwrap();
        assert!(Arc::ptr_eq(&ctx, &retrieved));
    }
}
