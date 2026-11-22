// arcella-broker/src/broker_core/security/session_context.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Core of L2 protocol session state management.
//!
//! This module defines the shared state and polymorphic interface for active
//! L2 connections. The architecture is designed to be highly concurrent,
//! lock-free on the frame-processing hot path,
//! and protected against address spoofing.
//!
//! # Key invariants
//! - L2 state (`l2_state`, buffers, and credit managers) is initialized once
//! at creation and never changes structurally.
//! - The last activity time (`last_activity_ms`) is updated atomically via `touch()`.
//! - Credit pools (`outgoing_credit_pool`) reflect the resources allocated to us by peers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::broker_core::l2::{
    CreditManager, 
    CreditPool, 
    HotBuffer,
    SessionState, 
    UnackedBuffer, 
};
use crate::protocol::ServerLimits;

/// Internal core of the session context, shared by client and server.
///
/// # Invariants
///
/// - `l2_state`, `outgoing_unacked_buffer` and `incoming_credit_manager`
///   are initialized once at creation and never changed.
/// - `last_activity_ms`  is updated atomically via `touch()`.
/// - `outgoing_credit_pool` reflects the credits granted to us by the peer.
///
/// # Thread Safety
///
/// All fields are either immutable after initialization or use
/// atomic types. This makes `SessionContextInner` fully
/// thread-safe without external locks.
#[derive(Debug)]
pub struct SessionContextInner {
    /// Unique session identifier (issued by the server).
    pub session_id: u64,

    /// Session token (server's local secret).
    /// Used to resume the session via Resume.
    pub session_token: [u8; 32],

    /// Limits negotiated during the handshake (from Welcome).
    pub limits: ServerLimits,

    /// L2 session state: monotonic sequence counters.
    /// Shared between `IPCEndpoint` и `SessionContext`.
    pub l2_state: Arc<SessionState>,

    /// Buffer of unsent (unacknowledged) outgoing frames.
    /// Shared between `IPCEndpoint` and `SessionContext`,
    /// providing cross-thread access for sending from other tasks
    /// (e.g., from the Ingress Bridge  on the server).
    pub outgoing_unacked_buffer: Arc<UnackedBuffer>,

    /// Credit pool for outgoing frames (granted to us by the peer).
    /// When the peer sends a CreditGrant, this pool is replenished.
    pub outgoing_credit_pool: Arc<CreditPool>,

    /// Buffer for incoming frames currently being processed (hot path).
    incoming_hot_buffer: Arc<HotBuffer>, 

    /// Credit manager for incoming frames (shadow counter).
    /// Used to detect protocol violations by the peer.
    pub incoming_credit_manager: CreditManager,

    /// Time of last activity in milliseconds (monotonic clock).
    /// Used to determine the session timeout.
    pub last_activity_ms: AtomicU64,
}

impl SessionContextInner {
    /// Creates a new session context with the full credit limit.
    ///
    /// # Arguments
    ///
    /// * `session_id` — unique session identifier.
    /// * `session_token` — session token (32 bytes).
    /// * `limits` — limits negotiated during the handshake.
    /// * `l2_state` — shared session state (sequence counters).
    /// * `outgoing_credits` — credits granted to us by the peer (from its CreditGrant).
    pub fn new(
        session_id: u64,
        session_token: [u8; 32],
        limits: ServerLimits,
        l2_state: Arc<SessionState>,
        outgoing_credits: u32,
    ) -> Self {
        let hot_credit = limits.hot_credit as usize;

        Self {
            session_id,
            session_token,
            limits: limits.clone(),
            l2_state,
            outgoing_unacked_buffer: Arc::new(UnackedBuffer::new(hot_credit)),
            outgoing_credit_pool: Arc::new(CreditPool::new(outgoing_credits)),
            incoming_hot_buffer: Arc::new(HotBuffer::new(limits.hot_credit as usize)),
            incoming_credit_manager: CreditManager::new(&limits),
            last_activity_ms: AtomicU64::new(Self::now_ms()),
        }
    }

    /// Hot buffer for incoming frames.
    #[inline]
    pub fn incoming_hot_buffer(&self) -> &Arc<HotBuffer> {
        &self.incoming_hot_buffer
    }    

    /// Updates the last activity time.
    #[inline]
    pub fn touch(&self) {
        self.last_activity_ms.store(Self::now_ms(), Ordering::Relaxed);
    }

    /// Checks whether the session timeout has expired.
    #[inline]
    pub fn is_expired(&self, timeout_ms: u64) -> bool {
        let now = Self::now_ms();
        let last = self.last_activity_ms.load(Ordering::Relaxed);
        now.saturating_sub(last) > timeout_ms
    }

    /// Current monotonic time in milliseconds.
    /// Resilient to system clock changes (uses `Instant`).
    #[inline]
    pub fn now_ms() -> u64 {
        static PROCESS_START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        PROCESS_START
            .get_or_init(Instant::now)
            .elapsed()
            .as_millis() as u64
    }
}

/// Trait for polymorphic access to the session context.
///
/// Implemented by both the client and server sides.
/// Provides a minimal set of methods for working with a session
/// without exposing implementation specifics.
///
/// # Performance
///
/// For the hot path, it is recommended to use concrete types
/// (`ClientSessionContext` / `ServerSessionContext`) instead of `dyn SessionContext`,
/// to avoid virtual call overhead.
///
/// # Example
///
/// ```rust,ignore
/// fn log_session_info(ctx: &dyn SessionContext) {
///     println!("Session {} active", ctx.session_id());
/// }
/// ```
pub trait SessionContext {
    /// Unique session identifier.
    fn session_id(&self) -> u64;

    /// L2 session state (sequence counters).
    fn l2_state(&self) -> &Arc<SessionState>;

    /// Grants additional outgoing credits to this session.
    fn grant_outgoing_credits(&self, amount: u32);

    /// Buffer of unsent outgoing frames.
    fn outgoing_unacked_buffer(&self) -> &Arc<UnackedBuffer>;

    /// Credit pool for outgoing frames (granted by the peer).
    fn outgoing_credit_pool(&self) -> &Arc<CreditPool>;

    /// Hot buffer for incoming frames.
    fn incoming_hot_buffer(&self) -> &Arc<HotBuffer>;

    /// Credit manager for incoming frames.
    fn incoming_credit_manager(&self) -> &CreditManager;

    /// Limits negotiated during the handshake.
    fn limits(&self) -> &ServerLimits;

    /// Returns the authenticated source prefix of this session.
    ///
    /// # Security (Critically important)
    /// This value is set once during a successful handshake
    /// based on the data provided by the client. It must never
    /// be overwritten or read from an L3 message header.
    /// Using this method is the primary defense against address spoofing:
    /// routing permission checks must always use the session's `source_prefix`
    /// rather than the one from the frame payload.
    ///
    /// # Performance
    /// Returns a `&str` slice. UTF-8 and address format validation is performed
    /// only once when the session is created, so this method is zero-cost.
    fn source_prefix(&self) -> &str;

    /// Updates the last activity time.
    fn touch(&self);

    /// Checks whether an outgoing frame can be sent.
    ///
    /// Returns `true` if:
    /// 1. There are credits from the peer (`outgoing_credit_pool`)
    /// 2. The buffer is not full (`outgoing_unacked_buffer`)
    #[inline]
    fn can_send_data(&self) -> bool {
        self.outgoing_credit_pool().available() > 0
            && !self.outgoing_unacked_buffer().is_full()
    }

    /// Returns the number of frames in flight (sent but not acknowledged).
    #[inline]
    fn in_flight_count(&self) -> usize {
        self.outgoing_unacked_buffer().len()
    }
}
