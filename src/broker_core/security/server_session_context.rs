// arcella-broker/src/broker_core/security/server_session_context.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{SessionContext, SessionContextInner};
use crate::broker_core::l2::{SessionState, UnackedBuffer, CreditManager, CreditPool};
use crate::protocol::ServerLimits;

/// Server-side session context.
///
/// Extends the base `SessionContextInner` with server-specific details:
/// - `CancellationToken` for forced closure on timeout
/// - `source_prefix`  for validating routing permissions
///
/// # Cross-thread access
///
/// Wrapped in `Arc` and stored in `SessionManager`. This allows
/// other tasks (e.g., Ingress Bridge) to send responses to clients
/// using `outgoing_unacked_buffer`.
///
/// # Example
///
/// ```rust,ignore
/// // В Ingress Bridge:
/// let session_ctx = session_manager.get_session(session_id)?;
/// if session_ctx.can_send_data() {
///     send_data_to_client(&mut endpoint, &session_ctx, addr, &[msg]).await?;
/// }
/// ```
#[derive(Debug)]
pub struct ServerSessionContext {
    /// Common core of the session context.
    pub inner: SessionContextInner,

    /// Source prefix for validating routing permissions.
    /// Extracted from the authenticated session (NOT from the message header!)
    pub source_prefix: Bytes,

    /// Token for forcibly terminating the connection-handling task.
    /// Triggered by:
    /// - Session timeout (via `SessionManager::cleanup_expired`)
    /// - An explicit close request
    pub cancellation_token: CancellationToken,
}

impl ServerSessionContext {
    /// Creates a new server-side session context.
    pub fn new(
        session_id: u64,
        session_token: [u8; 32],
        limits: ServerLimits,
        l2_state: Arc<SessionState>,
    ) -> Self {
        Self {
            inner: SessionContextInner::new(
                session_id,
                session_token,
                limits,
                l2_state,
                0,
            ),
            source_prefix: Bytes::new(),
            cancellation_token: CancellationToken::new(),
        }
    }

    /// Sets the source prefix (called during authentication).
    pub fn set_source_prefix(&mut self, prefix: Bytes) {
        self.source_prefix = prefix;
    }

    /// Forcibly closes the connection by cancelling the token.
    pub fn force_close(&self) {
        self.cancellation_token.cancel();
    }
}

// Delegating trait methods to `inner`
impl SessionContext for ServerSessionContext {
    #[inline]
    fn session_id(&self) -> u64 { self.inner.session_id }

    #[inline]
    fn l2_state(&self) -> &Arc<SessionState> { &self.inner.l2_state }

    #[inline]
    fn grant_outgoing_credits(&self, amount: u32) {
        self.inner.outgoing_credit_pool.grant(amount);
    }

    #[inline]
    fn outgoing_unacked_buffer(&self) -> &Arc<UnackedBuffer> {
        &self.inner.outgoing_unacked_buffer
    }

    #[inline]
    fn outgoing_credit_pool(&self) -> &Arc<CreditPool> {
        &self.inner.outgoing_credit_pool
    }

    #[inline]
    fn incoming_credit_manager(&self) -> &CreditManager {
        &self.inner.incoming_credit_manager
    }

    #[inline]
    fn limits(&self) -> &ServerLimits { &self.inner.limits }

    #[inline]
    fn touch(&self) { self.inner.touch() }
}
