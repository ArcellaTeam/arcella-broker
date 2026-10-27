// arcella-broker/src/broker_core/security/client_session_context.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;

use super::{SessionContext, SessionContextInner};
use crate::broker_core::l2::{SessionState, UnackedBuffer, CreditManager, CreditPool};
use crate::protocol::ServerLimits;

/// Client-side session context.
///
/// Extends the base `SessionContextInner` with client-specific details.
/// Unlike the server context, the client:
/// - Is not registered in `SessionManager`
/// - Does not manage timeouts of other sessions
/// - Decides for itself when to close the connection
///
/// # Lifecycle
///
/// Created after a successful handshake and destroyed when the
/// connection is dropped (Drop).
///
/// # Example
///
/// ```rust,ignore
/// let client_ctx = ClientSessionContext::new(
///     welcome.session_id,
///     welcome.session_token,
///     welcome.limits,
///     server_credits,
/// );
/// let endpoint = IPCEndpoint::with_session_context(transport, &client_ctx);
/// ```
#[derive(Debug)]
pub struct ClientSessionContext {
    /// Common core of the session context.
    pub inner: SessionContextInner,

    // Client-specific details may be added in the future:
    // - Statistics of sent/received frames
    // - Retry policy settings
    // - Callback channels for delivering incoming messages
}

impl ClientSessionContext {
    /// Creates a new client-side session context.
    ///
    /// # Arguments
    ///
    /// * `session_id` — the session identifier from `Welcome`.
    /// * `session_token` — the token from `Welcome`.
    /// * `limits` — the limits from `Welcome`.
    /// * `outgoing_credits` — the credits granted to us by the server (from its `CreditGrant`).
    pub fn new(
        session_id: u64,
        session_token: [u8; 32],
        limits: ServerLimits,
    ) -> Self {
        // The client creates its own SessionState with an initial value of 1
        let l2_state = Arc::new(SessionState::new(1));

        Self {
            inner: SessionContextInner::new(
                session_id,
                session_token,
                limits,
                l2_state,
                0,
            ),
        }

    }
}

// Delegating trait methods to `inner`
impl SessionContext for ClientSessionContext {
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
