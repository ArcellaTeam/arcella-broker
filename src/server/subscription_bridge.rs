// arcella-broker/src/server/subscription_bridge.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashMap;
use std::sync::Arc;

use crate::broker_core::registry::{AttachError, Attachment, LocalRegistry, RouteTarget};
use crate::broker_core::security::{SessionContext, ServerSessionContext};
use crate::broker_core::subscription::{Subscriber, SubscriptionHandle, SubscriptionError};
use crate::broker_core::transport::EgressTx;
use crate::protocol::Frame;
use crate::protocol::payloads::{
    SubscribeAckPayload, SubscribePayload, UnsubscribePayload,
    SUB_POLICY_EXCLUSIVE, SUB_POLICY_LOAD_BALANCED,
    SUB_STATUS_ADDRESS_OCCUPIED,
    SUB_STATUS_ADDRESS_NOT_FOUND, 
    SUB_STATUS_PERMISSION_DENIED,
    SUB_STATUS_QUOTA_EXCEEDED,
};
use crate::protocol::utils::validate_address;

struct Binding {
    /// RAII guard: drop cancels the binding token -> pump's recv() returns
    /// Detached. Never unregisters the address.
    handle: SubscriptionHandle,
    /// Kept for diagnostics; the pump stops via the binding token, not abort.
    _pump: tokio::task::JoinHandle<()>,
    /// The routing target this session attached to. Held so the attach claim
    /// is released exactly once on teardown (RouteTarget::try_attach contract).
    target: Arc<RouteTarget>,    
}

impl Drop for Binding {
    fn drop(&mut self) {
        // Cancel the binding token first so the pump observes Detached before
        // the slot claim is released and another consumer can attach.
        self.handle.cancel_token().cancel();
        // Release the attach claim exactly once. The address and its queue are
        // NOT removed from the registry (L2 §17.5).
        self.target.detach();
    }
}

pub struct SubscriptionBridge {
    registry: Arc<LocalRegistry>,
    egress_tx: EgressTx,
    session_ctx: Arc<ServerSessionContext>,
    bindings: HashMap<String, Binding>,
}

impl SubscriptionBridge {
    pub fn new(
        registry: Arc<LocalRegistry>,
        egress_tx: EgressTx,
        session_ctx: Arc<ServerSessionContext>,
    ) -> Self {
        Self {
            registry,
            egress_tx,
            session_ctx,
            bindings: HashMap::new(),
        }
    }

    /// L2 §17.3 subscription isolation rule.
    ///
    /// Allowed: the session's own address, any descendant of it, its immediate
    /// parent, or any descendant of that parent. Ancestors above the immediate
    /// parent are forbidden — otherwise a compromised node could intercept the
    /// whole domain's traffic.
    ///
    /// The source is read from the authenticated session context here (§17.2):
    /// the signature deliberately offers no way to pass one in.
    fn is_subscription_allowed(&self, address: &str) -> bool {
        fn self_or_descendant(base: &str, address: &str) -> bool {
            address == base
                || address.strip_prefix(base).is_some_and(|rest| rest.starts_with(':'))
        }

        let source = self.session_ctx.source_prefix();

        // Own address / own subtree is always allowed.
        if self_or_descendant(source, address) {
            return true;
        }

        match source.rfind(':') {
            Some(idx) => self_or_descendant(&source[..idx], address),
            // Root-level source has no parent: only its own subtree (checked above).
            None => false,
        }
    }    

    fn try_subscribe(
        &mut self,
        payload: SubscribePayload,
    ) -> Result<SubscribeAckPayload, SubscriptionError> {
        let request_id = payload.request_id;

        let address = std::str::from_utf8(&payload.address)
            .map_err(|_| SubscriptionError::Internal("address is not valid UTF-8".into()))?;

        if validate_address(address).is_err() {
            return Err(SubscriptionError::InvalidAddress(address.into()));
        }

        // §17.3: source is taken ONLY from the authenticated session identity.
        if !self.is_subscription_allowed(address) {
            let msg = format!("address '{address}' is outside the allowed subtree");
            tracing::debug!(request_id, SUB_STATUS_PERMISSION_DENIED, msg);
            let payload = SubscribeAckPayload::failure(request_id, SUB_STATUS_PERMISSION_DENIED, &msg);
            return Ok(payload);
        }

        self.bindings.retain(|_, b| !b.handle.is_detached());

        if self.bindings.contains_key(address) {
            return Err(SubscriptionError::AlreadySubscribed(address.into()));
        }

        let max_bindings = self.session_ctx.limits().max_addresses_per_session as usize;
        if self.bindings.len() >= max_bindings {
            let msg = format!("subscription quota exceeded ({max_bindings})");
            tracing::debug!(request_id, SUB_STATUS_QUOTA_EXCEEDED, msg);
            let payload = SubscribeAckPayload::failure(request_id, SUB_STATUS_QUOTA_EXCEEDED, &msg);
            return Ok(payload);
        }

        // Subscribe binds a consumer to an EXISTING address; it never creates
        // one. Address lifecycle belongs to the control plane. A missing route
        // is a semantic refusal (AddressNotFound, §7.15.2), session stays alive.
        let target = match self.registry.lookup(address) {
            Some(target) => target,
            None => {
                let msg = format!("address '{address}' is not registered");
                tracing::debug!(request_id, SUB_STATUS_ADDRESS_NOT_FOUND, msg);
                let payload = SubscribeAckPayload::failure(request_id, SUB_STATUS_ADDRESS_NOT_FOUND, &msg);
                return Ok(payload);
            }
        };

        let attachment = match target.try_attach() {
            Ok(a) => a,
            Err(AttachError::Occupied) => {
                let msg = format!("address '{address}' is held by another consumer");
                tracing::debug!(request_id, SUB_STATUS_ADDRESS_OCCUPIED, msg);
                let payload = SubscribeAckPayload::failure(request_id, SUB_STATUS_ADDRESS_OCCUPIED, &msg);
                return Ok(payload);
            }
            Err(AttachError::Removed) => {
                let msg = format!("address '{address}' was removed");
                tracing::debug!(request_id, SUB_STATUS_ADDRESS_NOT_FOUND, msg);
                let payload = SubscribeAckPayload::failure(request_id, SUB_STATUS_ADDRESS_NOT_FOUND, &msg);
                return Ok(payload);
            }
        };

        // The binding token is derived by Subscriber as a child of the address
        // token. Session-level cascade (§17.5) is enforced by ownership: the
        // connection supervisor drops this bridge on teardown, which cancels
        // every binding token and detaches every claim.
        let (subscriber, binding_token, routing_policy, queue_capacity) = match attachment {
            Attachment::Push { receiver, address_token, queue_capacity } => {
                let (sub, token) =
                    Subscriber::new_push(receiver, address.to_string(), address_token);
                (sub, token, SUB_POLICY_EXCLUSIVE, queue_capacity)
            }
            Attachment::Pull { req_sender, address_token, queue_capacity } => {
                let (sub, token) =
                    Subscriber::new_pull(req_sender, address.to_string(), address_token);
                (sub, token, SUB_POLICY_LOAD_BALANCED, queue_capacity)
            }
        };

        let pump = tokio::spawn(egress_pump(subscriber, self.egress_tx.clone()));
        let handle = SubscriptionHandle::new(address.to_string(), binding_token);

        self.bindings.insert(address.to_string(), Binding {
            handle,
            _pump: pump,
            target,
        });

        Ok(SubscribeAckPayload::success(request_id, queue_capacity as u32, routing_policy))
    }

    /// Handles Subscribe (0x20). Semantic refusals keep the session alive.
    pub fn subscribe(&mut self, payload: SubscribePayload) -> Frame {
        let request_id = payload.request_id;
        let ack = self.try_subscribe(payload)
            .unwrap_or_else(|e| {
                tracing::debug!(request_id, error = %e, "Subscribe refused");
                SubscribeAckPayload::failure(request_id, e.status(), &e.to_string())
            });
        ack.into()
    }

    /// Handles Unsubscribe (0x21). Detaches consumer binding from address.
    pub fn unsubscribe(&mut self, payload: UnsubscribePayload) -> Frame {
        let request_id = payload.request_id;
        let ack = self.try_unsubscribe(payload)
            .unwrap_or_else(|e| {
                tracing::debug!(request_id, error = %e, "Unsubscribe refused");
                SubscribeAckPayload::failure(request_id, e.status(), &e.to_string())
            });
        ack.into()
    }

    fn try_unsubscribe(
        &mut self,
        payload: UnsubscribePayload,
    ) -> Result<SubscribeAckPayload, SubscriptionError> {
        let request_id = payload.request_id;

        let address = std::str::from_utf8(&payload.address)
            .map_err(|_| SubscriptionError::Internal("address is not valid UTF-8".into()))?;

        if validate_address(address).is_err() {
            return Err(SubscriptionError::InvalidAddress(address.into()));
        }

        if !self.bindings.contains_key(address) {
            return Err(SubscriptionError::NotSubscribed(address.into()));
        }

        // Remove binding - cancels binding token, pump exits on Detached
        // Registry is NOT affected per spec 17.5
        self.bindings.remove(address);

        Ok(SubscribeAckPayload::success_unsubscribed(request_id))
    }    
}

impl Drop for SubscriptionBridge {
    fn drop(&mut self) {
        // Connection teardown (spec 17.5): dropping every handle cancels
        // every binding token; pumps exit on Detached. Registry untouched.
        self.bindings.clear();
    }
}


/// Per-binding egress pump: drains the L3 subscription queue into the
/// connection's bounded egress lane (L2 §13.5.3) with natural backpressure.
///
/// Exits when the binding token is cancelled (Detached), the address is
/// removed, or the writer actor is gone (egress channel closed).
async fn egress_pump(mut subscriber: Subscriber, egress_tx: EgressTx) {
    loop {
        match subscriber.recv().await {
            Ok(message) => {
                // Frame destination is the concrete message address.
                let address = message.address.clone();
                if egress_tx.send_data(address, vec![message]).await.is_err() {
                    // Writer actor dropped EgressRx: connection is tearing down.
                    break;
                }
            }
            Err(error) => {
                tracing::debug!(
                    address = subscriber.address(),
                    ?error,
                    "Subscription pump stopped"
                );
                break;
            }
        }
    }
}
