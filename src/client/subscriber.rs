// arcella-broker/src/client/subscriber.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Subscription management subsystem of the Arcella message broker.
//!
//! This module provides the `Subscriber` struct, which is the
//! entry point (ingress point) for messages directed to a specific
//! logical address in the broker (e.g., `arcella:core:users` or `arcella:web:**`).
//!
//! # Architectural role in the broker
//!
//! In the Arcella platform, components (whether trusted async tasks in the
//! main process or isolated WebAssembly instances in `arcella-worker`)
//! use `Subscriber` to receive incoming messages.
//!
//! Key guarantees this module provides to the broker:
//! 1. **Separation of concerns**:
//!    `Subscriber` is a pure data receiver — it owns the `MessageReceiver` 
//!    and provides `recv()` / `try_recv()` methods. It does NOT manage 
//!    the subscription lifecycle.
//!
//!    `SubscriptionHandle` is the RAII guard that owns the `Arc<SubscriptionSlot>` 
//!    and automatically unregisters the address from `LocalRegistry` upon drop.
//!    This separation allows moving `Subscriber` into a background task while 
//!    retaining control over the subscription lifetime in the parent scope.
//! 2. **Exclusivity guarantee**: The `bind` method registers the sender in the
//!    registry. Thanks to the strict rules of `LocalRegistry`, this guarantees
//!    that a single address cannot be occupied by two different subscribers at the same time,
//!    eliminating routing ambiguity.
//! 3. **Backpressure control**: The channel capacity is set
//!    explicitly when calling bind, which allows the broker to protect the process memory
//!    from overflow if the receiving component processes messages
//!    more slowly than they arrive.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::{
    broker_core::{
        registry::{
            LocalRegistry, 
            LoadBalancedGroup,
            SubscriptionSlot
        },
    },
};

enum CleanupStrategy {
    /// Exclusive: unregister from the registry
    Exclusive {
        slot: Arc<SubscriptionSlot>,
        registry: Arc<LocalRegistry>,
    },
    /// LoadBalanced: remove_consumer from the group
    LoadBalanced {
        group: Arc<LoadBalancedGroup>,
    },
}

pub struct SubscriptionHandle {
    /// Logical subscription address (for logging)
    address: String,

    cleanup: CleanupStrategy,

    /// Token bound to this specific subscription.
    /// Cancelled immediately on drop to wake up any pending `recv()` calls.
    cancel_token: CancellationToken,
}

impl SubscriptionHandle {
    /// Creates a guard for an Exclusive subscription (active cleanup on drop)
    pub(crate) fn new_exclusive(
        address: String,
        slot: Arc<SubscriptionSlot>,
        registry: Arc<LocalRegistry>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self { 
            address, 
            cleanup: CleanupStrategy::Exclusive { slot, registry },
            cancel_token,
        }
    }
    
    /// Creates a guard for a LoadBalanced subscription (passive, cleanup via refcount)
    pub(crate) fn new_load_balanced(
        address: String,
        group: Arc<LoadBalancedGroup>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self { 
            address, 
            cleanup: CleanupStrategy::LoadBalanced { group },
            cancel_token,
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }
}

/// # Architectural guarantee (RAII)
/// When a `SubscriptionHandle` goes out of scope:
/// 1. `registry.unregister(&self.address, &self.slot)` is called.
/// 2. `RouteTarget::remove_slot` marks the slot as removed and bumps 
///    the version, instantly invalidating all cached `InMemoryEndpoint`s.
/// 3. If the target is a Group, only this specific slot is removed; 
///    other group members continue receiving messages.
/// 4. The route is removed from the tree only when the last slot is gone.
impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        tracing::debug!("SubscriptionHandle drop: {}", self.address);

        match &self.cleanup {
            CleanupStrategy::Exclusive { slot, registry } => {
                if let Err(e) = registry.unregister(&self.address, slot) {
                    tracing::warn!(
                        address = %self.address, 
                        error = %e, 
                        "Failed to unregister subscription on drop"
                    );
                }
            }
            CleanupStrategy::LoadBalanced { group } => {
                group.remove_consumer();
            }
        }
    }
}
