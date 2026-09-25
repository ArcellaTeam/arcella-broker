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

use crate::{
    protocol::Message,
    registry::{
        LocalRegistry, 
        LoadBalancedGroup,
        SubscriptionSlot
    },
    transport::{
        MessageReceiver, 
        RequestSender,
        TryRecvError
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
}

impl SubscriptionHandle {
    /// Creates a guard for an Exclusive subscription (active cleanup on drop)
    pub(crate) fn new_exclusive(
        address: String,
        slot: Arc<SubscriptionSlot>,
        registry: Arc<LocalRegistry>,
    ) -> Self {
        Self { 
            address, 
            cleanup: CleanupStrategy::Exclusive { slot, registry },
        }
    }
    
    /// Creates a guard for a LoadBalanced subscription (passive, cleanup via refcount)
    pub(crate) fn new_load_balanced(
        address: String,
        group: Arc<LoadBalancedGroup>,
    ) -> Self {
        Self { 
            address, 
            cleanup: CleanupStrategy::LoadBalanced { group },
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

enum SubscriberMode {
    /// Push mode: messages arrive in MessageReceiver (Exclusive, Broadcast)
    Push(MessageReceiver),
    /// Pull mode: messages are requested via RequestSender (LoadBalanced)
    Pull(Arc<RequestSender>),
}

/// An active subscription to messages at a specific logical address.
///
/// This struct owns the receiving end of the channel (`MessageReceiver`).
/// As long as a `Subscriber` instance exists, the address is considered occupied in `LocalRegistry`,
/// and the broker will route incoming messages into this channel.
///
/// # Important note on ownership
/// `Subscriber` is intentionally lightweight — it holds only the `MessageReceiver` 
/// and the address string. It does NOT hold a reference to `LocalRegistry` 
/// and does NOT implement automatic cleanup.
///
/// Lifecycle management (unregistration) is the responsibility of 
/// `SubscriptionHandle`, which is returned alongside `Subscriber` from 
/// `BrokerClient::subscribe()`.
pub struct Subscriber {
    /// The receiving end of the asynchronous channel, protected against overflow.																  
    mode: SubscriberMode,

    /// The logical address the subscription is registered for (for logging and debugging).
    address: String,
}

impl Subscriber {
    /// Creates a Push subscriber (for Exclusive and Broadcast)
    pub(crate) fn new_push(
        rx: MessageReceiver,
        address: String,
    ) -> Self {
        Self {
            mode: SubscriberMode::Push(rx),
            address,
        }
    } 

    /// Creates a Pull subscriber (for LoadBalanced)
    pub(crate) fn new_pull(
        req_sender: Arc<RequestSender>,
        address: String,
    ) -> Self {
        Self {
            mode: SubscriberMode::Pull(req_sender),
            address,
        }
    }       

    /// Asynchronously waits for the next message.
    ///
    /// Returns `Some(Message)` on successful receipt.
    /// Returns `None` if all senders have been destroyed.
    /// In the Arcella architecture, this is unlikely while the subscription is active,
    /// since `LocalRegistry` holds at least one strong reference
    /// to the `MessageSender`, but the method is implemented for completeness of the `tokio::mpsc` API.
    pub async fn recv(&mut self) -> Option<Message> {
        match &mut self.mode {
            SubscriberMode::Push(rx) => rx.recv().await,
            SubscriberMode::Pull(req_sender) => {
                match req_sender.request().await {
                    Ok(reply_rx) => reply_rx.await.ok(),
                    Err(_) => None,
                }
            }
        }
    }

    /// A non-blocking attempt to retrieve a message from the queue.
    ///
    /// # Importance of the `#[must_use]` attribute
    /// The attribute requires the calling code to explicitly handle the result.
    /// Ignoring the return value will result in irretrievable loss of
    /// the message, which in the broker context could mean losing a critical
    /// event or request. Use this method only in polling
    /// loops or when implementing your own non-blocking event handlers.
    #[must_use = "The result must be handled, otherwise the message will be dropped"]
    pub fn try_recv(&mut self) -> Result<Message, TryRecvError> {
        match &mut self.mode {
            SubscriberMode::Push(rx) => rx.try_recv(),
            SubscriberMode::Pull(_) => Err(TryRecvError::Empty),
        }
    }  

    /// Returns the logical address this subscription is registered for.
    ///
    /// Useful for contextual logging, tracing, and
    /// debugging routing in multi-component Arcella deployments.
    pub fn address(&self) -> &str {
        &self.address
    }
}
