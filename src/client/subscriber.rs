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

use crate::protocol::Message;
use crate::registry::{LocalRegistry, RegistryError, SubscriptionSlot};
use crate::transport::{create_channel, MessageReceiver, TryRecvError};

pub struct SubscriptionHandle {
    /// Логический адрес подписки (для логирования)
    address: String,
 
    /// Ссылка на слот в реестре
    slot: Arc<SubscriptionSlot>,
 
    /// Ссылка на реестр для отмены регистрации
    registry: Arc<LocalRegistry>,
}

impl SubscriptionHandle {
    pub(crate) fn new(
        address: String,
        slot: Arc<SubscriptionSlot>,
        registry: Arc<LocalRegistry>,
    ) -> Self {
        Self { address, slot, registry }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn unsubscribe(self) {
        // Drop будет вызван автоматически
        drop(self);
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
        if let Err(e) = self.registry.unregister(&self.address, &self.slot) {
            tracing::warn!(
                address = %self.address, 
                error = %e, 
                "Failed to unregister subscription on drop"
            );
        }
    }
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
    rx: MessageReceiver,

    /// The logical address the subscription is registered for (for logging and debugging).
    address: String,

    // /// A reference to the registry for automatic unregistration upon destruction (`Drop`).
    //registry: Arc<LocalRegistry>,

    //slot: Arc<SubscriptionSlot>,
}

impl Subscriber {
    /// Internal constructor.
    ///
    /// Used by the `bind` factory method after successfully registering
    /// the sender in the registry.
    pub(crate) fn new(
        rx: MessageReceiver,
        address: String,
        //registry: Arc<LocalRegistry>,
        //slot: Arc<SubscriptionSlot>,
    ) -> Self {
        //Self { rx, address, registry, slot}
        Self { rx, address }
    }

    /// Asynchronously waits for the next message.
    ///
    /// Returns `Some(Message)` on successful receipt.
    /// Returns `None` if all senders have been destroyed.
    /// In the Arcella architecture, this is unlikely while the subscription is active,
    /// since `LocalRegistry` holds at least one strong reference
    /// to the `MessageSender`, but the method is implemented for completeness of the `tokio::mpsc` API.
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
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
        self.rx.try_recv()
    }  

    /// Returns the logical address this subscription is registered for.
    ///
    /// Useful for contextual logging, tracing, and
    /// debugging routing in multi-component Arcella deployments.
    pub fn address(&self) -> &str {
        &self.address
    }
}
