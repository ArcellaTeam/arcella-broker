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
//! 1. **Automatic lifecycle management (RAII)**:
//! When a `Subscriber` is destroyed (scope exit, task panic,
//! or graceful shutdown of a Wasm component), the address is automatically
//! unregistered in `LocalRegistry`. This prevents the appearance of "zombie" routes and
//! memory leaks, and also frees the address for subsequent re-registration
//! (e.g., when a component is updated or restarted).
//! 2. **Exclusivity guarantee**: The `bind` method registers the sender in the
//! registry. Thanks to the strict rules of `LocalRegistry`, this guarantees
//! that a single address cannot be occupied by two different subscribers at the same time,
//! eliminating routing ambiguity.
//! 3. **Backpressure control**: The channel capacity is set
//! explicitly when calling bind, which allows the broker to protect the process memory
//! from overflow if the receiving component processes messages
//! more slowly than they arrive.

use std::sync::Arc;

use crate::protocol::Message;
use crate::registry::{LocalRegistry, RegistryError};
use crate::transport::channel::{self, MessageReceiver, TryRecvError};

/// An active subscription to messages at a specific logical address.
///
/// This struct owns the receiving end of the channel (`MessageReceiver`).
/// As long as a `Subscriber` instance exists, the address is considered occupied in `LocalRegistry`,
/// and the broker will route incoming messages into this channel.
///
/// # Important note on ownership
/// `Subscriber` holds `Arc<LocalRegistry>` solely for the purpose of automatic
/// cleanup in the `Drop` method. It does not use the registry for receiving messages,
/// ensuring a zero-cost read path.
pub struct Subscriber {
    /// The receiving end of the asynchronous channel, protected against overflow.																  
    rx: MessageReceiver,

    /// The logical address the subscription is registered for (for logging and debugging).
    address: String,

    /// A reference to the registry for automatic unregistration upon destruction (`Drop`).
    registry: Arc<LocalRegistry>,
}

impl Subscriber {
    /// Internal constructor.
    ///
    /// Used by the `bind` factory method after successfully registering
    /// the sender in the registry.
    pub(crate) fn new(
        rx: MessageReceiver,
        address: String,
        registry: Arc<LocalRegistry>,
    ) -> Self {
        Self { rx, address, registry }
    }

/// Binds a new channel to the specified address in the broker registry.
///
/// This is the primary subscription initialization method. It performs the following steps:
/// 1. Creates a `MessageSender` / `MessageReceiver` pair with the given capacity.
/// 2. Registers the `MessageSender` in `LocalRegistry` at the specified address.
/// 3. Returns a `Subscriber` that owns the `MessageReceiver`.
///
/// # Parameters
/// * `address` - The logical address to subscribe to (exact addresses
/// and wildcard patterns are supported if permitted by the registry configuration).
/// * `registry` - A reference to the process-wide routing registry.
/// * `capacity` - The maximum number of messages in the queue. Determines
/// the backpressure threshold for senders.
///
/// # Errors
/// Returns `RegistryError::AddressAlreadyOccupied` or `RegistryError::ConflictsWithWildcard`
/// if the address or an overlapping pattern is already registered by another component.
    pub fn bind(
        address: String, 
        registry: Arc<LocalRegistry>, 
        capacity: usize
    ) -> Result<Self, RegistryError> {
        tracing::debug!("bind: {}", address);
        let (sender, receiver) = channel::create_channel(capacity);

        // Registration makes the address visible to the broker router.
        // If an error occurs here, the sender will be destroyed, and the channel will not leak.
        registry.register(address.clone(), sender)?;

        Ok(Self::new(receiver, address, registry))
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

/// Automatic resource cleanup when the subscriber's lifecycle ends.
///
/// # Architectural guarantee (RAII)
/// This method is critically important for broker stability.
/// When a `Subscriber` goes out of scope (e.g., when a Rust task finishes
/// or when a WebAssembly instance's host is destroyed), the following occurs:
					   
/// 1. `registry.unregister(&self.address)` is called.
/// 2. `LocalRegistry` marks the slot as removed and updates its version,
/// which instantly invalidates all cached ResolvedEndpoints
/// on the sender side (TOCTOU protection).
/// 3. `MessageReceiver` is destroyed, closing the channel for all remaining
/// senders and generating a `ChannelClosed` error for them.
///
/// This guarantees the absence of memory leaks and "dangling" pointers in the registry,
/// even in the event of an abnormal termination (trap) of a Wasm component.
impl Drop for Subscriber {
    fn drop(&mut self) {
        tracing::debug!("drop: {}", self.address);
        // Automatic cleanup when going out of scope
        // The error is ignored (with logging) if the address has already been removed
        // (e.g., during a forced shutdown of the entire broker), to
        // prevent a panic in the destructor.
																			 
        if let Err(e) = self.registry.unregister(&self.address) {
            tracing::warn!(address = %self.address, error = %e, "Failed to unregister subscriber on drop");
        }
    }
}
