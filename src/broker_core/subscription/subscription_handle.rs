// arcella-broker/src/broker_core/subscription/subscription_handle.rs
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
//!
//! 1. **Separation of concerns**:
//!    `Subscriber` is a pure data receiver — it owns the read engine and
//!    provides `recv()` / `try_recv()` methods. It does NOT manage the
//!    subscription lifecycle.
//!
//!    `SubscriptionHandle` is the RAII guard that owns the binding-scoped
//!    `CancellationToken` and automatically detaches the consumer upon drop.
//!    This separation allows moving `Subscriber` into a background task while
//!    retaining control over the subscription lifetime in the parent scope.
//!
//! 2. **Detach-only semantics (control plane / data plane boundary)**:
//!    Dropping a `SubscriptionHandle` **detaches the consumer** from the address,
//!    but does NOT delete the address itself. The address remains registered in
//!    `LocalRegistry`, its queue stays alive, and buffered messages are preserved
//!    for the next attached consumer. Address lifecycle (creation, configuration,
//!    deletion) belongs exclusively to the control plane (`BrokerControl`), never
//!    to the data plane (`Subscribe` / `Unsubscribe`).
//!
//! 3. **Backpressure control**: The channel capacity is set when the address is
//!    created by the control plane, which allows the broker to protect the process
//!    memory from overflow if the receiving component processes messages more
//!    slowly than they arrive. While no consumer is attached, messages accumulate
//!    up to the queue capacity and then apply backpressure to senders.

use tokio_util::sync::CancellationToken;

/// RAII guard that controls the lifetime of a consumer binding.
///
/// # Architectural guarantee (RAII)
///
/// When a `SubscriptionHandle` goes out of scope:
/// 1. The binding-scoped `CancellationToken` is cancelled.
/// 2. This wakes up any pending `Subscriber::recv()` or `Subscriber::try_recv()`
///    calls, causing them to return `Err(RecvError::Detached)` or
///    `Err(TryRecvError::Closed(RecvError::Detached))`.
/// 3. The consumer is detached from the address, but the address itself remains
///    registered in `LocalRegistry` with its queue intact.
///
/// # Control plane / data plane boundary
///
/// This guard **never** unregisters the address from `LocalRegistry`, never closes
/// the address queue, and never discards buffered messages. Address lifecycle
/// (creation, configuration, deletion) belongs exclusively to the control plane
/// (`BrokerControl`), not to the data plane (`Subscribe` / `Unsubscribe`).
///
/// # Cancellation hierarchy
///
/// The binding token stored in this guard is a **child** of the address-scoped
/// token owned by the `SubscriptionSlot`. This means:
/// - Cancelling the binding token detaches **only this consumer**.
/// - Cancelling the address token (via `BrokerControl::delete_address`) detaches
///   **all consumers** attached to the address and invalidates the queue.
///
/// Because address removal cancels both tokens (parent → child propagation),
/// `Subscriber::recv()` checks `address_cancel` **before** `cancel` to correctly
/// classify the terminal error as `AddressRemoved` rather than `Detached`.

pub struct SubscriptionHandle {
    /// Logical subscription address (for logging and diagnostics).
    address: String,

    /// Binding-scoped token. Cancelled immediately on drop to detach the consumer
    /// and wake up any pending `recv()` calls.
    ///
    /// This token is always a child of the address-scoped token owned by the
    /// `SubscriptionSlot`. Cancelling it detaches only this consumer without
    /// affecting the address or other consumers.
    cancel_token: CancellationToken,
}

impl SubscriptionHandle {
    /// Creates a guard for a consumer binding.
    ///
    /// # Arguments
    /// * `address` — logical address this binding is attached to (for logging).
    /// * `cancel_token` — binding-scoped token, must be a child of the address-scoped
    ///   token. Cancelling it detaches only this consumer.
    pub(crate) fn new(address: String, cancel_token: CancellationToken) -> Self {
        Self { address, cancel_token }
    }

    /// Returns the logical address this binding is attached to.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Returns a reference to the binding-scoped cancellation token.
    ///
    /// Useful for checking whether the binding has been detached without
    /// waiting for `recv()` to return an error.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// Whether this binding has been detached.
    pub fn is_detached(&self) -> bool {
        self.cancel_token.is_cancelled()
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        tracing::debug!(address = %self.address, "SubscriptionHandle drop: detaching consumer");
        
        // Cancel the binding token to wake up any pending recv() calls.
        // This does NOT unregister the address — that is the responsibility
        // of the control plane (BrokerControl), not the data plane.
        self.cancel_token.cancel();
    }
}
