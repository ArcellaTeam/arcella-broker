// arcella-broker/src/broker_core/transport/in_memory/endpoint.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! In-process transport for Arcella Broker.
//!
//! This module implements message delivery directly via asynchronous channels
//! tokio::sync::mpsc, completely bypassing inter-process communication (IPC)
//! mechanisms such as Unix sockets.
//!
//! # Architectural role in the Arcella platform
//!
//! In the Arcella architecture, applications can run in the main process (main)
//! or in isolated worker processes (arcella-worker). This transport
//! is used in two key scenarios:
//! 1. Inside the main process: Routing between trusted async
//! components (Trusted Async Components), where isolation is not required
//! and latency must be minimal (zero-cost).
//! 2. Inside a worker process: Routing between multiple replicas
//! (replicas > 1) of the same WebAssembly module running within
//! a single arcella-worker instance.
//!
//! # Key broker guarantees
//! - Zero serialization overhead: Messages (crate::protocol::Message)
//! are passed by reference via a reference counter (Bytes), without copying the payload.
//! - Natural backpressure: Asynchronous waiting (.await)
//! when the receiver's queue is full protects the process memory from exhaustion (OOM).
//! - Conflict-free route caching: Using InMemoryEndpoint with
//! version checking (cached_version) avoids expensive lookups
//! in the LocalRegistry (Radix trees) on every message send.

use std::{
    sync::Arc,
    future::Future,
};

use crate::protocol::Message;
use crate::broker_core::registry::RouteTarget;

use super::super::{
    Endpoint, 
    TransportResult,
};

/// A cached endpoint for in-process message delivery.
///
/// # Hot Path optimization
/// Instead of accessing the LocalRegistry on every message send
/// (which would require a Radix tree lookup or scanning wildcard rules),
/// InMemoryEndpoint stores a direct reference to a `RouteTarget`
///
/// Before sending, the is_valid() method performs an ultra-fast check:
/// 1. Whether the slot's current version matches the cached one (guaranteeing that the subscriber
/// was not removed and recreated with a new channel).
/// 2. Whether the physical channel is still alive (!is_closed()).
///
/// If both conditions hold, the send happens without any locks
/// or registry lookups.
pub struct InMemoryEndpoint {
    // A direct reference to the routing target in the registry for verifying version freshness.
    target: Arc<RouteTarget>,
    /// The slot version at the time of caching. Used to detect changes (TOCTOU).
    cached_version: u64,
}

impl InMemoryEndpoint {
    /// Creates a new cached endpoint based on the found registry slot.
    ///
    /// # Guarantees
    /// The expect call here is safe because this method is called exclusively
    /// from InMemoryTransport::resolve, which returns the slot only if
    /// it exists and contains a valid MessageSender.
    pub(crate) fn new(target: Arc<RouteTarget>) -> Self {
        Self {
            cached_version: target.version(),
            target,
        }
    }
}

impl Endpoint for InMemoryEndpoint {
    /// Sends a message using the cached channel.
    ///
    /// This method does not perform a repeated registry lookup. 
    /// For Single targets: if the channel was closed, returns ConnectionClosed.
    /// For Group targets: delegates to the group's send logic, which handles 
    /// individual channel failures internally (e.g., skipping dead members in Broadcast).
    #[inline]
    fn send(
        &self,
        message: Message,
    ) -> impl Future<Output = TransportResult<()>> + Send {
        async move {
            // Use cached sender without clone
            self.target.send(message).await
        }
    }
    
    /// Validates the cached endpoint.
    ///
    /// # Validation algorithm
    /// 1. Compares the atomic version of SubscriptionSlot with cached_version.
    /// If the version has changed, it means the subscriber was re-registered (e.g.,
    /// after a component restart), and the old channel is no longer relevant.
    /// 2. Checks the physical state of the channel via is_closed().
    #[inline]
    fn is_valid(&self) -> bool {
        // Check actual status
        self.target.version() == self.cached_version && !self.target.is_closed()
    }
}
