// arcella-broker/src/client/publisher.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! High-performance message sending component (Publisher) of the Arcella broker.
//!
//! This module implements the publisher abstraction, which is responsible for efficient
//! delivery of outgoing messages (egress) to a given logical address.
//!
//! # Architectural role in the Arcella platform
//!
//! In Arcella topologies (Main ↔ Main, Main ↔ Worker, Worker ↔ Worker), the frequency
//! of sending messages to the same address can be extremely high.
//! Constantly looking up the route in `LocalRegistry` (with wildcard rule checks)
//! or resolving an IPC connection on every `send` call would create unacceptable
//! overhead.
//!
//! `Publisher` solves this problem by implementing intelligent caching
//! of resolved endpoints (`ResolvedEndpoint`), providing the following guarantees:
//! 1. **Hot Path Optimization**: After the first
//!    successful address resolution, subsequent sends happen in O(1)
//!    without accessing the routing registry.
//! 2. **Protection against race conditions (TOCTOU)**: Using the `is_valid()` check
//!    guarantees that a message will not be sent to a "zombie" channel
//!    that has received a new physical channel.
//! 3. **Automatic recovery (Self-healing)**: When a connection break is detected
//!    (`ConnectionClosed`), the cache is automatically invalidated, forcing
//!    the next send attempt to re-resolve the current address.

use std::sync::Arc;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};

use crate::protocol::Message;
use crate::broker_core::transport::{
    Endpoint,
    ResolvedEndpoint,
    Transport,
    TransportError,
    TransportResult
};

/// A universal message publisher for a given logical address.
///
/// Parameterized by the transport type (`T`) and the endpoint type (`E`), which allows
/// a unified API to be used both for in-process delivery (`InMemoryTransport`)
/// and for future inter-process (IPC) or network routing.
///
/// # State management
/// Uses `parking_lot::RwLock` to store the cached endpoint.
/// This is the optimal choice for the broker, since read operations (cache checks)
/// occur orders of magnitude more frequently than write operations (cache updates on failure
/// or first address resolution), and `parking_lot` minimizes
/// locking overhead in a multi-threaded environment.
pub struct Publisher<T, E>
where
    T: Transport<E>,
    E: Endpoint,
{
    /// The target logical address of the recipient (e.g., "arcella:core:users").
    address: String,

    /// A reference to the transport layer used for address resolution and sending.
    transport: Arc<T>,

    /// Cache of the resolved endpoint. `None` means the address has not yet been resolved
    /// or the cache was forcibly invalidated due to a delivery error.
    cached_endpoint: RwLock<Option<ResolvedEndpoint<E>>>,
}

impl<T, E> Publisher<T, E>
where
    T: Transport<E>,
    E: Endpoint,
{
    /// Creates a new `Publisher` instance for the specified address and transport.
    ///
    /// Initially, the endpoint cache is empty (`None`). The first call to send
    /// initiates the "cold path" with full address resolution.
    pub(crate) fn new(address: String, transport: Arc<T>) -> Self {
        Self {
            address,
            transport,
            cached_endpoint: RwLock::new(None),
        }
    }

/// Retrieves the current endpoint using a double-checked locking caching strategy
/// to minimize lock contention.
///
/// # Algorithm
/// 1. **Fast Path**: Acquires a regular read lock.
///    If the endpoint exists and `is_valid()` returns `true` (the slot version
///    is fresh and the channel is not closed), it is returned immediately.
/// 2. **Address Resolution (Slow Path)**: If the cache is empty or invalid,
///    `transport.resolve` is called, which performs a registry lookup (O(L) or O(N) for wildcard).
/// 3. **Upgradable Read**: Before writing to the cache, it checks whether
///    another thread has updated the cache while the current thread was performing
///    the "slow" resolution.
/// 4. **Write**: If the cache is still invalid, the lock is upgraded to
///    an exclusive write lock, and the new `ResolvedEndpoint` is stored.
///
/// This pattern guarantees the absence of data races when a single `Publisher`
/// is used concurrently from multiple tasks (e.g., when scaling replicas).
    async fn get_or_resolve_endpoint(&self) -> TransportResult<ResolvedEndpoint<E>> {
        // 1. Fast endpoint check (read without write lock)
        {
            let guard = self.cached_endpoint.read();
            if let Some(ep) = guard.as_ref() {
                // Check alive and version of endpoint
                if ep.is_valid() {
                    tracing::trace!("Publisher fast resolve endpoint");
                    return Ok(ep.clone());
                }
            }
        }

        // 2. Slow path: address resolution via the transport (registry access)
        let resolved_ep = self.transport.resolve(&self.address).await?;

        // 3. Check before updating (avoid unnecessary lock privilege escalation)
        let guard = self.cached_endpoint.upgradable_read();
        if let Some(ep) = guard.as_ref() {
            if ep.is_valid() {
                tracing::trace!("Publisher endpoint already updated along with resolve");
                return Ok(ep.clone());
            }
        }

        // 4. Exclusive write to the cache
        let mut write_guard = RwLockUpgradableReadGuard::upgrade(guard);
        // Re-check after acquiring the exclusive lock (classic double-check)
        if let Some(ep) = write_guard.as_ref() {
            if ep.is_valid() {
                tracing::trace!("Publisher endpoint already updated befor write lock");
                return Ok(ep.clone());
            }
        }

        tracing::trace!("Publisher has been updated");
        *write_guard = Some(resolved_ep.clone());
        Ok(resolved_ep)
    }


    /// Asynchronously sends a message via a cached or dynamically resolved route.
    ///
    /// # Failure handling and self-healing
    /// If the transport returns `TransportError::ConnectionClosed`, this means
    /// the target component has terminated or its channel has been broken. 
    /// In this case, `Publisher` performs an **explicit
    /// cache invalidation** (`*self.cached_endpoint.write() = None`).
    ///
    /// This is critically important: on the next send attempt (or upon automatic
    /// restart of the component by the `WorkerManager`), `Publisher` will re-resolve
    /// the address and obtain a reference to a new, live channel, ensuring fault tolerance
    /// without the need to recreate the `Publisher` object itself.
    pub async fn send(&self, message: Message) -> TransportResult<()> {
        let ep = self.get_or_resolve_endpoint().await?;

        match self.transport.send_to(&ep, message).await {
            Err(TransportError::ConnectionClosed) => {
                // Forced cache invalidation on connection loss
                *self.cached_endpoint.write() = None;
                Err(TransportError::ConnectionClosed)
            }
            other => other,
        }
    }    

    /// Returns the logical address associated with this publisher.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Forces invalidation of the cached endpoint.
    ///
    /// # Usage scenarios in Arcella
    /// May be called explicitly upon receiving external lifecycle management
    /// events. This forces `Publisher` to immediately discard the old route and
    /// resolve a new one on the next send, minimizing the number of failed delivery
    /// attempts during the transition.
    pub fn invalidate_cache(&self) {
        *self.cached_endpoint.write() = None;
    }        
}
