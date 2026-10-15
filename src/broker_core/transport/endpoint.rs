// arcella-broker/src/broker_core/transport/endpoint.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::{
    future::Future,
    sync::Arc,
};

use crate::protocol::Message;

use super::TransportResult;

/// An abstract message delivery endpoint.
///
/// Each concrete transport implementation (InMemory, IPC, Network) provides
/// its own Endpoint type, encapsulating the specifics of delivery (e.g., a cloned
/// `mpsc::Sender` or a socket descriptor).
///
/// # Architectural note
/// The transport (`Transport`) does NOT know about the internal structure of the `Endpoint`.
/// It simply calls the `send` method, ensuring a strict separation of responsibilities
/// between *finding* a route and *using* it.
pub trait Endpoint: Send + Sync {
    /// Sends a message to this delivery endpoint.
    /// The implementation must provide natural backpressure
    /// where applicable to the given transport type, to protect the broker from OOM.
																	
    fn send(
        &self,
        message: Message,
    ) -> impl Future<Output = TransportResult<()>> + Send;

    /// Checks the freshness and liveness of the delivery point.
    ///
    /// # Guarantees for the broker
    /// Returns `true` only if the physical channel is alive AND its internal version
    /// (or state) is fresh. This is a key mechanism for caching
    /// in `Publisher`: it allows avoiding a race condition (TOCTOU) when a component
    /// was restarted and received a new channel, while the cache still references the old one.
    fn is_valid(&self) -> bool;
}

/// A type-safe, type-erased (via Arc) wrapper over a concrete
/// `Endpoint` implementation.
///
/// # Role in broker optimization
/// The broker client (`Publisher`) requests a `ResolvedEndpoint` once on the first
/// access to an address, then caches it. This allows subsequent
/// `send` calls to operate on the "hot path" (O(1)), bypassing the expensive lookup in
/// `LocalRegistry` (Radix trees or wildcard rule scanning).
pub struct ResolvedEndpoint<E: Endpoint> {
    inner: Arc<E>,
}

impl<E: Endpoint> Clone for ResolvedEndpoint<E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<E: Endpoint> ResolvedEndpoint<E> {
    /// Creates a new wrapper over the resolved delivery point.
    pub(crate) fn new(endpoint: E) -> Self {
        Self {
            inner: Arc::new(endpoint),
        }
    }
    
    /// Delegates message sending to the inner `Endpoint` implementation.
    pub async fn send(&self, message: Message) -> TransportResult<()> {
        self.inner.send(message).await
    }
    
    /// Delegates the validity check to the inner implementation.
    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }
}
