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
use super::InMemoryEndpoint;

/// A type-safe, type-erased (via Arc) wrapper over a concrete
/// `Endpoint` implementation.
///
/// # Role in broker optimization
/// The broker client (`Publisher`) requests a `ResolvedEndpoint` once on the first
/// access to an address, then caches it. This allows subsequent
/// `send` calls to operate on the "hot path" (O(1)), bypassing the expensive lookup in
/// `LocalRegistry` (Radix trees or wildcard rule scanning).
pub struct ResolvedEndpoint {
    inner: Arc<InMemoryEndpoint>,
}

impl Clone for ResolvedEndpoint {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl ResolvedEndpoint {
    /// Creates a new wrapper over the resolved delivery point.
    pub(crate) fn new(endpoint: InMemoryEndpoint) -> Self {
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
