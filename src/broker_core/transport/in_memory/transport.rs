// arcella-broker/src/broker_core/transport/in_memory/transport.rs
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
use crate::broker_core::registry::LocalRegistry;

use super::super::{
    ResolvedEndpoint, 
    Transport, 
    TransportError, 
    TransportResult,
};

use super::InMemoryEndpoint;

/// Transport for in-process delivery.
/// 
/// Messages are routed directly through `tokio::mpsc` channels,
/// bypassing IPC. It is used when the recipient is located
/// in the same process as the sender.
pub struct InMemoryTransport {
    /// Local registry for looking up recipient channels by address.
    registry: Arc<LocalRegistry>,
}

impl InMemoryTransport {
    /// Creates a new instance of `InMemoryTransport`.
    ///
    /// # Arguments
    /// * `registry` - a shared reference to the local routing registry.
    pub fn new(registry: Arc<LocalRegistry>) -> Self {
        Self {registry}
    }
}

impl Transport<InMemoryEndpoint> for InMemoryTransport {
    /// Resolve address for the recipient at the specified address.
    ///
    /// # Arguments
    /// * `address` - the string address of the recipient.
    ///
    /// # Returns
    /// `Ok(ResolvedEndpoint)` if the address is successfully resolved, or an error if
    /// the recipient is not found or the channel is closed.
    fn resolve<'a>(
        &'a self,
        address: &'a str,
    ) -> impl Future<Output = TransportResult<ResolvedEndpoint<InMemoryEndpoint>>> + Send + 'a {
        async move {
            match self.registry.lookup(address) {
                Some(channel) => {
                    // Create a type-erased endpoint
                    Ok(ResolvedEndpoint::new(InMemoryEndpoint::new(channel)))
                }
                None => Err(TransportError::RecipientNotFound(address.to_string())),
            }
        }
    }
 
    /// Send a message to resolved endpoint
    ///
    /// This is the "fast path". Delegates the send directly to the
    /// send method of the InMemoryEndpoint struct, avoiding a repeated lookup in the LocalRegistry.
    ///
    /// # Arguments
    /// * `endpoint` - the endpoint for resolved address of the recipient.
    /// * `message` - the message to be sent.
    ///
    /// # Returns
    /// `Ok(())` if the message is successfully queued in the channel, or an error if
    /// the recipient is not found or the channel is closed.
    #[inline]
    fn send_to<'a>(
        &'a self,
        endpoint: &'a ResolvedEndpoint<InMemoryEndpoint>,
        message: Message,
    ) -> impl Future<Output = TransportResult<()>> + Send + 'a {
        async move {
            // Delegate the sending to the endpoint itself
            endpoint.send(message).await
        }
    }    

    /// Sends a request to resolved endpoint and waits for a response with a timeout.
    ///
    /// See the documentation for request. The same architectural constraint applies.
    ///
    /// Uses `ReplyDispatcher` to register waiting for a response by `message_id`.
    ///
    /// # Arguments
    /// * `endpoint` - the endpoint for resolved address of the recipient.
    /// * `message` - the request message to be sent.
    ///
    /// # Returns
    /// The response message upon successful execution, or a timeout/connection closed error.
    fn request_to<'a>(
        &'a self,
        _endpoint: &'a ResolvedEndpoint<InMemoryEndpoint>,
        _message: Message,
    ) -> impl Future<Output = TransportResult<Message>> + Send + 'a {
        async move {
            Err(TransportError::Unsupported)
        }
    }   

    /// Method for receiving messages (stub for this implementation).
    ///
    /// In the current broker architecture, InMemoryTransport is responsible exclusively
    /// for sending (egress). Receiving messages (ingress) is managed directly
    /// by the receiving component via MessageReceiver, which it obtains when
    /// calling Subscriber::bind. This eliminates channel duplication and ensures
    /// strict control over the lifetime of the message queue.
    fn receive<'a>(
        &'a self,
    ) -> impl Future<Output = TransportResult<Message>> + Send + 'a {
        async move {
            // TODO: Implement if a unified receive interface is needed 
            // for all transport types. For now, return a connection closed error.
            Err(TransportError::ConnectionClosed)
        }
    }

    /// Closes the transport.
    ///
    /// For in-process transport, explicit closing is not required. 
    /// The lifetime of channels is managed by reference counting (ARC) rules and
    /// automatic cleanup when Drop is called for Subscriber or BrokerClient,
    /// which triggers LocalRegistry::unregister.
    fn close<'a>(
        &'a self,
    ) -> impl Future<Output = TransportResult<()>> + Send + 'a {
        async move { Ok(()) }
    }
}
