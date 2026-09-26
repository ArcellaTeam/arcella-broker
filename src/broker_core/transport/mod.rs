// arcella-broker/src/transport/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Abstract transport layer for Arcella broker message routing.
//!
//! This module defines the key traits and data types that separate
//! logical recipient addressing (e.g., arcella:core:users or arcella:web:**)
//! from the physical message delivery mechanisms.
//!
//! # Architectural role in the Arcella platform
//!
//! The Arcella broker must provide seamless message routing across various
//! deployment topologies described in the platform architecture:
//! 1. Main ↔ Main: Delivery between trusted async components (Trusted Async Components)
//! within the main process via zero-cost channels (InMemoryTransport).
//! 2. Main ↔ Worker: Routing messages from the main process to isolated
//! WebAssembly instances (arcella-worker) via IPC (Unix sockets).
//! 3. Worker ↔ Worker: (Future) Direct or indirect routing between
//! workers without necessarily going through the main process.
//!
//! This module provides a unified contract (Transport and Endpoint) that allows
//! the broker core and clients (Publisher) to work with recipients uniformly,
//! without knowing the implementation details of a specific communication channel.
//!
//! # Key broker guarantees
//! - Separation of responsibilities: The transport is responsible only for moving bytes/messages.
//! Routing logic (Radix trees, wildcard matching) is encapsulated in LocalRegistry.
//! - Lifecycle safety (RAII): Transport errors (e.g., ConnectionClosed)
//! serve as triggers for automatic resource cleanup in the registry, preventing leaks
//! during abnormal termination (trap) of WebAssembly components.
//! - Hot/Cold path support: The presence of paired methods
//! send (with address resolution) and send_to (via a cached ResolvedEndpoint)
//! allows the broker to optimize delivery for frequently used routes.

use std::{
    future::Future,
    sync::Arc,
};

mod channel;
pub mod in_memory;

use crate::protocol::{Message, ProtocolError};
use super::registry::RegistryError;

pub use channel::*;

/// Errors that occur at the broker's transport level during message delivery.
///
/// These errors are critically important for broker management systems, as they
/// signal the need for rerouting, resource cleanup, or
/// notifying the client that delivery is impossible.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The physical delivery channel is closed.
    ///
    /// In the Arcella context, this usually means that the target WebAssembly component
    /// or async task has terminated (was destroyed or crashed with a trap),
    /// and its `MessageReceiver` was destroyed (Drop).
    #[error("Connection closed")]
    ConnectionClosed,
    
    /// The operation timed out.
    ///
    /// Critically important for the InOut (Request/Response) pattern in the WebAssembly
    /// environment. Ensures that the broker will not block resources indefinitely if
    /// the target component has hung or entered an infinite loop.
    #[error("Operation timeout")]
    Timeout,
    
    /// No recipient with the specified logical address was found in the registry.
    ///
    /// Returned when `LocalRegistry` cannot match the address to any
    /// of the registered exact matches or wildcard patterns.
    #[error("Recipient not found: {0}")]
    RecipientNotFound(String),
    
    /// An error propagated from the routing registry subsystem.
    ///
    /// May occur when attempting invalid registration or dynamic
    /// modification of routing rules during transport operation.
    #[error("Registry error: {0}")]
    Registry(#[from] RegistryError),
    
    /// An error indicating a violation of the format or semantics of the Arcella message protocol.
    ///
    /// Occurs at the stage of decoding or validating a message before sending
    /// (e.g., invalid UTF-8 in the address, exceeding the payload limit).
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    
    /// A low-level I/O error.
    ///
    /// Characteristic of transport implementations that use IPC (Unix sockets)
    /// or network connections rather than in-process memory.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// The standard result type for broker transport layer operations.
pub type TransportResult<T> = Result<T, TransportError>;

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

/// Abstract transport for sending and receiving messages.
///
/// This trait is an extension point for supporting various topologies
/// in the Arcella architecture (Main, Worker, Network). The `E` parameter allows each
/// transport to use its own optimized Endpoint type.
pub trait Transport<E: Endpoint>: Send + Sync {
    /// Resolve address for the recipient at the specified address.
    ///
    /// This is the "cold path". The implementation must query the routing
    /// subsystem (e.g., `LocalRegistry::lookup`) to find the recipient.
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
    ) -> impl Future<Output = TransportResult<ResolvedEndpoint<E>>> + Send + 'a;

    /// Send a message to a recipient at the specified address.
    ///
    /// Used for one-off sends when creating a `Publisher` and caching
    /// the endpoint is impractical. Performs the full cycle: registry lookup -> send.
    ///
    /// # Arguments
    /// * `address` - the string address of the recipient.
    /// * `message` - the message to be sent.
    ///
    /// # Returns
    /// `Ok(())` if the message is successfully queued in the channel, or an error if
    /// the recipient is not found or the channel is closed.
    fn send<'a>(
        &'a self,
        address: &'a str,
        message: Message,
    ) -> impl Future<Output = TransportResult<()>> + Send + 'a;

    /// Send a message to resolved endpoint
    ///
    /// This is the "hot path". Guarantees minimal overhead,
    /// as it completely eliminates access to the routing registry.
    ///
    /// # Arguments
    /// * `endpoint` - the endpoint for resolved address of the recipient.
    /// * `message` - the message to be sent.
    ///
    /// # Returns
    /// `Ok(())` if the message is successfully queued in the channel, or an error if
    /// the recipient is not found or the channel is closed.
    fn send_to<'a>(
        &'a self,
        endpoint: &'a ResolvedEndpoint<E>,
        message: Message,
    ) -> impl Future<Output = TransportResult<()>> + Send + 'a;

    /// Send a request and wait for a response (InOut mode) to a recipient at the specified address.
    ///
    /// Uses `ReplyDispatcher` to register waiting for a response by `message_id`.
    ///
    /// # Architectural note
    /// Although this method is defined in the transport trait, in the current Arcella architecture
    /// the actual implementation of InOut interaction is delegated to the `BrokerClient` level.
    /// This is done intentionally for safety guarantees: only `BrokerClient` can
    /// correctly inject a protected `reply_to` address and register the wait
    /// in `ReplyDispatcher` while adhering to RAII principles for the WebAssembly environment.
    /// Transport implementations may return Unsupported for this method.
    ///
    /// # Arguments
    /// * `address` - the string address of the recipient.
    /// * `message` - the request message to be sent.
    ///
    /// # Returns
    /// The response message upon successful execution, or a timeout/connection closed error.
    fn request<'a>(
        &'a self,
        address: &'a str,
        message: Message,
    ) -> impl Future<Output = TransportResult<Message>> + Send + 'a;

    /// Send a request and wait for a response (InOut mode) to resolved endpoint.
    ///
    /// This is the "hot path". Guarantees minimal overhead,
    /// as it completely eliminates access to the routing registry.
    ///
    /// # Arguments
    /// * `endpoint` - the endpoint for resolved address of the recipient.
    /// * `message` - the message to be sent.
    ///
    /// # Returns
    /// `Ok(())` if the message is successfully queued in the channel, or an error if
    /// the recipient is not found or the channel is closed.
    fn request_to<'a>(
        &'a self,
        endpoint: &'a ResolvedEndpoint<E>,
        message: Message,
    ) -> impl Future<Output = TransportResult<Message>> + Send + 'a;

    /// Receive the next incoming message (used on the server side).
    ///
    /// # Separation of responsibilities (Ingress vs Egress)
    /// In the broker architecture, this method is intended primarily for entry
    /// points (ingress), such as the IPC connection listener in arcella-worker.
    /// For internal routing within a process, components use
    /// `Subscriber` and `MessageReceiver` directly, bypassing this transport method.
    fn receive<'a>(
        &'a self,
    ) -> impl Future<Output = TransportResult<Message>> + Send + 'a;

    /// Close the transport connection.
    ///
    /// For the in-process transport (`InMemoryTransport`), this is a no-op operation,
    /// since the lifecycle is managed by reference counting (Arc) and Drop.
    /// For network or IPC transports, this method must properly close
    /// listening sockets and terminate background processing tasks.
    fn close<'a>(
        &'a self,
    ) -> impl Future<Output = TransportResult<()>> + Send + 'a;
}
