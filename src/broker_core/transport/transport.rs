// arcella-broker/src/broker_core/transport/transport.rs
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

use std::future::Future;

use crate::protocol::{
    Message,
    ProtocolError,
};

pub use super::{
    Endpoint,
    ResolvedEndpoint,
};
pub use super::error::*;

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
    /// The recipient address is automatically extracted from `message.address`,
    /// eliminating parameter duplication and the risk of desynchronization.
    /// Used for one-off sends when creating a `Publisher` and caching
    /// the endpoint is impractical. Performs the full cycle: registry lookup -> send.
    ///
    /// # Arguments
    /// * `message` - the message to be sent.
    ///
    /// # Returns
    /// `Ok(())` if the message is successfully queued in the channel, or an error if
    /// the recipient is not found or the channel is closed.
    fn send<'a>(
        &'a self,
        message: Message,
    ) -> impl Future<Output = TransportResult<()>> + Send + 'a {
        async move {
            let address_str = std::str::from_utf8(&message.address)
                .map_err(|_| TransportError::Protocol(ProtocolError::InvalidAddressUtf8))?;
            
            let endpoint = self.resolve(address_str).await?;
            self.send_to(&endpoint, message).await
        }
    }

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
    ///
    /// The recipient address is automatically extracted from `message.address`,
    /// eliminating parameter duplication and the risk of desynchronization.
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
    /// * `message` - the request message to be sent.
    ///
    /// # Returns
    /// The response message upon successful execution, or a timeout/connection closed error.
    fn request<'a>(
        &'a self,
        message: Message,
    ) -> impl Future<Output = TransportResult<Message>> + Send + 'a{
       async move {
           let address_str = std::str::from_utf8(&message.address)
               .map_err(|_| TransportError::Protocol(ProtocolError::InvalidAddressUtf8))?;

           let endpoint = self.resolve(address_str).await?;
           self.request_to(&endpoint, message).await
       }
    }

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
