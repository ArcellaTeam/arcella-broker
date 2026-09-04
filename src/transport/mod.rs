// arcella-broker/src/transport/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
};

pub mod in_memory;

use crate::protocol::{Message, ProtocolError};
use crate::registry::RegistryError;

/// Transport error
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Connection closed")]
    ConnectionClosed,
    
    #[error("Operation timeout")]
    Timeout,
    
    #[error("Recipient not found: {0}")]
    RecipientNotFound(String),
    
    #[error("Registry error: {0}")]
    Registry(#[from] RegistryError),
    
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type TransportResult<T> = Result<T, TransportError>;

/// Abstract delivery endpoint.
/// 
/// Each transport implements its own type of Endpoint,
/// encapsulating delivery specifics (channel, IPC connection, TCP stream, etc.).
/// Transport does NOT know about the internals of Endpoint — it simply calls `send`.

pub trait Endpoint: Send + Sync {
    /// Sends a message to this delivery endpoint.
    fn send<'a>(
        &'a self,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + 'a>>;

    /// Checks whether the delivery endpoint is alive (not closed).
    fn is_alive(&self) -> bool;
}

/// Type-erased обёртка над конкретной реализацией Endpoint.
/// 
/// The client (Publisher) caches ResolvedEndpoint and uses it for repeated sends,
/// without knowing or caring which transport is behind it.
#[derive(Clone)]
pub struct ResolvedEndpoint {
    inner: Arc<dyn Endpoint>,
}

impl ResolvedEndpoint {
    /// Creates a ResolvedEndpoint from a concrete Endpoint implementation.
    /// Used only inside Transport implementations.
    pub(crate) fn new<E: Endpoint + 'static>(endpoint: E) -> Self {
        Self {
            inner: Arc::new(endpoint),
        }
    }
    
    /// Sends a message to the delivery endpoint.
    pub async fn send(&self, message: Message) -> TransportResult<()> {
        self.inner.send(message).await
    }
    
    /// Checks whether the delivery endpoint is alive.
    pub fn is_alive(&self) -> bool {
        self.inner.is_alive()
    }
}

/// Abstract transport for sending and receiving messages.
pub trait Transport: Send + Sync {
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
    ) -> Pin<Box<dyn Future<Output = TransportResult<ResolvedEndpoint>> + Send + 'a>>;

    /// Send a message to a recipient at the specified address.
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
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + 'a>>;

    /// Send a message to resolved endpoint
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
        endpoint: &'a ResolvedEndpoint,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + 'a>>;

    /// Send a request and wait for a response (InOut mode) to a recipient at the specified address.
    ///
    /// Uses `ReplyDispatcher` to register waiting for a response by `message_id`.
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
    ) -> Pin<Box<dyn Future<Output = TransportResult<Message>> + Send + 'a>>;

    /// Send a request and wait for a response (InOut mode) to resolved endpoint.
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
        endpoint: &'a ResolvedEndpoint,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Message>> + Send + 'a>>;

    /// Receive the next incoming message (used on the server side).
    fn receive<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Message>> + Send + 'a>>;

    /// Close the transport connection.
    fn close<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + 'a>>;
}
