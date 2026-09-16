// arcella-broker/src/transport/in_memory.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! In-process transport for Arcella Broker.
//!
//! This module implements message delivery directly via asynchronous `tokio::mpsc` channels,
//! bypassing inter-process communication (IPC) mechanisms. It is used in cases where
//! the sender and receiver are within the same process, ensuring
//! minimal latency and zero serialization overhead.

use std::{
    sync::Arc,
    future::Future,
    pin::Pin,
};

use crate::protocol::Message;
use crate::registry::{LocalRegistry, SubscriptionSlot};

use super::{Endpoint, ResolvedEndpoint, Transport, TransportError, TransportResult};

pub struct InMemoryEndpoint {
    channel: Arc<SubscriptionSlot>,
    cached_version: u64,
}

impl InMemoryEndpoint {
    pub(crate) fn new(channel: Arc<SubscriptionSlot>) -> Self {
        let (_, cached_version) = channel.load();
        Self { 
            channel, 
            cached_version,  
        }
    }
}

impl Endpoint for InMemoryEndpoint {
    fn send(
        &self,
        message: Message,
    ) -> impl Future<Output = TransportResult<()>> + Send {
        Box::pin(async move {
            let (sender, _) = self.channel.load();
            match sender {
                Some(sender) => {
                    sender.send(message).await.map_err(|_| {
                        TransportError::ConnectionClosed
                    })
                }
                None => {
                    Err(TransportError::ConnectionClosed)
                }
            }
        })
    }
    
    fn is_valid(&self) -> bool {
        let (sender, current_version) = self.channel.load();

        // Проверяем физическое состояние
        match sender {
            None => return false,
            Some(s) if s.is_closed() => return false,
            _ => {}
        }

        current_version == self.cached_version
    }
}

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
    ) -> Pin<Box<dyn Future<Output = TransportResult<ResolvedEndpoint<InMemoryEndpoint>>> + Send + 'a>> {
        Box::pin(async move {
            match self.registry.lookup(address) {
                Some(channel) => {
                    // Create a type-erased endpoint
                    Ok(ResolvedEndpoint::new(InMemoryEndpoint::new(channel)))
                }
                None => Err(TransportError::RecipientNotFound(address.to_string())),
            }
        })
    }
    
    /// Asynchronously sends a message to the specified address.
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
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + 'a>> {
        Box::pin(async move {
            match self.registry.lookup(address) {
                Some(channel) => {
					// IMPORTANT: Using .await on mpsc::Sender provides natural backpressure.
					// If the receiver's queue is full, the sender will be blocked, preventing
					// unbounded memory growth (OOM) with slow consumers or DoS attacks.																	 
                    let (sender, _) = channel.load();
                    match sender {
                        Some(sender) => {
                            sender.send(message).await.map_err(|_| {
                                TransportError::ConnectionClosed
                            })
                        }
                        None => {
                            Err(TransportError::ConnectionClosed)
                        }
                    }?;
                    Ok(())
                }
                None => Err(TransportError::RecipientNotFound(address.to_string())),
            }
        })
    }

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
        endpoint: &'a ResolvedEndpoint<InMemoryEndpoint>,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + 'a>> {
        Box::pin(async move {
            // Delegate the sending to the endpoint itself
            endpoint.send(message).await
        })
    }    

    /// Sends a request and waits for a response with a timeout.
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
        _address: &'a str,
        _message: Message,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Message>> + Send + 'a>> {
        Box::pin(async move {
            Err(TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Use BrokerClient::request for InOut mode to ensure proper reply_to injection and per-client dispatching",
            )))
        })
    }

    /// Sends a request to resolved endpoint and waits for a response with a timeout.
    ///
    /// Uses `ReplyDispatcher` to register waiting for a response by `message_id`.
    ///
    /// # Arguments
    /// * `address` - the string address of the recipient.
    /// * `message` - the request message to be sent.
    ///
    /// # Returns
    /// The response message upon successful execution, or a timeout/connection closed error.
    fn request_to<'a>(
        &'a self,
        _endpoint: &'a ResolvedEndpoint<InMemoryEndpoint>,
        _message: Message,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Message>> + Send + 'a>> {
        Box::pin(async move {
            Err(TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Use BrokerClient::request for InOut mode",
            )))
        })
    }   

    /// Method for receiving messages (stub for this implementation).
    ///
    /// # Note
    /// In the current architecture, `InMemoryTransport` is used primarily 
    /// for sending (send/request). Message reception is usually handled 
    /// by the component directly via `MessageReceiver` obtained during registration.
    fn receive<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Message>> + Send + 'a>> {
        Box::pin(async move {
            // TODO: Implement if a unified receive interface is needed 
            // for all transport types. For now, return a connection closed error.
            Err(TransportError::ConnectionClosed)
        })
    }

    /// Closes the transport.
    ///
    /// For in-process transport, explicit closing is not required, 
    /// as the lifetime of channels is managed by memory management rules and the registry's `Drop`.
    fn close<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}
