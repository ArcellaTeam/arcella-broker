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
    time::Duration,
};
use tokio::time;

use crate::protocol::Message;
use crate::registry::LocalRegistry;

use super::{Transport, TransportError, TransportResult};

/// Transport for in-process delivery.
/// 
/// Messages are routed directly through `tokio::mpsc` channels,
/// bypassing IPC. It is used when the recipient is located
/// in the same process as the sender.
pub struct InMemoryTransport {
    /// Local registry for looking up recipient channels by address.
    registry: Arc<LocalRegistry>,
    request_timeout: Duration,
}

impl InMemoryTransport {
    /// Creates a new instance of `InMemoryTransport`.
    ///
    /// # Arguments
    /// * `registry` - a shared reference to the local routing registry.
    pub fn new(registry: Arc<LocalRegistry>, request_timeout: Duration) -> Self {
        Self { registry, request_timeout }
    }
}

impl Transport for InMemoryTransport {
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
                    channel.send(message).await.map_err(|_| {
                        // If sending fails, we assume the recipient is unavailable
                        TransportError::ConnectionClosed
                    })?;
                    Ok(())
                }
                None => Err(TransportError::RecipientNotFound(address.to_string())),
            }
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
        address: &'a str,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Message>> + Send + 'a>> {
        Box::pin(async move {
            let dispatcher = self.registry.reply_dispatcher();
            let msg_id = message.header.message_id;

            // Register waiting for a response and get the receiver
            let (_guard, receiver) = dispatcher.register_waiter(msg_id)
                .map_err(TransportError::Registry)?;

            // Send the original message
            self.send(address, message).await?;

            // Set the response wait timeout (30 seconds)
            match time::timeout(self.request_timeout, receiver).await {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(_)) => Err(TransportError::ConnectionClosed),
                Err(_) => Err(TransportError::Timeout),
            }
        })
    }

    /// Method for receiving messages (stub for this implementation).
    ///
    /// # Note
    /// In the current architecture, `InMemoryTransport` is used primarily 
    /// for sending (send/request). Message reception is usually handled 
    /// by the component directly via `LocalReceiver` obtained during registration.
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