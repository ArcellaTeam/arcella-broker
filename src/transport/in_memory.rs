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
use crate::registry::{LocalChannel, LocalRegistry};

use super::{Endpoint, ResolvedEndpoint, Transport, TransportError, TransportResult};

pub struct InMemoryEndpoint {
    channel: LocalChannel,
}

impl InMemoryEndpoint {
    pub(crate) fn new(channel: LocalChannel) -> Self {
        Self { channel }
    }
}

impl Endpoint for InMemoryEndpoint {
    fn send<'a>(
        &'a self,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + 'a>> {
        Box::pin(async move {
            self.channel.send(message).await.map_err(|_| {
                TransportError::ConnectionClosed
            })
        })
    }
    
    fn is_alive(&self) -> bool {
        // mpsc::Sender считается живым, пока существует хотя бы один активный Receiver.
        !self.channel.is_closed()
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

    async fn perform_request(
        &self,
        send_action: impl Future<Output = TransportResult<()>>,
        msg_id: [u8; 16],
    ) -> TransportResult<Message> {
        let dispatcher = self.registry.reply_dispatcher();
        let (_guard, receiver) = dispatcher.register_waiter(msg_id)
            .map_err(TransportError::Registry)?;

        send_action.await?;

        match time::timeout(self.request_timeout, receiver).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(TransportError::ConnectionClosed),
            Err(_) => Err(TransportError::Timeout),
        }
    }

}

impl Transport for InMemoryTransport {
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
    ) -> Pin<Box<dyn Future<Output = TransportResult<ResolvedEndpoint>> + Send + 'a>> {
        Box::pin(async move {
            match self.registry.lookup(address) {
                Some(channel) => {
                    // Создаем type-erased endpoint
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
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + 'a>> {
        Box::pin(async move {
            // Делегируем отправку самому endpoint'у
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
        address: &'a str,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Message>> + Send + 'a>> {
        Box::pin(async move {
            let message_id = message.header.message_id;
            let send_future = self.send(address, message); 
            self.perform_request(send_future, message_id).await 
        })
    }

    fn request_to<'a>(
        &'a self,
        endpoint: &'a ResolvedEndpoint,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Message>> + Send + 'a>> {
        Box::pin(async move { 
            let message_id = message.header.message_id; 
            let send_future = endpoint.send(message); 
            self.perform_request(send_future, message_id).await 
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
