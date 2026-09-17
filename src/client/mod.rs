// arcella-broker/src/client/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Client frontend for the Arcella message broker.
//!
//! This module provides the `BrokerClient` struct, which is
//! the primary entry point for any component of the platform that
//! needs to send or receive messages.
//!
//! # Architectural role in the Arcella platform
//!
//! According to the architecture, `BrokerClient` abstracts away the details of transport and
//! routing, providing components with a unified, type-safe API, ensuring
//! zero-cost routing between asynchronous tasks.
//!
//! Key guarantees that this module provides to the broker:
//! 1. **Strict lifecycle management (RAII)**: When the client is destroyed
//! (for example, upon a task panic or graceful shutdown of a Wasm instance), its
//! address is automatically and synchronously removed from the `LocalRegistry`, preventing
//! the appearance of "zombie" routes and memory leaks.
//! 2. **Safety of the Request/Response (InOut) pattern**: The `request` method
//! forcibly overwrites the `reply_to` field, eliminating the possibility of
//! spoofing or interception of responses by malicious or erroneous components.
//! 3. **Hybrid sending model**: Support for both the "cold path" (one-off
//! send with a registry lookup) and the "hot path" (a caching Publisher)
//! to optimize high-load scenarios.

use bytes::Bytes;
use std::sync::Arc;

mod publisher;
mod reply_dispatcher;
pub mod subscriber;

use crate::{
    broker::Broker,
    config::{ClientConfig, SubscriberConfig},
    protocol::Message,
    registry::RegistryError,
    transport::{
        channel::MessageSender,
        in_memory::{
            InMemoryTransport, 
            InMemoryEndpoint,
        },
        Transport,
        TransportError, 
        TransportResult,
    }
};

use subscriber::Subscriber;
use publisher::Publisher;
use reply_dispatcher::ReplyDispatcher;

// The primary client interface for interacting with the Arcella message broker.
///
/// An instance of `BrokerClient` represents the logical identity of a component
/// in the routing system. It owns the channel for receiving replies (via
/// `ReplyDispatcher`) and provides methods for subscribing to events and sending
/// messages to other components.
///
/// # Resource management
/// The client implements the RAII pattern. Upon going out of scope or a task
/// panic, the `Drop` method guarantees synchronous cleanup of the client's address
/// from the registry, which is critically important for the correct operation of
/// lifecycle management commands.
pub struct BrokerClient {
    /// A reference to the broker core containing the configuration and the global registry.
    broker: Arc<Broker>,

    /// Transport layer for intra-process delivery.
    local: Arc<InMemoryTransport>,

    /// The unique logical address of this client within the broker's namespace
    /// (for example, "arcella:core:http-handler").
    client_address: String,

    /// The dispatcher that manages pending replies to requests (InOut).
    /// Guarantees the absence of memory leaks upon abnormal termination of the component.
    reply_dispatcher: ReplyDispatcher,
}

impl BrokerClient {
    /// Creates a new instance of the broker client.
    ///
    /// # Initialization algorithm
    /// 1. Creates and registers a `Subscriber` for the `client_address`,
    /// so that the component can receive replies to its requests (InOut).
    /// 2. Initializes the `ReplyDispatcher`, which starts a background task
    /// to correlate replies with pending requests by `message_id`.
    /// 3. Configures the `InMemoryTransport` for subsequent message sending.
    ///
    /// # Errors
    /// Returns `RegistryError` if the `client_address` is already occupied
    /// by another active component (guaranteeing binding exclusivity).
    pub(crate) fn new(broker: Arc<Broker>, config: ClientConfig, client_address: String) -> Result<Self, RegistryError> {
        // The reply channel registration must happen first, so that
        // the component is ready to accept a reply immediately after sending a request.
        let reply_subscriber = Subscriber::bind(client_address.clone(), broker.registry.clone(), config.reply_channel_capacity)?;
        let reply_dispatcher = ReplyDispatcher::new(reply_subscriber);

        let transport = InMemoryTransport::new(broker.registry.clone());
        let local = Arc::new(transport);

        Ok(Self { 
            broker, 
            local,
            client_address,
            reply_dispatcher,
        })
    }

    /// Returns the logical address with which this client is associated.
    pub fn client_address(&self) -> &str {
        &self.client_address
    }

    /// Creates a subscription to messages at the specified logical address.
    ///
    /// Creates a new `Subscriber` that registers its channel in the LocalRegistry.
    /// Supports both exact addresses and wildcard patterns (depending on
    /// the registry's validation rules). The channel capacity controls backpressure.
    pub fn subscribe(
        &self,
        address: String,
        config: SubscriberConfig,
    ) -> Result<Subscriber, RegistryError> {
        Subscriber::bind(address, self.broker.registry.clone(), config.channel_capacity)
    }
    
    /// Low-level registration of a custom sender at an address.
    ///
    /// # Note
    /// This method is intended for advanced scenarios where a component wants
    /// to provide the broker with an already-created `MessageSender` (for example, for
    /// integration with third-party queues). In standard scenarios, you should
    /// use `subscribe`, which encapsulates this logic.
    pub fn bind(&self, address: String, sender: MessageSender) -> Result<(), RegistryError> {
        self.broker.registry.register(address, sender)
    }
    
    /// Unregisters the receiver at the specified address.
    ///
    /// This results in the receiver's channel being closed. Any pending `recv()` calls
    /// on the receiver's side will immediately return `None`, and senders will receive
    /// a `ChannelClosed` error on their next send attempt.
    pub fn unbind(&self, address: &str) -> Result<(), RegistryError> {
        self.broker.registry.unregister(address)
    }

    /// Creates a high-performance `Publisher` for the specified address.
    ///
    /// # Optimization (Hot Path)
    /// Unlike the `send` method, `Publisher` caches the resolved endpoint
    /// (`ResolvedEndpoint`). This avoids the expensive lookup in the
    /// `LocalRegistry` (radix trees or wildcard scanning) on every
    /// send, which is critically important for high-load components.
    pub fn publisher(&self, address: String) -> Publisher<InMemoryTransport, InMemoryEndpoint> {
        Publisher::new(address, self.local.clone()) 
    }    

    /// Asynchronously sends a message without waiting for a reply (InOnly pattern).
    ///
    /// This is the "cold path". The address is resolved in the registry on every
    /// call. Use this method for rare or one-off messages.
    /// For frequent sending to the same address, it is preferable to use
    /// `self.publisher(address).send()`.
    pub async fn send(&self, address: &str, message: Message) -> TransportResult<()> {
        self.local.send(address, message).await
    }

    /// Sends a request and waits for a reply (InOut / Request-Response pattern).
    ///
    /// # Broker safety guarantees
    /// 1. **Forced injection of `reply_to`**: The method ignores any value of
    ///    `reply_to` passed in the original message and forcibly sets it
    ///    to `client_address`. This prevents reply-redirection attacks
    ///    and guarantees that the reply returns to exactly this client instance.
    /// 2. **Failure isolation (Timeout)**: Waiting for a reply is bounded by
    ///    `broker.config.request_timeout()`. If the target Wasm component hangs
    ///    or terminates with a trap, the call will return `TransportError::Timeout` rather than
    ///    blocking the resource forever.
    /// 3. **RAII cleanup**: The `WaiterGuard` created internally guarantees removal
    ///    of the pending wait from the `ReplyDispatcher` even in the event of a panic in the calling code.
    pub async fn request(&self, address: &str, mut message: Message) -> TransportResult<Message> {
        let message_id = message.header.message_id;

        // Force override of `reply_to` to ensure security and correct response routing.
        // Ignore any value that the calling code might have passed.
        message.reply_to = Bytes::from(self.client_address.clone());

        // Registering the wait for a reply. Returns a guard for automatic cleanup.
        let (_guard, receiver) = self.reply_dispatcher
            .register_waiter(message_id)
            .map_err(TransportError::Registry)?; 

        // Sending the request
        self.local.send(address, message).await?;

        // Waiting for the reply with a timeout
        match tokio::time::timeout(self.broker.config.request_timeout(), receiver).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(TransportError::ConnectionClosed),
            Err(_) => Err(TransportError::Timeout),
        }
    }

}

/// Automatic cleanup of the client's resources upon the termination of its lifecycle.
///
/// # Critical importance for Arcella's stability
/// This method implements a strict cleanup order to prevent state races
/// (TOCTOU) during rapid recreation of components:
/// 1. **Synchronous unregistration**: The `client_address` is removed from the
///    `LocalRegistry` before the asynchronous `ReplyDispatcher` begins to be destroyed.
/// 2. This guarantees that any new component attempting to occupy the same
///    address will not encounter an `AddressAlreadyOccupied` conflict due to
///    a delay in asynchronous cleanup.
/// 3. The error is ignored (with logging) if the address has already been removed (for example,
///    upon forced termination of the entire broker process).
impl Drop for BrokerClient {
    fn drop(&mut self) {
        // Synchronously clean up the client address from the registry BEFORE
        // the ReplyDispatcher begins its asynchronous task cleanup.
        // This prevents a race condition when quickly recreating a client with the same address.
        if let Err(e) = self.broker.registry.unregister(&self.client_address) {
            // Ignore the error if the address was already removed (e.g., during a panic),
            // but log it for debugging purposes.
            tracing::debug!(address = %self.client_address, error = %e, "Client address already unregistered or cleanup race");
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::transport::{TransportError};
    use crate::test_utils;

    #[tokio::test]
    async fn test_in_memory_message_delivery_in_only() {
        // 1. Initialize client
        let client = test_utils::test_client("test".to_string()).unwrap();

        // 2. Prepare the receiver (Actor pattern)
        let target_address = "arcella:core:test:receiver".to_string();
        let subscriber_config = SubscriberConfig::default();
        let mut subscriber = client.subscribe(target_address.clone(), subscriber_config).expect("Subscription should succeed");

        // 3. Create a test message
        let original_message = test_utils::dummy_in_only_message(Bytes::from("test:ping"),
            Bytes::from(target_address.clone()),
            Bytes::from("hello from sender"));

        // 4. Action: Send the message
        let send_result = client.send(&target_address, original_message.clone()).await;
        assert!(send_result.is_ok(), "Send operation should succeed");

        // 5. Verification: Receiving the message on the receiver side
        let received_message = subscriber.recv().await;
        assert!(received_message.is_some(), "Receiver should get a message");
        assert_eq!(
            original_message, 
            received_message.unwrap(), 
            "Received message should exactly match the sent message"
        );
    }

    #[tokio::test]
    async fn test_in_memory_message_delivery_to_unknown_address() {
        // 1. Initialize client
        let client = test_utils::test_client("test".to_string()).unwrap();

        let msg = test_utils::dummy_in_only_message(Bytes::from("test:ping"),
            Bytes::from("arcella:unknown:address"),
            Bytes::from(""));

        // Attempt to send to an unregistered address
        let result = client.send("arcella:unknown:address", msg).await;

        // Expect a `RecipientNotFound` error
        assert!(matches!(result, Err(TransportError::RecipientNotFound(_))));
    }

    #[tokio::test]
    async fn test_multi_recipient_routing() {
        // 1. Initialize client
        let client = test_utils::test_client("test".to_string()).unwrap();

        // 2. Register multiple receivers with different addresses
        let addresses = vec![
            "arcella:core:users",
            "arcella:web:api",
            "arcella:batch:processor",
        ];

        let subscriber_config = SubscriberConfig::default();

        let mut subscribers = Vec::new();
        for addr in &addresses {
            subscribers.push(client.subscribe(addr.to_string(), subscriber_config.clone()).expect("Subscription should succeed"));
        }

        // 3. Send mixed messages to different addresses
        let test_messages = vec![
            ("arcella:core:users", "user:created", Bytes::from("user1")),
            ("arcella:web:api", "http:request", Bytes::from("GET /api")),
            ("arcella:batch:processor", "batch:job", Bytes::from("job1")),
            ("arcella:core:users", "user:updated", Bytes::from("user2")),
            ("arcella:web:api", "http:response", Bytes::from("200 OK")),
            ("arcella:batch:processor", "batch:complete", Bytes::from("job1:done")),
            ("arcella:core:users", "user:deleted", Bytes::from("user3")),
        ];

        for (addr, msg_type, payload) in &test_messages {
            let msg = test_utils::dummy_in_only_message(Bytes::from(*msg_type),
                Bytes::from(*addr),
                payload.clone());

            let result = client.send(addr, msg).await;
            assert!(result.is_ok(), "Send to {} should succeed", addr);
        }

        // 4. Verification: each receiver got only its messages in the correct order
        // Receiver 0: arcella:core:users
        let msg1 = subscribers[0].recv().await.unwrap();
        assert_eq!(msg1.msg_type, "user:created");
        assert_eq!(msg1.payload, Bytes::from("user1"));

        let msg2 = subscribers[0].recv().await.unwrap();
        assert_eq!(msg2.msg_type, "user:updated");
        assert_eq!(msg2.payload, Bytes::from("user2"));

        let msg3 = subscribers[0].recv().await.unwrap();
        assert_eq!(msg3.msg_type, "user:deleted");
        assert_eq!(msg3.payload, Bytes::from("user3"));

        // Receiver 1: arcella:web:api
        let msg4 = subscribers[1].recv().await.unwrap();
        assert_eq!(msg4.msg_type, "http:request");
        assert_eq!(msg4.payload, Bytes::from("GET /api"));

        let msg5 = subscribers[1].recv().await.unwrap();
        assert_eq!(msg5.msg_type, "http:response");
        assert_eq!(msg5.payload, Bytes::from("200 OK"));

        // Receiver 2: arcella:batch:processor
        let msg6 = subscribers[2].recv().await.unwrap();
        assert_eq!(msg6.msg_type, "batch:job");
        assert_eq!(msg6.payload, Bytes::from("job1"));

        let msg7 = subscribers[2].recv().await.unwrap();
        assert_eq!(msg7.msg_type, "batch:complete");
        assert_eq!(msg7.payload, Bytes::from("job1:done"));

        // 5. Verification: channels are empty (no more messages)
        assert!(subscribers[0].try_recv().is_err(), "users channel should be empty");
        assert!(subscribers[1].try_recv().is_err(), "api channel should be empty");
        assert!(subscribers[2].try_recv().is_err(), "processor channel should be empty");
    }

    #[tokio::test]
    async fn test_dynamic_registration() {
        // 1. Initialize client
        let client = test_utils::test_client("test".to_string()).unwrap();

        let address = Bytes::from_static(b"arcella:test");
        let payload = Bytes::from_static(b"");

        // Sending before registration should return an error
        let msg = test_utils::dummy_in_only_message(Bytes::from_static(b"test"),
            address.clone(),
            payload.clone());
        
        assert!(client.send("arcella:test", msg.clone()).await.is_err());

        let subscriber_config = SubscriberConfig::default();

        // Registration
        let mut subscriber = client.subscribe("arcella:test".to_string(), subscriber_config).expect("Subscription should succeed");

        // Now sending should succeed
        assert!(client.send("arcella:test", msg).await.is_ok());
        assert!(subscriber.recv().await.is_some());

        // Unregistration
        assert!(client.unbind("arcella:test").is_ok());

        // Should return an error again
        let msg2 = test_utils::dummy_in_only_message(Bytes::from_static(b"test2"),
            address.clone(),
            payload.clone());
        assert!(client.send("arcella:test", msg2).await.is_err());
    }    

    #[tokio::test]
async fn test_subscription_cleanup_and_re_registration() {
        // 1. Initialize client
        let client = test_utils::test_client("test".to_string()).unwrap();
																								  
        
													   
																		  
        let addr = "arcella:test:duplicate";

        let subscriber_config = SubscriberConfig::default();

        // 1. First subscription
        let sub1 = client.subscribe(addr.to_string(), subscriber_config.clone()).expect("First subscription should succeed");
        
        // 2. Second subscription to the same address 
        let sub2_result = client.subscribe(addr.to_string(), subscriber_config.clone());
        assert!(
            matches!(sub2_result, Err(RegistryError::AddressAlreadyOccupied(_))),
            "Second subscription to the same address must be rejected with AddressAlreadyOccupied"
        );

        // 3. sub1 goes out of scope
        drop(sub1);
        // Drop triggers: self.registry.unregister(&self.address);
        // Registry is now empty! tx2 (owned by sub2) is removed from the registry.

													 
        let mut sub3 = client.subscribe(addr.to_string(), subscriber_config).expect("Subscription after drop should succeed");

        // 4. Attempt to send a message
        let msg = test_utils::dummy_in_only_message(
            Bytes::from("test"), 
            Bytes::from(addr), 
            Bytes::from("hello")
        );
        
        // Send should succeed, as sub3 is alive and ready to receive.
        let result = client.send(addr, msg).await;
        
        assert!(!result.is_err());
        assert!(!sub3.try_recv().is_err(), "sub3 received the message");
    }

}
