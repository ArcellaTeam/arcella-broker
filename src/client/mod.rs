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
//! 1. **Strict lifecycle management (RAII)**: When the client is destroyed, its
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
use tokio_util::sync::CancellationToken;

mod publisher;
mod reply_dispatcher;

use crate::{
    broker::Broker,
    error::BrokerError,
    config::{ClientConfig, SubscriberConfig},
    protocol::Message,
    broker_core::{
        registry::{
            AddressSpec,
            AttachError,
            Attachment,
            RegistryError, 
            RegisterResult,
            RoutingPolicy,
            RouteTarget,
            SubscriptionSlot,
        },
        transport::{
            InMemoryTransport, 
            TransportError, 
            TransportResult,
        },
        subscription::{Subscriber, SubscriptionHandle}
    },
};

use publisher::Publisher;
use reply_dispatcher::ReplyDispatcher;

/// RAII guard for a client consumer binding.
///
/// Owns two things that must be released together:
/// - the `SubscriptionHandle` (binding-scoped cancellation token);
/// - the claim taken by [`RouteTarget::try_attach`] on the address.
///
/// Dropping this guard detaches the consumer AND releases the claim: the
/// Exclusive attach flag is cleared, or the LoadBalanced consumer counter is
/// decremented. The address itself, its queue and buffered messages are never
/// touched — address lifecycle belongs to the control plane.
///
/// Without this pairing the claim would leak: an Exclusive address would stay
/// permanently occupied, and a LoadBalanced group would never observe emptiness.
#[must_use = "dropping the handle immediately detaches the consumer"]
pub struct ClientSubscriptionHandle {
    handle: SubscriptionHandle,
    target: Arc<RouteTarget>,
}

impl ClientSubscriptionHandle {
    fn new(handle: SubscriptionHandle, target: Arc<RouteTarget>) -> Self {
        Self { handle, target }
    }

    /// Returns the logical address this binding is attached to.
    #[inline]
    pub fn address(&self) -> &str {
        self.handle.address()
    }

    /// Returns a reference to the binding-scoped cancellation token.
    #[inline]
    pub fn cancel_token(&self) -> &CancellationToken {
        self.handle.cancel_token()
    }

    /// Whether this binding has been detached.
    #[inline]
    pub fn is_detached(&self) -> bool {
        self.handle.is_detached()
    }
}

impl Drop for ClientSubscriptionHandle {
    fn drop(&mut self) {
        // Stop the consumer FIRST, then release the claim. The reverse order
        // would let a new Exclusive consumer attach while this one is still
        // reading from the same queue.
        self.handle.cancel_token().cancel();
        self.target.detach();
        // `handle` drops next; its own cancel is idempotent.
    }
}

/// The primary client interface for interacting with the Arcella message broker.
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
    pub(crate) local: Arc<InMemoryTransport>,

    /// The unique logical address of this client within the broker's namespace
    /// (for example, "arcella:core:http-handler").
    client_address: String,

    _reply_handle: SubscriptionHandle,

    /// The dispatcher that manages pending replies to requests (InOut).
    /// Guarantees the absence of memory leaks upon abnormal termination of the component.
    reply_dispatcher: Arc<ReplyDispatcher>,

    /// Reply channel slot, held to release the Exclusive consumer claim on drop.
    reply_slot: Arc<SubscriptionSlot>,    
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
    pub(crate) fn new(broker: Arc<Broker>, config: ClientConfig, client_address: String) -> Result<Self, BrokerError> {
        // 1. Create reply address from control plane
        let reply_spec = AddressSpec::reply_channel(config.reply_channel_capacity);
        let reply_slot = broker.create_address(client_address.clone(), reply_spec)?;  

        // 2. Claim the Exclusive consumer binding of the reply channel, so that
        //    no other consumer (including `subscribe(client_address)`) can attach
        //    to it. The slot is freshly created, so this normally succeeds.
        if reply_slot.try_attach_exclusive().is_err() {
            // Roll back the control-plane registration: leaving the route
            // registered without an owner would leak a zombie address.
            let _ = broker.delete_address(&client_address);
            return Err(BrokerError::RegistryError(
                RegistryError::AddressAlreadyOccupied(client_address.clone()),
            ));
        }
        
        // 3. Extracting the receiver from the slot for the ReplyDispatcher
        let reply_receiver = reply_slot.receiver();        

        // 4. Create the subscriber and dispatcher to handle responses.
        //    The REAL address token is passed: the constructor derives the
        //    binding token from it, keeping `Detached` distinguishable from
        //    `AddressRemoved`.
        let (reply_subscriber, reply_cancel_token) = Subscriber::new_push(
            reply_receiver,
            client_address.clone(), 
            reply_slot.address_token().clone(),
        );
        let reply_dispatcher = Arc::new(ReplyDispatcher::new(reply_subscriber));
        let reply_handle = SubscriptionHandle::new(client_address.clone(), reply_cancel_token);

        let transport = InMemoryTransport::new(broker.registry.clone());
        let local = Arc::new(transport);

        Ok(Self { 
            broker, 
            local,
            client_address,
            _reply_handle: reply_handle,
            reply_dispatcher,
            reply_slot,
        })
    }

    /// Returns the logical address with which this client is associated.
    pub fn client_address(&self) -> &str {
        &self.client_address
    }

    /// Creates a subscription to messages at the specified logical address.
    ///
    /// Returns a tuple of:
    /// - `Subscriber`: the data receiver (move into a background task).
    /// - `ClientSubscriptionHandle`: the RAII guard (keep in the parent scope).
    ///   Dropping it detaches the consumer and releases the address claim; the
    ///   address and its queue are never unregistered.
    ///
    /// # Routing Policy
    /// The delivery model is a property of the address, fixed by the control
    /// plane at creation time — the client cannot choose it:
    /// - `Exclusive`: a single attached consumer. A second attach fails with
    ///   `AddressAlreadyOccupied`; re-attach after detach is allowed.
    /// - `LoadBalanced`: joins the Round-Robin consumer group.
    ///
    /// # Errors
    /// - `AddressNotFound`: no route for the address, or it was removed by the
    ///   control plane.
    /// - `AddressAlreadyOccupied`: the Exclusive binding is held by another
    ///   consumer.
    pub fn subscribe(
        &self,
        address: String,
    ) -> Result<(Subscriber, ClientSubscriptionHandle), BrokerError> {
        let target = self.broker.registry.lookup(&address)
            .ok_or_else(|| BrokerError::RegistryError(RegistryError::AddressNotFound(address.clone())))?;

        // Atomically claim the consumer binding according to the address policy:
        // CAS on the Exclusive flag, or a group join for LoadBalanced. From here
        // on the claim MUST be paired with exactly one `detach`, which the
        // returned guard performs on drop.
        let attachment = target.try_attach().map_err(|e| match e {
            AttachError::Occupied => BrokerError::RegistryError(
                RegistryError::AddressAlreadyOccupied(address.clone()),
            ),
            AttachError::Removed => BrokerError::RegistryError(
                RegistryError::AddressNotFound(address.clone()),
            ),
        })?;

        // The constructors derive the binding token from the address token
        // internally; that token is what the guard must own.
        let (subscriber, binding_token) = match attachment {
            Attachment::Push { receiver, address_token, .. } => {
                Subscriber::new_push(receiver, address.clone(), address_token)
            }
            Attachment::Pull { req_sender, address_token, .. } => {
                Subscriber::new_pull(req_sender, address.clone(), address_token)
            }
        };

        let handle = SubscriptionHandle::new(address, binding_token);
        Ok((subscriber, ClientSubscriptionHandle::new(handle, target)))
    }
    
    /// Creates a high-performance `Publisher` for the specified address.
    ///
    /// # Optimization (Hot Path)
    /// Unlike the `send` method, `Publisher` caches the resolved endpoint
    /// (`ResolvedEndpoint`). This avoids the expensive lookup in the
    /// `LocalRegistry` (radix trees or wildcard scanning) on every
    /// send, which is critically important for high-load components.
    pub fn publisher(&self, address: String) -> Publisher {
        Publisher::new(
            address,
            self.local.clone(),
            self.client_address.clone(),
            self.reply_dispatcher.clone(),
            self.broker.config.clone(),
        ) 
    }    

    /// Asynchronously sends a message without waiting for a reply (InOnly pattern).
    ///
    /// This is the "cold path". The destination address is extracted directly 
    /// from `message.address`, eliminating parameter duplication and ensuring 
    /// Single Source of Truth (SSOT) for routing.
    /// 
    /// Use this method for rare or one-off messages. For frequent sending to
    ///  the same address, it is preferable to use `self.publisher(address).send()`.
    pub async fn send(&self, message: Message) -> TransportResult<()> {
        self.local.send(message).await
    }

    /// Sends a request and waits for a reply (InOut / Request-Response pattern).
    ///
    /// The destination address is extracted directly from `message.address`.
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
    pub async fn request(&self, mut message: Message) -> TransportResult<Message> {
        let message_id = message.header.message_id;

        // Ensure InOut mode (Request/Response).
        // If the calling code accidentally created a message with the InOnly flag,
        // we forcefully correct this to ensure proper response handling by the receiver.
        message.header.flags = (message.header.flags & !crate::protocol::TRANSFER_MODE_MASK) 
            | crate::protocol::TransferMode::InOut.to_flags();

        // Force override of `reply_to` to ensure security and correct response routing.
        // Ignore any value that the calling code might have passed.
        message.reply_to = Bytes::from(self.client_address.clone());

        // Registering the wait for a reply. Returns a guard for automatic cleanup.
        let (_guard, receiver) = self.reply_dispatcher
            .register_waiter(message_id)
            .map_err(TransportError::Registry)?; 

        // Sending the request
        self.local.send(message).await?;

        // Waiting for the reply with a timeout
        match tokio::time::timeout(self.broker.config.request_timeout(), receiver).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(TransportError::ConnectionClosed),
            Err(_) => Err(TransportError::Timeout),
        }
    }

}

impl Drop for BrokerClient {
    fn drop(&mut self) {
        // Detach the reply consumer first, then release its claim, and only
        // then let the control plane remove the address itself.
        self._reply_handle.cancel_token().cancel();
        self.reply_slot.detach_exclusive();
        
        if let Err(e) = self.broker.delete_address(&self.client_address) {
            tracing::warn!(
                address = %self.client_address,
                error = %e,
                "Failed to delete reply address on client drop"
            );
        }
    }    
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::test;

    use super::*;
    use crate::test_utils;

    #[test]
    async fn test_dynamic_registration() {
        // 1. Initialize the broker and the client
        let broker = Arc::new(Broker::default());
        let client = broker.create_client(ClientConfig::default(), "test".into()).unwrap();
        let address = "arcella:test";

        let msg = test_utils::dummy_in_only_message(
            Bytes::from_static(b"test"),
            Bytes::from_static(address.as_bytes()),
            Bytes::from_static(b""),
        );
        
        // Sending before the address is created -> error
        assert!(client.send(msg.clone()).await.is_err());

        // 2. CONTROL PLANE: Explicitly create the address
        broker.create_address(
            address.to_string(),
            AddressSpec::exclusive(1024)
        ).unwrap();

        // 3. DATA PLANE: Attaching
        let (mut subscriber, handle) = client
            .subscribe(address.to_string())
            .expect("Subscription should succeed");

        // Now sending works
        assert!(client.send(msg).await.is_ok());
        assert!(subscriber.recv().await.is_ok());

        // 4. Detaching. The address remains in the registry!
        drop(handle);

        // 5. CONTROL PLANE: Explicitly remove the address
        broker.delete_address(address).unwrap();

        // Now sending should return an error again
        let msg2 = test_utils::dummy_in_only_message(
            Bytes::from_static(b"test2"),
            Bytes::from_static(address.as_bytes()),
            Bytes::from_static(b""),
        );
        assert!(client.send(msg2).await.is_err());
    }

    #[test]
    async fn test_subscription_cleanup_and_re_registration() {
        // 1. Initialize the broker and the client
        let broker = Arc::new(Broker::default());
        let client = broker.create_client(ClientConfig::default(), "test".into()).unwrap();
        let addr = "arcella:test:duplicate";

        // 1. CONTROL PLANE: Explicitly create the address
        broker.create_address(
            addr.to_string(),
            AddressSpec::exclusive(1024)
        ).unwrap();

        // 2. DATA PLANE: First attaching
        let (_sub1, hdl1) = client
            .subscribe(addr.to_string())
            .expect("First subscription should succeed");
        
        // 3. DATA PLANE: Detaching first address
        drop(hdl1);

        // 4. DATA PLANE: Second attaching
        let (mut subscriber, _hdl) = client
            .subscribe(addr.to_string())
            .expect("Subscription after drop should succeed");

        // 5.  Now sending works
        let original_message = test_utils::dummy_in_only_message(
            Bytes::from("test"), 
            Bytes::from(addr), 
            Bytes::from("hello")
        );
        
        let send_result = client.send(original_message.clone()).await;
        assert!(send_result.is_ok());

        let received_message = subscriber.recv().await;
        assert!(received_message.is_ok(), "sub3 received the message");

        assert_eq!(
            original_message, 
            received_message.unwrap(), 
            "Received message should exactly match the sent message"
        );        
        
        // 6. CONTROL PLANE: Explicitly remove the address
        broker.delete_address(addr).unwrap();
    }    

    #[tokio::test]
    async fn test_multi_recipient_routing() {
        // 1. Initialize the broker and the client
        let broker = Arc::new(Broker::default());
        let client = broker.create_client(ClientConfig::default(), "test".into()).unwrap();

        // 2. Addresses for testing
        let addresses = vec![
            "arcella:core:users",
            "arcella:web:api",
            "arcella:batch:processor",
        ];

        // 3. CONTROL PLANE: Create address over Broker
        for addr in &addresses {
            broker
                .create_address(addr.to_string(), AddressSpec::exclusive(1024))
                .expect(&format!("Failed to create address {}", addr));
        }

        // 4. DATA PLANE: Subscribe at addresses
        let mut subscribers = Vec::new();
        let mut handles = Vec::new();
        
        for addr in &addresses {
            let (subscriber, handle) = client
                .subscribe(addr.to_string())
                .expect(&format!("Subscription to {} should succeed", addr));
            subscribers.push(subscriber);
            handles.push(handle);
        }

        // 5. Send messages to all addresses
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
            let msg = test_utils::dummy_in_only_message(
                Bytes::from(*msg_type),
                Bytes::from(*addr),
                payload.clone(),
            );

            let result = client.send(msg).await;
            assert!(result.is_ok(), "Send to {} should succeed", addr);
        }

        // 6. Check: each receiver get only own messages at right order
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

        // 8. Check: channels is empty
        assert!(subscribers[0].try_recv().is_err(), "users channel should be empty");
        assert!(subscribers[1].try_recv().is_err(), "api channel should be empty");
        assert!(subscribers[2].try_recv().is_err(), "processor channel should be empty");

        // 9. Detaching. The address remains in the registry!
        drop(handles);

        // 10. CONTROL PLANE: delete address from broker
        for addr in &addresses {
            broker.delete_address(addr).expect(&format!("Failed to delete address {}", addr));
        }
    }    
}
