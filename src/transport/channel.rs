// arcella-broker/src/transport/channel.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Transport layer for Arcella message broker channels.
//!
//! This module provides a type-safe wrapper over asynchronous channels
//! (tokio::sync::mpsc), which serves as a fundamental building block
//! for internal message routing.
//!
//! # Architectural role in the broker
//!
//! Channels are used to connect publishers and subscribers,
//! as well as to deliver responses in the Request/Response (InOut) pattern.
//! The key guarantees this module provides for the broker:
//!
//! 1. Natural Backpressure:
//! If a subscriber processes messages more slowly than they arrive,
//! the channel queue fills up. The MessageSender::send method asynchronously blocks
//! the sender, preventing unbounded memory consumption growth (OOM)
//! and protecting the broker from "slow consumer" attacks (Slow Consumer DoS).
//!
//! 2. Strict typing:
//! Channels transmit exclusively crate::protocol::Message, which guarantees
//! compliance with the broker protocol and eliminates the possibility of transmitting invalid
//! or third-party data through the internal bus.
//!
//! 3. Observability:
//! The is_closed and load methods allow the broker registry (LocalRegistry)
//! and telemetry systems to track channel state, promptly
//! detect "orphaned" subscriptions, and monitor queue depth.
//!
//! 4. Hang protection (Timeouts):
//! The recv_timeout method allows the broker or client to interrupt waiting for a response
//! if the remote component terminated with an error (trap) or stopped responding.

use tokio::sync::mpsc;

use crate::protocol::Message;
use crate::error::BrokerError;

/// Errors that occur when attempting a non-blocking receive of a message from a channel.
///
/// Used by the broker in scenarios where waiting is unacceptable,
/// for example, when polling a channel in an event processing loop with strict timing constraints.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel queue is empty. There are no messages to process at the moment.
    #[error("Channel is empty")]
    Empty,

    /// All senders (MessageSender) have been destroyed.
    /// The channel is closed, and no new messages will arrive.
    /// The broker must initiate resource cleanup for this address.
    #[error("Channel is disconnected")]
    Disconnected,
}

impl From<mpsc::error::TryRecvError> for TryRecvError {
    fn from(err: mpsc::error::TryRecvError) -> Self {
        match err {
            mpsc::error::TryRecvError::Empty => TryRecvError::Empty,
            mpsc::error::TryRecvError::Disconnected => TryRecvError::Disconnected,
        }
    }
}

/// Type-safe message sender.
///
/// It is a lightweight, cloneable wrapper over tokio::sync::mpsc::Sender.
/// Cloning the sender does not create new queues;
/// all clones share the same receiver queue, which allows
/// multiple broker tasks to send messages to a single subscriber in parallel.
#[derive(Clone)]
pub struct MessageSender {
    inner: mpsc::Sender<Message>,
}

impl MessageSender {
    /// Creates a new instance of the sender.
    ///
    /// Internal use: Called by the create_channel factory function
    /// or by the registry when registering a new subscription.
    pub(crate) fn new(inner: mpsc::Sender<Message>) -> Self {
        tracing::debug!("new");
        Self { inner }
    }

    /// Asynchronous send with natural backpressure.
    ///
    /// If the receiver's queue is full, the current task will be suspended (yield)
    /// until space becomes available in the queue. This is the primary mechanism protecting the broker
    /// from memory overflow when there is an imbalance between producer and consumer speeds.
    ///
    /// # Errors
    /// Returns BrokerError::ChannelClosed if the receiver (MessageReceiver)
    /// has been dropped and the message cannot be delivered.
    pub async fn send(&self, message: Message) -> Result<(), BrokerError> {
        tracing::trace!("send");
        self.inner
            .send(message)
            .await
            .map_err(|e| match e {
                mpsc::error::SendError(msg) => BrokerError::ChannelClosed(msg),
            })
    }

    /// Non-blocking send attempt.
    ///
    /// Used by the broker in scenarios where blocking the sender is unacceptable
    /// (for example, when handling critical system events or when attempting
    /// to send a response to an already closed reply_to channel).
    ///
    /// # Errors
    /// - BrokerError::ChannelFull: The queue is full. The message is returned to the calling code.
    /// - BrokerError::ChannelClosed: The receiver has been destroyed.
    pub fn try_send(&self, message: Message) -> Result<(), BrokerError> {
        tracing::trace!("try_send");
        self.inner
            .try_send(message)
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(msg) => BrokerError::ChannelFull(msg),
                mpsc::error::TrySendError::Closed(msg) => BrokerError::ChannelClosed(msg),
            })
    }

    /// Checks whether the receiver is alive.
    ///
    /// Returns `true` if all `MessageReceiver` instances associated with this
    /// channel have been dropped. The broker uses this method to
    /// validate cached endpoints before attempting to send.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Current channel utilization (for metrics/telemetry).
    ///
    /// Allows the broker to monitor queue depth and detect slow consumers
    /// before they cause complete blocking of senders.
    pub fn load(&self) -> ChannelLoad {
        ChannelLoad {
            capacity: self.inner.capacity(),
            max_capacity: self.inner.max_capacity(),
        }
    }
}

impl Drop for MessageSender {
    fn drop(&mut self) {
    // If the reference count drops to 0, the channel is automatically closed for the receiver.
    // Logging helps track the lifecycle of subscriptions.
        tracing::debug!("drop");
    }
}

/// Type-safe message receiver.
///
/// Owns the message queue. In the broker architecture, each MessageReceiver
/// is strictly bound to a single address (or pattern) and is usually encapsulated
/// inside a Subscriber or ReplyDispatcher structure.
///
/// Dropping this object automatically closes the channel for all
/// active MessageSenders, signaling them with a ChannelClosed error.
pub struct MessageReceiver {
    inner: mpsc::Receiver<Message>,
}

impl MessageReceiver {
    /// Creates a new instance of the recipient.
    pub(crate) fn new(inner: mpsc::Receiver<Message>) -> Self {
        tracing::debug!("new");
        Self { inner }
    }

    /// Asynchronously receives the next message.
    ///
    /// # Return value
    /// Returns Some(Message) on successful receipt.
    /// Returns None if all senders have been dropped and the channel is closed.
    /// This is a signal for the broker's processing loop to terminate work with this subscriber.
    pub async fn recv(&mut self) -> Option<Message> {
        tracing::trace!("recv");
        self.inner.recv().await
    }

    /// Non-blocking receive attempt.
    ///
    /// Useful for implementing polling or hybrid processing loops,
    /// where the broker needs to check for messages without blocking the execution thread.
    pub fn try_recv(&mut self) -> Result<Message, TryRecvError> {
        tracing::trace!("try_recv");
        self.inner.try_recv().map_err(Into::into)
    }

    /// Receive with a timeout (critical for Wasm environments).
    ///
    /// This method guarantees that the broker will not hold resources indefinitely
    /// (for example, the WaiterGuard in ReplyDispatcher), but will correctly interrupt the wait
    /// and return control.
    pub async fn recv_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Option<Message> {
        match tokio::time::timeout(timeout, self.inner.recv()).await {
            Ok(msg) => msg,
            Err(_) => None,
        }
    }
}

impl Drop for MessageReceiver {
    fn drop(&mut self) {
    // When the tokio receiver is dropped, the channel is closed automatically.
    // Logging helps track the lifecycle of subscriptions.
        tracing::debug!("drop");
    }
}

/// Channel utilization information (for metrics).
///
/// Used by broker management systems to make decisions about
/// scaling, throttling, or forcibly disconnecting slow clients.
pub struct ChannelLoad {
    /// The current number of free slots in the channel's queue.
    pub capacity: usize,
    /// The maximum queue capacity specified when the channel was created.
    pub max_capacity: usize,
}

impl ChannelLoad {
    /// Returns the current number of messages waiting to be processed in the queue.
    pub fn pending_messages(&self) -> usize {
        self.max_capacity.saturating_sub(self.capacity)
    }

    /// Returns the queue fill percentage (from 0.0 to 1.0).
    pub fn utilization_ratio(&self) -> f64 {
        if self.max_capacity == 0 {
            0.0
        } else {
            self.pending_messages() as f64 / self.max_capacity as f64
        }
    }
}

/// Creates a new linked sender-receiver pair with the given capacity.
///
/// The capacity (capacity) is strictly controlled by the broker configuration
/// (SubscriberConfig or ClientConfig) to prevent exhaustion of
/// system memory and ensure predictable backpressure.
///
/// # Example
/// ```rust
/// use arcella_broker::transport::channel::create_channel;
/// 
/// // Creating a channel with a capacity of 1024 messages 
/// let (sender, mut receiver) = create_channel(1024); 
/// 
/// assert_eq!(sender.load().max_capacity, 1024); 
/// assert!(!sender.is_closed()); 
/// ```
pub fn create_channel(capacity: usize) -> (MessageSender, MessageReceiver) {
    let (tx, rx) = tokio::sync::mpsc::channel::<Message>(capacity);
    (MessageSender::new(tx), MessageReceiver::new(rx))
}
