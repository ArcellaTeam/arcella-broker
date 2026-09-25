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

use tokio::sync::{mpsc, oneshot};
use thiserror::Error;

use crate::protocol::Message;

#[derive(Debug, Error)]
#[error("Channel is closed")]
pub struct SendError<T>(pub T);

impl<T> From<mpsc::error::SendError<T>> for SendError<T> {
    fn from(err: mpsc::error::SendError<T>) -> Self {
        SendError(err.0)
    }
}

#[derive(Debug, Error)]
pub enum TrySendError<T> {
    #[error("Channel is full")]
    Full(T),    

    /// All senders (MessageSender) have been destroyed.
    /// The channel is closed, and no new messages will arrive.
    /// The broker must initiate resource cleanup for this address.
    #[error("Channel is disconnected")]
    Disconnected(T),
}

impl<T> From<mpsc::error::TrySendError<T>> for TrySendError<T> {
    fn from(err: mpsc::error::TrySendError<T>) -> Self {
        match err {
            mpsc::error::TrySendError::Full(msg) => TrySendError::Full(msg),
            mpsc::error::TrySendError::Closed(msg) => TrySendError::Disconnected(msg),
        }
    }
}

/// Errors that occur when attempting a non-blocking receive of a message from a channel.
///
/// Used by the broker in scenarios where waiting is unacceptable,
/// for example, when polling a channel in an event processing loop with strict timing constraints.
#[derive(Debug, Error, PartialEq, Eq)]
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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RecvTimeoutError {
    #[error("Channel is disconnected")]
    Disconnected,

    #[error("Receive operation timed out")]
    Timeout,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TryRequestError {
    /// The request queue is full. The consumer should retry later
    /// or use the async `request()` method which provides natural backpressure.
    #[error("Request channel is full")]
    Full,

    /// All receivers have been dropped. The target component has terminated
    /// and cannot accept new requests.
    #[error("Request channel is disconnected")]
    Disconnected,
}

/// Channel utilization information (for metrics).
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

/// Generic asynchronous channel sender.
///
/// It is a lightweight, cloneable wrapper over `tokio::sync::mpsc::Sender`.
/// Cloning the sender does not create new queues; all clones share the same
/// receiver queue, enabling parallel sends to a single logical recipient.
#[derive(Clone)]
pub struct Sender<T> {
    inner: mpsc::Sender<T>,
}

impl<T: Send> Sender<T> {
    pub(crate) fn new(inner: mpsc::Sender<T>) -> Self {
        tracing::trace!("Sender new");
        Self { inner }
    }

    /// Asynchronous send with natural backpressure.
    ///
    /// If the receiver's queue is full, the current task will yield
    /// until space becomes available, preventing unbounded memory growth (OOM).
    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        tracing::trace!("Sender send");
        self.inner.send(msg).await.map_err(Into::into)
    }

    /// Non-blocking send attempt.
    ///
    /// Returns the message back in the error variant if the queue is full or closed,
    /// allowing the caller to handle or retry the operation.
    #[must_use = "The result must be processed, otherwise the message/request will be lost"]
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        tracing::trace!("Sender try_send");
        self.inner.try_send(msg).map_err(Into::into)
    }

    /// Checks whether the receiver is alive.
    ///
    /// Returns `true` if all `Receiver` instances associated with this
    /// channel have been dropped.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Current channel utilization (for metrics/telemetry).
    pub fn load(&self) -> ChannelLoad {
        ChannelLoad {
            capacity: self.inner.capacity(),
            max_capacity: self.inner.max_capacity(),
        }
    }
}

/// Generic asynchronous channel receiver.
///
/// Owns the message queue. Dropping this object automatically closes 
/// the channel for all active `Sender`s.
pub struct Receiver<T> {
    inner: mpsc::Receiver<T>,
}

impl<T: Send> Receiver<T> {
    pub(crate) fn new(inner: mpsc::Receiver<T>) -> Self {
        tracing::trace!("Receiver new");
        Self { inner }
    }

    /// Asynchronously receives the next message.
    ///
    /// Returns `None` if all senders have been dropped and the channel is closed.
    pub async fn recv(&mut self) -> Option<T> {
        tracing::trace!("Receiver recv");
        self.inner.recv().await
    }

    /// Non-blocking receive attempt.
    #[must_use = "The result must be handled, otherwise the data will be dropped"]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        tracing::trace!("Receiver try_recv");
        self.inner.try_recv().map_err(Into::into)
    }

    /// Receive with a timeout.
    ///
    /// Guarantees that the broker will not hold resources indefinitely,
    /// correctly interrupting the wait and returning control.
    pub async fn recv_timeout(&mut self, timeout: std::time::Duration) -> Result<T, RecvTimeoutError> {
        match tokio::time::timeout(timeout, self.inner.recv()).await {
            Ok(Some(msg)) => Ok(msg),
            Ok(None) => Err(RecvTimeoutError::Disconnected),
            Err(_) => Err(RecvTimeoutError::Timeout),
        }
    }
}

// ============================================================================
// Domain-Specific Type Aliases & Extensions
// ============================================================================

/// Sender for standard broker messages.
pub type MessageSender = Sender<Message>;
/// Receiver for standard broker messages.
pub type MessageReceiver = Receiver<Message>;

/// Token used by a consumer to receive a reply from a producer.
pub type RequestToken = oneshot::Sender<Message>;

/// Sender for requesting messages from a shared LoadBalanced queue.
/// (Sends ReplyTokens to the producer).
pub type RequestSender = Sender<RequestToken>;
/// Receiver for managing incoming pull-requests from consumers.
pub type RequestReceiver = Receiver<RequestToken>;

impl RequestSender {
    /// Asynchronously requests the next message from the shared LoadBalanced queue.
    /// 
    /// This method implements the **"Pull" pattern**: it creates a `oneshot` channel,
    /// sends the `oneshot::Sender` (as a `RequestToken`) to the producer,
    /// and returns the `oneshot::Receiver` to the caller.
    /// 
    /// This is the primary mechanism for consumers in a LoadBalanced group to
    /// signal readiness and receive messages with natural backpressure, ensuring
    /// that the producer never sends a message to a dead or busy consumer.
    ///
    /// # Returns
    /// - `Ok(oneshot::Receiver<Message>)` — the request was successfully queued.
    ///   The caller **MUST** await this receiver to get the response.
    /// - `Err(SendError<RequestToken>)` — the target component has terminated (channel is closed).
    ///   The `RequestToken` is returned inside the error, allowing for clean resource drop.
    #[must_use = "The returned receiver must be awaited to receive the message. \
                  Dropping it silently cancels the request and may cause the producer to hang \
                  while trying to send to a dropped receiver."]
    pub async fn request(&self) -> Result<oneshot::Receiver<Message>, SendError<RequestToken>> {
        tracing::trace!("RequestSender request");
        let (reply_tx, reply_rx) = oneshot::channel();
        
        // If the channel is closed, `send` will immediately return Err(SendError(reply_tx)).
        // This is safer than a pre-check (is_closed) as it avoids TOCTOU race conditions
        // and correctly provides the allocated token back to the caller for cleanup.
        self.send(reply_tx).await?;
        
        Ok(reply_rx)
    }

    /// Non-blocking request attempt.
    ///
    /// Attempts to register a pull-request in the shared LoadBalanced queue
    /// without blocking the current task. This is useful in scenarios where
    /// the caller cannot afford to yield (e.g., in tight polling loops or
    /// when implementing custom scheduling logic).
    ///
    /// # Returns
    /// - `Ok(oneshot::Receiver<Message>)` — the request was successfully queued.
    ///   The caller MUST await this receiver to get the response, otherwise the
    ///   response will be lost when the receiver is dropped.
    /// - `Err(TryRequestError::Full)` — the queue is full. The caller should
    ///   either retry later or switch to the async `request()` method.
    /// - `Err(TryRequestError::Disconnected)` — the target component has terminated.
    ///   No further requests will be accepted.
    ///
    /// # Example
    /// ```rust,ignore
    /// match request_sender.try_request() {
    ///     Ok(receiver) => {
    ///         // Successfully queued, now wait for response
    ///         let response = receiver.await?;
    ///         // Handle response
    ///     }
    ///     Err(TryRequestError::Full) => {
    ///         // Queue is full, retry or use async request()
    ///         tracing::warn!("Request queue full, backing off");
    ///     }
    ///     Err(TryRequestError::Disconnected) => {
    ///         // Target component is dead, clean up resources
    ///         return Err(TransportError::ConnectionClosed);
    ///     }
    /// }
    /// ```
    #[must_use = "The returned receiver must be awaited to receive the message. Dropping it silently cancels the request."]
    pub fn try_request(&self) -> Result<oneshot::Receiver<Message>, TryRequestError> {
        // Fast-path: avoid allocating a oneshot channel if the dispatcher is already dead.
        // This is a common scenario in Wasm environments when a component traps.
        if self.is_closed() {
            return Err(TryRequestError::Disconnected);
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        
        match self.try_send(reply_tx) {
            Ok(()) => Ok(reply_rx),
            // If Full or Disconnected, the reply_tx is consumed by TrySendError 
            // and immediately dropped here, preventing memory leaks.
            Err(TrySendError::Full(_)) => Err(TryRequestError::Full), 
            Err(TrySendError::Disconnected(_)) => Err(TryRequestError::Disconnected),
        }
    }
}

// ============================================================================
// Factories
// ============================================================================

/// Creates a new linked sender-receiver pair with the given capacity.
///
/// The capacity (capacity) is strictly controlled by the broker configuration
/// (SubscriberConfig or ClientConfig) to prevent exhaustion of
/// system memory and ensure predictable backpressure.
///
/// # Example
/// ```rust
/// use arcella_broker::transport::create_channel;
/// 
/// // Creating a channel with a capacity of 1024 messages 
/// let (sender, mut receiver) = create_channel(1024); 
/// 
/// assert_eq!(sender.load().max_capacity, 1024); 
/// assert!(!sender.is_closed()); 
/// ```
pub fn create_channel(capacity: usize) -> (MessageSender, MessageReceiver) {
    let (tx, rx) = mpsc::channel::<Message>(capacity);
    (MessageSender::new(tx), MessageReceiver::new(rx))
}

/// Creates a new linked sender-receiver pair for request routing with the given capacity.
pub fn create_request_channel(capacity: usize) -> (RequestSender, RequestReceiver) {
    let (tx, rx) = mpsc::channel::<RequestToken>(capacity);
    (RequestSender::new(tx), RequestReceiver::new(rx))
}
