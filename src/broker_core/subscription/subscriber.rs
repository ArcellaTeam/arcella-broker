// arcella-broker/src/broker_core/subscription/subscriber.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! The shared message read engine (Push / Pull).

use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, oneshot};
use tokio_util::sync::CancellationToken;

use crate::protocol::Message;
use crate::broker_core::transport::{
    MessageReceiver, 
    RequestSender,
    ClaimReceiver, 
    TryRecvError as TransportTryRecvError, 
    TryRequestError,
};

/// Why reading ended permanently.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RecvError {
    /// The binding is detached (normal completion via Unsubscribe or session drop).
    #[error("Binding is detached")]
    Detached,        
    
    /// The address was removed by the control plane.
    #[error("Address was removed")]
    AddressRemoved,
    
    /// Pull: the group dispatcher has terminated.
    #[error("Group dispatcher has closed")]
    GroupClosed,

    /// Invariant violated: the sender disappeared while the address is alive.
    #[error("Channel was closed")]
    ChannelClosed,
}

/// Result of a non-blocking attempt.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TryRecvError {
    /// No data right now.
    #[error("Empty")]
    Empty,
    
    /// Push: the receiver mutex is currently busy (contended).
    #[error("Receiver mutex is busy")]
    Busy,

    /// Pull: the claim queue is full (backpressure).
    #[error("Claim queue is full")]
    Backpressure,

    /// Terminal state: the subscription is permanently closed.
    #[error("Closed: {0}")]
    Closed(#[from] RecvError),
}

/// Delivery model, derived from the address routing policy.
///
/// The consumer never chooses this: it is a property of the address, fixed at
/// creation time by the control plane.
enum SubscriberMode {
    /// Exclusive (and future Broadcast): read the address queue directly.
    Push(Arc<Mutex<MessageReceiver>>),

    /// LoadBalanced: request the next message from the group dispatcher.
    Pull{
        req_sender: Arc<RequestSender>,
        /// Holds the active oneshot receiver while waiting for a message.
        /// Must be cleared on cancellation to prevent claim leaks.
        pending: Option<ClaimReceiver>,
    },
}

/// An attached consumer of a logical address.
///
/// `Subscriber` is a pure data receiver: it does not own the address, does not
/// touch the registry and performs no cleanup on drop. Lifetime control belongs
/// to `SubscriptionGuard`, which detaches the binding.
///
/// Both `recv()` and `try_recv()` are cancel-safe: a cancelled operation never
/// consumes a message. For Pull mode, dropping the pending claim channel makes
/// the group dispatcher hand the message to the next consumer.
pub struct Subscriber {
    mode: SubscriberMode,
    address: String,
    /// Binding-scoped token. Cancelled on detach.
    cancel: CancellationToken,
}

impl Subscriber {
    /// Creates a Push consumer (Exclusive / Broadcast).
    pub(crate) fn new_push(
        rx: Arc<Mutex<MessageReceiver>>,
        address: String,
        cancel: CancellationToken,
    ) -> Self {
        Self { mode: SubscriberMode::Push(rx), address, cancel }
    }

    /// Creates a Pull consumer (LoadBalanced).
    pub(crate) fn new_pull(
        req_sender: Arc<RequestSender>,
        address: String,
        cancel: CancellationToken,
    ) -> Self {
        Self { mode: SubscriberMode::Pull { req_sender, pending: None }, address, cancel }
    }

    /// Waits for the next message.
    ///
    /// Returns `Err(RecvError::Detached)` when the binding is detached.
    /// Buffered messages remain in the address queue and will be delivered 
    /// to the next attached consumer; they are NOT drained here.
    pub async fn recv(&mut self) -> Result<Message, RecvError> {
        if self.cancel.is_cancelled() {
            return Err(RecvError::Detached);
        }

        match &mut self.mode {
            SubscriberMode::Push(receiver) => {
                // 1. Fast path: try to acquire lock and read synchronously
                let mut rx = match receiver.try_lock() {
                    Ok(guard) => guard,
                    Err(_) => {
                        tokio::select! {
                            biased;
                            _ = self.cancel.cancelled() => return Err(RecvError::Detached),
                            gw = receiver.lock() => gw,
                        }
                    },
                };

                // 2. Fast path: check if message is already available
                match rx.try_recv() {
                    Ok(msg) => return Ok(msg),
                    Err(TransportTryRecvError::Disconnected) => return Err(RecvError::ChannelClosed),
                    Err(TransportTryRecvError::Empty) => {}
                }

                // 3. Slow path: wait for message
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => Err(RecvError::Detached),
                    msg = rx.recv() => {
                        match msg {
                            Some(m) => Ok(m),
                            None => Err(RecvError::ChannelClosed),
                        }
                    }
                }
            }
            SubscriberMode::Pull { req_sender, pending } => {
                // 1. Fast path: try to send request token synchronously
                if pending.is_none() {
                    let rx = match req_sender.try_request() {
                        Ok(rx) => rx,
                        Err(TryRequestError::Disconnected) => return Err(RecvError::GroupClosed),
                        Err(TryRequestError::Full) => {
                            // Queue is full, fallback to async wait for a slot
                            let req_result = tokio::select! {
                                biased;
                                _ = self.cancel.cancelled() => return Err(RecvError::Detached),
                                rq = req_sender.request() => rq,
                            };
                            match req_result {
                                Ok(rx) => rx,
                                Err(_) => return Err(RecvError::GroupClosed),
                            }    
                        }
                    };
                    *pending = Some(rx);
                }

                // 2. Get claim as owner
                let mut claim = match pending.take() {
                    Some(c) => c,
                    _ => return Err(RecvError::GroupClosed),
                };


                // 3. Fast path: check if response is already available (rare but possible)
                match claim.try_recv() {
                    Ok(msg) => return Ok(msg),
                    Err(oneshot::error::TryRecvError::Closed) => return Err(RecvError::GroupClosed),
                    Err(oneshot::error::TryRecvError::Empty) => {}
                }

                // 4. Slow path: wait for the actual response
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => {
                        match claim.try_recv() {
                            Ok(msg) => Ok(msg),
                            _ => Err(RecvError::Detached),
                        } 
                    }
                    res = &mut claim => match res {
                        Ok(msg) => Ok(msg),
                        Err(_) => Err(RecvError::GroupClosed),
                    },
                }                
            }
        }
    }

    /// Non-blocking receive attempt.
    #[must_use = "The result must be handled, otherwise the message will be dropped"]
    pub async fn try_recv(&mut self) -> Result<Message, TryRecvError> {
        if self.cancel.is_cancelled() {
            return Err(TryRecvError::Closed(RecvError::Detached));
        }

        match &mut self.mode {
            SubscriberMode::Push(receiver) => {
                let mut rx = match receiver.try_lock() {
                    Ok(guard) => guard,
                    Err(_) => {
                        tokio::select! {
                            biased;
                            _ = self.cancel.cancelled() => return Err(TryRecvError::Closed(RecvError::Detached)),
                            guard = receiver.lock() => guard,
                        }
                    },
                };
                match rx.try_recv() {
                    Ok(msg) => Ok(msg),
                    Err(TransportTryRecvError::Empty) => Err(TryRecvError::Empty),
                    Err(TransportTryRecvError::Disconnected) => {
                        Err(TryRecvError::Closed(RecvError::ChannelClosed))
                    }
                }
            }
            SubscriberMode::Pull { req_sender, pending } => {
                // Issue a request only if none is outstanding. A `Full` result means
                // nothing was enqueued, so this is pure backpressure with no side effect.
                if pending.is_none() {
                    match req_sender.try_request() {
                        Ok(rx) => *pending = Some(rx),
                        Err(TryRequestError::Disconnected) => return Err(TryRecvError::Closed(RecvError::GroupClosed)),
                        Err(TryRequestError::Full) => return Err(TryRecvError::Backpressure),
                    }
                }

                let reply_rx = match pending.as_mut() {
                    Some(p) => p,
                    _ => return Err(TryRecvError::Closed(RecvError::GroupClosed)),
                };

                match reply_rx.try_recv() {
                    Ok(msg) => {
                        *pending = None;
                        Ok(msg)
                    }
                    Err(oneshot::error::TryRecvError::Empty) => Err(TryRecvError::Empty),
                    Err(oneshot::error::TryRecvError::Closed) => {
                        *pending = None;
                        Err(TryRecvError::Closed(RecvError::GroupClosed))
                    }
                }
            }
        }
    }

    /// Returns the logical address this subscription is registered for.
    ///
    /// Useful for contextual logging, tracing, and
    /// debugging routing in multi-component Arcella deployments.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Whether this binding has been detached.
    pub fn is_detached(&self) -> bool {
        self.cancel.is_cancelled()
    }
}
