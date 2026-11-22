// arcella-broker/src/broker_core/subscription/subscriber.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! The shared message read engine (Push / Pull).
//!
//! # Cancellation hierarchy
//!
//! Every consumer owns two tokens:
//!
//! ```text
//! address_cancel   — owned by the SubscriptionSlot / control plane
//!       └── cancel — binding-scoped, ALWAYS a child of address_cancel
//! ```
//!
//! Detaching a consumer cancels only `cancel`, leaving the address queue intact
//! for the next attached consumer. Removing an address cancels `address_cancel`,
//! which propagates to every binding.
//!
//! Because address removal cancels *both* tokens, every wait site checks
//! `address_cancel` **before** `cancel`, and every ambiguous terminal result
//! (`None` from the receiver, `Disconnected`, `GroupClosed`) is re-classified
//! through [`terminal_error`]. Otherwise address removal would be misreported
//! as `Detached` or `ChannelClosed`.

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

/// Classifies an ambiguous terminal condition.
///
/// Address removal cancels the binding token too (parent → child), so the
/// address token MUST be inspected first. Returns `None` when neither token is
/// cancelled, i.e. the failure is a genuine source-side closure.
///
/// Takes the tokens as separate arguments rather than `&self` so that it can be
/// called while `self.mode` is mutably borrowed inside a `match` arm.
#[inline]
fn terminal_error(
    address_cancel: &CancellationToken,
    cancel: &CancellationToken,
) -> Option<RecvError> {
    if address_cancel.is_cancelled() {
        Some(RecvError::AddressRemoved)
    } else if cancel.is_cancelled() {
        Some(RecvError::Detached)
    } else {
        None
    }
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
        /// Taken into local ownership by `recv()` so that it is dropped on
        /// every exit path, including cancellation.
        pending: Option<ClaimReceiver>,
    },
}

/// An attached consumer of a logical address.
///
/// `Subscriber` is a pure data receiver: it does not own the address, does not
/// touch the registry and performs no cleanup on drop. Lifetime control belongs
/// to `SubscriptionGuard`, which detaches the binding by cancelling the token
/// returned from the constructor.
///
/// Both `recv()` and `try_recv()` are cancel-safe: a cancelled operation never
/// consumes a message. For Pull mode, dropping the pending claim channel makes
/// the group dispatcher hand the message to the next consumer.
pub struct Subscriber {
    mode: SubscriberMode,
    
    address: String,

    /// Address-scoped token, owned by the slot / control plane.
    /// Cancelling it invalidates the queue for **all** consumers.
    address_cancel: CancellationToken,

    /// Binding-scoped token, always a child of `address_cancel`.
    /// Cancelling it detaches only this consumer.
    cancel: CancellationToken,
}

impl Subscriber {
    /// Creates a Push consumer (Exclusive / Broadcast).
    ///
    /// The binding token is derived internally as a child of `address_cancel`,
    /// which makes the hierarchy impossible to violate. The returned token must
    /// be handed to the subscription guard, which cancels it on detach.
    pub(crate) fn new_push(
        rx: Arc<Mutex<MessageReceiver>>,
        address: String,
        address_cancel: CancellationToken,
    ) -> (Self, CancellationToken) {
        let cancel = address_cancel.child_token();
        let subscriber = Self {
            mode: SubscriberMode::Push(rx),
            address,
            address_cancel,
            cancel: cancel.clone(),
        };
        (subscriber, cancel)
    }

    /// Creates a Pull consumer (LoadBalanced).
    ///
    /// `address_cancel` must come from the **existing** group's slot, not from
    /// the joining client, so that detaching one consumer cannot kill the group.
    pub(crate) fn new_pull(
        req_sender: Arc<RequestSender>,
        address: String,
        address_cancel: CancellationToken,
    ) -> (Self, CancellationToken) {
        let cancel = address_cancel.child_token();
        let subscriber = Self {
            mode: SubscriberMode::Pull { req_sender, pending: None },
            address,
            address_cancel,
            cancel: cancel.clone(),
        };
        (subscriber, cancel)
    }

    #[inline]
    fn abandon_pending(&mut self) {
        if let SubscriberMode::Pull { pending, .. } = &mut self.mode {
            *pending = None;
        }
    }

    /// Waits for the next message.
    ///
    /// # Contract
    ///
    /// - `Ok(msg)` — a message was consumed and belongs to this caller.
    /// - `Err(RecvError::Detached)` — this binding was detached.
    /// - `Err(RecvError::AddressRemoved)` — the address was deleted by the
    ///   control plane; takes precedence over `Detached`.
    /// - other `Err` variants — terminal source failure (see [`RecvError`]).
    ///
    /// Buffered messages are **not** drained here. In Push mode they remain in
    /// the address queue for the next attached consumer. In Pull mode the
    /// dispatcher has already removed the message from the queue before handing
    /// it to this claim, so it can no longer "stay in the queue".
    ///
    /// TODO(claim-requeue): the Pull fast path has an unavoidable race window —
    /// `claim.try_recv()` may consume a message an instant before detach. Closing
    /// it fully requires a requeue entry point on the routing target, or an
    /// explicit at-most-once statement for this window.
    pub async fn recv(&mut self) -> Result<Message, RecvError> {
        if let Some(err) = terminal_error(&self.address_cancel, &self.cancel) {
            self.abandon_pending();
            return Err(err);
        }

        match &mut self.mode {
            SubscriberMode::Push(receiver) => {
                // 1. Acquire the lock (address removal wins over detach)
                let mut rx = match receiver.try_lock() {
                    Ok(guard) => guard,
                    Err(_) => tokio::select! {
                        biased;
                        _ = self.address_cancel.cancelled() => return Err(RecvError::AddressRemoved),
                        _ = self.cancel.cancelled() => return Err(RecvError::Detached),
                        gw = receiver.lock() => gw,
                    }
                };

                // 2. Fast path: a message may already be queued
                match rx.try_recv() {
                    Ok(msg) => return Ok(msg),
                    Err(TransportTryRecvError::Disconnected) => {
                        return Err(terminal_error(&self.address_cancel, &self.cancel)
                            .unwrap_or(RecvError::ChannelClosed));
                    }
                    Err(TransportTryRecvError::Empty) => {}
                }

                // 3. Slow path. `Receiver::recv` also returns `None` when its own
                //    (address) token is cancelled, hence the re-classification.
                tokio::select! {
                    biased;
                    _ = self.address_cancel.cancelled() => Err(RecvError::AddressRemoved),
                    _ = self.cancel.cancelled() => Err(RecvError::Detached),
                    msg = rx.recv() => match msg {
                        Some(m) => Ok(m),
                        // `Receiver::recv` returns `None` both when all senders are dropped
                        // AND when its internal (address-scoped) token is cancelled. We must
                        // re-classify to distinguish genuine channel closure from address removal.
                        None => Err(terminal_error(&self.address_cancel, &self.cancel)
                            .unwrap_or(RecvError::ChannelClosed)),
                    },
                }
            }
            SubscriberMode::Pull { req_sender, pending } => {
                // 1. Issue a claim if none is outstanding
                if pending.is_none() {
                    let rx = match req_sender.try_request() {
                        Ok(rx) => rx,
                        Err(TryRequestError::Disconnected) => {
                            return Err(terminal_error(&self.address_cancel, &self.cancel)
                                .unwrap_or(RecvError::GroupClosed));
                        }
                        Err(TryRequestError::Full) => {
                            let req_result = tokio::select! {
                                biased;
                                _ = self.address_cancel.cancelled() => return Err(RecvError::AddressRemoved),
                                _ = self.cancel.cancelled() => return Err(RecvError::Detached),
                                rq = req_sender.request() => rq,
                            };
                            match req_result {
                                Ok(rx) => rx,
                                Err(_) => {
                                    return Err(terminal_error(&self.address_cancel, &self.cancel)
                                        .unwrap_or(RecvError::GroupClosed));
                                }
                            }    
                        }
                    };
                    *pending = Some(rx);
                }

                // 2. Take the claim into local ownership: it is dropped on every
                //    exit path below, so the dispatcher can re-deliver.
                let mut claim = match pending.take() {
                    Some(c) => c,
                    None => return Err(RecvError::GroupClosed),
                };

                // 3. Fast path: the claim may already be fulfilled
				match claim.try_recv() {
                    Ok(msg) => return Ok(msg),
                    Err(oneshot::error::TryRecvError::Closed) => {
                        return Err(terminal_error(&self.address_cancel, &self.cancel)
                            .unwrap_or(RecvError::GroupClosed));
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                }

                // 4. Slow path
                tokio::select! {
                    biased;
                    _ = self.address_cancel.cancelled() => {
                        // Address is gone: drop the claim, no salvage.
                        Err(RecvError::AddressRemoved)
                    }
                    _ = self.cancel.cancelled() => {
                        // TODO(pull-salvage): see the method doc.
                        match claim.try_recv() {
                            Ok(msg) => Ok(msg),
                            _ => Err(RecvError::Detached),
                        } 
                    }
                    res = &mut claim => match res {
                        Ok(msg) => Ok(msg),
                        Err(_) => Err(terminal_error(&self.address_cancel, &self.cancel)
                            .unwrap_or(RecvError::GroupClosed)),
                    },
                }                
            }
        }
    }

    /// Non-blocking receive attempt.
    #[must_use = "The result must be handled, otherwise the message will be dropped"]
    pub fn try_recv(&mut self) -> Result<Message, TryRecvError> {
        if let Some(err) = terminal_error(&self.address_cancel, &self.cancel) {
            self.abandon_pending();
            return Err(TryRecvError::Closed(err));
        }

        match &mut self.mode {
            SubscriberMode::Push(receiver) => {
                let mut rx = match receiver.try_lock() {
                    Ok(guard) => guard,
                    Err(_) => return Err(TryRecvError::Busy),
                };
                match rx.try_recv() {
                    Ok(msg) => Ok(msg),
                    Err(TransportTryRecvError::Empty) => Err(TryRecvError::Empty),
                    Err(TransportTryRecvError::Disconnected) => Err(TryRecvError::Closed(
                        terminal_error(&self.address_cancel, &self.cancel)
                            .unwrap_or(RecvError::ChannelClosed),
                    )),
                }
            }
            SubscriberMode::Pull { req_sender, pending } => {
                // A `Full` result means nothing was enqueued: pure backpressure.
                if pending.is_none() {
                    match req_sender.try_request() {
                        Ok(rx) => *pending = Some(rx),
                        Err(TryRequestError::Disconnected) => {
                            return Err(TryRecvError::Closed(
                                terminal_error(&self.address_cancel, &self.cancel)
                                    .unwrap_or(RecvError::GroupClosed),
                            ));
                        }
                        Err(TryRequestError::Full) => return Err(TryRecvError::Backpressure),
                    }
                }

                let reply_rx = match pending.as_mut() {
                    Some(p) => p,
                    None => return Err(TryRecvError::Closed(RecvError::GroupClosed)),
                };

                match reply_rx.try_recv() {
                    Ok(msg) => {
                        *pending = None;
                        Ok(msg)
                    }
                    Err(oneshot::error::TryRecvError::Empty) => Err(TryRecvError::Empty),
                    Err(oneshot::error::TryRecvError::Closed) => {
                        *pending = None;
                        Err(TryRecvError::Closed(
                            terminal_error(&self.address_cancel, &self.cancel)
                                .unwrap_or(RecvError::GroupClosed),
                        ))
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
    ///
    /// Note: address removal also cancels the binding token (parent → child),
    /// so this returns `true` in that case as well. Use [`Self::is_address_removed`]
    /// to distinguish the two.
    pub fn is_detached(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Whether the address itself was removed by the control plane.
    pub fn is_address_removed(&self) -> bool {
        self.address_cancel.is_cancelled()
    }
}
