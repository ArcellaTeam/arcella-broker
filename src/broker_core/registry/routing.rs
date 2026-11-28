// arcella-broker/src/broker_core/registry/routing.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    Weak,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::protocol::Message;

use super::LocalRegistry;
use super::super::transport::{
    MessageReceiver,
    RequestSender,
    TransportError, 
};

use super::load_balanced_group::LoadBalancedGroup;
use super::slot::AttachError;
use super::SubscriptionSlot;

/// A successfully claimed consumer binding on a routing target.
///
/// Carries everything needed to build a consumer's read engine without reaching
/// into the target's internals. The registry stays unaware of `Subscriber`: it
/// hands out read handles, not read engines.
///
/// # Contract
///
/// This is a claimed resource, not a passive value. Dropping it does NOT release
/// anything: the owner MUST call [`RouteTarget::detach`] exactly once, otherwise
/// the Exclusive flag stays set forever or the LoadBalanced consumer counter
/// leaks (and the group never observes emptiness).
#[must_use = "a claimed binding must be released with RouteTarget::detach"]
pub enum Attachment {
    /// Exclusive: read the address queue directly (Push).
    Push {
        receiver: Arc<Mutex<MessageReceiver>>,
        address_token: CancellationToken,
        queue_capacity: usize,
    },

    /// LoadBalanced: request messages from the group dispatcher (Pull).
    Pull {
        req_sender: Arc<RequestSender>,
        address_token: CancellationToken,
        queue_capacity: usize,
    },
}

impl Attachment {
    /// Address-scoped token the binding token must be derived from.
    #[inline]
    pub fn address_token(&self) -> &CancellationToken {
        match self {
            Self::Push { address_token, .. } | Self::Pull { address_token, .. } => address_token,
        }
    }

    /// Configured queue capacity of the address (for metrics / protocol acks).
    #[inline]
    pub fn queue_capacity(&self) -> usize {
        match self {
            Self::Push { queue_capacity, .. } | Self::Pull { queue_capacity, .. } => *queue_capacity,
        }
    }
}

/// Message Delivery Policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingPolicy {
    /// Exclusive binding: one consumer per address.
    Exclusive,

    /// LoadBalanced group: Round-Robin between consumers.
    /// `claim_capacity` — capacity of the pull-request channel (how many
    /// consumers can simultaneously wait for a message).
    LoadBalanced {
        claim_capacity: usize,
    },
    
    //Broadcast,      // Fan-out to all subscribers
}

/// Routing target (zero-cost for single subscriptions)
pub enum RouteTarget {
    Single(Arc<SubscriptionSlot>),
    LoadBalanced(Arc<LoadBalancedGroup>),
    //Broadcast(Arc<BroadcastGroup>),
}

impl RouteTarget {
    /// Creates a routing target for an Exclusive subscription.
    pub fn new_exclusive(slot: Arc<SubscriptionSlot>) -> Arc<Self> {
        Arc::new(Self::Single(slot))
    }

    /// Creates a routing target for a LoadBalanced group.
    pub fn new_load_balanced(
        slot: Arc<SubscriptionSlot>,
        req_channel_capacity: usize,
        registry: Weak<LocalRegistry>,
        address: String,
    ) -> Arc<Self> {
        Arc::new(Self::LoadBalanced(LoadBalancedGroup::new(
            slot,
            req_channel_capacity,
            registry,
            address,
        )))
    }

    /// Atomically registers one consumer on this target according to its policy.
    ///
    /// This is the single concurrency-safe entry point for attaching a consumer:
    ///
    /// * **Exclusive** — one CAS claims the slot's binding flag. Concurrent
    ///   callers get [`AttachError::Occupied`]; a removed address yields
    ///   [`AttachError::Removed`]. Re-attach after detach is allowed.
    /// * **LoadBalanced** — joins the consumer group; many consumers are legal by
    ///   definition, so no occupancy check applies.
    ///
    /// # Contract
    ///
    /// Every `Ok` MUST be paired with exactly one [`Self::detach`] by its owner.
    /// The claim is not released on drop of [`Attachment`].
    pub fn try_attach(&self) -> Result<Attachment, AttachError> {
        match self {
            Self::Single(slot) => {
                slot.try_attach_exclusive()?;
                Ok(Attachment::Push {
                    receiver: slot.receiver(),
                    address_token: slot.address_token().clone(),
                    queue_capacity: slot.capacity(),
                })
            }
            Self::LoadBalanced(group) => {
                // A group with zero consumers is alive, so `is_empty` is NOT a
                // removal signal. The group token is: `shutdown` cancels it, and
                // it is a child of the slot token, so control-plane removal
                // propagates here as well.
                if group.address_token().is_cancelled() {
                    return Err(AttachError::Removed);
                }
                Ok(Attachment::Pull {
                    req_sender: group.subscribe(),
                    address_token: group.address_token().clone(),
                    queue_capacity: group.slot.capacity(),
                })
            }
        }
    }

    /// Releases a claim obtained from [`Self::try_attach`].
    ///
    /// # Contract
    ///
    /// Exactly once per successful attach, by its owner. Calling it without a
    /// claim releases somebody else's Exclusive binding or underflows the group's
    /// consumer counter.
    pub fn detach(&self) {
        match self {
            Self::Single(slot) => slot.detach_exclusive(),
            Self::LoadBalanced(group) => group.remove_consumer(),
        }
    }

    /// Configured queue capacity of the underlying address.
    pub fn capacity(&self) -> usize {
        match self {
            Self::Single(slot) => slot.capacity(),
            Self::LoadBalanced(group) => group.slot.capacity(),
        }
    }

    /// Sends a message. Delegates either directly to the channel or to the group.
    pub async fn send(&self, message: Message) -> Result<(), TransportError> {
        match self {
            Self::Single(slot) => {
                let  guard = slot.sender.load();
                if let Some(sender) = guard.as_ref() {
                    sender.send(message).await.map_err(|_| TransportError::ConnectionClosed)
                } else {
                    Err(TransportError::ConnectionClosed)
                }
            }
            Self::LoadBalanced(group) => {
                let  guard = group.slot.sender.load();
                if let Some(sender) = guard.as_ref() {
                    sender.send(message).await.map_err(|_| TransportError::ConnectionClosed)
                } else {
                    Err(TransportError::ConnectionClosed)
                }
            }
            //Self::Broadcast(group) => group.send(message).await,
        }
    }

    /// Removes this target from the registry (control plane operation).
    /// 
    /// For **Single**: marks the slot as removed, invalidating cached endpoints.
    /// For **LoadBalanced**: shuts down the dispatcher group, which stops the
    /// background task and marks the slot as removed.
    /// 
    /// This method is idempotent and safe to call multiple times.
    pub(crate) fn remove_self(&self) {
        match self {
            Self::Single(slot) => {
                slot.mark_removed();
            }
            Self::LoadBalanced(group) => {
                group.shutdown();
            }
        }
    }    

    pub fn remove_slot(&self, slot: &Arc<SubscriptionSlot>) -> bool {
        tracing::debug!("RouteTarget remove_slot");
        match self {
            Self::Single(existing_slot) => {
                if Arc::ptr_eq(existing_slot, slot) {
                    existing_slot.mark_removed(); // Instant cache invalidation!
                    true // Needs to be removed from the tree
                } else {
                    false
                }
            }
            //Self::LoadBalanced(group) | Self::Broadcast(group) => {
            Self::LoadBalanced(group) => {
                if Arc::ptr_eq(&group.slot, slot) {
                    group.shutdown();
                    true // Needs to be removed from the tree
                } else {
                    false
                }
            }
        }
    }

    /// Returns a RequestSender if this is a LoadBalanced group.
    /// Used by the client to join an existing group.
    pub fn request_sender(&self) -> Option<Arc<RequestSender>> {
        match self {
            Self::LoadBalanced(group) => Some(group.subscribe()),
            _ => None,
        }
    }

    /// Return a LoadBalancedGroup if this is a LoadBalanced group.
    /// Used by the client to get subscriber group
    pub fn load_balanced_group(&self) -> Option<&Arc<LoadBalancedGroup>> {
        match self {
            Self::LoadBalanced(group) => Some(group),
            _ => None,
        }
    }            

    // Return address token
    pub fn address_token(&self) -> Option<&CancellationToken> {
        match self {
            RouteTarget::Single(slot) => Some(slot.address_token()),
            RouteTarget::LoadBalanced(group) => Some(group.address_token()),
        }
    }

    /// Unique version for cache protection (TOCTOU).
    pub fn version(&self) -> u64 {
        match self {
            Self::Single(slot) => slot.version.load(std::sync::atomic::Ordering::Acquire),
            Self::LoadBalanced(group) => group.version(),
            //Self::Broadcast(group) => group.version(),
        }
    }

    /// Liveness check for cache invalidation.
    pub fn is_closed(&self) -> bool {
        match self {
            Self::Single(slot) => slot.is_closed(),
            Self::LoadBalanced(group) => group.is_empty(),
            //Self::Broadcast(group) => group.is_empty(),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn slot() -> Arc<SubscriptionSlot> {
        SubscriptionSlot::new(8, CancellationToken::new())
    }

    #[test]
    fn exclusive_attach_is_exclusive_and_reattachable() {
        let target = RouteTarget::new_exclusive(slot());

        let first = target.try_attach().unwrap();
        assert!(matches!(first, Attachment::Push { .. }));
        assert_eq!(first.queue_capacity(), 8);

        assert_eq!(target.try_attach().err(), Some(AttachError::Occupied));

        target.detach();
        assert!(target.try_attach().is_ok());
        target.detach();
    }

    #[test]
    fn removed_target_refuses_attach() {
        let target = RouteTarget::new_exclusive(slot());
        target.remove_self();
        assert_eq!(target.try_attach().err(), Some(AttachError::Removed));
    }

    #[test]
    fn concurrent_exclusive_attach_has_one_winner() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let target = RouteTarget::new_exclusive(slot());
        let winners = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..32 {
            let target = Arc::clone(&target);
            let winners = Arc::clone(&winners);
            handles.push(thread::spawn(move || {
                if target.try_attach().is_ok() {
                    winners.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(winners.load(Ordering::SeqCst), 1);
        target.detach();
        assert!(target.try_attach().is_ok());
        target.detach();
    }

    #[tokio::test]
    async fn load_balanced_allows_many_consumers() {
        let target = RouteTarget::new_load_balanced(
            slot(),
            4,
            Weak::new(),
            "arcella:lb:test".to_string(),
        );

        let a = target.try_attach().unwrap();
        let b = target.try_attach().unwrap();
        assert!(matches!(a, Attachment::Pull { .. }));
        assert!(matches!(b, Attachment::Pull { .. }));

        let group = target.load_balanced_group().unwrap();
        assert_eq!(group.consumer_count(), 2);

        target.detach();
        assert_eq!(group.consumer_count(), 1);
        target.detach();
        assert_eq!(group.consumer_count(), 0);

        target.remove_self();
        assert_eq!(target.try_attach().err(), Some(AttachError::Removed));
    }
}
