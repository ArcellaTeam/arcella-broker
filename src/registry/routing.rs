// arcella-broker/src/registry/routing.rs
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

use crate::{
    protocol::Message,
    registry::LocalRegistry,
    transport::{
        MessageReceiver,
        TransportError, 
    },
};

use super::load_balanced_group::LoadBalancedGroup;

use super::SubscriptionSlot;

/// Message Delivery Policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingPolicy {
    Exclusive,      // Only one subscriber
    LoadBalanced,   // Round-Robin between subscribers
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
        main_receiver: MessageReceiver,
        req_channel_capacity: usize,
        registry: Weak<LocalRegistry>,
        address: String,
    ) -> Arc<Self> {
        Arc::new(Self::LoadBalanced(LoadBalancedGroup::new(
            slot,
            main_receiver,
            req_channel_capacity,
            registry,
            address,
        )))
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
    pub fn request_sender(&self) -> Option<Arc<crate::transport::RequestSender>> {
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
