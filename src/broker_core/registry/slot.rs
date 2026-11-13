// arcella-broker/src/broker_core/registry/slot.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use arc_swap::ArcSwap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::super::transport::{MessageSender, MessageReceiver, create_channel};

pub struct SubscriptionSlot {
    /// Current sender. `None` means the subscription has been removed.
    /// We use `ArcSwap` for lock-free updates.
    pub sender: ArcSwap<Option<MessageSender>>,

    /// The receiver side of the channel.
    /// Wrapped in Arc<Mutex<>> so it can be safely shared (e.g., with an egress task 
    /// or LoadBalancedGroup dispatcher) and mutated for `recv()`.
    pub (crate) receiver: Arc<Mutex<MessageReceiver>>,
    
    /// Subscription version. Increments on ANY change:
    /// - register (initial or re-registration)
    /// - unregister
    /// - replacement of the sender
    pub version: AtomicU64,

    /// Token bound to this specific subscription.
    pub cancel_token: CancellationToken,    
}

impl SubscriptionSlot {
    pub fn new(capacity: usize, cancel_token: CancellationToken) -> Arc<Self> {
        let (sender, receiver) = create_channel(capacity, cancel_token.clone());
        Arc::new(Self {
            sender: ArcSwap::from(Arc::new(Some(sender))),
            receiver: Arc::new(Mutex::new(receiver)),
            version: AtomicU64::new(1),
            cancel_token,
        })
    }
    
    /// Marks the slot as deleted and version increment.
    pub fn mark_removed(&self) {
        self.sender.store(Arc::new(None));
        self.version.fetch_add(1, Ordering::Release);
    }
    
    /// Checks if the underlying channel is closed or the slot is marked as removed
    pub fn is_closed(&self) -> bool {
        let guard = self.sender.load();
        match guard.as_ref() {
            Some(sender) => sender.is_closed(),
            None => true, // Slot was deleted explicitly with mark_removed
        }
    }

    pub fn receiver(&self) -> Arc<Mutex<MessageReceiver>> {
        Arc::clone(&self.receiver)
    }
}

impl Drop for SubscriptionSlot {
    fn drop(&mut self) {
        tracing::debug!("SubscriptionSlot drop");
    }
}
