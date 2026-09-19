// arcella-broker/src/registry/slot.rs
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

use crate::transport::MessageSender;

pub struct SubscriptionSlot {
    /// Current sender. `None` means the subscription has been removed.
    /// We use `ArcSwap` for lock-free updates.
    pub sender: ArcSwap<Option<MessageSender>>,
    
    /// Subscription version. Increments on ANY change:
    /// - register (initial or re-registration)
    /// - unregister
    /// - replacement of the sender
    pub version: AtomicU64,
}

impl SubscriptionSlot {
    pub fn new(sender: MessageSender) -> Arc<Self> {
        Arc::new(Self {
            sender: ArcSwap::from(Arc::new(Some(sender))),
            version: AtomicU64::new(1),
        })
    }
    
    /// Update the sender and version increment.
    pub fn update(&self, new_sender: MessageSender) {
        self.sender.store(Arc::new(Some(new_sender)));
        self.version.fetch_add(1, Ordering::Release);
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
}

impl Drop for SubscriptionSlot {
    fn drop(&mut self) {
        tracing::debug!("SubscriptionSlot drop");
    }
}
