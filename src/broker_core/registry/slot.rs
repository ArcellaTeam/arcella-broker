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
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::super::transport::{MessageSender, MessageReceiver, create_message_channel};

/// Failure to claim the Exclusive consumer binding of a slot.
///
/// Deliberately address-free: the slot does not know its own name, so the caller
/// (which does) supplies the address when mapping this onto a protocol error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AttachError {
    /// The Exclusive binding is already held by another consumer.
    #[error("Exclusive binding is already attached")]
    Occupied,

    /// The address was removed by the control plane.
    #[error("Address was removed")]
    Removed,
}

/// Bit flags of the slot state, kept in a single atomic word.
///
/// Claiming the Exclusive binding and removing the address must not race. With
/// one word, `try_attach_exclusive` observes `REMOVED` and publishes `ATTACHED`
/// in the same CAS, and `mark_removed` publishes `REMOVED` before invalidating
/// the sender. Two separate atomics would leave a check-then-act window between
/// "is the address alive?" and "claim it".
const STATE_ATTACHED: usize = 0b01;
const STATE_REMOVED: usize = 0b10;

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
    pub address_token: CancellationToken, 

    /// Configured queue capacity
    capacity: usize,   

    /// Lifecycle + Exclusive occupancy (`STATE_*` bits).
    state: AtomicUsize,
}

impl SubscriptionSlot {
    pub fn new(capacity: usize, address_token: CancellationToken) -> Arc<Self> {
        let (sender, receiver) = create_message_channel(capacity, address_token.clone());
        Arc::new(Self {
            sender: ArcSwap::from(Arc::new(Some(sender))),
            receiver: Arc::new(Mutex::new(receiver)),
            version: AtomicU64::new(1),
            address_token,
            capacity,
            state: AtomicUsize::new(0),
        })
    }
    
    /// Returns the address-scoped token (for creating binding tokens).
    #[inline]
    pub fn address_token(&self) -> &CancellationToken {
        &self.address_token
    }

    /// Atomically claims the Exclusive consumer binding of this address.
    ///
    /// Lock-free and safe under any number of concurrent callers: exactly one
    /// wins, the rest get [`AttachError::Occupied`]. A removed address always
    /// yields [`AttachError::Removed`] — the removal flag is observed by the same
    /// CAS that publishes the claim, so there is no check-then-act window.
    ///
    /// # Contract
    ///
    /// The winner MUST release the claim with [`Self::detach_exclusive`] exactly
    /// once, typically from the RAII guard that owns the binding. The registry
    /// never releases it on its own: address lifecycle and consumer lifecycle are
    /// separate planes.
    pub fn try_attach_exclusive(&self) -> Result<(), AttachError> {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            // Removal wins over occupancy: a removed address is never attachable,
            // even if a stale claim has not been released yet.
            if current & STATE_REMOVED != 0 {
                return Err(AttachError::Removed);
            }
            if current & STATE_ATTACHED != 0 {
                return Err(AttachError::Occupied);
            }
            match self.state.compare_exchange_weak(
                current,
                current | STATE_ATTACHED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    /// Releases the Exclusive claim taken by [`Self::try_attach_exclusive`].
    ///
    /// Never resurrects a removed address: the `REMOVED` bit is preserved, so
    /// detach after removal leaves the slot permanently unattachable.
    ///
    /// # Contract
    ///
    /// Must be called only by the holder of a successful claim; releasing a claim
    /// owned by somebody else would detach that consumer's binding.
    #[inline]
    pub fn detach_exclusive(&self) {
        self.state.fetch_and(!STATE_ATTACHED, Ordering::AcqRel);
    }

    /// Whether an Exclusive consumer binding is currently held.
    ///
    /// Diagnostics only: never use this as a pre-check before attaching — the
    /// result is stale by construction. Use [`Self::try_attach_exclusive`].
    #[inline]
    pub fn is_exclusive_attached(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_ATTACHED != 0
    }

    /// Marks the slot as deleted and version increment.
    pub fn mark_removed(&self) {
        // Publish REMOVED first: from this point on no new Exclusive claim can
        // succeed. A claim that won just before this store is invalidated by the
        // sender swap below, and its consumer observes the closed source.
        self.state.fetch_or(STATE_REMOVED, Ordering::AcqRel);
        self.sender.store(Arc::new(None));
        self.version.fetch_add(1, Ordering::Release);
    }

    /// Whether the address was removed by the control plane.
    #[inline]
    pub fn is_removed(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_REMOVED != 0
    }
    
    /// Checks if the underlying channel is closed or the slot is marked as removed
    pub fn is_closed(&self) -> bool {
        // Monotonic: once removed, always closed — even before the sender swap
        // becomes visible to this reader.
        if self.is_removed() {
            return true;
        }
        let guard = self.sender.load();
        match guard.as_ref() {
            Some(sender) => sender.is_closed(),
            None => true, // Slot was deleted explicitly with mark_removed
        }
    }

    #[inline]
    pub fn receiver(&self) -> Arc<Mutex<MessageReceiver>> {
        Arc::clone(&self.receiver)
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }    
}

impl Drop for SubscriptionSlot {
    fn drop(&mut self) {
        tracing::debug!("SubscriptionSlot drop");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot() -> Arc<SubscriptionSlot> {
        SubscriptionSlot::new(8, CancellationToken::new())
    }

    #[test]
    fn attach_is_exclusive() {
        let s = slot();
        assert_eq!(s.try_attach_exclusive(), Ok(()));
        assert_eq!(s.try_attach_exclusive(), Err(AttachError::Occupied));
        assert!(s.is_exclusive_attached());
    }

    #[test]
    fn detach_allows_reattach() {
        let s = slot();
        assert_eq!(s.try_attach_exclusive(), Ok(()));
        s.detach_exclusive();
        assert!(!s.is_exclusive_attached());
        assert_eq!(s.try_attach_exclusive(), Ok(()));
    }

    #[test]
    fn removed_slot_refuses_attach() {
        let s = slot();
        s.mark_removed();
        assert!(s.is_removed());
        assert!(s.is_closed());
        assert_eq!(s.try_attach_exclusive(), Err(AttachError::Removed));
    }

    #[test]
    fn detach_does_not_resurrect_removed_slot() {
        let s = slot();
        assert_eq!(s.try_attach_exclusive(), Ok(()));
        s.mark_removed();
        s.detach_exclusive();
        assert!(!s.is_exclusive_attached());
        assert_eq!(s.try_attach_exclusive(), Err(AttachError::Removed));
    }

    #[test]
    fn attach_version_is_untouched() {
        let s = slot();
        let before = s.version.load(Ordering::Acquire);
        assert_eq!(s.try_attach_exclusive(), Ok(()));
        s.detach_exclusive();
        assert_eq!(s.version.load(Ordering::Acquire), before);
    }

    #[test]
    fn concurrent_attach_has_exactly_one_winner() {
        use std::thread;

        let s = slot();
        let winners = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..64 {
            let s = Arc::clone(&s);
            let winners = Arc::clone(&winners);
            handles.push(thread::spawn(move || {
                if s.try_attach_exclusive().is_ok() {
                    winners.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(winners.load(Ordering::SeqCst), 1);
        assert!(s.is_exclusive_attached());
    }

    /// Removal racing against attach must never yield a claim on a removed slot
    /// that survives the removal flag.
    #[test]
    fn concurrent_attach_and_remove() {
        use std::thread;

        for _ in 0..200 {
            let s = slot();
            let a = {
                let s = Arc::clone(&s);
                thread::spawn(move || s.try_attach_exclusive())
            };
            let b = {
                let s = Arc::clone(&s);
                thread::spawn(move || s.mark_removed())
            };
            let attached = a.join().unwrap();
            b.join().unwrap();

            assert!(s.is_removed());
            // Whatever the interleaving, the slot ends up unattachable.
            if attached.is_ok() {
                s.detach_exclusive();
            }
            assert_eq!(s.try_attach_exclusive(), Err(AttachError::Removed));
        }
    }
}
