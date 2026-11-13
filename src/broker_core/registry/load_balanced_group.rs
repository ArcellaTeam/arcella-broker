// arcella-broker/src/broker_core/registry/load_balanced_group.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::{
    Arc,
    Weak,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

use crate::broker_core::{
    registry::LocalRegistry,
    transport::{
        create_request_channel,
        MessageReceiver,
        RequestSender,
    },
};

use super::slot::SubscriptionSlot;

pub struct LoadBalancedGroup {
    /// Slot in the registry (for managing the version and the deletion flag)
    pub(crate) slot: Arc<SubscriptionSlot>,

    /// Control handle for the background dispatcher task
    dispatcher_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,

    /// Version for invalidating the InMemoryEndpoint cache
    version: AtomicU64,

    /// Request channel. The group holds one strong reference so that `req_rx` does not close prematurely.
    /// Consumers receive clones of this reference via `subscribe()`.
    /// Wrapped in Arc solely to allow checking Arc::strong_count in the is_empty() method,
    /// in order to determine whether there are active consumers.
    req_tx: Arc<RequestSender>,

    address: String,

    /// Counter of active consumers. Used for fast `is_empty()` determination
    /// and to trigger `shutdown()` when the last consumer leaves.
    active_consumers: AtomicUsize,

    /// Cancellation token, a child of the main_receiver token.
    /// Allows the group to be cancelled independently, but guarantees cancellation when the main channel is closed.
    cancel_token: CancellationToken,
}

impl LoadBalancedGroup {
    /// Creates a new group and starts a background dispatcher
    pub fn new(
        slot: Arc<SubscriptionSlot>,
        req_channel_capacity: usize,
        registry: Weak<LocalRegistry>,
        address: String,
    ) -> Arc<Self> {

        let group_token = slot.cancel_token.child_token();
        let (req_tx, mut req_rx) = create_request_channel(
            req_channel_capacity,
            group_token.clone()
        );

        let group = Arc::new(Self {
            slot: slot.clone(),
            dispatcher_handle: RwLock::new(None),
            version: AtomicU64::new(1),
            req_tx: Arc::new(req_tx),
            address: address.clone(),
            active_consumers: AtomicUsize::new(0),
            cancel_token: group_token,
        });

        let slot_for_drop: Arc<SubscriptionSlot> = slot.clone();
        let registry_for_task = registry;
        let address_for_task = address;
        let main_receiver_arc = slot.receiver();

        let handle = tokio::spawn(async move {
            loop {
                // 1. Wait for a request from any available consumer
                let mut reply_tx = match req_rx.recv().await {
                    Some(tx) => tx,
                    None => {
                        tracing::debug!("LoadBalancedGroup dispatcher: no more consumers, shutting down");
                        break // All RequestSenders have been destroyed, the group is closing
                    }
                };

                // 2. Take the next message from the shared queue
                let mut receiver = main_receiver_arc.lock().await;
                let mut current_msg = match receiver.recv().await {
                    Some(m) => m,
                    None => {
                        tracing::debug!("LoadBalancedGroup dispatcher: main queue closed, shutting down");
                        // The main queue is closed (e.g., on unregister)
                        // Break the loop; the oneshot::Sender will simply drop,
                        // which will correctly notify the consumer of the error.
                        break;
                    }
                };

               // 3. Message delivery loop with loss protection
                loop {
                    match reply_tx.send(current_msg) {
                        Ok(_) => {
                            break;
                        }
                        Err(msg) => {
                            // The consumer dropped (timeout or panic) before we sent the message.
                            // We do NOT lose the message! We keep it and try to hand it to the next consumer.
                            tracing::warn!("LoadBalancedGroup: consumer dropped before delivery, retrying with next consumer");
                            current_msg = msg;

                            // Wait for the next consumer. If there are no more, req_rx will return None,
                            // and we'll exit the loop, losing the message only in the event of a total group failure.
                            match req_rx.recv().await {
                                Some(next_reply_tx) => reply_tx = next_reply_tx,
                                None => break, // The group is dead
                            }
                        }
                    }
                };
            }

            slot_for_drop.mark_removed();

            // Automatic address cleanup on natural termination.
            // Weak::upgrade will return None if the Registry has already been destroyed —
            // in that case there is nothing to clean up.
            if let Some(registry) = registry_for_task.upgrade() {
                if let Err(e) = registry.unregister(&address_for_task, &slot_for_drop) {
                    tracing::debug!(
                        address = %address_for_task,
                        error = %e,
                        "LoadBalancedGroup auto-unregister"
                    );
                }
            }
        });

        *group.dispatcher_handle.write() = Some(handle);
        group
    }

    pub fn subscribe(&self) -> Arc<RequestSender> {
        self.active_consumers.fetch_add(1, Ordering::Release);
        self.req_tx.clone()
    }

    pub fn remove_consumer(&self) {
        let prev_count = self.active_consumers.fetch_sub(1, Ordering::AcqRel);
        
        if prev_count == 1 {
            // This was the last consumer — initiate group termination
            tracing::debug!(
                address = %self.address,
                "LoadBalancedGroup: last consumer removed, shutting down"
            );
            self.shutdown();
        }
    }

    /// Returns the current number of active consumers.
    pub fn consumer_count(&self) -> usize {
        self.active_consumers.load(Ordering::Acquire)
    }            

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.active_consumers.load(Ordering::Acquire) == 0
    }

    /// Terminates the dispatcher. Does NOT call unregister —
    /// this method is called FROM unregister, and calling it again
    /// would lead to a deadlock on register_mutex.
    pub fn shutdown(&self) {
        // 1. Cancel the token. This will instantly wake up any pending `recv()` inside the dispatcher.
        self.cancel_token.cancel();

        // 2. Abort the background task. This will instantly interrupt any .await inside it
        // and lead to dropping main_receiver, closing the main channel.
        if let Some(handle) = self.dispatcher_handle.write().take() {
            handle.abort();
        }
        
        // 3. Instant invalidation of all cached InMemoryEndpoint
        self.slot.mark_removed();
        self.version.fetch_add(1, Ordering::Release);
    }
}

impl Drop for LoadBalancedGroup {
    fn drop(&mut self) {
        tracing::debug!("LoadBalancedGroup drop");
        // `shutdown()` is idempotent, so it is safe to call from Drop
        self.shutdown();
    }
}
