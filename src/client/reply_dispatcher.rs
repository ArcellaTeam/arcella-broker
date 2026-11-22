// arcella-broker/src/client/reply_dispatcher.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Response dispatcher for correlating requests and responses (InOut pattern).
//!
//! This module manages waiting for responses to asynchronous requests within the process.
//! The architecture is built on strict RAII principles to guarantee the absence of resource leaks.
//! This mechanism is critical for the WebAssembly environment, where instances
//! can be destroyed asynchronously or due to a trap. It guarantees the absence of
//! "zombie" waits and memory leaks.
//!
//! # Architecture
//! - Each `BrokerClient` owns its exclusive `ReplyDispatcher`.
//! - The dispatcher takes ownership of a `Subscriber` registered to the client's address.
//! - A background Tokio task reads responses and routes them to waiting `oneshot` channels by `message_id`.
//! - Upon destruction (`Drop`), the task is forcibly aborted (`abort`), which guarantees that
//!   `Subscriber`'s `Drop` is called and automatically cancels the client address registration
//!   in the `LocalRegistry`.

use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::task::JoinHandle;

use crate::{
    //client::subscriber::Subscriber,
    broker_core::subscription::{
        RecvError,
        Subscriber,
    },
    broker_core::transport::{
        ClaimSender, 
        ClaimReceiver,
        create_request_token
    },
};
use super::RegistryError;

// ============================================================================
// Shared waiter state
// ============================================================================

/// Pending response waits, protected by a single mutex.
///
/// # Why `closed` lives here and not in a separate atomic
/// Registration and termination must be serialized. With a standalone
/// `AtomicBool` the following interleaving is possible:
///
/// 1. registrar reads `closed == false`;
/// 2. listener terminates and clears the map;
/// 3. registrar inserts its waiter into the now-dead map.
///
/// The waiter would then hang until timeout. Keeping the flag under the same
/// mutex as the map makes the two operations mutually exclusive.
struct Waiters {
    /// Set to `true` when the listener has terminated (or the dispatcher dropped).
    closed: bool,
    /// `message_id` -> one-shot reply channel.
    map: HashMap<[u8; 16], ClaimSender>,
}

impl Waiters {
    fn new() -> Self {
        Self { closed: false, map: HashMap::new() }
    }

    /// Terminates the state: rejects future registrations and fails all pending
    /// waiters. Idempotent — safe to call from both the listener task and `Drop`.
    fn close(&mut self) {
        self.closed = true;
        // Dropping the `ClaimSender`s resolves every pending `ClaimReceiver`
        // with `RecvError`, which the caller maps to `ConnectionClosed`.
        self.map.clear();
    }
}

// ============================================================================
// WaiterGuard
// ============================================================================

/// RAII guard for managing the lifetime of a response wait.
///
/// Registers a `ClaimSender` in the waiters map on creation and removes it on
/// destruction (normal scope exit, cancellation or panic), preventing leaks.
///
/// Holds a direct `Arc` to the shared state rather than to the whole dispatcher:
/// this avoids the need for `Clone` on `ReplyDispatcher` and prevents premature
/// termination of the background task.
pub struct WaiterGuard {
    /// Reference to the waiters map for cleanup upon drop.
    waiters: Arc<Mutex<Waiters>>,
    /// The message identifier for which a response is expected.
    message_id: [u8; 16],
}


impl WaiterGuard {
    /// Creates a new guard and registers the response wait.
    ///
    /// # Arguments
    /// * `waiters` - Arc reference to the dispatcher's waiter map.
    /// * `message_id` - unique message identifier.
    ///
    /// # Returns
    /// A tuple containing the created `WaiterGuard` and an `oneshot::Receiver` to receive the response.
    pub fn new(
        waiters: Arc<Mutex<Waiters>>,
        message_id: [u8; 16],
    ) -> Result<(Self, ClaimReceiver), RegistryError> {
        let mut state = waiters.lock();

        if state.closed {
            return Err(RegistryError::ReplyDispatcherClosed);
        }        
        if state.map.contains_key(&message_id) {
            return Err(RegistryError::WaiterAlreadyExists);
        }

        let (tx, rx) = create_request_token();
        state.map.insert(message_id, tx);
        drop(state);

        tracing::trace!("WaiterGuard registered");
        
        Ok((Self { waiters, message_id }, rx))
    }
}

impl Drop for WaiterGuard {
    /// Automatically removes the wait from the dispatcher when the guard is destroyed.
    fn drop(&mut self) {
        tracing::debug!("WaiterGuard drop");
        self.waiters.lock().map.remove(&self.message_id);
    }
}

/// Response dispatcher for InOut (Request/Response) mode.
///
/// # Ownership and Lifetime
/// This structure is the exclusive owner of the background listening task.
/// It intentionally **does not implement** the `Clone` trait. This guarantees that the background task
/// is aborted (`JoinHandle::abort`) exactly once, and strictly at the moment of destruction
/// of the `BrokerClient` that owns this dispatcher.
pub struct ReplyDispatcher {
    /// Map of pending requests: `message_id` -> `oneshot::Sender`.
    /// Uses `parking_lot::Mutex` for high performance during short locks.
    waiters: Arc<Mutex<Waiters>>,
    
    /// Background task that reads responses from the shared wildcard channel and distributes them.
    /// Stored directly (without Arc) to emphasize exclusive ownership and correct Drop behavior.
    listener_task: JoinHandle<()>,

    /// Reply address, kept for diagnostics after the subscriber is moved out.
    address: String,    
}

impl ReplyDispatcher {
    /// Creates a new dispatcher and starts the background listening task.
    ///
    /// # Arguments
    /// * `subscriber` - `Subscriber` from which incoming responses are read.
    ///   The dispatcher takes it **by value** (ownership). This is critical:
    ///   when the task is aborted, `subscriber` goes out of scope and is destroyed,
    ///   which triggers its `Drop` and automatically cancels the address registration in the registry.
    pub(crate) fn new(mut subscriber: Subscriber) -> Self {

        let waiters = Arc::new(Mutex::new(Waiters::new()));
        let waiters_for_task = Arc::clone(&waiters);
        let address = subscriber.address().to_string(); 
        let address_for_task = address.clone(); 

        tracing::debug!(address = %address, "ReplyDispatcher new");

        // Background task that runs for the entire lifetime of the dispatcher
        let listener_task = tokio::spawn(async move {
            tracing::debug!(address = %address_for_task, "ReplyDispatcher listener started");

            // Read messages from the channel until it is closed
            loop {
            //while let Some(response) = subscriber.recv().await {
                let response = match subscriber.recv().await {
                    Ok(response) => response,
                    Err(RecvError::Detached) => {
                        tracing::debug!(
                            address = %address_for_task,
                            "Reply binding detached, listener stopping"
                        );
                        break;
                    }
                    Err(RecvError::AddressRemoved) => {
                        tracing::warn!(
                            address = %address_for_task,
                            "Reply address removed by control plane, listener stopping"
                        );
                        break;
                    }                    
                    Err(RecvError::GroupClosed) | Err(RecvError::ChannelClosed) => {
                        tracing::error!(
                            address = %address_for_task,
                            "Reply source lost unexpectedly, listener stopping"
                        );
                        break;
                    }
                };

                let msg_id = response.header.message_id;

                // Take the sender under the lock, deliver outside of it.
                let sender = waiters_for_task.lock().map.remove(&msg_id);
                
                match sender {
                    Some(tx) => {
                        // Ignore the error: the receiver may already be
                        // gone (client-side timeout / cancelled request).
                        let _ = tx.send(response);
                    }
                    None => {
                        // Late reply after a timeout, or an unsolicited
                        // response. Not an error — just an observable event.
                        tracing::debug!(
                            address = %address_for_task,
                            "Reply with no pending waiter (late or unsolicited)"
                        );
                    }
                }
                
            }

            // Fail-fast: no response can arrive anymore. Resolve every pending
            // waiter immediately and reject subsequent registrations.
            waiters_for_task.lock().close();

            tracing::debug!(address = %address_for_task, "ReplyDispatcher listener stopped");
        });

        Self { 
            waiters, 
            listener_task, 
            address 
        }
    }

    /// Registers a new request awaiting a response.
    ///
    /// Returns `ReplyDispatcherClosed` if the listener has already terminated —
    /// the caller fails fast instead of waiting for `request_timeout_ms`.
    pub fn register_waiter(
        &self, 
        message_id: [u8; 16]
    ) -> Result<(WaiterGuard, ClaimReceiver), RegistryError> {
         WaiterGuard::new(self.waiters.clone(), message_id)
    }

    /// Removes the response wait by `message_id`.
    ///
    /// Usually invoked automatically by `WaiterGuard::drop`; kept public for
    /// explicit cleanup scenarios.
    pub fn remove_waiter(&self, message_id: &[u8; 16]) {
        self.waiters.lock().map.remove(message_id);
    }

    /// Whether the listener has terminated and no further replies can arrive.
    pub fn is_closed(&self) -> bool {
        self.waiters.lock().closed
    }    

    /// Number of currently pending response waits (metrics / diagnostics).
    pub fn pending_count(&self) -> usize {
        self.waiters.lock().map.len()
    }

    /// Reply address this dispatcher serves.
    pub fn address(&self) -> &str {
        &self.address
    }
}

impl Drop for ReplyDispatcher {
    fn drop(&mut self) {
        tracing::debug!(address = %self.address, "ReplyDispatcher drop");

        // 1. Synchronous fail-fast BEFORE aborting.
        //    `abort()` only marks the task for cancellation; its captured state is
        //    released asynchronously, when the scheduler next polls it. Clearing
        //    here guarantees that all hung `ClaimReceiver`s resolve immediately,
        //    without depending on scheduler timing.
        //
        //    Note: this does NOT unregister the address — that is the responsibility
        //    of the subscription guard / control plane, not of the dispatcher.
        self.waiters.lock().close();

        // 2. Stop the background task.
        self.listener_task.abort();
    }
}
