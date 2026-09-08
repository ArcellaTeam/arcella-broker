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
use tokio::{
    sync::oneshot,
    task::JoinHandle,
};

use crate::{
    client::subscriber::Subscriber,
    protocol::Message,
};
use super::RegistryError;

/// RAII guard for managing the lifetime of a response wait.
/// 
/// Upon creation, it registers an `oneshot::Sender` in the waiters map.
/// Upon destruction (going out of scope or panicking), it automatically 
/// removes the registration, preventing memory leaks in the `HashMap`.
///
/// # Design Note
/// The structure holds a direct `Arc` reference to the `waiters` map, not to the entire `ReplyDispatcher`.
/// This eliminates the need to implement `Clone` for the dispatcher and prevents potential
/// footguns where cloning could lead to premature background task termination.
pub struct WaiterGuard {
    /// Reference to the waiters map for cleanup upon drop.
    waiters: Arc<Mutex<HashMap<[u8; 16], oneshot::Sender<Message>>>>,
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
        waiters: Arc<Mutex<HashMap<[u8; 16], oneshot::Sender<Message>>>>,
        message_id: [u8; 16],
    ) -> Result<(Self, oneshot::Receiver<Message>), RegistryError> {
        let (tx, rx) = oneshot::channel();
        let mut map = waiters.lock();
        
        // Protection against duplicate registrations (unlikely with correct UUID generation, but necessary)
		if map.contains_key(&message_id) {
            return Err(RegistryError::WaiterAlreadyExists);
        }
        
        map.insert(message_id, tx);
        Ok((Self { 
            waiters: waiters.clone(),
            message_id,
        }, rx))
    }
}

impl Drop for WaiterGuard {
    /// Automatically removes the wait from the dispatcher when the guard is destroyed.
    fn drop(&mut self) {
        self.waiters.lock().remove(&self.message_id);
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
    waiters: Arc<Mutex<HashMap<[u8; 16], oneshot::Sender<Message>>>>,
    
    /// Background task that reads responses from the shared wildcard channel and distributes them.
    /// Stored directly (without Arc) to emphasize exclusive ownership and correct Drop behavior.
    listener_task: JoinHandle<()>,
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
        let waiters: Arc<Mutex<HashMap<[u8; 16], oneshot::Sender<Message>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let waiters_clone = waiters.clone(); 

        // Background task that runs for the entire lifetime of the dispatcher
        let listener_task = tokio::spawn(async move {
            // Read messages from the channel until it is closed
            while let Some(response) = subscriber.recv().await {
                let msg_id = response.header.message_id;
                let mut map = waiters_clone.lock();
                
                // Extract the sender and remove it from the map (one-time use)
                let sender = map.remove(&msg_id);
                if let Some(sender) = sender {
                    // Ignore send error if the receiver has already been destroyed (client-side timeout)
                    let _ = sender.send(response);    
                };
            }
        });

        Self {
            waiters,
            listener_task,
        }
    }

    /// Registers a new request awaiting a response.
    pub fn register_waiter(&self, message_id: [u8; 16]) -> Result<(WaiterGuard, oneshot::Receiver<Message>), RegistryError> {
         WaiterGuard::new(self.waiters.clone(), message_id)
    }

    /// Removes the response wait by `message_id`.
    /// Usually called automatically via the `Drop` implementation of `WaiterGuard`.
    /// but the method is left public for cases of explicit cleanup, should it be needed.
    pub fn remove_waiter(&self, message_id: &[u8; 16]) {
        self.waiters.lock().remove(message_id);
    }
}

impl Drop for ReplyDispatcher {
    fn drop(&mut self) {
        // CRITICAL RAII MECHANISM:
        // 1. Cancel the Tokio background task.
        //    IMPORTANT: abort() only marks the task for cancellation. The actual destruction
        //    of captured variables (including `subscriber` and the call to its `Drop` with `unregister`)
        //    will occur asynchronously, when the Tokio scheduler next processes this task.
        //
        // 2. To avoid a race condition when rapidly recreating a client with the same address,
        //    synchronous cleanup of the address (`registry.unregister`) must be performed
        //    by the dispatcher owner (e.g., in the `Drop` implementation for `BrokerClient`).
        //
        // 3. Immediately clear the pending responses map. This guarantees fail-fast behavior:
        //    all hung `oneshot::Receiver`s will immediately receive a `RecvError`,
        //    allowing the client to correctly handle connection breakage (e.g.,
        //    transforming it into a `TransportError::ConnectionClosed`), without waiting
        //    for asynchronous completion of the background task and release of the `subscriber`.
        self.listener_task.abort();
        self.waiters.lock().clear();
    }
}
