// arcella-broker/src/registry/reply_dispatcher.rs
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
//! It uses a background task (`tokio::spawn`) that listens to a shared wildcard channel 
//! of responses and forwards the received messages to the corresponding waiting 
//! one-shot channels (`oneshot`), matching them by `message_id`.

use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::{
    sync::oneshot,
    task::JoinHandle,
};

use crate::protocol::Message;

use super::{
    LocalReceiver, 
    RegistryError,
};

/// RAII guard for managing the lifetime of a response wait.
/// 
/// Upon creation, it registers an `oneshot::Sender` in the dispatcher.
/// Upon destruction (going out of scope or panicking), it automatically 
/// removes the registration from the dispatcher, preventing memory leaks in the `HashMap`.
pub struct WaiterGuard {
    /// Reference to the dispatcher for cleanup upon drop.
    dispatcher: ReplyDispatcher,
    /// The message identifier for which a response is expected.
    message_id: [u8; 16],
    /// Flag indicating whether the registration was successful (to prevent double removal).
    registered: bool,
}

impl WaiterGuard {
    /// Creates a new guard and registers the response wait.
    ///
    /// # Arguments
    /// * `dispatcher` - reference to the response dispatcher.
    /// * `message_id` - unique message identifier.
    ///
    /// # Returns
    /// A tuple containing the created `WaiterGuard` and an `oneshot::Receiver` to receive the response.
    pub fn new(dispatcher: &ReplyDispatcher, message_id: [u8; 16]) -> Result<(Self, oneshot::Receiver<Message>), RegistryError> {
        let (tx, rx) = oneshot::channel();
        let mut map = dispatcher.waiters.lock();
        
        if map.contains_key(&message_id) {
            return Err(RegistryError::WaiterAlreadyExists);
        }
        
        map.insert(message_id, tx);
        Ok((Self { 
            dispatcher: dispatcher.clone(),
            message_id,
            registered: true
        }, rx))
    }
}

impl Drop for WaiterGuard {
    /// Automatically removes the wait from the dispatcher when the guard is destroyed.
    fn drop(&mut self) {
        if self.registered {
            self.dispatcher.remove_waiter(&self.message_id);
        }
    }
}

/// Response dispatcher for InOut (Request/Response) mode.
#[derive(Clone)]
pub struct ReplyDispatcher {
    /// Map of pending requests: `message_id` -> `oneshot::Sender`.
    /// Uses `parking_lot::Mutex` for high performance during short locks.
    waiters: Arc<Mutex<HashMap<[u8; 16], oneshot::Sender<Message>>>>,
    
    /// Background task that reads responses from the shared wildcard channel and distributes them.
    /// Stored as `Arc<JoinHandle>` so the task is not interrupted when the dispatcher is cloned.
    _listener_task: Arc<JoinHandle<()>>,
}

impl ReplyDispatcher {
    /// Creates a new dispatcher and starts the background listening task.
    ///
    /// # Arguments
    /// * `rx` - the receiver (`LocalReceiver`) from which incoming responses are read.
    pub(crate) fn new(rx: LocalReceiver) -> Self {
        let waiters: Arc<Mutex<HashMap<[u8; 16], oneshot::Sender<Message>>>> = Arc::new(Mutex::new(HashMap::new()));
        let waiters_clone = waiters.clone(); 

        // Background task that runs for the entire lifetime of the dispatcher
        let listener_task = tokio::spawn(async move {
            let mut rx = rx;
            // Read messages from the channel until it is closed
            while let Some(response) = rx.recv().await {
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
            _listener_task: Arc::new(listener_task),
        }
    }

    /// Registers a new request awaiting a response.
    pub fn register_waiter(&self, message_id: [u8; 16]) -> Result<(WaiterGuard, oneshot::Receiver<Message>), RegistryError> {
         WaiterGuard::new(self, message_id)
    }

    /// Removes the response wait by `message_id`.
    /// Usually called automatically via the `Drop` implementation of `WaiterGuard`.
    pub fn remove_waiter(&self, message_id: &[u8; 16]) {
        let mut map = self.waiters.lock();
        map.remove(message_id);
    }        
}