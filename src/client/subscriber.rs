// arcella-broker/src/client/subscriber.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;

use crate::protocol::Message;
use crate::registry::{LocalRegistry, RegistryError};
use crate::transport::channel::{self, MessageReceiver, TryRecvError};

pub struct Subscriber {
    rx: MessageReceiver,
    address: String,
    registry: Arc<LocalRegistry>,
}

impl Subscriber {
    pub(crate) fn new(
        rx: MessageReceiver,
        address: String,
        registry: Arc<LocalRegistry>,
    ) -> Self {
        Self { rx, address, registry }
    }

    pub fn bind(
        address: String, 
        registry: Arc<LocalRegistry>, 
        capacity: usize
    ) -> Result<Self, RegistryError> {
        let (sender, receiver) = channel::create_channel(capacity);
        registry.register(address.clone(), sender)?;
        Ok(Self::new(receiver, address, registry))
    }    

    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    #[must_use = "The result must be handled, otherwise the message will be dropped"]
    pub fn try_recv(&mut self) -> Result<Message, TryRecvError> {
        self.rx.try_recv()
    }  

    pub fn address(&self) -> &str {
        &self.address
    }

}

impl Drop for Subscriber {
    fn drop(&mut self) {
        // Automatic cleanup when going out of scope
        if let Err(e) = self.registry.unregister(&self.address) {
            tracing::warn!(address = %self.address, error = %e, "Failed to unregister subscriber on drop");
        }
    }
}
