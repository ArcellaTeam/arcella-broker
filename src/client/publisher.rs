// arcella-broker/src/client/publisher.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};

use crate::protocol::Message;
use crate::transport::{ResolvedEndpoint, Transport, TransportError, TransportResult};

pub struct Publisher {
    address: String,
    transport: Arc<dyn Transport>,
    cached_endpoint: RwLock<Option<ResolvedEndpoint>>,
}

impl Publisher {
    pub(crate) fn new(address: String, transport: Arc<dyn Transport>) -> Self {
        Self {
            address,
            transport,
            cached_endpoint: RwLock::new(None),
        }
    }

    async fn get_or_resolve_endpoint(&self) -> TransportResult<ResolvedEndpoint> {
        // 1. Fast endpoint check
        {
            let guard = self.cached_endpoint.read();
            if let Some(ep) = guard.as_ref() {
                if ep.is_alive() {
                    return Ok(ep.clone());
                }
            }
        }

        // 2. Slow endpoint check
        let resolved_ep = self.transport.resolve(&self.address).await?;

        let guard = self.cached_endpoint.upgradable_read();

        if let Some(ep) = guard.as_ref() {
            if ep.is_alive() {
                return Ok(ep.clone());
            }
        }

        // 3. Resolve address fron transport
        let mut write_guard = RwLockUpgradableReadGuard::upgrade(guard);
        if let Some(ep) = write_guard.as_ref() {
            if ep.is_alive() {
                return Ok(ep.clone());
            }
        }
        *write_guard = Some(resolved_ep.clone());
        
        Ok(resolved_ep)
    }


    /// Отправляет сообщение, используя кэшированный канал.
    pub async fn send(&self, message: Message) -> TransportResult<()> {
        let ep = self.get_or_resolve_endpoint().await?;
        match self.transport.send_to(&ep, message).await {
            Err(TransportError::ConnectionClosed) => {
                *self.cached_endpoint.write() = None; // Явная инвалидация
                Err(TransportError::ConnectionClosed)
            }
            other => other,
        }
    }    

    pub async fn request(&self, message: Message) -> TransportResult<Message> {
        let ep = self.get_or_resolve_endpoint().await?;
        match self.transport.request_to(&ep, message).await {
            Err(TransportError::ConnectionClosed) => {
                *self.cached_endpoint.write() = None; // Явная инвалидация
                Err(TransportError::ConnectionClosed)
            }
            other => other,
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }    
}
