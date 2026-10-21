// arcella-broker/src/server/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

mod config;
pub mod listener;
pub mod connection_handler;
//pub mod ingress_bridge;

#[cfg(test)]
mod integration_test;

use config::ServerConfig;

use crate::broker_core::security::{
    SessionManager,
    HandshakeHandler,
};
use crate::broker_core::registry::LocalRegistry;

/// Main server component of the Arcella Broker
pub struct BrokerServer {
    config: ServerConfig,
    session_manager: Arc<SessionManager>,
    handshake_handler: Arc<HandshakeHandler>,
    local_registry: Arc<LocalRegistry>,
    shutdown_token: CancellationToken,
}

impl BrokerServer {
    pub fn new(
        config: ServerConfig,
        session_manager: Arc<SessionManager>,
        handshake_handler: Arc<HandshakeHandler>,
        local_registry: Arc<LocalRegistry>,
    ) -> Self {
        Self {
            config,
            session_manager,
            handshake_handler,
            local_registry,
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Returns a reference to the SessionManager (for integration tests and metrics).
    pub fn session_manager(&self) -> &Arc<SessionManager> {
        &self.session_manager
    }    

    /// Starts the server (accept loop + background GC)
    pub async fn run(&self) -> anyhow::Result<()> {
        // 1. Start the background session GC
        let gc_handle = self.session_manager.spawn_cleanup_task(
            self.config.session_timeout_ms,
            self.config.gc_interval_ms,
        );

        // 2. Start the listener
        let listener_handle = listener::run_listener(
            self.config.clone(),
            Arc::clone(&self.session_manager),
            Arc::clone(&self.handshake_handler),
            Arc::clone(&self.local_registry),
            self.shutdown_token.clone(),
        );

        // 3. Wait for the shutdown signal or an error
        tokio::select! {
            res = listener_handle => {
                if let Err(e) = res {
                    tracing::error!("Listener failed: {}", e);
                }
            }
            _ = self.shutdown_token.cancelled() => {
                tracing::info!("BrokerServer received shutdown signal");
            }
        }

        gc_handle.abort();
        Ok(())
    }

    /// Graceful shutdown
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }
}
