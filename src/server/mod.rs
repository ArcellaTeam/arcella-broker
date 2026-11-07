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

use crate::config::ServerConfig;
use crate::broker_core::IngressBridge;
use crate::broker_core::registry::LocalRegistry;
use crate::broker_core::security::{
    SessionManager,
    HandshakeHandler,
};

mod listener;
mod connection_handler;

pub use listener::run_listener;
pub use connection_handler::handle_connection;

#[cfg(test)]
mod integration_test;

/// Main server component of the Arcella Broker
pub struct BrokerServer<B>
where
    B: IngressBridge + 'static
{
    config: ServerConfig,
    session_manager: Arc<SessionManager>,
    handshake_handler: Arc<HandshakeHandler>,
    local_registry: Arc<LocalRegistry>,
    shutdown_token: CancellationToken,
    ingress_bridge: Arc<B>,
}

impl<B> BrokerServer<B>
where
    B: IngressBridge + 'static,
{
    pub fn new_with_bridge(
        config: ServerConfig,
        session_manager: Arc<SessionManager>,
        handshake_handler: Arc<HandshakeHandler>,
        local_registry: Arc<LocalRegistry>,
        ingress_bridge: Arc<B>,
    ) -> Self {
        Self {
            config,
            session_manager,
            handshake_handler,
            local_registry,
            shutdown_token: CancellationToken::new(),
            ingress_bridge,
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
            self.ingress_bridge.clone(),
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
