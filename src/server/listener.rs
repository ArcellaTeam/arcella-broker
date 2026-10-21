// arcella-broker/src/server/listener.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.


use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;

use crate::broker_core::security::{HandshakeHandler, SessionManager};
use crate::broker_core::registry::LocalRegistry;
use super::config::ServerConfig;
use super::connection_handler;

/// Starts the accept loop for incoming IPC connections.
/// Terminates upon receiving a shutdown signal or on a socket error.
pub async fn run_listener(
    config: ServerConfig,
    session_manager: Arc<SessionManager>,
    handshake_handler: Arc<HandshakeHandler>,
    local_registry: Arc<LocalRegistry>,
    shutdown_token: CancellationToken,
) -> anyhow::Result<()> {
    let listener = UnixListener::bind(&config.socket_path)?;
    tracing::info!("BrokerServer listening on {:?}", config.socket_path);

    loop {
        tokio::select! {
            // Global server shutdown signal
            _ = shutdown_token.cancelled() => {
                tracing::info!("Listener received shutdown signal, stopping accept loop");
                break;
            }
            
            // Waiting for a new connection
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _addr)) => {
                        let sm = Arc::clone(&session_manager);
                        let hh = Arc::clone(&handshake_handler);
                        let lr = Arc::clone(&local_registry);
                        
                        // Spawn connection handling in a separate task
                        tokio::spawn(async move {
                            if let Err(e) = connection_handler::handle_connection(
                                stream, sm, hh, lr
                            ).await {
                                tracing::debug!("Connection terminated: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to accept connection: {}", e);
                    }
                }
            }
        }
    }
    
    Ok(())
}
