// arcella-broker/src/server/listener.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::fs::remove_file;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;

use crate::broker_core::security::{HandshakeHandler, SessionManager};
use crate::broker_core::registry::LocalRegistry;
use crate::broker_core::IngressBridge;
use crate::config::ServerConfig;
use super::connection_handler;

/// Starts the accept loop for incoming IPC connections.
/// Terminates upon receiving a shutdown signal or on a socket error.
pub async fn run_listener<B>(
    config: ServerConfig,
    session_manager: Arc<SessionManager>,
    handshake_handler: Arc<HandshakeHandler>,
    local_registry: Arc<LocalRegistry>,
    shutdown_token: CancellationToken,
    ingress_bridge: Arc<B>,
) -> anyhow::Result<()>
where
    B: IngressBridge + 'static,
{
    let socket_path = &config.socket_path;

    if socket_path.exists() {
        tracing::warn!(
            ?socket_path, 
            "Socket file already exists. Attempting to remove it before binding..."
        );
        if let Err(e) = remove_file(socket_path) {
            tracing::error!(
                ?socket_path, 
                error = %e, 
                "Failed to remove existing socket file. Is another broker instance running?"
            );
            return Err(anyhow::anyhow!(
                "Failed to remove existing socket file at {:?}: {}", 
                socket_path, e
            ));
        }
        tracing::info!(?socket_path, "Existing socket file successfully removed");
    }

    let listener = UnixListener::bind(socket_path)?;
    tracing::info!("BrokerServer listening on {:?}", socket_path);

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

                        let peer_addr = stream
                                .peer_addr()
                                .ok()
                                .and_then(|a| a.as_pathname().map(|p| p.to_string_lossy().into_owned()))
                                .unwrap_or_else(|| "unix:unnamed".to_string());                        

                        // Spawn connection handling in a separate task
                        let ingress_bridge_clone = ingress_bridge.clone(); // Clone for async move
                        tokio::spawn(async move {
                            if let Err(e) = connection_handler::handle_connection(
                                stream, peer_addr, sm, hh, lr, ingress_bridge_clone,
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
