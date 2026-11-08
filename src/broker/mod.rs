// arcella-broker/src/broker/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Core of the Arcella message broker.
//!
//! This module contains the `Broker` struct, which is the central node
//! and the Single Source of Truth for the entire message routing subsystem
//! within a single process.
//!
//! # Architectural role in the Arcella platform
//!
//! According to the platform architecture, the broker acts as the "nervous system"
//! for component interaction. In the **Main ↔ Main** topology, it provides
//! zero-cost routing between Trusted Async Components.
//! In the **Worker** topology, it can be used for internal routing
//! between replicas of the same WebAssembly module.
//!
//! Key guarantees that the broker core provides:
//! 1. **Centralized state management**: Stores a single configuration
//!    and a global route registry, ensuring that all broker clients
//!    operate under consistent rules (timeouts, TTL, queue limits).
//! 2. **Thread-Safe Sharing**: Uses `Arc`
//!    for safe and efficient sharing of state between
//!    multiple asynchronous tasks without excessive cloning of heavy structures.
//! 3. **Strict lifecycle control**: Acts as a factory for
//!    creating `BrokerClient`, ensuring that every new client is correctly
//!    initialized, registered in the registry, and bound to the current configuration.

use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::broker_core::registry::LocalRegistry;
use crate::config::BrokerConfig;
use crate::error::BrokerError;

#[cfg(feature = "client")]
use crate::client::BrokerClient;
#[cfg(feature = "client")]
use crate::config::ClientConfig;

#[cfg(feature = "server")]
use crate::broker_core::{
    l3::RegistryIngressBridge,
    security::{SessionManager, HandshakeHandler},
};
#[cfg(feature = "server")]
use crate::config::ServerConfig;
#[cfg(feature = "server")]
use crate::server::BrokerServer;

// ============================================================================
// Broker Mode Definitions
// ============================================================================

/// Client mode state.
///
/// Currently empty — all client state lives in individual `BrokerClient` instances.
/// In future (Stage 7: Client Unification), this will optionally hold
/// a connection to an external broker for hybrid routing.
#[cfg(feature = "client")]
struct ClientMode {
    // TODO: remote: Option<Arc<RemoteConnection>>,
}

/// Server mode state.
///
/// Contains the server instance, cancellation token for graceful shutdown,
/// and an optional join handle for the server task.
#[cfg(feature = "server")]
struct ServerMode {
    /// The broker server instance handling IPC connections.
    server: Arc<BrokerServer<RegistryIngressBridge>>,
    
    /// Token used to signal graceful shutdown to the server and its background tasks.
    shutdown_token: CancellationToken,
    
    /// Handle to the spawned server task (accept loop + GC).
    /// Populated after `run()` is called.
    server_handle: Option<JoinHandle<anyhow::Result<()>>>,
}

/// Enum representing the mutually exclusive operating modes of the broker.
enum BrokerMode {
    /// Client mode: in-process routing + optional future IPC to external broker.
    #[cfg(feature = "client")]
    Client(ClientMode),
    
    /// Server mode: accepts IPC connections, routes between clients.
    #[cfg(feature = "server")]
    Server(ServerMode),
}

// ============================================================================
// Broker Struct
// ============================================================================

/// The central core of the Arcella message broker.
///
/// This struct does not perform the actual sending or receiving of messages.
/// Instead, it stores the shared state necessary for the operation of
/// transport layers (`InMemoryTransport`) and client facades (`BrokerClient`).
///
/// # Memory management and concurrency
/// All fields are wrapped in `Arc`, which makes the `Broker` struct itself lightweight to
/// clone by reference. This allows creating many `BrokerClients` in
/// different threads or async tasks, while all of them will reference
/// the same instance of the registry and configuration, ensuring consistency
/// of routing throughout the entire process.
///
/// # Mode-specific behavior
/// The broker operates in exactly one mode: either `Client` or `Server`.
/// Methods like `create_client()` are only valid in client mode and will
/// panic or return an error if called on a server-mode broker.
pub struct Broker {
    /// Global configuration of this broker instance.
    /// Defines default parameters, such as request timeouts (InOut)
    /// and the TTL value for cascade routing.
    pub(crate) config: Arc<BrokerConfig>,
    
    /// Local routing registry.
    /// A high-performance data structure (based on radix trees and arc-swap),
    /// providing lock-free read access and strict exclusivity
    /// of address binding to prevent ambiguous routing.
    pub(crate) registry: Arc<LocalRegistry>,

    /// The operating mode of this broker (Client or Server).
    pub(crate) mode: BrokerMode,
}

// ============================================================================
// Broker Implementation
// ============================================================================

impl Broker {
    /// Creates a new broker instance in **client mode** with the given configuration.
    ///
    /// # Initialization
    /// 1. Wraps the passed configuration in `Arc` for safe shared use.
    /// 2. Initializes an empty but ready-to-use `LocalRegistry`.
    /// 3. Sets the mode to `BrokerMode::Client`.
    ///
    /// # Usage
    /// This method is typically called once at the startup of `ArcellaRuntime` or
    /// during initialization of the internal context of `arcella-worker`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use arcella_broker::broker::Broker;
    /// use arcella_broker::config::BrokerConfig;
    ///
    /// let broker = Arc::new(Broker::client(BrokerConfig::default()));
    /// let client = broker.create_client(ClientConfig::default(), "worker:1".into())?;
    /// ```
    #[cfg(feature = "client")]
    pub fn as_client(config: BrokerConfig) -> Self {
        let config = Arc::new(config);
        let registry = Arc::new(LocalRegistry::new());
        let mode = BrokerMode::Client(ClientMode {});
        
        Self { config, registry, mode }
    }

    /// Creates a new broker instance in **server mode** with the given configurations.
    ///
    /// # Initialization
    /// 1. Wraps the passed configuration in `Arc` for safe shared use.
    /// 2. Initializes an empty but ready-to-use `LocalRegistry`.
    /// 3. Creates `SessionManager` and `HandshakeHandler` with server limits.
    /// 4. Creates `RegistryIngressBridge` for L2→L3 message processing.
    /// 5. Creates `BrokerServer` with all required dependencies.
    /// 6. Sets the mode to `BrokerMode::Server`.
    ///
    /// # Usage
    /// This method is called when starting the standalone `brokerd` process.
    /// After creation, call `run()` to start the accept loop.
    ///
    /// # Example
    /// ```rust,ignore
    /// use arcella_broker::broker::Broker;
    /// use arcella_broker::config::{BrokerConfig, ServerConfig};
    ///
    /// let broker = Broker::server(
    ///     BrokerConfig::default(),
    ///     ServerConfig::default(),
    /// );
    /// broker.run().await?;
    /// ```
    #[cfg(feature = "server")]
    pub fn as_server(config: BrokerConfig, server_config: ServerConfig) -> Self {
        let config = Arc::new(config);
        let registry = Arc::new(LocalRegistry::new());
        let server_config = Arc::new(server_config);

        // Create server-side dependencies
        let session_manager = Arc::new(SessionManager::new());        
        let handshake_handler = Arc::new(HandshakeHandler::with_limits(server_config.limits.clone()));

        // Create ingress bridge for L2 → L3 routing
        let ingress_bridge = Arc::new(RegistryIngressBridge::new(
            Arc::clone(&registry),
            Arc::clone(&server_config),
        ));

        // Create the broker server
        let server = Arc::new(BrokerServer::new_with_bridge(
            server_config,
            session_manager,
            Arc::clone(&handshake_handler),
            Arc::clone(&registry),
            Arc::clone(&ingress_bridge),
        ));

        let shutdown_token = CancellationToken::new();        

        let mode = BrokerMode::Server(ServerMode {
            server,
            shutdown_token,
            server_handle: None,
        });

        Self { config, registry, mode }
    }    

    /// Returns the default broker configuration.
    ///
    /// Useful for quick startup, testing, or creating nested
    /// components that do not require specific timeout or TTL settings.
    pub fn default_config() -> BrokerConfig {
        BrokerConfig::default()
    }

    /// Returns `true` if this broker is in client mode.
    pub fn is_client(&self) -> bool {
        #[cfg(feature = "client")]
        {
            matches!(self.mode, BrokerMode::Client(_))
        }
        #[cfg(not(feature = "client"))]
        {
            false
        }
    }

    /// Returns `true` if this broker is in server mode.
    pub fn is_server(&self) -> bool {
        #[cfg(feature = "server")]
        {
            matches!(self.mode, BrokerMode::Server(_))
        }
        #[cfg(not(feature = "server"))]
        {
            false
        }
    }

    /// Creates a new client interface (`BrokerClient`) for interacting with the broker.
    ///
    /// # Architectural pattern: Factory with shared ownership
    /// The method takes `self: &Arc<Self>` rather than `&self` or `self`. This is critically
    /// important for two reasons:
    /// 1. It prevents accidental cloning of the `Broker` struct itself
    /// (which could lead to desynchronization of registries).
    /// 2. It allows the new `BrokerClient` to obtain its own strong reference
    /// (`Arc::clone`) to the core, guaranteeing that the broker core will live as long
    /// as at least one active client exists.
    ///
    /// # Usage
    /// This is the only sanctioned way for Trusted Async Components
    /// or internal subsystems to join the Arcella message bus.
    /// Upon creation, the client automatically reserves its `client_address`
    /// for receiving replies in InOut mode (Request/Response).
    ///
    /// # Errors
    /// - Returns `BrokerError::InvalidMode` if called on a server-mode broker.
    /// - Returns `BrokerError::Registry` if resources for the client could not be allocated
    ///   or if the specified `client_address` is already occupied by another active component.
    #[cfg(feature = "client")]
    pub fn create_client(
        self: &Arc<Self>,
        config: ClientConfig,
        client_address: String
    ) -> Result<BrokerClient, BrokerError> {
        // Validate that we're in client mode
        if !self.is_client() {
            return Err(BrokerError::InvalidMode(
                "create_client() can only be called on a client-mode broker".into()
            ));
        }

        Ok(BrokerClient::new(self.clone(), config, client_address)?)
    }

    /// Starts the broker server (accept loop + background GC tasks).
    ///
    /// This method spawns the server's accept loop and cleanup tasks as background
    /// Tokio tasks, then blocks until the server is shut down via `shutdown()` or
    /// a fatal error occurs.
    ///
    /// # Errors
    /// - Returns `BrokerError::InvalidMode` if called on a client-mode broker.
    /// - Returns `anyhow::Error` if the server encounters a fatal error during operation.
    #[cfg(feature = "server")]
    pub async fn run(&self) -> anyhow::Result<()> {
        if let BrokerMode::Server(mode) = &self.mode {
            let server = Arc::clone(&mode.server);

            let handle = tokio::spawn(async move {
                server.run().await
            });

            handle.await??;

            return Ok(());
        }
        
        Err(BrokerError::InvalidMode(
            "run() can only be called on a server-mode broker".into()
        ).into())
    }             

    /// Initiates a graceful shutdown of the broker server.
    ///
    /// This method signals the server's accept loop and background tasks to stop
    /// accepting new connections and finish processing in-flight requests.
    ///
    /// # Behavior
    /// - If the broker is in server mode, cancels the shutdown token.
    /// - If the broker is in client mode, this is a no-op (clients don't have background tasks).
    ///
    /// # Usage
    /// Typically called in response to a SIGINT/SIGTERM signal or when the application
    /// is shutting down.
    #[cfg(feature = "server")]
    pub fn shutdown(&self) {
        if let BrokerMode::Server(mode) = &self.mode {
            mode.shutdown_token.cancel();
        }
    }
}

/// Implementation of the `Default` trait for quickly creating a broker with standard settings.
///
/// Uses `BrokerConfig::default()`, which sets:
/// - Request timeout: 30,000 ms (30 seconds)
/// - Default TTL: 64 (sufficient for deep cascade routing)
impl Default for Broker {
    fn default() -> Self {
        #[cfg(feature = "client")]
        {
            Self::as_client(BrokerConfig::default())
        }
        #[cfg(not(feature = "client"))]
        {
            panic!("Cannot create default Broker: client feature is not enabled")
        }
    }
}
