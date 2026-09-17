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

use crate::client::BrokerClient;
use crate::config::{BrokerConfig, ClientConfig};
use crate::error::BrokerError;
use crate::registry::LocalRegistry;

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
}

impl Broker {
    /// Creates a new instance of the broker core with the given configuration.
    ///
    /// # Initialization
    /// 1. Wraps the passed configuration in `Arc` for safe shared use.
    /// 2. Initializes an empty but ready-to-use `LocalRegistry`.
    ///
    /// This method is typically called once at the startup of `ArcellaRuntime` or
    /// during initialization of the internal context of `arcella-worker`.
    pub fn new(config: BrokerConfig) -> Self {
        let config = Arc::new(config);
        let registry = Arc::new(LocalRegistry::new());
        
        Self { config, registry }
    }

    /// Returns the default broker configuration.
    ///
    /// Useful for quick startup, testing, or creating nested
    /// components that do not require specific timeout or TTL settings.
    pub fn default_config() -> BrokerConfig {
        BrokerConfig::default()
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
    /// Returns `BrokerError` if resources for the client could not be allocated
    /// or if the specified `client_address` is already occupied by another active component.
    pub fn client(self: &Arc<Self>, config: ClientConfig, client_address: String) -> Result<BrokerClient, BrokerError> {
         Ok(BrokerClient::new(self.clone(), config, client_address)?)
    }         
}

/// Implementation of the `Default` trait for quickly creating a broker with standard settings.
///
/// Uses `BrokerConfig::default()`, which sets:
/// - Request timeout: 30,000 ms (30 seconds)
/// - Default TTL: 64 (sufficient for deep cascade routing)
impl Default for Broker {
    fn default() -> Self {
        Self::new(BrokerConfig::default())
    }
}
