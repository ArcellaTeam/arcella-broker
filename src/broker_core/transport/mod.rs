// arcella-broker/src/broker_core/transport/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Abstract transport layer for Arcella broker message routing.
//!
//! This module defines the key traits and data types that separate
//! logical recipient addressing (e.g., arcella:core:users or arcella:web:**)
//! from the physical message delivery mechanisms.
//!
//! # Architectural role in the Arcella platform
//!
//! The Arcella broker must provide seamless message routing across various
//! deployment topologies described in the platform architecture:
//! 1. Main ↔ Main: Delivery between trusted async components (Trusted Async Components)
//! within the main process via zero-cost channels (InMemoryTransport).
//! 2. Main ↔ Worker: Routing messages from the main process to isolated
//! WebAssembly instances (arcella-worker) via IPC (Unix sockets).
//! 3. Worker ↔ Worker: (Future) Direct or indirect routing between
//! workers without necessarily going through the main process.
//!
//! This module provides a unified contract (Transport and Endpoint) that allows
//! the broker core and clients (Publisher) to work with recipients uniformly,
//! without knowing the implementation details of a specific communication channel.
//!
//! # Key broker guarantees
//! - Separation of responsibilities: The transport is responsible only for moving bytes/messages.
//! Routing logic (Radix trees, wildcard matching) is encapsulated in LocalRegistry.
//! - Lifecycle safety (RAII): Transport errors (e.g., ConnectionClosed)
//! serve as triggers for automatic resource cleanup in the registry, preventing leaks
//! during abnormal termination (trap) of WebAssembly components.
//! - Hot/Cold path support: The presence of paired methods
//! send (with address resolution) and send_to (via a cached ResolvedEndpoint)
//! allows the broker to optimize delivery for frequently used routes.

mod channel;
mod endpoint;
mod error;
mod in_memory;
mod transport;

pub use channel::{
    create_channel, 
    create_request_channel, 
    MessageSender, 
    MessageReceiver, 
    RequestSender,
    TryRecvError,
};
pub use endpoint::{
    Endpoint,
    ResolvedEndpoint,
};
pub use error::*;
pub use in_memory::{
    InMemoryEndpoint, 
    InMemoryTransport,
};
pub use transport::Transport;

// ============================================================================
// IPC Transport (optional, enabled by the "ipc" feature)
// ============================================================================

#[cfg(all(feature = "ipc", unix))]
mod ipc;

#[cfg(all(feature = "ipc", unix))]
pub use ipc::{
    IPCConnector,
    IPCEndpoint,
    IPCTransport,
    UdsConnector,
};
