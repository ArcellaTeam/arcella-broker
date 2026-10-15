// arcella-broker/src/broker_core/transport/error.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::protocol::{
    ProtocolError,
    codec::FrameCodecError,
    payloads::DataFrameError,
};
use crate::broker_core::{
    l2::UnackedBufferError,
    registry::RegistryError,
};

/// Errors that occur at the broker's transport level during message delivery.
///
/// These errors are critically important for broker management systems, as they
/// signal the need for rerouting, resource cleanup, or
/// notifying the client that delivery is impossible.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The physical delivery channel is closed.
    ///
    /// In the Arcella context, this usually means that the target WebAssembly component
    /// or async task has terminated (was destroyed or crashed with a trap),
    /// and its `MessageReceiver` was destroyed (Drop).
    #[error("Connection closed")]
    ConnectionClosed,
    
    /// The operation timed out.
    ///
    /// Critically important for the InOut (Request/Response) pattern in the WebAssembly
    /// environment. Ensures that the broker will not block resources indefinitely if
    /// the target component has hung or entered an infinite loop.
    #[error("Operation timeout")]
    Timeout,
    
    /// No recipient with the specified logical address was found in the registry.
    ///
    /// Returned when `LocalRegistry` cannot match the address to any
    /// of the registered exact matches or wildcard patterns.
    #[error("Recipient not found: {0}")]
    RecipientNotFound(String),

    #[error("Session state error: {0}")]
    Session(String),
    
    /// An error propagated from the routing registry subsystem.
    ///
    /// May occur when attempting invalid registration or dynamic
    /// modification of routing rules during transport operation.
    #[error("Registry error: {0}")]
    Registry(#[from] RegistryError),
    
    /// An error indicating a violation of the format or semantics of the Arcella message protocol.
    ///
    /// Occurs at the stage of decoding or validating a message before sending
    /// (e.g., invalid UTF-8 in the address, exceeding the payload limit).
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("Frame codec error: {0}")]
    FrameCodec(#[from] FrameCodecError),

    #[error("Data frame payload error: {0}")]
    DataFrame(#[from] DataFrameError), 

    #[error("L2 unacked buffer error: {0}")]
    UnackedBuffer(#[from] UnackedBufferError),       

    #[error("Operation is unsupported")]
    Unsupported,
    
    /// A low-level I/O error.
    ///
    /// Characteristic of transport implementations that use IPC (Unix sockets)
    /// or network connections rather than in-process memory.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// The standard result type for broker transport layer operations.
pub type TransportResult<T> = Result<T, TransportError>;
