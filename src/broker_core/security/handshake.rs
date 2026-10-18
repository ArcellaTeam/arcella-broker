// arcella-broker/src/broker_core/security/handshake.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::broker_core::transport::TransportError;

#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("protocol version mismatch: expected {expected}, got {got}")]
    ProtocolVersionMismatch { expected: u16, got: u16 },

    #[error("transport error: {0}")]
    Transport(#[from] TransportError),

    #[error("connection closed during handshake")]
    ConnectionClosed,

    #[error("unsupported authentication algorithm: 0x{0:02x}")]
    UnsupportedAlgorithm(u8),

    #[error("payload decode error: {0}")]
    PayloadDecode(String),
}