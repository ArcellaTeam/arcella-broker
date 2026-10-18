// arcella-broker/src/protocol/payloads/hello.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Hello payload (0x00) — Client → Server
//!
//! Format:
//! ```text
//! - protocol_version: u16 LE
//! - client_id_length: u16 LE
//! - client_id: [u8; client_id_length] LE
//! - algorithm: 
//! - auth_data_length: u16 LE
//! - auth_data: [u8; auth_data_length]
//! - capabilities: u32 LE
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use super::L2_PROTOCOL_VERSION;

/// Errors specific to Hello payload parsing
#[derive(Debug, Error, PartialEq, Eq)]
pub enum HelloPayloadError {
    #[error("Insufficient data to read Hello header")]
    IncompleteHeader,

    #[error("Insufficient data to read client_id: expected {expected}, available {available}")]
    IncompleteClientId { expected: usize, available: usize },

    #[error("Insufficient data to read auth_data: expected {expected}, available {available}")]
    IncompleteAuthData { expected: usize, available: usize },

    #[error("Unsupported protocol version: {0}")]
    UnsupportedVersion(u16),
}

/// Hello payload sent by client to initiate connection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloPayload {
    /// Protocol version (must be L2_PROTOCOL_VERSION)
    pub protocol_version: u16,
    /// Client identifier (for logging/tracking)
    pub client_id: Bytes,
    //
    pub algorithm: u8,
    /// Authentication data (opaque to L2, validated by security layer)
    pub auth_data: Bytes,
    /// Client capabilities (feature flags)
    pub capabilities: u32,
}

impl HelloPayload {
    /// Encodes the Hello payload into bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(
            2 + 2 + self.client_id.len() + 1 + 2 + self.auth_data.len() + 4,
        );
        
        buf.put_u16_le(self.protocol_version);
        let client_id_len = self.client_id.len();
        assert!(client_id_len <= u16::MAX as usize, "client_id too long");
        buf.put_u16_le(client_id_len as u16);
        buf.put_slice(&self.client_id);
        buf.put_u8(self.algorithm);
        let auth_data_len = self.auth_data.len();
        assert!(auth_data_len <= u16::MAX as usize, "auth_data too long");
        buf.put_u16_le(auth_data_len as u16);
        buf.put_slice(&self.auth_data);
        buf.put_u32_le(self.capabilities);
        
        buf.freeze()
    }

    /// Decodes the Hello payload from bytes
    pub fn decode(buf: &mut impl Buf) -> Result<Self, HelloPayloadError> {
        if buf.remaining() < 2 {
            return Err(HelloPayloadError::IncompleteHeader);
        }

        let protocol_version = buf.get_u16_le();
        if protocol_version != L2_PROTOCOL_VERSION {
            return Err(HelloPayloadError::UnsupportedVersion(protocol_version));
        }

        if buf.remaining() < 2 {
            return Err(HelloPayloadError::IncompleteHeader);
        }

        let client_id_len = buf.get_u16_le() as usize;
        if buf.remaining() < client_id_len {
            return Err(HelloPayloadError::IncompleteClientId { expected: client_id_len, available: buf.remaining() });
        }
        let client_id = buf.copy_to_bytes(client_id_len);

        if buf.remaining() < 1 {
            return Err(HelloPayloadError::IncompleteHeader);
        }
        let algorithm = buf.get_u8();

        if buf.remaining() < 2 {
            return Err(HelloPayloadError::IncompleteHeader);
        }
        let auth_data_len = buf.get_u16_le() as usize;
        if buf.remaining() < auth_data_len {
            return Err(HelloPayloadError::IncompleteAuthData { expected: auth_data_len, available: buf.remaining() });
        }
        let auth_data = buf.copy_to_bytes(auth_data_len);

        if buf.remaining() < 4 {
            return Err(HelloPayloadError::IncompleteHeader);
        }
        let capabilities = buf.get_u32_le();

        Ok(Self {
            protocol_version,
            client_id,
            algorithm,
            auth_data,
            capabilities,
        })
    }
}
