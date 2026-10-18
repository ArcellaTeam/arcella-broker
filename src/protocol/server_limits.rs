// arcella-broker/src/l2/server_limits.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Server limits advertised during the Welcome handshake.
//!
//! These limits define the resource boundaries for a single client session,
//! including frame sizes, credit allocations, and session lifetime.

use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

pub const DEFAULT_MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024; // 16 MB
pub const DEFAULT_MAX_SESSIONS_PER_CLIENT: u32 = 100;
pub const DEFAULT_MAX_ADDRESSES_PER_SESSION: u32 = 1000;
pub const DEFAULT_HOT_CREDIT: u32 = 50;
pub const DEFAULT_COLD_CREDIT: u32 = 10;
pub const DEFAULT_CELL_TTL_MS: u32 = 50;
pub const DEFAULT_POOL_SIZE: u32 = 10;
pub const SESSION_TTL_MS: u32 = 3_600_000; // 1 hour

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ServerLimitsError {
    #[error("Insufficient data to read ServerLimits (expected 32 bytes)")]
    IncompleteData,
}

/// Server limits advertised to the client during the Welcome handshake.
///
/// All fields are `u32` and encoded in Little-Endian. The total encoded size
/// is fixed at 32 bytes (8 fields × 4 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerLimits {
    /// Maximum payload size for a single frame (default: 16 MB).
    pub max_frame_size: u32,
    /// Maximum number of concurrent sessions per client.
    pub max_sessions_per_client: u32,
    /// Maximum number of addresses per session.
    pub max_addresses_per_session: u32,
    /// Hot credit: max in-flight frames per free address (pipelining).
    pub hot_credit: u32,
    /// Cold credit: max buffer cells for blocked addresses (backpressure).
    pub cold_credit: u32,
    /// Time-to-live for a buffer cell in milliseconds.
    pub cell_ttl_ms: u32,
    /// Total number of buffer cells (= cold_credit).
    pub buffer_pool_size: u32,
    /// Session lifetime in milliseconds (default: 1 hour).
    pub session_ttl_ms: u32,
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_sessions_per_client: DEFAULT_MAX_SESSIONS_PER_CLIENT,
            max_addresses_per_session: DEFAULT_MAX_ADDRESSES_PER_SESSION,
            hot_credit: DEFAULT_HOT_CREDIT,
            cold_credit: DEFAULT_COLD_CREDIT,
            cell_ttl_ms: DEFAULT_CELL_TTL_MS,
            buffer_pool_size: DEFAULT_POOL_SIZE,
            session_ttl_ms: SESSION_TTL_MS,
        }
    }
}

impl ServerLimits {
    /// Fixed encoded size: 8 fields × 4 bytes = 32 bytes.
    pub const ENCODED_SIZE: usize = 32;

    /// Encodes the limits into the destination buffer (Little-Endian).
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_u32_le(self.max_frame_size);
        dst.put_u32_le(self.max_sessions_per_client);
        dst.put_u32_le(self.max_addresses_per_session);
        dst.put_u32_le(self.hot_credit);
        dst.put_u32_le(self.cold_credit);
        dst.put_u32_le(self.cell_ttl_ms);
        dst.put_u32_le(self.buffer_pool_size);
        dst.put_u32_le(self.session_ttl_ms);
    }

    /// Decodes the limits from the source buffer (Little-Endian).
    pub fn decode(src: &mut impl Buf) -> Result<Self, ServerLimitsError> {
        if src.remaining() < Self::ENCODED_SIZE {
            return Err(ServerLimitsError::IncompleteData);
        }
        Ok(Self {
            max_frame_size: src.get_u32_le(),
            max_sessions_per_client: src.get_u32_le(),
            max_addresses_per_session: src.get_u32_le(),
            hot_credit: src.get_u32_le(),
            cold_credit: src.get_u32_le(),
            cell_ttl_ms: src.get_u32_le(),
            buffer_pool_size: src.get_u32_le(),
            session_ttl_ms: src.get_u32_le(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits() {
        let limits = ServerLimits::default();
        assert_eq!(limits.max_frame_size, 16 * 1024 * 1024);
        assert_eq!(limits.hot_credit, 50);
        assert_eq!(limits.cold_credit, 10);
        assert_eq!(limits.session_ttl_ms, 3_600_000);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = ServerLimits {
            max_frame_size: 8 * 1024 * 1024,
            max_sessions_per_client: 50,
            max_addresses_per_session: 500,
            hot_credit: 100,
            cold_credit: 20,
            cell_ttl_ms: 100,
            buffer_pool_size: 20,
            session_ttl_ms: 7_200_000,
        };

        let mut buf = BytesMut::new();
        original.encode(&mut buf);
        assert_eq!(buf.len(), ServerLimits::ENCODED_SIZE);

        let decoded = ServerLimits::decode(&mut buf.freeze()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_decode_insufficient_data() {
        let buf = bytes::Bytes::from_static(&[0u8; 16]); // Only 16 bytes, need 32
        let mut src = buf;
        let result = ServerLimits::decode(&mut src);
        assert!(matches!(result, Err(ServerLimitsError::IncompleteData)));
    }
}
