// arcella-broker/src/protocol/payloads/credit_grant.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! CreditGrant payload (0x02) — Bidirectional
//!
//! Used to grant credits to the peer for sending Data frames.
//! Sent immediately after successful authentication to initialize
//! the credit system, and dynamically during the session to replenish
//! credits as frames are acknowledged.
//!
//! # Binary Format
//! ```text
//! - session_sequence: u64 LE      // Sequence number of this control frame
//! - address_length:   u16 LE      // Length of the address field
//! - address:          [u8; N]     // Address prefix for which credits are granted
//! - credit_type:      u8          // 0 = hot, 1 = cold
//! - credit_amount:    u32 LE      // Number of credits being granted
//! ```
//!
//! # Design Notes
//! - CreditGrant frames share the same `session_sequence` counter as Data frames.
//! - They are NOT stored in `UnackedBuffer` (no acknowledgment required).
//! - The `address` field can be empty for global credits or contain a specific prefix.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Maximum allowed address length in a CreditGrant payload (1024 bytes).
/// Matches the L3 Message address limit for consistency.
pub const MAX_CREDIT_GRANT_ADDRESS_LEN: usize = 1024;

/// Errors specific to CreditGrant payload parsing/encoding
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CreditGrantPayloadError {
    #[error("Insufficient data to read CreditGrant header")]
    IncompleteHeader,

    #[error("Insufficient data to read address: expected {expected}, available {available}")]
    IncompleteAddress { expected: usize, available: usize },

    #[error("Address too long: {len} bytes (max {max})")]
    AddressTooLong { len: usize, max: usize },
}

/// CreditGrant payload (frame type 0x02).
///
/// Grants credits to the peer, allowing it to send Data frames
/// for the specified address. This is the core mechanism of the
/// L2 flow control system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreditGrantPayload {
    /// Sequence number of this control frame within the session.
    /// CreditGrant frames share the same monotonic counter as Data frames,
    /// ensuring strict ordering of all frames in the session.
    pub session_sequence: u64,

    /// Address (or prefix) for which credits are granted.
    /// - Empty `Bytes` = global credits (apply to all addresses)
    /// - Non-empty = credits for a specific address prefix
    pub address: Bytes,

    /// Type of credit being granted:
    /// - `CREDIT_TYPE_HOT` (0): for pipelining on free addresses
    /// - `CREDIT_TYPE_COLD` (1): for backpressure buffer cells
    pub credit_type: u8,

    /// Number of credits being granted.
    /// Must be > 0 for the grant to be meaningful.
    pub credit_amount: u32,
}

impl CreditGrantPayload {
    /// Credit type constant for hot credits (pipelining on free addresses).
    pub const CREDIT_TYPE_HOT: u8 = 0;

    /// Credit type constant for cold credits (backpressure buffer cells).
    pub const CREDIT_TYPE_COLD: u8 = 1;

    /// Creates a new CreditGrant payload.
    ///
    /// # Arguments
    /// * `session_sequence` — monotonic frame number within the session
    /// * `address` — address prefix for which credits are granted
    /// * `credit_type` — `CREDIT_TYPE_HOT` or `CREDIT_TYPE_COLD`
    /// * `credit_amount` — number of credits being granted
    pub fn new(
        session_sequence: u64,
        address: Bytes,
        credit_type: u8,
        credit_amount: u32,
    ) -> Self {
        Self {
            session_sequence,
            address,
            credit_type,
            credit_amount,
        }
    }

    /// Creates a hot credit grant for the given address.
    pub fn hot(session_sequence: u64, address: Bytes, amount: u32) -> Self {
        Self::new(session_sequence, address, Self::CREDIT_TYPE_HOT, amount)
    }

    /// Creates a cold credit grant for the given address.
    pub fn cold(session_sequence: u64, address: Bytes, amount: u32) -> Self {
        Self::new(session_sequence, address, Self::CREDIT_TYPE_COLD, amount)
    }

    /// Encodes the CreditGrant payload into the binary wire format.
    ///
    /// # Errors
    /// Returns `CreditGrantPayloadError::AddressTooLong` if the address
    /// exceeds `MAX_CREDIT_GRANT_ADDRESS_LEN` (1024 bytes).
    pub fn encode(&self) -> Result<Bytes, CreditGrantPayloadError> {
        let addr_len = self.address.len();
        if addr_len > MAX_CREDIT_GRANT_ADDRESS_LEN {
            return Err(CreditGrantPayloadError::AddressTooLong {
                len: addr_len,
                max: MAX_CREDIT_GRANT_ADDRESS_LEN,
            });
        }

        // Exact size: 8 (seq) + 2 (addr_len) + addr_len + 1 (type) + 4 (amount)
        let mut buf = BytesMut::with_capacity(8 + 2 + addr_len + 1 + 4);
        buf.put_u64_le(self.session_sequence);
        buf.put_u16_le(addr_len as u16);
        buf.put_slice(&self.address);
        buf.put_u8(self.credit_type);
        buf.put_u32_le(self.credit_amount);
        Ok(buf.freeze())
    }

    /// Decodes a CreditGrant payload from raw bytes.
    ///
    /// # Errors
    /// Returns `CreditGrantPayloadError::IncompleteHeader` or
    /// `CreditGrantPayloadError::IncompleteAddress` if the payload
    /// is truncated or malformed.
    pub fn decode(payload: &Bytes) -> Result<Self, CreditGrantPayloadError> {
        let mut buf = payload.clone();

        // Minimum size: 8 (seq) + 2 (addr_len) + 0 (addr) + 1 (type) + 4 (amount) = 15
        if buf.remaining() < 8 + 2 {
            return Err(CreditGrantPayloadError::IncompleteHeader);
        }

        let session_sequence = buf.get_u64_le();
        let addr_len = buf.get_u16_le() as usize;

        // Check if we have enough data for address + type + amount
        if buf.remaining() < addr_len + 1 + 4 {
            return Err(CreditGrantPayloadError::IncompleteAddress {
                expected: addr_len + 1 + 4,
                available: buf.remaining(),
            });
        }

        let address = buf.copy_to_bytes(addr_len);
        let credit_type = buf.get_u8();
        let credit_amount = buf.get_u32_le();

        Ok(Self {
            session_sequence,
            address,
            credit_type,
            credit_amount,
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip_hot() {
        let original = CreditGrantPayload::hot(
            42,
            Bytes::from("worker:1:instance"),
            50,
        );

        let encoded = original.encode().unwrap();
        let decoded = CreditGrantPayload::decode(&encoded).unwrap();

        assert_eq!(original, decoded);
        assert_eq!(decoded.credit_type, CreditGrantPayload::CREDIT_TYPE_HOT);
        assert_eq!(decoded.credit_amount, 50);
        assert_eq!(decoded.session_sequence, 42);
    }

    #[test]
    fn test_encode_decode_roundtrip_cold() {
        let original = CreditGrantPayload::cold(
            100,
            Bytes::from("arcella:core:service"),
            10,
        );

        let encoded = original.encode().unwrap();
        let decoded = CreditGrantPayload::decode(&encoded).unwrap();

        assert_eq!(original, decoded);
        assert_eq!(decoded.credit_type, CreditGrantPayload::CREDIT_TYPE_COLD);
    }

    #[test]
    fn test_encode_decode_empty_address() {
        let original = CreditGrantPayload::hot(1, Bytes::new(), 100);

        let encoded = original.encode().unwrap();
        let decoded = CreditGrantPayload::decode(&encoded).unwrap();

        assert_eq!(original, decoded);
        assert!(decoded.address.is_empty());
    }

    #[test]
    fn test_address_too_long() {
        let long_address = Bytes::from(vec![b'a'; MAX_CREDIT_GRANT_ADDRESS_LEN + 1]);
        let payload = CreditGrantPayload::hot(1, long_address, 10);

        let result = payload.encode();
        assert!(matches!(
            result,
            Err(CreditGrantPayloadError::AddressTooLong { .. })
        ));
    }

    #[test]
    fn test_decode_incomplete_header() {
        let truncated = Bytes::from(vec![0u8; 5]); // Need at least 10 bytes
        let result = CreditGrantPayload::decode(&truncated);
        assert!(matches!(result, Err(CreditGrantPayloadError::IncompleteHeader)));
    }

    #[test]
    fn test_decode_incomplete_address() {
        // Valid header but truncated address
        let mut buf = BytesMut::with_capacity(20);
        buf.put_u64_le(1);        // session_sequence
        buf.put_u16_le(100);      // address_length = 100
        buf.put_slice(&[0u8; 10]); // but only 10 bytes of address
        let truncated = buf.freeze();

        let result = CreditGrantPayload::decode(&truncated);
        assert!(matches!(
            result,
            Err(CreditGrantPayloadError::IncompleteAddress { .. })
        ));
    }

    #[test]
    fn test_constants() {
        assert_eq!(CreditGrantPayload::CREDIT_TYPE_HOT, 0);
        assert_eq!(CreditGrantPayload::CREDIT_TYPE_COLD, 1);
        assert_eq!(MAX_CREDIT_GRANT_ADDRESS_LEN, 1024);
    }

    #[test]
    fn test_zero_credits() {
        // Zero credits is technically valid (though not useful)
        let payload = CreditGrantPayload::hot(1, Bytes::from("test"), 0);
        let encoded = payload.encode().unwrap();
        let decoded = CreditGrantPayload::decode(&encoded).unwrap();
        assert_eq!(decoded.credit_amount, 0);
    }

    #[test]
    fn test_max_u32_credits() {
        let payload = CreditGrantPayload::hot(1, Bytes::from("test"), u32::MAX);
        let encoded = payload.encode().unwrap();
        let decoded = CreditGrantPayload::decode(&encoded).unwrap();
        assert_eq!(decoded.credit_amount, u32::MAX);
    }
}
