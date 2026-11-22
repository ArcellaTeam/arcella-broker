// arcella-broker/src/protocol/payloads/subscribe.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Subscribe (0x20), Client → Server.
//!
//! # Architectural note (control plane / data plane separation)
//!
//! `Subscribe` **attaches the session as a consumer to an already existing
//! address**. It does NOT create an address. Addresses (with their routing
//! policy and queue capacity) are created, configured and deleted exclusively
//! through the broker control API (`BrokerControl`), never through the client
//! data channel.
//!
//! Consequently this payload carries neither `routing_policy` nor
//! `channel_capacity`: both are properties of the address, fixed at creation
//! time. The effective values are reported back to the client in
//! [`super::SubscribeAckPayload`] for informational purposes.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use super::{
    super::MAX_ADDRESS_LEN,
    PayloadError
};

/// Fixed part of Subscribe: request_id(8) + addr_len(2).
pub const SUBSCRIBE_HEADER_LEN: usize = 10;

/// Error family shared by the subscription payloads
/// (`Subscribe`, `Unsubscribe`, `SubscribeAck`).
///
/// The name is kept for compatibility with existing `PayloadError` wiring;
/// a rename to `SubscriptionPayloadError` can be done as a separate refactor.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubscribePayloadError {
    #[error("Incomplete subscription header")]
    IncompleteHeader,

    #[error("Incomplete address")]
    IncompleteAddress,

    #[error("Address too long")]
    AddressTooLong,

    #[error("Incomplete error message")]
    IncompleteMessage,

    #[error("Error message too long")]
    MessageTooLong,

    #[error("Unknown subscription status code: {0}")]
    InvalidStatus(u8),

    #[error("Unknown routing policy code: {0}")]
    InvalidPolicy(u8),

    #[error("Inconsistent subscription ack fields")]
    InconsistentFields,
}

/// Subscribe (0x20), Client -> Server.
///
/// Wire format (all integers Little-Endian):
/// ```text
/// request_id : u64
/// addr_len   : u16
/// address    : [u8; addr_len]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribePayload {
    /// Client-generated request identifier, echoed in `SubscribeAck`.
    pub request_id: u64,
    /// Name of an existing registered address (exact name or wildcard pattern).
    pub address: Bytes,
}

impl SubscribePayload {
    pub fn new(request_id: u64, address: Bytes) -> Self {
        Self { request_id, address }
    }

    pub fn encode(&self) -> Result<Bytes, PayloadError> {
        if self.address.len() > MAX_ADDRESS_LEN {
            return Err(SubscribePayloadError::AddressTooLong.into());
        }
        let mut buf = BytesMut::with_capacity(SUBSCRIBE_HEADER_LEN + self.address.len());
        buf.put_u64_le(self.request_id);
        buf.put_u16_le(self.address.len() as u16);
        buf.put_slice(&self.address);
        Ok(buf.freeze())
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, PayloadError> {
        if buf.remaining() < SUBSCRIBE_HEADER_LEN {
            return Err(SubscribePayloadError::IncompleteHeader.into());
        }
        let request_id = buf.get_u64_le();
        let len = buf.get_u16_le() as usize;
        if len > MAX_ADDRESS_LEN {
            return Err(SubscribePayloadError::AddressTooLong.into());
        }
        if buf.remaining() < len {
            return Err(SubscribePayloadError::IncompleteAddress.into());
        }
        let address = buf.copy_to_bytes(len);
        Ok(Self { request_id, address })
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    const ADDR: &[u8] = b"arcella:workers:client2";

    #[test]
    fn roundtrip() {
        let p = SubscribePayload::new(42, Bytes::from_static(ADDR));
        let mut bytes = p.encode().unwrap();
        assert_eq!(bytes.len(), SUBSCRIBE_HEADER_LEN + ADDR.len());
        assert_eq!(SubscribePayload::decode(&mut bytes).unwrap(), p);
        assert_eq!(bytes.remaining(), 0);
    }

    #[test]
    fn empty_address_roundtrip() {
        // Codec-level empty address is allowed; semantic validation
        // (validate_address) is performed by the server, not the codec.
        let p = SubscribePayload::new(1, Bytes::new());
        let mut bytes = p.encode().unwrap();
        assert_eq!(SubscribePayload::decode(&mut bytes).unwrap(), p);
    }

    #[test]
    fn max_len_address_roundtrip() {
        let addr = Bytes::from(vec![b'a'; MAX_ADDRESS_LEN]);
        let p = SubscribePayload::new(7, addr);
        let mut bytes = p.encode().unwrap();
        assert_eq!(SubscribePayload::decode(&mut bytes).unwrap(), p);
    }

    #[test]
    fn encode_address_too_long() {
        let p = SubscribePayload::new(1, Bytes::from(vec![b'a'; MAX_ADDRESS_LEN + 1]));
        assert_eq!(
            p.encode().unwrap_err(),
            PayloadError::from(SubscribePayloadError::AddressTooLong)
        );
    }

    #[test]
    fn decode_incomplete_header() {
        let mut bytes = Bytes::from_static(&[0u8; SUBSCRIBE_HEADER_LEN - 1]);
        assert_eq!(
            SubscribePayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::IncompleteHeader)
        );
    }

    #[test]
    fn decode_incomplete_address() {
        let p = SubscribePayload::new(1, Bytes::from_static(ADDR));
        let bytes = p.encode().unwrap();
        let mut truncated = bytes.slice(..bytes.len() - 1);
        assert_eq!(
            SubscribePayload::decode(&mut truncated).unwrap_err(),
            PayloadError::from(SubscribePayloadError::IncompleteAddress)
        );
    }

    #[test]
    fn decode_declared_len_exceeds_max() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1);
        buf.put_u16_le((MAX_ADDRESS_LEN + 1) as u16);
        let mut bytes = buf.freeze();
        assert_eq!(
            SubscribePayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::AddressTooLong)
        );
    }

    #[test]
    fn decode_tolerates_trailing_bytes() {
        // Forward compatibility: extra bytes after the payload are ignored.
        let p = SubscribePayload::new(9, Bytes::from_static(ADDR));
        let mut bytes = BytesMut::from(p.encode().unwrap().as_ref());
        bytes.extend_from_slice(&[0xAA, 0xBB]);
        let mut bytes = bytes.freeze();
        assert_eq!(SubscribePayload::decode(&mut bytes).unwrap(), p);
        assert_eq!(bytes.remaining(), 2);
    }
}
