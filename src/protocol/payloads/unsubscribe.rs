// arcella-broker/src/protocol/payloads/unsubscribe.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Unsubscribe (0x21), Client → Server.
//!
//! Detaches the session from a previously subscribed address.
//! The address itself (and its queue) is **not** affected: address lifecycle
//! is owned by the broker control plane. Buffered messages remain in the
//! address queue and will be delivered to the next attached consumer.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{
    super::MAX_ADDRESS_LEN,
    PayloadError,
    SubscribePayloadError,
};

/// Fixed part of Unsubscribe: request_id(8) + addr_len(2).
pub const UNSUBSCRIBE_HEADER_LEN: usize = 10;

/// Unsubscribe (0x21), Client → Server.
///
/// Wire format (all integers Little-Endian):
/// ```text
/// request_id : u64
/// addr_len   : u16
/// address    : [u8; addr_len]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribePayload {
    /// Client-generated request identifier, echoed in `SubscribeAck`.
    pub request_id: u64,

    /// Exact address name previously passed to `Subscribe`.
    pub address: Bytes,
}

impl UnsubscribePayload {
    pub fn new(request_id: u64, address: Bytes) -> Self {
        Self { request_id, address }
    }

    pub fn encode(&self) -> Result<Bytes, PayloadError> {
        if self.address.len() > MAX_ADDRESS_LEN {
            return Err(SubscribePayloadError::AddressTooLong.into());
        }
        let mut buf = BytesMut::with_capacity(UNSUBSCRIBE_HEADER_LEN + self.address.len());
        buf.put_u64_le(self.request_id);
        buf.put_u16_le(self.address.len() as u16);
        buf.put_slice(&self.address);
        Ok(buf.freeze())
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, PayloadError> {
        if buf.remaining() < UNSUBSCRIBE_HEADER_LEN {
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
        let p = UnsubscribePayload::new(77, Bytes::from_static(ADDR));
        let mut bytes = p.encode().unwrap();
        assert_eq!(bytes.len(), UNSUBSCRIBE_HEADER_LEN + ADDR.len());
        assert_eq!(UnsubscribePayload::decode(&mut bytes).unwrap(), p);
        assert_eq!(bytes.remaining(), 0);
    }

    #[test]
    fn encode_address_too_long() {
        let p = UnsubscribePayload::new(1, Bytes::from(vec![b'a'; MAX_ADDRESS_LEN + 1]));
        assert_eq!(
            p.encode().unwrap_err(),
            PayloadError::from(SubscribePayloadError::AddressTooLong)
        );
    }

    #[test]
    fn decode_incomplete_header() {
        let mut bytes = Bytes::from_static(&[0u8; UNSUBSCRIBE_HEADER_LEN - 1]);
        assert_eq!(
            UnsubscribePayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::IncompleteHeader)
        );
    }

    #[test]
    fn decode_incomplete_address() {
        let p = UnsubscribePayload::new(1, Bytes::from_static(ADDR));
        let bytes = p.encode().unwrap();
        let mut truncated = bytes.slice(..bytes.len() - 1);
        assert_eq!(
            UnsubscribePayload::decode(&mut truncated).unwrap_err(),
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
            UnsubscribePayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::AddressTooLong)
        );
    }
}
