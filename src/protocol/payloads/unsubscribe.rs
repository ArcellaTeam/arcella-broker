// arcella-broker/src/protocol/payloads/unsubscribe.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{
    super::MAX_ADDRESS_LEN,
    PayloadError,
    SubscribePayloadError,
};

/// Unsubscribe (0x21), Client → Server
/// wire: request_id u64 | addr_len u16 | addr
#[derive(Debug, Clone)]
pub struct UnsubscribePayload {
    pub request_id: u64,
    pub address: Bytes,
}

impl UnsubscribePayload {
    pub fn encode(&self) -> Result<Bytes, PayloadError> {
        if self.address.len() > MAX_ADDRESS_LEN {
            return Err(SubscribePayloadError::AddressTooLong.into());
        }
        let mut buf = BytesMut::with_capacity(10 + self.address.len());
        buf.put_u64_le(self.request_id);
        buf.put_u16_le(self.address.len() as u16);
        buf.put_slice(&self.address);
        Ok(buf.freeze())
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, PayloadError> {
        if buf.remaining() < 10 {
            return Err(SubscribePayloadError::IncompleteHeader.into());
        }
        let request_id = buf.get_u64_le();
        let len = buf.get_u16_le() as usize;
        if len > MAX_ADDRESS_LEN || buf.remaining() < len {
            return Err(SubscribePayloadError::IncompleteAddress.into());
        }
        Ok(Self { request_id, address: buf.copy_to_bytes(len) })
    }
}
