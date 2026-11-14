// arcella-broker/src/protocol/payloads/subscribe.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use super::{
    super::MAX_ADDRESS_LEN,
    PayloadError
};

pub const SUB_STATUS_SUCCESS: u8 = 0;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubscribePayloadError {
    #[error("Incomplete subscribe header")]
    IncompleteHeader,

    #[error("Incomplete address")]
    IncompleteAddress,

    #[error("Address too long")]
    AddressTooLong,
}

/// Subscribe (0x20), Client → Server
/// wire: request_id u64 | policy u8 | capacity u32 | addr_len u16 | addr
#[derive(Debug, Clone)]
pub struct SubscribePayload {
    pub request_id: u64,
    pub routing_policy: u8,
    pub channel_capacity: u32,
    pub address: Bytes,
}

impl SubscribePayload {
    pub fn encode(&self) -> Result<Bytes, PayloadError> {
        if self.address.len() > MAX_ADDRESS_LEN {
            return Err(SubscribePayloadError::AddressTooLong.into());
        }
        let mut buf = BytesMut::with_capacity(15 + self.address.len());
        buf.put_u64_le(self.request_id);
        buf.put_u8(self.routing_policy);
        buf.put_u32_le(self.channel_capacity);
        buf.put_u16_le(self.address.len() as u16);
        buf.put_slice(&self.address);
        Ok(buf.freeze())
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, PayloadError> {
        if buf.remaining() < 15 {
            return Err(SubscribePayloadError::IncompleteHeader.into());
        }
        let request_id = buf.get_u64_le();
        let routing_policy = buf.get_u8();
        let channel_capacity = buf.get_u32_le();
        let len = buf.get_u16_le() as usize;
        if len > MAX_ADDRESS_LEN {
            return Err(SubscribePayloadError::AddressTooLong.into());
        }
        if buf.remaining() < len {
            return Err(SubscribePayloadError::IncompleteAddress.into());
        }
        Ok(Self { request_id, routing_policy, channel_capacity, address: buf.copy_to_bytes(len) })
    }
}
