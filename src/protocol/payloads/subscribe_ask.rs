// arcella-broker/src/protocol/payloads/subscribe_ask.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{
    SUB_STATUS_SUCCESS,
    PayloadError,
    SubscribePayloadError,
};

/// SubscribeAck (0x22), Server → Client (ответ на Subscribe и Unsubscribe)
/// wire: request_id u64 | status u8 | msg_len u16 | msg
#[derive(Debug, Clone)]
pub struct SubscribeAckPayload {
    pub request_id: u64,
    pub status: u8,
    pub error_message: Bytes,
}

impl SubscribeAckPayload {
    pub fn success(request_id: u64) -> Self {
        Self { request_id, status: SUB_STATUS_SUCCESS, error_message: Bytes::new() }
    }

    pub fn failure(request_id: u64, status: u8, msg: &str) -> Self {
        Self { request_id, status, error_message: Bytes::from(msg.to_owned()) }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(11 + self.error_message.len());
        buf.put_u64_le(self.request_id);
        buf.put_u8(self.status);
        buf.put_u16_le(self.error_message.len() as u16);
        buf.put_slice(&self.error_message);
        buf.freeze()
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, PayloadError> {
        if buf.remaining() < 11 {
            return Err(SubscribePayloadError::IncompleteHeader.into());
        }
        let request_id = buf.get_u64_le();
        let status = buf.get_u8();
        let len = buf.get_u16_le() as usize;
        if buf.remaining() < len {
            return Err(SubscribePayloadError::IncompleteAddress.into());
        }
        Ok(Self { request_id, status, error_message: buf.copy_to_bytes(len) })
    }
}
