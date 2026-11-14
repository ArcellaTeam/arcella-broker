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

/// Fixed part of SubscribeAck: request_id(8) + status(1) + queue_capacity(4) + msg_len(2).
pub const ACK_HEADER_LEN: usize = 15;

/// SubscribeAck (0x22), Server -> Client (response to Subscribe and Unsubscribe)
/// wire: request_id u64 | status u8 | queue_capacity u32 | msg_len u16 | msg
///
/// # Queue capacity negotiation
///
/// `Subscribe.capacity` is a **request**, not a command. The server clamps it to
/// its own configured bounds and reports the **granted** value here. The client
/// MUST use `queue_capacity` (not its own requested value) for any local
/// accounting, metrics or watermark logic.
///
/// `queue_capacity == 0` means "no queue is associated with this response":
/// either the operation failed, or it was an `Unsubscribe`.
#[derive(Debug, Clone)]
pub struct SubscribeAckPayload {
    pub request_id: u64,
    pub status: u8,
    pub queue_capacity: u32,
    pub error_message: Bytes,
}

impl SubscribeAckPayload {
    /// Successful response carrying the effective queue capacity.
    pub fn success(request_id: u64, queue_capacity: u32) -> Self {
        Self {
            request_id,
            status: SUB_STATUS_SUCCESS,
            queue_capacity,
            error_message: Bytes::new(),
        }
    }

    /// Successful `Unsubscribe` response — no queue is attached.
    pub fn success_unsubscribed(request_id: u64) -> Self {
        Self::success(request_id, 0)
    }

    pub fn failure(request_id: u64, status: u8, msg: &str) -> Self {
        Self {
            request_id,
            status,
            queue_capacity: 0,
            error_message: Bytes::from(msg.to_owned()),
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(ACK_HEADER_LEN + self.error_message.len());
        buf.put_u64_le(self.request_id);
        buf.put_u8(self.status);
        buf.put_u32_le(self.queue_capacity);
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
        let queue_capacity = buf.get_u32_le();
        let len = buf.get_u16_le() as usize;
        if buf.remaining() < len {
            return Err(SubscribePayloadError::IncompleteAddress.into());
        }
        Ok(Self {
            request_id,
            status,
            queue_capacity,
            error_message: buf.copy_to_bytes(len),
        })
    }
}
