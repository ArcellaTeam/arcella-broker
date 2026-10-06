// arcella-broker/src/test_utils.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::Bytes;

use super::{Message, TransferMode};

pub fn dummy_in_only_message(msg_type: Bytes, address: Bytes, payload: Bytes) -> Message {
    Message::new(
        TransferMode::InOnly,
        [0u8; 32],
        [1u8; 16],
        [0u8; 4],
        0,
        64,
        msg_type,
        address,
        Bytes::new(),
        payload,
    ).unwrap()
}

pub fn dummy_in_out_message(msg_type: Bytes, address: Bytes, reply_to: Bytes, payload: Bytes) -> Message {
    Message::new(
        TransferMode::InOut,
        [0u8; 32],
        [1u8; 16],
        [0u8; 4],
        0,
        64,
        msg_type,
        address,
        reply_to, // <-- For InOut, there must be a valid address here
        payload,
    ).unwrap()
}
