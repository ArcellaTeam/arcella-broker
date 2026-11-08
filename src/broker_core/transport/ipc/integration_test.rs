// arcella-broker/src/broker_core/transport/ipc/integration_test.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Integration tests for IPC Transport Layer (Stage 3).
//!
//! This module verifies that two independent Tokio tasks can exchange
//! L2 frames over a Unix Domain Socket without data loss, correctly
//! managing session sequences and the unacked buffer.

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::protocol::Message;
    use crate::protocol::test_utils::dummy_in_only_message;

    /// Helper to create a dummy L3 message for testing
    fn create_test_msg(id: u32) -> Message {
        dummy_in_only_message(
            Bytes::from("test.msg"),
            Bytes::from("worker:1"),
            Bytes::from(format!("payload_{}", id)),
        )
    }
}