// arcella-broker/src/protocol/payloads/data.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Data payload (0xfe) — Bidirectional
//!
//! This module implements the encoding and decoding of the L2 Data frame payload,
//! which is the primary carrier of L3 messages within the Arcella protocol.
//!
//! # Architectural Split: Encoder vs Decoder
//!
//! The L2 protocol has an inherent asymmetry between the send and receive paths,
//! which is reflected in the two distinct types provided by this module:
//!
//! ## 1. [`DataPayload`] — Encoder (Sender Side)
//!
//! Used by the L2 sender (e.g., `IPCEndpoint::send_data`) to construct the
//! payload of a Data frame. It holds structured data:
//! - `session_sequence`: the monotonic frame number for ordering/acknowledgment.
//! - `address`: the destination address (extracted from L3 messages by L2 sender).
//! - `messages`: a `Vec<Message>` containing one or more L3 messages to be
//!   aggregated (coalesced) into a single frame for efficiency.
//!
//! The `encode()` method serializes this structured data into the binary format
//! expected by the L2 protocol.
//!
//! ## 2. [`ParsedDataPayload`] — Decoder (Receiver Side)
//!
//! Used by the L2 receiver to parse an incoming Data frame payload. It holds:
//! - `session_sequence`: extracted for the credit system and acknowledgment.
//! - `address`: extracted for the credit system (hot/cold path decision).
//! - `messages_raw`: the remaining bytes containing the serialized L3 messages,
//!   passed **as-is** (zero-copy) to the Ingress Bridge for full deserialization.
//!
//! The `decode()` method uses `Bytes::slice()` to extract `address` and
//! `messages_raw` without copying data, ensuring high throughput for large frames.
//!
//! # Why Two Types Instead of One?
//!
//! A single type with both `messages: Vec<Message>` and `messages_raw: Bytes`
//! would be semantically confusing and inefficient:
//! - On the send path, we need `Vec<Message>` to serialize, but we don't have
//!   `messages_raw` yet (it doesn't exist until after encoding).
//! - On the receive path, we have `messages_raw` (zero-copy bytes), but we
//!   deliberately do NOT deserialize into `Vec<Message>` at the L2 level
//!   (this is the responsibility of the Ingress Bridge).
//!
//! By separating the types, we make the code self-documenting and enforce
//! the correct usage patterns at compile time.
//!
//! # Payload Format (Binary)
//!
//! ```text
//! ┌────────────────────┬──────────────────┬─────────────────────┬──────────────────┬──────────────────┐
//! │ session_sequence   │ address_length   │ address             │ message_count    │ messages         │
//! │ (u64 LE, 8 bytes)  │ (u16 LE, 2 bytes)│ ([u8; N])           │ (u16 LE, 2 bytes)│ ([Message; M])   │
//! └────────────────────┴──────────────────┴─────────────────────┴──────────────────┴──────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use arcella_broker::protocol::payloads::{DataPayload, ParsedDataPayload};
//! use arcella_broker::protocol::Message;
//! use bytes::Bytes;
//!
//! // === Sender Side ===
//! let msg = create_message("test.msg", Bytes::from("worker:1"), Bytes::from("hello"));
//! let payload = DataPayload::new(
//!     42,                                    // session_sequence
//!     Bytes::from("worker:1"),               // address
//!     vec![msg],                             // messages
//! );
//! let encoded_bytes = payload.encode()?;
//!
//! // === Receiver Side ===
//! let parsed = ParsedDataPayload::decode(&encoded_bytes)?;
//! assert_eq!(parsed.session_sequence, 42);
//! assert_eq!(parsed.address, Bytes::from("worker:1"));
//! // parsed.messages_raw is passed to the Ingress Bridge for deserialization
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use super::super::{
    Message,
    MAX_ADDRESS_LEN,
};
use super::PayloadError;

/// Errors specific to parsing/encoding the payload of a Data frame (0xfe).
///
/// These errors are automatically convertible to [`PayloadError`] via the
/// `From` trait, allowing seamless use of the `?` operator in higher-level code.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DataPayloadError {
    /// Not enough data to read the Data payload header (session_sequence).
    #[error("Not enough data to read the Data payload header")]
    IncompleteHeader,

    /// The address length exceeds the maximum allowed by the protocol.
    #[error("Address length ({0}) exceeds the limit {MAX_ADDRESS_LEN}")]
    AddressTooLong(usize),

    /// Not enough data to read the address bytes.
    #[error("Not enough data to read the address")]
    IncompleteAddress,

    /// Not enough data to read the message count field.
    #[error("Not enough data to read the message count")]
    IncompleteMessageCount,

    /// Not enough data to read the declared number of messages.
    #[error("Not enough data to read messages: expected {expected}, available {available}")]
    IncompleteMessages { expected: usize, available: usize },
}

// =========================================================
// 1. ENCODER: DataPayload (Sender Side)
// =========================================================

/// Data payload for the **sender** side of the L2 protocol.
///
/// This structure holds the structured data needed to construct a Data frame
/// payload. It is used by the L2 sender (e.g., `IPCEndpoint::send_data`) to
/// serialize one or more L3 messages into the binary format expected by the
/// L2 protocol.
///
/// # Fields
///
/// * `session_sequence` — Monotonic frame number within the session, used for
///   ordering, acknowledgment, and the credit system.
/// * `address` — Destination address (extracted by the L2 sender from the L3
///   messages). Used by the L2 credit system for routing and flow control.
/// * `messages` — One or more L3 `Message` instances to be aggregated
///   (coalesced) into a single Data frame for efficiency.
///
/// # Usage
///
/// ```rust,ignore
/// let payload = DataPayload::new(
///     session.next_sequence(),
///     Bytes::from("worker:1"),
///     vec![msg1, msg2],
/// );
/// let encoded_bytes = payload.encode()?;
/// let frame = Frame::data(encoded_bytes);
/// ```
#[derive(Debug, Clone)]
pub struct DataPayload {
    /// Monotonic frame number within the session.
    pub session_sequence: u64,
    /// Destination address (extracted by the L2 sender from Message).
    pub address: Bytes,
	/// L3 messages to be aggregated into a single Data frame.
    pub messages: Vec<Message>, 
}

impl DataPayload {
    /// Creates a new `DataPayload` for sending.
    ///
    /// # Arguments
    ///
    /// * `session_sequence` — Monotonic frame number within the session.
    /// * `address` — Destination address (extracted from L3 messages by L2 sender).
    /// * `messages` — Slice of L3 messages to be aggregated (coalesced).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let payload = DataPayload::new(42, Bytes::from("worker:1"), vec![msg]);
    /// ```
    pub fn new(session_sequence: u64, address: Bytes, messages: Vec<Message>) -> Self {
        Self { 
            session_sequence, 
            address, 
            messages 
        }
    }

    /// Encodes the `DataPayload` into the binary format expected by the L2 protocol.
    ///
    /// # Binary Format
    ///
    /// ```text
    /// - session_sequence: u64 LE
    /// - address_length: u16 LE
    /// - address: [u8; address_length]
    /// - message_count: u16 LE
    /// - messages: [Message; message_count]
    /// ```
    ///
    /// # Returns
    ///
    /// `Bytes` with the encoded payload, ready to be inserted into a Frame.
    ///
    /// # Errors
    ///
    /// Returns [`PayloadError::Data`] wrapping a [`DataPayloadError::AddressTooLong`]
    /// if the address exceeds the maximum allowed length.
    ///
    /// # Performance
    ///
    /// The method pre-computes the exact size of the output buffer to avoid
    /// reallocations during encoding. This is critical for the hot path.
    pub fn encode(&self) -> Result<Bytes, PayloadError> {
        if self.address.len() > MAX_ADDRESS_LEN {
            return Err(DataPayloadError::AddressTooLong(self.address.len()).into());
        }

        // Precompute the exact size for a single allocation
        let mut estimated_size = 8 // session_sequence (u64)
            + 2 // address_length (u16)
            + self.address.len()
            + 2; // message_count (u16)

        for msg in &self.messages {
            estimated_size += msg.size();
        }

        let mut buf = BytesMut::with_capacity(estimated_size);

        // 1. session_sequence (u64 LE)
        buf.put_u64_le(self.session_sequence);

        // 2. address_length (u16 LE) + address bytes
        buf.put_u16_le(self.address.len() as u16);
        buf.put_slice(&self.address);
												   

        // 3. message_count (u16 LE)
        let message_count = self.messages.len() as u16;
        buf.put_u16_le(message_count);

        // 4. Serialize all messages
        for msg in &self.messages {
            msg.encode(&mut buf);
        }

        Ok(buf.freeze())
    }
}

// =============================================================================
// 2. DECODER: ParsedDataPayload (Receiver Side)
// =============================================================================

/// Data payload for the **receiver** side of the L2 protocol.
///
/// This structure holds the result of parsing an incoming Data frame payload.
/// It separates L2-level metadata (needed for the credit system and
/// acknowledgment) from L3-level data (passed to the Ingress Bridge).
///
/// # Zero-Copy Guarantee
///
/// The `address` and `messages_raw` fields are extracted using `Bytes::slice()`,
/// which creates new `Bytes` instances that share memory with the original
/// payload buffer. No data is copied; only reference counts are incremented.
/// This is critical for high-throughput scenarios where frames may be large
/// (up to 16 MB).
///
/// # Fields
///
/// * `session_sequence` — Monotonic frame number within the session, used for
///   the credit system and acknowledgment.
/// * `address` — Destination address, extracted by L2 for the credit system
///   (hot/cold path decision).
/// * `messages_raw` — The remaining bytes containing the serialized L3 messages
///   (including the `message_count` field). Passed **as-is** to the Ingress
///   Bridge for full deserialization.
///
/// # Usage
///
/// ```rust,ignore
/// let parsed = ParsedDataPayload::decode(&frame.payload)?;
///
/// // L2 uses session_sequence and address for the credit system
/// if credit_manager.has_hot_credit(&parsed.address) {
///     // Hot path: pass to Ingress Bridge immediately
///     ingress_bridge.process(parsed.address, parsed.messages_raw);
///     session.send_ack(parsed.session_sequence);
/// } else {
///     // Cold path: buffer or drop
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDataPayload {
    /// Sequence number of the frame within the session (for the Ack/Drop/Credit system).
    pub session_sequence: u64,
    /// Destination address (extracted by L2 for the credit system).
    pub address: Bytes,
    /// "Raw" bytes containing message_count + serialized Messages.
    /// The Ingress Bridge must deserialize them into `Vec<Message>`.
    pub messages_raw: Bytes, 
} 

impl ParsedDataPayload {
    /// Decodes the payload of a Data frame, separating L2 metadata from L3 data.
    ///
    /// This function implements the requirement of specification section 10.2:
    /// L2 extracts `address` for the credit system, and `messages_raw`
    /// is passed to the Ingress Bridge for full deserialization.
    ///
    /// # Arguments
    ///
    /// * `payload` — Raw bytes of the Data frame (0xfe) payload.
    ///
    /// # Returns
    ///
    /// `ParsedDataPayload` with L2 metadata and raw bytes for the Ingress Bridge.
    ///
    /// # Errors
    ///
    /// Returns [`PayloadError::Data`] wrapping a [`DataPayloadError`] if the
    /// payload is truncated or malformed.
    ///
    /// # Zero-Copy Guarantee
    ///
    /// The `address` and `messages_raw` fields are extracted using `Bytes::slice()`,
    /// which creates new `Bytes` instances that share memory with the original
    /// payload buffer. No data is copied.
    pub fn decode(payload: &Bytes) -> Result<Self, PayloadError> {
        let mut buf = payload.as_ref();

        // 1. Read session_sequence (u64 LE, 8 bytes)
        if buf.remaining() < 8 {
            return Err(DataPayloadError::IncompleteHeader.into());
        }
        let session_sequence = buf.get_u64_le();

        // 2. Read address_length (u16 LE) and address bytes
        if buf.remaining() < 2 {
            return Err(DataPayloadError::IncompleteAddress.into());
        }
        let address_len = buf.get_u16_le() as usize;
        if address_len > MAX_ADDRESS_LEN {
            return Err(DataPayloadError::AddressTooLong(address_len).into());
        }
        if buf.remaining() < address_len {
            return Err(DataPayloadError::IncompleteAddress.into());
        }
        
        // Zero-copy address extraction using Bytes::slice()
        let address_start = payload.len() - buf.remaining();
        let address = payload.slice(address_start..address_start + address_len);
        buf.advance(address_len);

        // 3. Basic validation for message_count presence (u16 LE, 2 bytes)
        let messages_raw_len = buf.remaining();
        if messages_raw_len < 2 {
            return Err(DataPayloadError::IncompleteMessageCount.into());
        }

        // 4. The remaining bytes (including message_count + serialized messages)
        let messages_raw_start = payload.len() - messages_raw_len;
        let messages_raw = payload.slice(messages_raw_start..);

        Ok(Self {
            session_sequence,
            address,
            messages_raw,
        })
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::test_utils;

    // -------------------------------------------------------------------------
    // Roundtrip Tests (encode -> decode)
    // -------------------------------------------------------------------------

    #[test]
    fn test_encode_decode_data_payload_single_message() {
        let session_seq = 42_u64;
        let address = Bytes::from("core:worker:1");
        
        let msg = test_utils::dummy_in_only_message(
            Bytes::from("test.type"), 
            Bytes::from("core:worker:1"),
            Bytes::from("hello"),
        );

        let payload = DataPayload::new(session_seq, address.clone(), vec![msg.clone()]);
        let encoded = payload.encode().unwrap();
        let parsed = ParsedDataPayload::decode(&encoded).unwrap();

        assert_eq!(parsed.session_sequence, session_seq);
        assert_eq!(parsed.address, address);
        
        // Verify that the Ingress Bridge can successfully decode messages_raw
        let mut buf = parsed.messages_raw.as_ref();
        let msg_count = buf.get_u16_le();
        assert_eq!(msg_count, 1);
        
        let decoded_msg = Message::decode(&mut buf).unwrap();
        assert_eq!(decoded_msg, msg);
        assert_eq!(buf.remaining(), 0);
    }

    #[test]
    fn test_encode_decode_data_payload_multiple_messages() {
        let session_seq = 100_u64;
        let address = Bytes::from("batch:queue");

        let msg1 = test_utils::dummy_in_only_message(
            Bytes::from("test.1"), 
            Bytes::from("batch:queue"),
            Bytes::from("data1"),
        );
        let msg2 = test_utils::dummy_in_only_message(
            Bytes::from("test.2"), 
            Bytes::from("batch:queue"),
            Bytes::from("data2"),
        );
        
        let payload = DataPayload::new(
            session_seq,
            address.clone(),
            vec![msg1.clone(), msg2.clone()],
        );
        let encoded = payload.encode().unwrap();
        let parsed = ParsedDataPayload::decode(&encoded).unwrap();

        assert_eq!(parsed.session_sequence, session_seq);
        assert_eq!(parsed.address, address);

        let mut buf = parsed.messages_raw.as_ref();
        let msg_count = buf.get_u16_le();
        assert_eq!(msg_count, 2);
        
        let decoded_msg1 = Message::decode(&mut buf).unwrap();
        let decoded_msg2 = Message::decode(&mut buf).unwrap();
        
        assert_eq!(decoded_msg1, msg1);
        assert_eq!(decoded_msg2, msg2);
        assert_eq!(buf.remaining(), 0);
    }

    #[test]
    fn test_zero_messages() {
        let session_seq = 5_u64;
        let address = Bytes::from("empty:queue");

        let payload = DataPayload::new(session_seq, address.clone(), vec![]);
        let encoded = payload.encode().unwrap();
        let parsed = ParsedDataPayload::decode(&encoded).unwrap();

        assert_eq!(parsed.session_sequence, session_seq);
        assert_eq!(parsed.address, address);

        let mut msg_buf = parsed.messages_raw.as_ref();
        let count = msg_buf.get_u16_le();
        assert_eq!(count, 0);
        assert_eq!(msg_buf.remaining(), 0);
    }

    // -------------------------------------------------------------------------
    // Zero-Copy Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_zero_copy_address_and_messages() {
        let session_seq = 1_u64;
        let address = Bytes::from("test:addr");

        let msg = test_utils::dummy_in_only_message(
            Bytes::from("test"), 
            Bytes::from("test:addr"),
            Bytes::from("data"),
        );

        let payload = DataPayload::new(session_seq, address, vec![msg]);
        let encoded = payload.encode().unwrap();
        
        let original_ptr: usize = encoded.as_ptr() as usize;
        let original_len: usize = encoded.len();

        let parsed = ParsedDataPayload::decode(&encoded).unwrap();

        let address_ptr: usize = parsed.address.as_ptr() as usize;
        let messages_ptr: usize = parsed.messages_raw.as_ptr() as usize;

        // The address must be a slice of the original payload (zero-copy)
        assert!(address_ptr >= original_ptr, 
            "address pointer {} is before original buffer {}", address_ptr, original_ptr);
        assert!(address_ptr < original_ptr + original_len,
            "address pointer {} is beyond original buffer end {}", address_ptr, original_ptr + original_len);

        // messages_raw must be a slice of the original payload (zero-copy)
        assert!(messages_ptr >= original_ptr,
            "messages pointer {} is before original buffer {}", messages_ptr, original_ptr);
        assert!(messages_ptr < original_ptr + original_len,
            "messages pointer {} is beyond original buffer end {}", messages_ptr, original_ptr + original_len);
    }

    // -------------------------------------------------------------------------
    // Error Tests (Encoding)
    // -------------------------------------------------------------------------

    #[test]
    fn test_address_too_long_encode() {
        let address = Bytes::from(vec![b'a'; MAX_ADDRESS_LEN + 1]);

        let msg = test_utils::dummy_in_only_message(
            Bytes::from("t"), 
            Bytes::from("x"),
            Bytes::from("y"),
        );

        let payload = DataPayload::new(1, address, vec![msg]);
        let err = payload.encode().unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Data(DataPayloadError::AddressTooLong(_))
        ));
    }

    // -------------------------------------------------------------------------
    // Error Tests (Decoding)
    // -------------------------------------------------------------------------

    #[test]
    fn test_incomplete_header() {
        let payload = Bytes::from(vec![0u8; 7]); // At least 8 bytes are needed for session_sequence

        let err = ParsedDataPayload::decode(&payload).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Data(DataPayloadError::IncompleteHeader)
        ));
    }

    #[test]
    fn test_payload_truncated_before_message_count() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1); // session_sequence
        buf.put_u16_le(5); // address_length
        buf.put_slice(b"test:"); // address
        // message_count is missing!

        let payload = buf.freeze();
        let err = ParsedDataPayload::decode(&payload).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Data(DataPayloadError::IncompleteMessageCount)
        ));
    }

    #[test]
    fn test_address_length_mismatch() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1);
        buf.put_u16_le(1000); // Declare 1000 bytes of address
        buf.put_slice(b"short"); // But only put 5

        let payload = buf.freeze();
        let err = ParsedDataPayload::decode(&payload).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Data(DataPayloadError::IncompleteAddress)
        ));
    }

    #[test]
    fn test_address_length_exceeds_limit_in_decode() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1);
        buf.put_u16_le((MAX_ADDRESS_LEN + 1) as u16);
        buf.put_slice(&vec![b'a'; MAX_ADDRESS_LEN + 1]);

        let payload = buf.freeze();
        let err = ParsedDataPayload::decode(&payload).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Data(DataPayloadError::AddressTooLong(_))
        ));
    }

    // -------------------------------------------------------------------------
    // Message Count Mismatch Tests (Ingress Bridge Responsibility)
    // -------------------------------------------------------------------------

    /// Test: message_count declares 10 messages, but there is only enough data for 1.
    /// The Ingress Bridge, when attempting to decode the 10th message, must get an error.
    #[test]
    fn test_message_count_mismatch_too_many_declared() {
        let session_seq = 1_u64;
        let address = Bytes::from("test:addr");

        let msg = test_utils::dummy_in_only_message(
            Bytes::from("test"),
            Bytes::from("test:addr"),
            Bytes::from("data"),
        );

        let mut buf = BytesMut::new();
        buf.put_u64_le(session_seq);
        buf.put_u16_le(address.len() as u16);
        buf.put_slice(&address);
        buf.put_u16_le(10); // Declare 10 messages
        msg.encode(&mut buf); // But only put 1

        let payload = buf.freeze();
        let parsed = ParsedDataPayload::decode(&payload).unwrap();

        assert_eq!(parsed.session_sequence, session_seq);
        assert_eq!(parsed.address, address);

        // Now the Ingress Bridge tries to decode 10 messages
        let mut msg_buf = parsed.messages_raw.as_ref();
        let declared_count = msg_buf.get_u16_le();
        assert_eq!(declared_count, 10);

        // The first message decodes successfully
        let first = Message::decode(&mut msg_buf);
        assert!(first.is_ok());

        // The second message is an error, there is no more data
        let second = Message::decode(&mut msg_buf);
        assert!(second.is_err());
    }

    /// Test: message_count declares 1, but there is enough data for 2 messages.
    /// The Ingress Bridge must stop after the first (according to the declared count).
    #[test]
    fn test_message_count_mismatch_too_few_declared() {
        let session_seq = 2_u64;
        let address = Bytes::from("test:addr");

        let msg1 = test_utils::dummy_in_only_message(
            Bytes::from("test1"),
            Bytes::from("test:addr"),
            Bytes::from("data1"),
        );
        let msg2 = test_utils::dummy_in_only_message(
            Bytes::from("test2"),
            Bytes::from("test:addr"),
            Bytes::from("data2"),
        );

        let mut buf = BytesMut::new();
        buf.put_u64_le(session_seq);
        buf.put_u16_le(address.len() as u16);
        buf.put_slice(&address);
        buf.put_u16_le(1); // Declare only 1 message
        msg1.encode(&mut buf);
        msg2.encode(&mut buf); // But put 2

        let payload = buf.freeze();
        let parsed = ParsedDataPayload::decode(&payload).unwrap();

        let mut msg_buf = parsed.messages_raw.as_ref();
        let declared_count = msg_buf.get_u16_le();
        assert_eq!(declared_count, 1);

        // The Ingress Bridge decodes exactly 1 message (according to the declared count)
        let first = Message::decode(&mut msg_buf).unwrap();
        assert_eq!(first.msg_type.as_ref(), b"test1");

        // The remainder of the buffer contains the "extra" second message — this is data for the next frame
        // or a protocol error, but the L2 parser has done its job correctly
        assert!(msg_buf.remaining() > 0);
    }
}
