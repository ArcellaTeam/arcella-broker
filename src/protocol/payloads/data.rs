// arcella-broker/src/protocol/payloads/data.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::protocol::{
    Message,
    MAX_ADDRESS_LEN,
};

/// Errors specific to parsing the payload of a Data frame (0xfe).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DataFrameError {
    #[error("Not enough data to read the Data payload header")]
    IncompleteHeader,

    #[error("Address length ({0}) exceeds the limit {MAX_ADDRESS_LEN}")]
    AddressTooLong(usize),

    #[error("Not enough data to read the address")]
    IncompleteAddress,

    #[error("Not enough data to read the message count")]
    IncompleteMessageCount,

    #[error("Not enough data to read messages: expected {expected}, available {available}")]
    IncompleteMessages { expected: usize, available: usize },
}

/// Result of parsing the payload of a Data frame.
/// 
/// Returns L2-level metadata (session_sequence, address) and the "raw" message bytes
/// for the Ingress Bridge. L2 does not deserialize Message; this task is performed by the Ingress Bridge.
#[derive(Debug)]
pub struct ParsedDataFrame {
    /// Sequence number of the frame within the session (for the Ack/Drop/Credit system).
    pub session_sequence: u64,
    /// Destination address (extracted by L2 for the credit system).
    pub address: Bytes,
    /// "Raw" bytes containing message_count + serialized Messages.
    /// The Ingress Bridge must deserialize them into Vec<Message>.
    pub messages_raw: Bytes,
}

/// Encodes one or more L3 messages into the payload of a Data frame (0xfe).
///
/// Payload format:
/// ```text
/// - session_sequence: u64 LE
/// - address_length: u16 LE
/// - address: [u8; address_length]
/// - message_count: u16 LE
/// - messages: [Message; message_count]
/// ```
///
/// # Arguments
/// * `session_sequence` - monotonic frame number within the session.
/// * `address` - destination address (extracted by the L2 sender from Message).
/// * `messages` - slice of L3 messages for aggregation (coalescing).
///
/// # Returns
/// `Bytes` with the encoded payload, ready to be inserted into a Frame.
pub fn encode_data_payload(
    session_sequence: u64,
    address: &Bytes,
    messages: &[Message],
) -> Result<Bytes, DataFrameError> {
    if address.len() > MAX_ADDRESS_LEN {
        return Err(DataFrameError::AddressTooLong(address.len()));
    }

    let message_count = messages.len() as u16;
    
    // Precompute the size for a single allocation
    let mut estimated_size = 8 // session_sequence
        + 2 // address_length
        + address.len()
        + 2; // message_count

    for msg in messages {
        estimated_size += msg.size();
    }

    let mut buf = BytesMut::with_capacity(estimated_size);

    // 1. session_sequence (u64 LE)
    buf.put_u64_le(session_sequence);

    // 2. address_length (u16 LE) + address
    buf.put_u16_le(address.len() as u16);
    buf.put_slice(address);

    // 3. message_count (u16 LE)
    buf.put_u16_le(message_count);

    // 4. Serialize all messages
    for msg in messages {
        msg.encode(&mut buf);
    }

    Ok(buf.freeze())
}

/// Decodes the payload of a Data frame, separating L2 metadata from L3 data.
///
/// This function implements the requirement of specification section 10.2:
/// L2 extracts `address` for the credit system, and `messages_raw`
/// is passed to the Ingress Bridge for full deserialization.
///
/// # Arguments
/// * `payload` - raw bytes of the Data frame (0xfe) payload.
///
/// # Returns
/// `ParsedDataFrame` with L2 metadata and raw bytes for the Ingress Bridge.
pub fn decode_data_payload(payload: &Bytes) -> Result<ParsedDataFrame, DataFrameError> {
    let mut buf = payload.as_ref();

    // 1. Read session_sequence
    if buf.remaining() < 8 {
        return Err(DataFrameError::IncompleteHeader);
    }
    let session_sequence = buf.get_u64_le();

    // 2. Read address_length and address
    if buf.remaining() < 2 {
        return Err(DataFrameError::IncompleteAddress);
    }
    let address_len = buf.get_u16_le() as usize;
    if address_len > MAX_ADDRESS_LEN {
        return Err(DataFrameError::AddressTooLong(address_len));
    }
    if buf.remaining() < address_len {
        return Err(DataFrameError::IncompleteAddress);
    }
    
    // Zero-copy address extraction
    let address_start = payload.len() - buf.remaining();
    let address = payload.slice(address_start..address_start + address_len);
    buf.advance(address_len);

    // 3. Read message_count
    let messages_raw_len = buf.remaining();
    if messages_raw_len < 2 {
        return Err(DataFrameError::IncompleteMessageCount);
    }
    let _message_count = buf.get_u16_le(); // The Ingress Bridge will read this again

    // 4. The remaining bytes are the raw messages for the Ingress Bridge
    let messages_raw_start = payload.len() - messages_raw_len;
    let messages_raw = payload.slice(messages_raw_start..);

    Ok(ParsedDataFrame {
        session_sequence,
        address,
        messages_raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::test_utils;

    #[test]
    fn test_encode_decode_data_payload_single_message() {
        let session_seq = 42_u64;
        let address = Bytes::from("core:worker:1");
        
        let msg = test_utils::dummy_in_only_message(
            Bytes::from("test.type"), 
            Bytes::from("core:worker:1"),
            Bytes::from("hello"),
        );

        let payload = encode_data_payload(session_seq, &address, &[msg.clone()]).unwrap();
        let parsed = decode_data_payload(&payload).unwrap();

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
        
        let payload = encode_data_payload(session_seq, &address, &[msg1.clone(), msg2.clone()]).unwrap();
        let parsed = decode_data_payload(&payload).unwrap();

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
    fn test_zero_copy_address_and_messages() {
        let session_seq = 1_u64;
        let address = Bytes::from("test:addr");

        let msg = test_utils::dummy_in_only_message(
            Bytes::from("test"), 
            Bytes::from("test:addr"),
            Bytes::from("data"),
        );
        
        let payload = encode_data_payload(session_seq, &address, &[msg]).unwrap();
        let original_ptr = payload.as_ptr() as usize;

        let parsed = decode_data_payload(&payload).unwrap();

        // The address must be a slice of the original payload
        assert!(parsed.address.as_ptr() as usize >= original_ptr);
        
        // messages_raw must be a slice of the original payload
        assert!(parsed.messages_raw.as_ptr() as usize >= original_ptr);
    }

    #[test]
    fn test_address_too_long() {
        let address = Bytes::from(vec![b'a'; MAX_ADDRESS_LEN + 1]);

        let msg = test_utils::dummy_in_only_message(
            Bytes::from("t"), 
            Bytes::from("x"),
            Bytes::from("y"),
        );
        
        let err = encode_data_payload(1, &address, &[msg]).unwrap_err();
        assert_eq!(err, DataFrameError::AddressTooLong(MAX_ADDRESS_LEN + 1));
    }

    #[test]
    fn test_incomplete_header() {
        let payload = Bytes::from(vec![0u8; 7]); // At least 8 bytes are needed for session_sequence
        
        let err = decode_data_payload(&payload).unwrap_err();
        assert_eq!(err, DataFrameError::IncompleteHeader);
    }
}
