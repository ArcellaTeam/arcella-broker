// arcella-broker/src/protocol/payloads/auth_result.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Errors specific to AuthResult payload parsing
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthResultPayloadError {
    #[error("Insufficient data to read AuthResult header")]
    IncompleteHeader,

    #[error("Insufficient data to read error_message: expected {expected}, available {available}")]
    IncompleteErrorMessage { expected: usize, available: usize },
}

/// AuthResult payload sent by server to indicate authentication outcome (0x06)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthResultPayload {
    pub session_id: u64,
    pub status: u8, // 0 = success, 1 = invalid_response, 2 = session_not_found, 3 = replay_detected, 4 = algorithm_mismatch
    pub error_message: Bytes,
}

impl AuthResultPayload {
    /// Encodes the AuthResult payload into bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8 + 1 + 2 + self.error_message.len());
        
        buf.put_u64_le(self.session_id);
        buf.put_u8(self.status);
        
        let msg_len = self.error_message.len();
        assert!(msg_len <= u16::MAX as usize, "error_message too long");
        buf.put_u16_le(msg_len as u16);
        buf.put_slice(&self.error_message);
        
        buf.freeze()
    }

    /// Decodes the AuthResult payload from bytes
    pub fn decode(buf: &mut impl Buf) -> Result<Self, AuthResultPayloadError> {
        if buf.remaining() < 9 { // 8 (session_id) + 1 (status)
            return Err(AuthResultPayloadError::IncompleteHeader);
        }
        let session_id = buf.get_u64_le();
        let status = buf.get_u8();

        if buf.remaining() < 2 {
            return Err(AuthResultPayloadError::IncompleteHeader);
        }
        let msg_len = buf.get_u16_le() as usize;
        
        if buf.remaining() < msg_len {
            return Err(AuthResultPayloadError::IncompleteErrorMessage { 
                expected: msg_len, 
                available: buf.remaining() 
            });
        }
        let error_message = buf.copy_to_bytes(msg_len);

        Ok(Self {
            session_id,
            status,
            error_message,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_result_success_roundtrip() {
        let original = AuthResultPayload {
            session_id: 12345,
            status: 0, // success
            error_message: Bytes::new(),
        };

        let encoded = original.encode();
        let mut buf = encoded.clone();
        let decoded = AuthResultPayload::decode(&mut buf).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_auth_result_failure_roundtrip() {
        let original = AuthResultPayload {
            session_id: 999,
            status: 2, // session_not_found
            error_message: Bytes::from("Session expired"),
        };

        let encoded = original.encode();
        let mut buf = encoded.clone();
        let decoded = AuthResultPayload::decode(&mut buf).unwrap();

        assert_eq!(original, decoded);
    }
}
