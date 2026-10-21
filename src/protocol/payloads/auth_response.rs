// arcella-broker/src/protocol/payloads/auth_response.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use super::PayloadError;

/// Errors specific to AuthResponse payload parsing
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthResponsePayloadError {
    #[error("Insufficient data to read AuthResponse header")]
    IncompleteHeader,

    #[error("Insufficient data to read client_nonce (expected 32 bytes)")]
    IncompleteNonce,

    #[error("Insufficient data to read response: expected {expected}, available {available}")]
    IncompleteResponse { expected: usize, available: usize },
}

/// AuthResponse payload sent by client to prove identity (0x05)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthResponsePayload {
    pub session_id: u64,
    pub client_nonce: [u8; 32],
    pub response: Bytes,
}

impl AuthResponsePayload {
    /// Encodes the AuthResponse payload into bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8 + 32 + 2 + self.response.len());
        
        buf.put_u64_le(self.session_id);
        buf.put_slice(&self.client_nonce);
        
        let response_len = self.response.len();
        assert!(response_len <= u16::MAX as usize, "response too long");
        buf.put_u16_le(response_len as u16);
        buf.put_slice(&self.response);
        
        buf.freeze()
    }

    /// Decodes the AuthResponse payload from bytes
    pub fn decode(buf: &mut impl Buf) -> Result<Self, PayloadError> {
        if buf.remaining() < 8 {
            return Err(AuthResponsePayloadError::IncompleteHeader.into());
        }
        let session_id = buf.get_u64_le();

        if buf.remaining() < 32 {
            return Err(AuthResponsePayloadError::IncompleteNonce.into());
        }
        let mut client_nonce = [0u8; 32];
        buf.copy_to_slice(&mut client_nonce);

        if buf.remaining() < 2 {
            return Err(AuthResponsePayloadError::IncompleteHeader.into());
        }
        let response_len = buf.get_u16_le() as usize;
        
        if buf.remaining() < response_len {
            return Err(AuthResponsePayloadError::IncompleteResponse { 
                expected: response_len, 
                available: buf.remaining() 
            }.into());
        }
        let response = buf.copy_to_bytes(response_len);

        Ok(Self {
            session_id,
            client_nonce,
            response,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_response_roundtrip() {
        let original = AuthResponsePayload {
            session_id: 12345,
            client_nonce: [0xAA; 32],
            response: Bytes::from("dummy_hmac_for_none_alg"),
        };

        let encoded = original.encode();
        let mut buf = encoded.clone();
        let decoded = AuthResponsePayload::decode(&mut buf).unwrap();

        assert_eq!(original, decoded);
    }
}
