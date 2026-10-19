// arcella-broker/src/protocol/payloads/welcome.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use super::super::server_limits::ServerLimits;
use super::PayloadError;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WelcomePayloadError {
    #[error("Insufficient data to read Welcome payload")]
    IncompleteData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WelcomePayload {
    pub protocol_version: u16,
    pub session_id: u64,
    pub session_token: [u8; 32],
    pub server_challenge: [u8; 32],
    pub server_capabilities: u32,
    pub limits: ServerLimits,
}

impl WelcomePayload {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(2 + 8 + 32 + 32 + 4 + ServerLimits::ENCODED_SIZE);
        buf.put_u16_le(self.protocol_version);
        buf.put_u64_le(self.session_id);
        buf.put_slice(&self.session_token);
        buf.put_slice(&self.server_challenge);
        buf.put_u32_le(self.server_capabilities);
        self.limits.encode(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, PayloadError> {
        let min_size = 2 + 8 + 32 + 32 + 4 + ServerLimits::ENCODED_SIZE;
        if buf.remaining() < min_size {
            return Err(WelcomePayloadError::IncompleteData.into());
        }
        let protocol_version = buf.get_u16_le();
        let session_id = buf.get_u64_le();
        
        let mut session_token = [0u8; 32];
        buf.copy_to_slice(&mut session_token);
        
        let mut server_challenge = [0u8; 32];
        buf.copy_to_slice(&mut server_challenge);
        
        let server_capabilities = buf.get_u32_le();
        // SAFETY: min_size check above guarantees this will not fail
        let limits = ServerLimits::decode( buf).map_err(|_| WelcomePayloadError::IncompleteData)?;

        Ok(Self { protocol_version, session_id, session_token, server_challenge, server_capabilities, limits })
    }
}
