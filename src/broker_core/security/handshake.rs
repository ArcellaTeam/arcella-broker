// arcella-broker/src/broker_core/security/handshake.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::Bytes;
use getrandom::fill as getrandom_fill;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::protocol::{
    ServerLimits,
    payloads::{
        HelloPayload,
        WelcomePayload,
        AuthResponsePayload,
        AuthResultPayload,
    },
};

use crate::broker_core::transport::TransportError;

#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("protocol version mismatch: expected {expected}, got {got}")]
    ProtocolVersionMismatch { expected: u16, got: u16 },

    #[error("connection closed during handshake")]
    ConnectionClosed,

    #[error("unsupported authentication algorithm: 0x{0:02x}")]
    UnsupportedAlgorithm(u8),

    #[error("payload decode error: {0}")]
    PayloadDecode(String),

    #[error("Cryptographic RNG failure")]
    RngFailure(#[from] getrandom::Error),

    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}

/// Server-side handshake handler.
///
/// Responsible for:
/// - Generating unique `session_id` (monotonic counter).
/// - Generating cryptographically secure `session_token` and `server_challenge`.
/// - Advertising `ServerLimits` to the client during the Welcome phase.
///
/// # Configuration
/// The handler can be configured with custom `ServerLimits` to override defaults.
/// This allows the broker to tune resource boundaries (credits, frame sizes, TTLs)
/// based on deployment requirements without changing the core logic.
pub struct HandshakeHandler {
    /// Monotonic counter for generating unique session_ids.
    /// Starts at 1, since 0 may be used as a reserved value.
    session_counter: AtomicU64,
    
    /// Server limits advertised to clients during the Welcome handshake.
    /// These limits define the resource boundaries for each client session.
    limits: ServerLimits,
}

impl Default for HandshakeHandler {
    fn default() -> Self {
        Self {
            session_counter: AtomicU64::new(1),
            limits: ServerLimits::default(),
        }
    }
}

impl HandshakeHandler {
    /// Creates a new `HandshakeHandler` with default `ServerLimits`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `HandshakeHandler` with custom `ServerLimits`.
    ///
    /// This is the preferred constructor for production deployments,
    /// where limits should be configured via the broker's configuration file.
    pub fn with_limits(limits: ServerLimits) -> Self {
        Self {
            session_counter: AtomicU64::new(1),
            limits,
        }
    }

    /// Returns a reference to the currently configured `ServerLimits`.
    pub fn limits(&self) -> &ServerLimits {
        &self.limits
    }        

    /// Handles an incoming Hello packet and forms a Welcome response.
    ///
    /// # Algorithm
    /// 1. Verifies that the requested authentication algorithm is None (0x00).
    /// 2. Generates a unique `session_id` using an atomic increment.
    /// 3. Generates a cryptographically secure `session_token` (`[u8; 32]`).
    /// 4. Generates a `server_challenge` (`[u8; 32]`) for compatibility with the Welcome structure.
    /// 5. Returns the assembled `WelcomePayload` with the configured `ServerLimits`.
    pub fn process_hello(&self, hello: &HelloPayload) -> Result<WelcomePayload, HandshakeError> {
        // TODO:  only the None (0x00) algorithm is supported
        if hello.algorithm != 0x00 {
            return Err(HandshakeError::UnsupportedAlgorithm(hello.algorithm));
        }

        // 1. Generating session_id (monotonically increasing u64)
        let session_id = self.session_counter.fetch_add(1, Ordering::SeqCst);

        // 2. Generating session_token (32 bytes, CSPRNG)
        let mut session_token = [0u8; 32];
        getrandom_fill(&mut session_token)?;

        // 3. Generating server_challenge (32 bytes, CSPRNG)
        // Required by the WelcomePayload specification, even though it is not yet used for HMAC
        let mut server_challenge = [0u8; 32];
        getrandom_fill(&mut server_challenge)?;

        // 4. Forming the response with configured limits
        Ok(WelcomePayload {
            protocol_version: hello.protocol_version,
            session_id,
            session_token,
            server_challenge,
            server_capabilities: 0, // Reserved for future server capability flags
            limits: self.limits.clone(),
        })
    }

    /// Handles an incoming AuthResponse packet and forms an AuthResult response.
    ///
    /// # Algorithm
    /// 1. Validates that the session_id is valid (basic check for now).
    /// 2. Since algorithm is 0x00, skips cryptographic HMAC verification.
    /// 3. Returns AuthResult with status = 0 (success).
    pub fn process_auth_response(
        &self,
        auth_response: &AuthResponsePayload,
    ) -> Result<AuthResultPayload, HandshakeError> {
        // В Этапе 5 здесь будет проверка: 
        // if !self.session_manager.exists(auth_response.session_id) { return Err(...) }
        
        // Для Этапа 4: просто принимаем любой запрос для алгоритма None, 
        // так как криптография отключена.
        
        Ok(AuthResultPayload {
            session_id: auth_response.session_id,
            status: 0, // 0 = success
            error_message: Bytes::new(),
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    use crate::protocol::{
        payloads::L2_PROTOCOL_VERSION,
        DEFAULT_MAX_FRAME_SIZE,
        DEFAULT_HOT_CREDIT,
        DEFAULT_COLD_CREDIT,
    };

    fn make_hello(algorithm: u8) -> HelloPayload {
        HelloPayload {
            protocol_version: L2_PROTOCOL_VERSION,
            client_id: Bytes::from("test-client"),
            algorithm,
            auth_data: Bytes::new(),
            capabilities: 0,
        }
    }    

    #[test]
    fn test_process_hello_success_algorithm_none() {
        let handler = HandshakeHandler::new();
        let hello = make_hello(0x00);

        let welcome = handler.process_hello(&hello).unwrap();

        // Checking basic invariants
        assert_eq!(welcome.protocol_version, L2_PROTOCOL_VERSION);
        assert_eq!(welcome.session_id, 1); // The first call should return 1
        assert_ne!(welcome.session_token, [0u8; 32]); // The token must not be zero
        assert_ne!(welcome.server_challenge, [0u8; 32]); // The challenge must not be zero
        
        // Checking the default limits
        assert_eq!(welcome.limits.hot_credit, DEFAULT_HOT_CREDIT);
        assert_eq!(welcome.limits.cold_credit, DEFAULT_COLD_CREDIT);
        assert_eq!(welcome.limits.max_frame_size, DEFAULT_MAX_FRAME_SIZE);
    }

    #[test]
    fn test_process_hello_with_custom_limits() {
        const MAX_FRAME_SIZE: u32 = 8 * 1024 * 1024;
        const MAX_SESSIONS_PER_CLIENT: u32 = 50;
        const MAX_ADDRESSES_PER_SESSION: u32 = 500;
        const HOT_CREDIT: u32 = 100;
        const COLD_CREDIT: u32 = 20;
        const CELL_TTL_MS: u32 = 100;
        const POOL_SIZE: u32 = 20;
        const SESSION_TTL_MS: u32 = 7_200_000;      // 2 hour

        // Setting custom limits (as if they came from the broker's config)
        let custom_limits = ServerLimits {
            max_frame_size: MAX_FRAME_SIZE,  // 8 MB
            max_sessions_per_client: MAX_SESSIONS_PER_CLIENT,
            max_addresses_per_session: MAX_ADDRESSES_PER_SESSION,
            hot_credit: HOT_CREDIT,                   // Increased hot credit
            cold_credit: COLD_CREDIT,                   // Increased cold credit
            cell_ttl_ms: CELL_TTL_MS,
            buffer_pool_size: POOL_SIZE,
            session_ttl_ms: SESSION_TTL_MS,
        };

        let handler = HandshakeHandler::with_limits(custom_limits.clone());
        let hello = make_hello(0x00);
        
        let welcome = handler.process_hello(&hello).unwrap();

        // Checking that the client received exactly the custom limits
        assert_eq!(welcome.limits, custom_limits);
        assert_eq!(welcome.limits.hot_credit, HOT_CREDIT);
        assert_eq!(welcome.limits.cold_credit, COLD_CREDIT);
        assert_eq!(welcome.limits.max_frame_size, MAX_FRAME_SIZE);
        assert_eq!(welcome.limits.session_ttl_ms, SESSION_TTL_MS);
    }

    #[test]
    fn test_limits_accessor() {
        let custom_limits = ServerLimits {
            max_frame_size: 4 * 1024 * 1024,
            ..ServerLimits::default()
        };
        let handler = HandshakeHandler::with_limits(custom_limits.clone());
        
        assert_eq!(handler.limits(), &custom_limits);
    }

    #[test]
    fn test_process_hello_unsupported_algorithm() {
        let handler = HandshakeHandler::new();
        let hello = make_hello(0x01); // HMAC-SHA256 (not yet supported)

        let result = handler.process_hello(&hello);
        assert!(matches!(result, Err(HandshakeError::UnsupportedAlgorithm(0x01))));
    }

    #[test]
    fn test_session_id_monotonicity() {
        let handler = HandshakeHandler::new();
        let hello = make_hello(0x00);

        let welcome1 = handler.process_hello(&hello).unwrap();
        let welcome2 = handler.process_hello(&hello).unwrap();
        let welcome3 = handler.process_hello(&hello).unwrap();

        assert_eq!(welcome1.session_id, 1);
        assert_eq!(welcome2.session_id, 2);
        assert_eq!(welcome3.session_id, 3);
    }

    #[test]
    fn test_welcome_payload_roundtrip_with_limits() {
        // Integration test: verify that WelcomePayload correctly
        // serializes and deserializes ServerLimits through the wire format
        let custom_limits = ServerLimits {
            max_frame_size: 4 * 1024 * 1024,
            hot_credit: 25,
            cold_credit: 5,
            ..ServerLimits::default()
        };
        
        let handler = HandshakeHandler::with_limits(custom_limits.clone());
        let hello = make_hello(0x00);
        
        let welcome_original = handler.process_hello(&hello).unwrap();
        
        // Serializing to wire format
        let encoded = welcome_original.encode();
        
        // Deserializing back
        let mut buf = encoded;
        let welcome_decoded = crate::protocol::payloads::WelcomePayload::decode(&mut buf).unwrap();
        
        // Checking that the limits were preserved after the roundtrip
        assert_eq!(welcome_decoded.limits, custom_limits);
        assert_eq!(welcome_decoded.session_id, welcome_original.session_id);
        assert_eq!(welcome_decoded.session_token, welcome_original.session_token);
    }

    #[test]
    fn test_process_auth_response_success_none() {
        let handler = HandshakeHandler::new();
        
        let auth_response = AuthResponsePayload {
            session_id: 1,
            client_nonce: [0xBB; 32],
            response: Bytes::new(), // Пустой response для алгоритма None
        };

        let result = handler.process_auth_response(&auth_response).unwrap();

        assert_eq!(result.session_id, 1);
        assert_eq!(result.status, 0); // success
        assert!(result.error_message.is_empty());
    }
}