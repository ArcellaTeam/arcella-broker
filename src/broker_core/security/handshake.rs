// arcella-broker/src/broker_core/security/handshake.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! L2 Protocol handshake implementation.
//!
//! This module implements the server-side of the L2 protocol handshake sequence:
//! `Hello` → `Welcome` → `AuthResponse` → `AuthResult`.
//!
//! # Current Implementation Status (Stage 5)
//! This implementation currently supports only the `None` (0x00) authentication algorithm.
//! Full Challenge-Response authentication (HMAC-SHA256, BLAKE3-HMAC, GOST) will be
//! implemented in Stage 11 according to the roadmap.
//!
//! # Thread Safety
//! The [`HandshakeHandler`] is designed to be shared across multiple connection handlers
//! via `Arc<HandshakeHandler>`. All mutable state is protected by atomic operations,
//! making it safe for concurrent use in an async Tokio runtime.

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

/// Errors that can occur during the L2 handshake process.
///
/// # Error Categories
/// - **Protocol Errors**: Version mismatches, unexpected frames, unsupported algorithms
/// - **Authentication Errors**: Invalid credentials, session ID mismatches
/// - **System Errors**: RNG failures, connection drops, payload decode errors
///
/// # Error Handling
/// All errors in this enum are considered fatal for the handshake. Upon encountering
/// any of these errors, the connection should be terminated with an appropriate
/// `Close` (0x07) frame or immediate socket closure.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    /// Authentication failed due to invalid credentials or session mismatch.
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Client and server protocol versions are incompatible.
    #[error("Protocol version mismatch: expected {expected}, got {got}")]
    ProtocolVersionMismatch { expected: u16, got: u16 },

    /// The connection was closed before the handshake completed.
    #[error("Connection closed during handshake")]
    ConnectionClosed,

    /// The client requested an authentication algorithm not supported by the server.
    #[error("Unsupported authentication algorithm: 0x{0:02x}")]
    UnsupportedAlgorithm(u8),

    /// Failed to decode a handshake payload (malformed or corrupted data).
    #[error("Payload decode error: {0}")]
    PayloadDecode(String),

    /// Received an unexpected frame type during the handshake sequence.
    #[error("Get unexpected frame: {0}")]
    UnexpectedFrame(String),

    /// Cryptographically secure random number generator failed.
    /// 
    /// # Severity
    /// This is a critical system error. If RNG fails, the server cannot generate
    /// secure session tokens or challenges, making it impossible to establish
    /// secure sessions. The server should log this error and potentially refuse
    /// new connections until the issue is resolved.
    #[error("Cryptographic RNG failure")]
    RngFailure(#[from] getrandom::Error),
}

/// Server-side handshake handler.
///
/// Responsible for:
/// - Generating unique `session_id` (monotonic counter).
/// - Generating cryptographically secure `session_token` and `server_challenge`.
/// - Advertising `ServerLimits` to the client during the Welcome phase.
/// - Validating client responses during the authentication sequence.
///
/// # Configuration
/// The handler can be configured with custom `ServerLimits` to override defaults.
/// This allows the broker to tune resource boundaries (credits, frame sizes, TTLs)
/// based on deployment requirements without changing the core logic.
///
/// # Thread Safety
/// This struct is safe to share across multiple threads via `Arc<HandshakeHandler>`.
/// The `session_counter` field uses atomic operations for lock-free concurrent access.
/// All other fields are immutable after construction.
///
/// # Lifecycle
/// 1. Create a handler using [`HandshakeHandler::new`] or [`HandshakeHandler::with_limits`].
/// 2. Share it across connection handlers via `Arc`.
/// 3. For each incoming connection, call [`HandshakeHandler::process_hello`] and
///    [`HandshakeHandler::process_auth_response`] in sequence.
/// 4. The handler maintains no per-connection state; all state is returned in the
///    response payloads.
pub struct HandshakeHandler {
    /// Monotonic counter for generating unique session_ids.
    /// 
    /// # Why Atomic?
    /// Using `AtomicU64` instead of a `Mutex` allows multiple connection handlers
    /// to generate session IDs concurrently without blocking, which is critical
    /// for high-throughput scenarios where many clients connect simultaneously.
    ///
    /// # Starting Value
    /// Starts at 1, since 0 may be used as a reserved value in some contexts
    /// (e.g., to indicate "no session" or "uninitialized").
    session_counter: AtomicU64,
    
    /// Server limits advertised to clients during the Welcome handshake.
    /// 
    /// # Purpose
    /// These limits define the resource boundaries for each client session:
    /// - `hot_credit` / `cold_credit`: Flow control parameters
    /// - `max_frame_size`: Maximum payload size
    /// - `session_ttl_ms`: Session lifetime
    /// - And other limits defined in [`ServerLimits`]
    ///
    /// # Immutability
    /// Once set during construction, these limits cannot be changed. This ensures
    /// that all clients connecting to the same handler receive consistent limits,
    /// preventing confusion and potential protocol violations.
    limits: ServerLimits,
}

impl Default for HandshakeHandler {
    /// Creates a `HandshakeHandler` with default `ServerLimits`.
    ///
    /// # Default Values
    /// The default limits are suitable for development and testing:
    /// - `hot_credit`: 50
    /// - `cold_credit`: 10
    /// - `max_frame_size`: 16 MB
    /// - `session_ttl_ms`: 1 hour
    ///
    /// # Production Use
    /// For production deployments, prefer [`HandshakeHandler::with_limits`] to
    /// configure limits according to your deployment requirements.
    fn default() -> Self {
        Self {
            session_counter: AtomicU64::new(1),
            limits: ServerLimits::default(),
        }
    }
}

impl HandshakeHandler {
    /// Creates a new `HandshakeHandler` with default `ServerLimits`.
    ///
    /// # Equivalent To
    /// ```rust
    /// use arcella_broker::protocol::ServerLimits;
    /// use arcella_broker::broker_core::security::HandshakeHandler;
    /// 
    /// HandshakeHandler::with_limits(ServerLimits::default());
    /// ```
    ///
    /// # Use Case
    /// Suitable for development, testing, and scenarios where default resource
    /// limits are acceptable. For production deployments, prefer [`Self::with_limits`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `HandshakeHandler` with custom `ServerLimits`.
    ///
    /// # Arguments
    /// * `limits` - The `ServerLimits` to advertise to connecting clients.
    ///
    /// # Use Case
    /// This is the preferred constructor for production deployments, where limits
    /// should be configured via the broker's configuration file or environment
    /// variables to match the deployment's resource constraints and performance
    /// requirements.
    ///
    /// # Example
    /// ```rust
    /// use arcella_broker::protocol::ServerLimits;
    /// use arcella_broker::broker_core::security::HandshakeHandler;
    /// 
    /// let limits = ServerLimits {
    ///     hot_credit: 100,
    ///     cold_credit: 20,
    ///     max_frame_size: 8 * 1024 * 1024, // 8 MB
    ///     ..ServerLimits::default()
    /// };
    /// let handler = HandshakeHandler::with_limits(limits);
    /// ```
    pub fn with_limits(limits: ServerLimits) -> Self {
        Self {
            session_counter: AtomicU64::new(1),
            limits,
        }
    }

    /// Returns a reference to the currently configured `ServerLimits`.
    ///
    /// # Use Case
    /// Useful for logging, metrics, or debugging to inspect the limits that will
    /// be advertised to connecting clients.
    ///
    /// # Performance
    /// This is a zero-cost operation that simply returns a reference to the
    /// immutable `limits` field.
    pub fn limits(&self) -> &ServerLimits {
        &self.limits
    }        

    /// Handles an incoming `Hello` packet and forms a `Welcome` response.
    ///
    /// # Algorithm
    /// 1. **Algorithm Validation**: Verifies that the requested authentication algorithm
    ///    is `None` (0x00). Other algorithms will be supported in Stage 11.
    /// 2. **Session ID Generation**: Generates a unique `session_id` using an atomic
    ///    increment of the internal counter.
    /// 3. **Session Token Generation**: Generates a cryptographically secure 32-byte
    ///    `session_token` using the system's CSPRNG.
    /// 4. **Server Challenge Generation**: Generates a 32-byte `server_challenge` for
    ///    compatibility with the `WelcomePayload` structure (not yet used for HMAC).
    /// 5. **Response Assembly**: Returns the assembled `WelcomePayload` with the
    ///    configured `ServerLimits`.
    ///
    /// # Arguments
    /// * `hello` - The `HelloPayload` received from the client.
    ///
    /// # Returns
    /// * `Ok(WelcomePayload)` - The response to send back to the client.
    /// * `Err(HandshakeError::UnsupportedAlgorithm)` - If the client requested an
    ///   unsupported authentication algorithm.
    /// * `Err(HandshakeError::RngFailure)` - If the CSPRNG failed to generate
    ///   secure random bytes.
    ///
    /// # Thread Safety
    /// This method is safe to call concurrently from multiple threads. The only
    /// mutable state is the `session_counter`, which uses atomic operations.
    ///
    /// # Security Considerations
    /// - The `session_token` and `server_challenge` are generated using
    ///   cryptographically secure random number generation.
    /// - The `session_id` is monotonically increasing and unique per handler instance.
    /// - Currently, only the `None` (0x00) algorithm is supported, which provides
    ///   no authentication. This is acceptable only for development/testing or
    ///   trusted network environments.
    pub fn process_hello(&self, hello: &HelloPayload) -> Result<WelcomePayload, HandshakeError> {
        // TODO:  only the None (0x00) algorithm is supported
        if hello.algorithm != 0x00 {
            return Err(HandshakeError::UnsupportedAlgorithm(hello.algorithm));
        }

        // 1. Generating session_id (monotonically increasing u64)
        let session_id = self.session_counter.fetch_add(1, Ordering::Relaxed);

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

    /// Handles an incoming `AuthResponse` packet and forms an `AuthResult` response.
    ///
    /// # Algorithm
    /// 1. **Session ID Validation**: Validates that the `session_id` in the response
    ///    matches the expected value (the one we issued in the `Welcome`).
    /// 2. **Authentication Verification**: Since algorithm is 0x00 (None), skips
    ///    cryptographic HMAC verification.
    /// 3. **Response Assembly**: Returns `AuthResult` with status = 0 (success).
    ///
    /// # Arguments
    /// * `expected_session_id` - The `session_id` that was issued to the client
    ///   in the `Welcome` packet.
    /// * `auth_response` - The `AuthResponsePayload` received from the client.
    ///
    /// # Returns
    /// * `Ok(AuthResultPayload)` - The response to send back to the client.
    ///   Status will be 0 (success) for the `None` algorithm.
    /// * `Err(HandshakeError::AuthenticationFailed)` - If the `session_id` in the
    ///   response does not match the expected value.
    ///
    /// # Thread Safety
    /// This method is safe to call concurrently from multiple threads. It performs
    /// only read operations on immutable data.
    ///
    /// # Security Considerations
    /// - Currently, only the `None` (0x00) algorithm is supported, which provides
    ///   no authentication beyond session ID validation.
    /// - In Stage 11, this method will verify the HMAC signature using the
    ///   `server_challenge` and the shared `client_secret`.
    /// - The `client_nonce` in the response will be checked for replay attacks.
    ///
    /// # Future Implementation (Stage 11)
    /// The following checks will be added:
    /// ```text
    /// 1. Verify HMAC(client_secret, server_challenge || client_nonce) == response
    /// 2. Check that client_nonce has not been used before (replay protection)
    /// 3. Validate that the response was received within the challenge TTL
    /// ```
     pub fn process_auth_response(
        &self,
        expected_session_id: u64,
        auth_response: &AuthResponsePayload,
    ) -> Result<AuthResultPayload, HandshakeError> {
        // Strict check: the client must respond with the session_id we issued to it
        if auth_response.session_id != expected_session_id {
            return Err(HandshakeError::AuthenticationFailed(
                "Session ID mismatch in AuthResponse".into(),
            ));
        }

        // TODO: In Stage 11, verification of HMAC(client_secret, server_challenge || client_nonce) will be added here
        
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

        let expected_session_id = 1;
        
        let auth_response = AuthResponsePayload {
            session_id: expected_session_id,
            client_nonce: [0xBB; 32],
            response: Bytes::new(), // Empty response for the None algorithm
        };

        let result = handler.process_auth_response(expected_session_id, &auth_response).unwrap();

        assert_eq!(result.session_id, expected_session_id);
        assert_eq!(result.status, 0); // success
        assert!(result.error_message.is_empty());
    }
}
