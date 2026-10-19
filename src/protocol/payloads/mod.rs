// arcella-broker/src/protocol/payloads/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use thiserror::Error;

mod auth_response;
mod auth_result;
mod data;
mod hello;
mod welcome;

pub use auth_response::*;
pub use auth_result::*;
pub use data::*;
pub use hello::*;
pub use welcome::*;

/// Current L2 protocol version
pub const L2_PROTOCOL_VERSION: u16 = 1;

/// Unified error type for all L2 payload operations.
///
/// This enum aggregates all possible errors from individual payload modules
/// into a single type, allowing callers to handle payload errors uniformly
/// without needing to know about the specific payload type.
///
/// # Variants
///
/// Each variant corresponds to a specific payload type and contains the
/// detailed error from that payload's module. The `#[error(...)]` attribute
/// provides human-readable context about which payload failed.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PayloadError {
    /// Error parsing/encoding a Hello payload (0x00).
    ///
    /// This typically indicates malformed client identification data,
    /// unsupported protocol version, or truncated auth data.
    #[error("Hello payload (0x00): {0}")]
    Hello(#[from] HelloPayloadError),

    /// Error parsing/encoding a Welcome payload (0x01).
    ///
    /// This typically indicates truncated server response data
    /// (e.g., missing session token or server limits).
    #[error("Welcome payload (0x01): {0}")]
    Welcome(#[from] WelcomePayloadError),

    /// Error parsing/encoding an AuthResponse payload (0x05).
    ///
    /// This typically indicates malformed client authentication response,
    /// missing nonce, or truncated HMAC response.
    #[error("AuthResponse payload (0x05): {0}")]
    AuthResponse(#[from] AuthResponsePayloadError),

    /// Error parsing/encoding an AuthResult payload (0x06).
    ///
    /// This typically indicates truncated server authentication result
    /// or malformed error message.
    #[error("AuthResult payload (0x06): {0}")]
    AuthResult(#[from] AuthResultPayloadError),

    /// Error parsing/encoding a Data payload (0xfe).
    ///
    /// This typically indicates address length violation, truncated message
    /// data, or message count mismatch in the L3 aggregation.
    #[error("Data payload (0xfe): {0}")]
    Data(#[from] DataPayloadError),
}

/// Convenience type alias for payload operation results.
pub type PayloadResult<T> = Result<T, PayloadError>;
