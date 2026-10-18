// arcella-broker/src/protocol/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use thiserror::Error;

pub mod codec;
mod frame;
mod message;
pub mod payloads;
mod server_limits;
pub mod utils;

pub use frame::{
    Frame, 
    FrameType,
    CRC_SIZE,
    FRAME_HEADER_SIZE, 
    FRAME_MAX_PAYLOAD_LENGTH, 
    FRAME_OVERHEAD,
};

use message::{
    MessageError,
};

pub use message::{
    Message,
    TransferMode,
};

pub use server_limits::{
    ServerLimits,
    DEFAULT_HOT_CREDIT,
    DEFAULT_COLD_CREDIT,
};

/// Utility functions and helpers for testing the broker.
#[allow(missing_docs)]
#[cfg(test)]
pub mod test_utils;

// ============================================================================
// Protocol constants
// ============================================================================

/// Maximum recipient address size (bytes)
pub const MAX_ADDRESS_LEN: usize = 1024;

/// Mask for extracting the transfer mode from the flags field (bit 0)
pub const TRANSFER_MODE_MASK: u8 = 0x01;

/// Shift for the transfer mode in the flags field
pub const TRANSFER_MODE_SHIFT: u8 = 0;

// ============================================================================
// Protocol errors
// ============================================================================

/// Top-level errors that can occur during L2/L3 protocol processing.
///
/// This enum aggregates errors related to **address validation** and acts as
/// a wrapper for [`MessageError`] via the `From` trait, allowing ergonomic
/// error propagation with the `?` operator from deeper parsing layers.
///
/// # Design notes
/// - All variants carry enough context for diagnostics (e.g., the offending length).
/// - The enum is `PartialEq + Eq` to simplify assertions in tests.
/// - Address-related errors are centralized here because addresses appear in
///   both L2 frames (Data payload) and L3 messages, making validation a
///   cross-cutting concern.
///
/// # See also
/// - [`MessageError`] for errors specific to L3 message parsing.
/// - [`crate::protocol::utils::validate_address`] for the validation rules.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    /// The recipient address exceeds the protocol limit of [`MAX_ADDRESS_LEN`] bytes.
    ///
    /// Contains the actual length that was rejected.
    #[error("Address length ({0}) exceeds limit {MAX_ADDRESS_LEN}")]
    AddressTooLong(u16),

    /// The recipient address contains bytes that are not valid UTF-8.
    ///
    /// Addresses must be pure ASCII (see [`crate::protocol::utils::validate_address`]),
    /// so this error typically indicates a corrupted frame or a buggy sender.
    #[error("Invalid UTF-8 in address")]
    InvalidAddressUtf8,

    /// The recipient address contains characters outside the allowed set.
    ///
    /// Allowed characters: `a-zA-Z0-9`, `-`, `_`, `:`.
    /// The inner string contains a human-readable description of the violation.
    #[error("Invalid address format: {0}")]
    InvalidAddressFormat(String),

    /// The recipient address contains an empty level (e.g., `a::b`, `:leading`, or `trailing:`).
    ///
    /// Empty levels are forbidden by the Arcella addressing scheme to prevent
    /// ambiguous routing and wildcard matching.
   #[error("Empty level in address (double colon)")]
    EmptyAddressLevel,

    /// A wrapped [`MessageError`] from the L3 message parsing layer.
    ///
    /// This variant enables automatic conversion via `?`, so callers can
    /// handle both address and message errors uniformly.
    #[error("Message error: {0}")]
    MessageError (#[from] MessageError),
}
