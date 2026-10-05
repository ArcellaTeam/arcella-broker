// arcella-broker/src/protocol/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::str;
use thiserror::Error;

mod frame;
mod message;

use message::{
    MessageError,
};

pub use message::{
    Message,
    TransferMode,
};

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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("Address length ({0}) exceeds limit {MAX_ADDRESS_LEN}")]
    AddressTooLong(u16),

    #[error("Invalid UTF-8 in address")]
    InvalidAddressUtf8,

    #[error("Invalid address format: {0}")]
    InvalidAddressFormat(String),

    #[error("Empty level in address (double colon)")]
    EmptyAddressLevel,

    #[error("Message error: {0}")]
    MessageError (#[from] MessageError),
}

// ============================================================================
// Address validation
// ============================================================================

/// Validates the recipient address format
/// 
/// Rules:
/// - Only allowed characters: a-zA-Z0-9, -, _, :
/// - Empty levels (::) are forbidden
/// - Address cannot start or end with ':'
pub fn validate_address(address: &str) -> Result<(), ProtocolError> {
    if address.is_empty() {
        return Err(ProtocolError::InvalidAddressFormat(
            "Address cannot be empty".to_string(),
        ));
    }

    // Check for empty levels (::) and leading/trailing ':'
    if address.starts_with(':') || address.ends_with(':') || address.contains("::") {
        return Err(ProtocolError::EmptyAddressLevel);
    }

    // Byte-level check (all allowed characters are ASCII)
    let is_valid = address.as_bytes().iter().all(|b| {
        b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_' || *b == b':'
    });	

    if !is_valid {
        return Err(ProtocolError::InvalidAddressFormat(
            "Invalid character in address".to_string(),
        ));
    }

    Ok(())
}
