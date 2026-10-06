// arcella-broker/src/protocol/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use thiserror::Error;

mod codec;
mod frame;
mod message;
pub mod payloads;
pub mod utils;

use message::{
    MessageError,
};

pub use message::{
    Message,
    TransferMode,
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
