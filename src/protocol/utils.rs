// arcella-broker/src/protocol/utils.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::str;

use super::ProtocolError;

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

#[cfg(test)]
mod address_validation_tests {
    use super::*;
    use super::super::*;

    #[test]
    fn test_valid_addresses() {
        assert!(validate_address("core:worker:1").is_ok());
        assert!(validate_address("simple").is_ok());
        assert!(validate_address("a:b:c:d:e").is_ok());
        assert!(validate_address("my-service_v2:queue-1").is_ok());
        assert!(validate_address("A-Z_0-9:test").is_ok());
    }

    #[test]
    fn test_empty_address() {
        let err = validate_address("").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidAddressFormat(_)));
    }

    #[test]
    fn test_leading_colon() {
        let err = validate_address(":invalid").unwrap_err();
        assert_eq!(err, ProtocolError::EmptyAddressLevel);
    }

    #[test]
    fn test_trailing_colon() {
        let err = validate_address("invalid:").unwrap_err();
        assert_eq!(err, ProtocolError::EmptyAddressLevel);
    }

    #[test]
    fn test_double_colon() {
        let err = validate_address("a::b").unwrap_err();
        assert_eq!(err, ProtocolError::EmptyAddressLevel);
    }

    #[test]
    fn test_invalid_characters() {
        // Space
        assert!(matches!(
            validate_address("a b").unwrap_err(),
            ProtocolError::InvalidAddressFormat(_)
        ));
        // Dot
        assert!(matches!(
            validate_address("a.b").unwrap_err(),
            ProtocolError::InvalidAddressFormat(_)
        ));
        // Slash
        assert!(matches!(
            validate_address("a/b").unwrap_err(),
            ProtocolError::InvalidAddressFormat(_)
        ));
        // Unicode
        assert!(matches!(
            validate_address("тест").unwrap_err(),
            ProtocolError::InvalidAddressFormat(_)
        ));
        // Control characters
        assert!(matches!(
            validate_address("a\tb").unwrap_err(),
            ProtocolError::InvalidAddressFormat(_)
        ));
    }
}
