// arcella-broker/src/registry/error.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use thiserror::Error;

/// Errors that can occur during registry operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    /// Returned when attempting to register an address or pattern that is already registered.
    #[error("Address or pattern '{0}' is already occupied")]
    AddressAlreadyOccupied(String),
    
    /// Returned when a new wildcard pattern overlaps with an existing exact address.
    #[error("Wildcard subscription '{0}' conflicts with existing exact address '{1}'")]
    WildcardConflict(String, String),
    
    /// Returned when an exact address is registered that falls under an existing wildcard pattern.
    #[error("Exact address '{0}' conflicts with existing wildcard subscription '{1}'")]
    ConflictsWithWildcard(String, String),

    /// Returned when a wildcard pattern violates syntax rules.
    /// 
    /// Common causes:
    /// - Empty pattern or empty segments (e.g., `a::b`).
    /// - `**` is not the last segment (e.g., `a:**:b`).
    /// - Malformed segments containing `*` alongside other characters (e.g., `a*`, `*b`).
    #[error("Invalid wildcard format: {0}")]
    InvalidWildcardFormat(String),

    #[error("Waiter already exists")]
    WaiterAlreadyExists,
}
