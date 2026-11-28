// arcella-broker/src/broker_core/subscription/error.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Subscription errors, mapped 1:1 onto `SubscribeAck` status codes.

use thiserror::Error;

use crate::protocol::payloads::{
    SUB_STATUS_ADDRESS_NOT_FOUND,
    SUB_STATUS_ADDRESS_OCCUPIED,
    SUB_STATUS_POLICY_CONFLICT,
    SUB_STATUS_INVALID_ADDRESS,
    SUB_STATUS_PERMISSION_DENIED,
    SUB_STATUS_QUOTA_EXCEEDED,
    SUB_STATUS_INTERNAL_ERROR,
    SUB_STATUS_ALREADY_SUBSCRIBED,
    SUB_STATUS_POLICY_UNSUPPORTED,
    SUB_STATUS_NOT_SUBSCRIBED,
};

/// Failure to attach or detach a consumer binding.
///
/// Kept separate from `RegistryError`: the registry knows nothing about
/// consumers, and these variants mirror the wire protocol, not tree conflicts.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubscriptionError {
    /// `0x01` — No address with the given name is registered in the broker.
    #[error("Address not found: {0}")]
    AddressNotFound(String),

    /// `0x02` — The address is occupied by an Exclusive binding of another session.
    #[error("Exclusive address already occupied: {0}")]
    ExclusiveOccupied(String),

    /// `0x03` — Routing policy conflict or incompatible wildcard pattern with
    /// an existing subscription on the same address.
    #[error("Routing policy or wildcard pattern conflict: {0}")]
    PolicyConflict(String),
    
    /// `0x04` — The address name or wildcard pattern is syntactically invalid.
    #[error("Invalid address or wildcard format: {0}")]
    InvalidAddress(String),

    /// `0x05` — The session is not authorized to consume this address
    /// (ACL check or hierarchy constraint violation).
    #[error("Permission denied for address: {0}")]
    PermissionDenied(String),

    /// `0x06` — The session already holds `ServerLimits::max_addresses_per_session`
    /// consumer bindings and cannot attach one more.
    #[error("Consumer binding quota exceeded: {0}")]
    QuotaExceeded(String),

    /// `0x07` — Internal broker error (registry, channel, task spawn, I/O, ...).
    #[error("Internal subscription error: {0}")]
    Internal(String),

    /// `0x08` — This session is already subscribed to the given address.
    #[error("Session already subscribed to: {0}")]
    AlreadySubscribed(String),

    /// `0x09` — The address uses a routing policy not supported by the current
    /// broker version (e.g. Broadcast before Stage N).
    #[error("Routing policy not supported for address: {0}")]
    PolicyUnsupported(String),

    /// `0x0A` — `Unsubscribe` failed: this session has no binding for the address.
    #[error("No consumer binding for address: {0}")]
    NotSubscribed(String),
}

impl SubscriptionError {
    /// Wire status code for `SubscribeAck`.
    pub fn status(&self) -> u8 {
        match self {
            Self::AddressNotFound(_)   => SUB_STATUS_ADDRESS_NOT_FOUND,
            Self::ExclusiveOccupied(_) => SUB_STATUS_ADDRESS_OCCUPIED,
            Self::PolicyConflict(_)    => SUB_STATUS_POLICY_CONFLICT,
            Self::InvalidAddress(_)    => SUB_STATUS_INVALID_ADDRESS,
            Self::PermissionDenied(_)  => SUB_STATUS_PERMISSION_DENIED,
            Self::QuotaExceeded(_)     => SUB_STATUS_QUOTA_EXCEEDED,
            Self::Internal(_)          => SUB_STATUS_INTERNAL_ERROR,
            Self::AlreadySubscribed(_) => SUB_STATUS_ALREADY_SUBSCRIBED,
            Self::PolicyUnsupported(_) => SUB_STATUS_POLICY_UNSUPPORTED,
            Self::NotSubscribed(_)     => SUB_STATUS_NOT_SUBSCRIBED,
        }
    }
}