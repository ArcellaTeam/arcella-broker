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
    SUB_STATUS_ADDRESS_NOT_FOUND, SUB_STATUS_ALREADY_SUBSCRIBED,
    SUB_STATUS_BINDING_LIMIT, SUB_STATUS_EXCLUSIVE_OCCUPIED,
    SUB_STATUS_INTERNAL_ERROR, SUB_STATUS_NOT_SUBSCRIBED,
    SUB_STATUS_PERMISSION_DENIED, SUB_STATUS_POLICY_UNSUPPORTED,
};

/// Failure to attach or detach a consumer binding.
///
/// Kept separate from `RegistryError`: the registry knows nothing about
/// consumers, and these variants mirror the wire protocol, not tree conflicts.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubscriptionError {
    #[error("Address not found: {0}")]
    AddressNotFound(String),

    #[error("Exclusive address already occupied: {0}")]
    ExclusiveOccupied(String),

    #[error("Session already subscribed to: {0}")]
    AlreadySubscribed(String),

    #[error("Not allowed to consume address: {0}")]
    PermissionDenied(String),

    #[error("Routing policy not supported for address: {0}")]
    PolicyUnsupported(String),

    #[error("Consumer binding limit reached: {0}")]
    BindingLimit(usize),

    #[error("No consumer binding for address: {0}")]
    NotSubscribed(String),

    #[error("Internal subscription error: {0}")]
    Internal(String),
}

impl SubscriptionError {
    /// Wire status code for `SubscribeAck`.
    pub fn status(&self) -> u8 {
        match self {
            Self::AddressNotFound(_) => SUB_STATUS_ADDRESS_NOT_FOUND,
            Self::ExclusiveOccupied(_) => SUB_STATUS_EXCLUSIVE_OCCUPIED,
            Self::AlreadySubscribed(_) => SUB_STATUS_ALREADY_SUBSCRIBED,
            Self::PermissionDenied(_) => SUB_STATUS_PERMISSION_DENIED,
            Self::PolicyUnsupported(_) => SUB_STATUS_POLICY_UNSUPPORTED,
            Self::BindingLimit(_) => SUB_STATUS_BINDING_LIMIT,
            Self::NotSubscribed(_) => SUB_STATUS_NOT_SUBSCRIBED,
            Self::Internal(_) => SUB_STATUS_INTERNAL_ERROR,
        }
    }
}