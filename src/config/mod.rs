// arcella-broker/src/config/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DEFAULT_REPLY_CHANNEL_CAPACITY: usize = 1024;
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;
pub const MAX_CHANNEL_CAPACITY: usize = 65_536;
pub const MAX_REPLY_CHANNEL_CAPACITY: usize = 65_536;
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_TTL: u8 = 64;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("Channel capacity must be greater than 0")]
    ZeroCapacity,

    #[error("Channel capacity {0} exceeds maximum allowed value {MAX_CHANNEL_CAPACITY}")]
    CapacityExceedsLimit(usize),

    #[error("Reply channel capacity {0} exceeds maximum allowed value {MAX_REPLY_CHANNEL_CAPACITY}")]
    ReplyCapacityExceedsLimit(usize),
}

/// Global configuration for the Arcella message broker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    /// Default timeout for InOut operations in milliseconds.
    pub request_timeout_ms: u64,

    /// Default Time-To-Live (TTL) value for cascaded routing.
    pub default_ttl: u8,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            default_ttl: DEFAULT_TTL,
        }
    }
}

impl BrokerConfig {
    /// Sets the request timeout in milliseconds.
    #[must_use]
    pub fn with_request_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.request_timeout_ms = timeout_ms;
        self
    }

    /// Sets the default TTL.
    #[must_use]
    pub fn with_default_ttl(mut self, ttl: u8) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Returns the timeout as a `std::time::Duration` for convenient use in Tokio.
    pub fn request_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.request_timeout_ms)
    }
}

/// Configuration for an individual subscriber (specific channel).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    /// Capacity of the client channel for dispatching responses in InOut (Request/Response) mode.
    pub reply_channel_capacity: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            reply_channel_capacity: DEFAULT_REPLY_CHANNEL_CAPACITY,
        }
    }
}

impl ClientConfig {
    /// Sets the capacity of the reply channel (InOut).
    pub fn with_reply_channel_capacity(mut self, capacity: usize) -> Result<Self, ConfigError> {
        if capacity == 0 {
            return Err(ConfigError::ZeroCapacity);
        }
        if capacity > MAX_REPLY_CHANNEL_CAPACITY {
            return Err(ConfigError::ReplyCapacityExceedsLimit(capacity));
        }
        self.reply_channel_capacity = capacity;
        Ok(self)
    }
}

/// Configuration for an individual subscriber (specific channel).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriberConfig {
    /// Maximum number of messages the subscriber channel can buffer.
    /// Upon overflow, the sender will be blocked (backpressure) or receive an error.
    pub channel_capacity: usize,
}

impl Default for SubscriberConfig {
    /// Creates a new subscriber configuration with default values.
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY
        }
    }
}

impl SubscriberConfig {
    /// Modifies the channel capacity (builder pattern).
    pub fn with_channel_capacity(mut self, channel_capacity: usize)  -> Result<Self, ConfigError> {
        if channel_capacity == 0 {
            return Err(ConfigError::ZeroCapacity);
        }
        if channel_capacity > MAX_CHANNEL_CAPACITY {
            return Err(ConfigError::CapacityExceedsLimit(channel_capacity));
        }
        self.channel_capacity = channel_capacity;
        Ok(self)
    }    
}
