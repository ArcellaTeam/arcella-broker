// arcella-broker/src/config/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;

use crate::broker_core::registry::RoutingPolicy;
use crate::protocol::ServerLimits;

// ============================================================================
// Channel Constants
// ============================================================================

/// Default capacity for client reply channels (InOut mode).
pub const DEFAULT_REPLY_CHANNEL_CAPACITY: usize = 1024;

/// Default capacity for subscriber message channels.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Absolute maximum capacity for any message channel.
pub const MAX_CHANNEL_CAPACITY: usize = 65_536;

/// Absolute maximum capacity for reply channels.
pub const MAX_REPLY_CHANNEL_CAPACITY: usize = 65_536;

// ============================================================================
// Timing & Routing Constants
// ============================================================================

/// Default timeout for InOut (Request/Response) operations, in milliseconds (30 seconds).
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

/// Default Time-To-Live (TTL) for cascaded routing (sufficient for deep hierarchies).
pub const DEFAULT_TTL: u8 = 64;

// ============================================================================
// Server Constants
// ============================================================================

/// Default path for the Unix Domain Socket (or Named Pipe on Windows).
pub const DEFAULT_SOCKET_PATH: &str = "/tmp/arcella-broker.sock";

/// Default session inactivity timeout in milliseconds (5 minutes).
/// If no frames are received within this period, the session is considered
/// expired and the connection is forcibly closed by the background GC task.
pub const DEFAULT_SESSION_TIMEOUT_MS: u64 = 300_000;

/// Default interval for the background session garbage collector, in milliseconds (10 seconds).
/// Recommended to be ~10% of `DEFAULT_SESSION_TIMEOUT_MS` for timely cleanup
/// without excessive CPU wakeups.
pub const DEFAULT_GC_INTERVAL_MS: u64 = 10_000;

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("Channel capacity must be greater than 0")]
    ZeroCapacity,

    #[error("Channel capacity {0} exceeds maximum allowed value {MAX_CHANNEL_CAPACITY}")]
    CapacityExceedsLimit(usize),

    #[error("Reply channel capacity {0} exceeds maximum allowed value {MAX_REPLY_CHANNEL_CAPACITY}")]
    ReplyCapacityExceedsLimit(usize),
}

// ============================================================================
// Broker Configs
// ============================================================================

/// Global configuration for the Arcella message broker.
/// 
/// Used by both the standalone server (`BrokerServer`) and the in-process 
/// client/core (`Broker`). Defines fundamental routing and timing behaviors.
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
    #[must_use]
    pub fn request_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.request_timeout_ms)
    }
}

// ============================================================================
// Client Configs
// ============================================================================

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
    pub routing_policy: RoutingPolicy,
}

impl Default for SubscriberConfig {
    /// Creates a new subscriber configuration with default values.
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            routing_policy: RoutingPolicy::Exclusive,
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

// ============================================================================
// Server Config
// ============================================================================

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Global broker configuration (TTL, timeouts, etc.)
    pub broker: BrokerConfig,

    /// Path to the Unix Domain Socket (or Named Pipe on Windows)
    pub socket_path: PathBuf,

    /// Limits sent to clients in `Welcome`.
    pub limits: ServerLimits,

    /// Session inactivity timeout (ms)
    pub session_timeout_ms: u64,

    /// Background session GC interval (ms)
    pub gc_interval_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            broker: BrokerConfig::default(),
            socket_path: PathBuf::from(DEFAULT_SOCKET_PATH),
            limits: ServerLimits::default(),
            session_timeout_ms: DEFAULT_SESSION_TIMEOUT_MS,
            gc_interval_ms: DEFAULT_GC_INTERVAL_MS,
        }
    }
}


