// arcella-broker/src/server/config.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::path::PathBuf;

use crate::protocol::ServerLimits;

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
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
            socket_path: PathBuf::from("/tmp/arcella-broker.sock"),
            limits: ServerLimits::default(),
            session_timeout_ms: 300_000, // 5 minutes
            gc_interval_ms: 10_000,      // 10 seconds
        }
    }
}
