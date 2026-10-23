// arcella-broker/src/test_utils.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::{Arc, OnceLock};
use tracing_subscriber::{fmt, EnvFilter};

static TRACING_INIT: OnceLock<()> = OnceLock::new();

use crate::{
    broker::Broker,
    client::BrokerClient,
    config::ClientConfig,
    error::BrokerError,
};

pub use crate::protocol::test_utils::*;

/// Create a test client with a local reestr
pub fn test_client(address: String) -> Result<BrokerClient, BrokerError> {
    // 1. Initialize config, broker and client
    let broker_config = Broker::default_config();
    let broker = Arc::new(Broker::new(broker_config));
    let client_config = ClientConfig::default();
    broker.client(client_config, address)
}

pub fn init_tracing(directives: &str) {
    TRACING_INIT.get_or_init(|| {    
        // Attempt to read the logging level from RUST_LOG,
        // if not set, default to "info"
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(directives));

        fmt()
            .with_env_filter(env_filter)
            .with_thread_ids(true)       // Useful for multithreaded tests (shows thread ID)
            .with_target(true)           // Optional: hides the long module path for brevity
            .with_level(true)            // Shows the level (DEBUG, TRACE, INFO)
            .init();
    });    
}
