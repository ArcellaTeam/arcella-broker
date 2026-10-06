// arcella-broker/src/test_utils.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;

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
