// arcella-broker/src/broker/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;
use crate::client::BrokerClient;
use crate::config::{BrokerConfig, ClientConfig};
use crate::error::BrokerError;
use crate::registry::LocalRegistry;

pub struct Broker {
    /// Shared configuration for this broker instance.
    pub(crate) config: Arc<BrokerConfig>,
    
    /// Local routing registry.
    pub(crate) registry: Arc<LocalRegistry>,
}

impl Broker {
    pub fn new(config: BrokerConfig) -> Self {
        let config = Arc::new(config);
        let registry = Arc::new(LocalRegistry::new());
        
        Self { config, registry }
    }

    pub fn default_config() -> BrokerConfig {
        BrokerConfig::default()
    }

    pub fn client(self: &Arc<Self>, config: ClientConfig, client_address: String) -> Result<BrokerClient, BrokerError> {
         Ok(BrokerClient::new(self.clone(), config, client_address)?)
    }         
}

impl Default for Broker {
    fn default() -> Self {
        Self::new(BrokerConfig::default())
    }
}
