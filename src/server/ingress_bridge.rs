// arcella-broker/src/server/ingress_bridge.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::Bytes;
use std::sync::Arc;

use crate::broker_core::{
    l2::{IngressBridge, IngressResult},
    registry::LocalRegistry,
    security::{SessionContext},
};

use super::config::ServerConfig;

pub struct RegistryIngressBridge {
    registry: Arc<LocalRegistry>,
    config: Arc<ServerConfig>,
}

impl RegistryIngressBridge {
    pub fn new(registry: Arc<LocalRegistry>, config: Arc<ServerConfig>) -> Self {
        Self {
            registry,
            config: config,
        }
    }    
}

impl IngressBridge for RegistryIngressBridge {
    fn process<'a, C>(
        &'a self,
        _address: &Bytes,
        _messages_raw: &Bytes,
        session_ctx: &C,
    ) -> impl std::future::Future<Output = IngressResult> + Send + 'a
    where
        C: SessionContext + ?Sized
    {
        let source_prefix_bytes = session_ctx.source_prefix();


        async move {
            IngressResult::Accepted
        }
    }
}
