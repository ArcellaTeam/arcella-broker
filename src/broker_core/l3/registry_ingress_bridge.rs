// arcella-broker/src/server/ingress_bridge.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, Bytes};
use std::sync::Arc;

use crate::broker_core::{
    IngressBridge,
    IngressResult,
    registry::LocalRegistry,
    security::{SessionContext},
};
use crate::config::ServerConfig;
use crate::protocol::Message;

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

    /// Checks compliance with the hierarchical domain isolation rule.
    /// Rule: "A source may send messages only to its immediate
    /// parent or to any descendant of its immediate parent."
    fn is_routing_allowed(source: &str, destination: &str) -> bool {
        if source.is_empty() || destination.is_empty() {
            return false;
        }

        // Find the immediate parent prefix of the source
        let parent = match source.rfind(':') {
            Some(idx) => &source[..idx],
            None => source, // Root level — it is considered its own parent
        };

        // 1. Exact match with the parent (sending upward)
        if destination == parent {
            return true;
        }

        // 2. Is the destination a descendant of the parent (sending downward or to a sibling)
        // Example: parent="a:b", destination="a:b:c:d" -> starts with "a:b:"
        if destination.len() > parent.len() {
            if destination.starts_with(parent) && destination.as_bytes()[parent.len()] == b':' {
                return true;
            }
        }

        false
    }
}

impl IngressBridge for RegistryIngressBridge {
    fn process<'a, C>(
        &'a self,
        _address: &'a Bytes,
        messages_raw: &'a Bytes,
        session_ctx: &'a C,
    ) -> impl std::future::Future<Output = IngressResult> + Send + 'a
    where
        C: SessionContext + ?Sized
    {
        let source_str = session_ctx.source_prefix();

        let mut buf = messages_raw.clone();

        async move {
            while buf.has_remaining() {
                let msg = match Message::decode(&mut buf) {
                    Ok(m) => m,
                    Err(e) => {
                        return IngressResult::Rejected(format!("L3 decode error: {}", e));
                    }
                };

                let dest_str = match str::from_utf8(&msg.address) {
                    Ok(s) => s,
                    Err(_) => {
                        return IngressResult::Rejected("Invalid UTF-8 in message destination".to_string());
                    }
                };

                if !Self::is_routing_allowed(source_str, dest_str) {
                    return IngressResult::Rejected(format!(
                        "Routing forbidden: source '{}' cannot send to '{}'",
                        source_str, dest_str
                    ));
                }

                let route_target = match self.registry.lookup(dest_str) {
                    Some(target) => target,
                    None => {
                        return IngressResult::Rejected(format!("No route found for '{}'", dest_str));
                    }
                };

                if let Err(e) = route_target.send(msg).await {
                    return IngressResult::Rejected(format!("Channel closed or send failed: {}", e));
                }
            }

            if messages_raw.is_empty() {
                return IngressResult::Rejected("Empty payload".to_string());
            }
           
            IngressResult::Accepted
        }
    }
}
