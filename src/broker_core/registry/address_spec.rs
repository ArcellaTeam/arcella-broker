// arcella-broker/src/broker_core/registry/address_spec.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::RoutingPolicy;

/// Specification of an address created via the control plane.
///
/// Defines the routing policy, queue capacity, and parameters.
#[derive(Debug, Clone)]
pub struct AddressSpec {
    /// Message delivery policy
    pub routing_policy: RoutingPolicy,
    
    /// Address queue capacity (number of messages)
    pub queue_capacity: usize,
}

impl AddressSpec {
    /// Exclusive address with the given capacity
    pub fn exclusive(queue_capacity: usize) -> Self {
        Self {
            routing_policy: RoutingPolicy::Exclusive,
            queue_capacity,
        }
    }
    
    /// LoadBalanced address with the given capacity
    pub fn load_balanced(queue_capacity: usize, claim_capacity: usize) -> Self {
        Self {
            routing_policy: RoutingPolicy::LoadBalanced {claim_capacity},
            queue_capacity,
        }
    }
    
    /// Reply-channel for the client (Exclusive, small capacity)
    pub fn reply_channel(capacity: usize) -> Self {
        Self::exclusive(capacity)
    }
}