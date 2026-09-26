// arcella-broker/src/broker_core/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

/// Local recipient registry for routing messages to local recipients.
pub mod registry;

/// Abstract transport layer for message routing and delivery.
pub mod transport;

