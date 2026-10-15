// arcella-broker/src/broker_core/transport/in_memory/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

pub mod endpoint;
pub mod transport;

pub use endpoint::InMemoryEndpoint;
pub use transport::InMemoryTransport;

