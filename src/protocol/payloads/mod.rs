// arcella-broker/src/protocol/payloads/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

mod auth_response;
mod auth_result;
mod data;
mod hello;
mod welcome;

pub use auth_response::*;
pub use auth_result::*;
pub use data::*;
pub use hello::*;
pub use welcome::*;

/// Current L2 protocol version
pub const L2_PROTOCOL_VERSION: u16 = 1;
