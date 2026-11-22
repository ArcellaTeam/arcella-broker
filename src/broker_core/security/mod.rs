// arcella-broker/src/broker_core/security/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

mod handshake;
mod session_context;

#[cfg(feature = "client")]
mod client_session_context;

#[cfg(feature = "server")]
mod server_session_context;

#[cfg(feature = "server")]
mod session_manager;

pub use handshake::{HandshakeError, HandshakeHandler};
pub use session_context::{SessionContext, SessionContextInner};

#[cfg(feature = "client")]
pub use client_session_context::ClientSessionContext;

#[cfg(feature = "server")]
pub use server_session_context::ServerSessionContext;

#[cfg(feature = "server")]
pub use session_manager::SessionManager;


#[cfg(test)]
mod integration_test;
