// arcella-broker/src/broker_core/subscription/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Consumer-side subscription primitives (data plane).
//!
//! # Architectural role
//!
//! This module owns the **read engine** shared by both delivery paths:
//! - in-process: the application calls `Subscriber::recv()` directly;
//! - IPC: the server's egress pump calls `Subscriber::recv()` and wraps the
//!   result into a `Data` frame.
//!
//! Keeping the engine here (rather than in `client`) preserves the dependency
//! direction `client -> broker_core <- server`: the server must never depend on
//! the client frontend.
//!
//! # Control plane / data plane boundary
//!
//! A subscription is a **consumer binding**, not an address. Addresses (with
//! their routing policy and queue capacity) are created and deleted exclusively
//! through the broker control plane. Consequently:
//!
//! - dropping a [`SubscriptionGuard`] **detaches the consumer**;
//! - it never unregisters the address, never closes its queue and never
//!   discards buffered messages;
//! - while no consumer is attached, messages accumulate up to the queue
//!   capacity and then apply backpressure to senders.
//!
//! # Cancellation
//!
//! Each binding carries its own [`CancellationToken`], a child of the address
//! (slot) token. `Subscriber::recv()` selects on it explicitly, so detaching a
//! consumer wakes its pending `recv()` immediately without affecting the
//! address or any other consumer.

mod error;
mod subscription_handle;
mod subscriber;

pub use error::SubscriptionError;
pub use subscription_handle::SubscriptionHandle;
pub use subscriber::{
    Subscriber, 
    RecvError,
    TryRecvError,
};
