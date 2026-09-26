// arcella-broker/src/lib.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! High-performance message routing and local recipient registry 
//! for the Arcella WebAssembly application platform.
//! 
//! This crate provides zero-cost intra-process messaging, strict 
//! RAII lifecycle management, and support for wildcard routing.
//! It is designed to serve as the "nervous system" for component 
//! interaction within a single process (Main ↔ Main) or inside 
//! an isolated WebAssembly worker (arcella-worker).
//!
//! # Key Features
//! - **Zero-cost routing**: In-memory message delivery via `tokio::sync::mpsc`.
//! - **RAII Lifecycle Management**: Automatic cleanup of routes upon component termination or panic.
//! - **Wildcard Support**: Pattern matching with `*` (single segment) and `**` (trailing segments).
//! - **Backpressure**: Built-in protection against slow consumers via bounded channels.

#![deny(unsafe_code)]
//#![warn(missing_docs)]

/// Configuration structures for the Arcella broker.
pub mod config;

/// Core of the Arcella message broker.
pub mod broker;

/// Top-level error types for the Arcella broker.
pub mod error;

/// Message protocol definitions, including headers, transfer modes, and validation.
pub mod protocol;

pub mod broker_core;

/// Client frontend for interacting with the Arcella message broker.
#[cfg(feature = "client")]
pub mod client;

/// Server-side components for handling incoming connections (IPC/Network).
#[cfg(feature = "server")]
pub(crate) mod server;

/// Utility functions and helpers for testing the broker.
#[allow(missing_docs)]
#[cfg(test)]
mod test_utils;
