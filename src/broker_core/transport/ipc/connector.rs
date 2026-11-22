// arcella-broker/src/broker_core/transport/ipc/connector.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::future::Future;

use tokio::io::{AsyncRead, AsyncWrite};

use super::super::TransportError;

// ============================================================================
// IPCConnector trait
// ============================================================================

/// Abstract connector for establishing IPC connections.
///
/// Implementations are responsible for creating the underlying byte stream
/// (Unix Domain Socket, Named Pipe, TCP, etc.). The stream must implement
/// `AsyncRead + AsyncWrite + Unpin + Send` to be compatible with `FrameCodec`.
///
/// # Design notes
///
/// - Uses native `impl Future` (Rust 1.75+) instead of `#[async_trait]` for zero overhead.
/// - Returns a generic `Stream` type, allowing each connector to use its optimal
///   stream implementation (e.g., `UnixStream`, `TcpStream`).
/// - The trait is object-safe only if `Stream` is fixed, but we prefer generics
///   for performance (monomorphization).
pub trait IPCConnector: Send + Sync {
    /// The underlying byte stream type produced by this connector.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    /// Establishes a connection and returns the byte stream.
    fn connect(&self) -> impl Future<Output = Result<Self::Stream, TransportError>> + Send;
}
