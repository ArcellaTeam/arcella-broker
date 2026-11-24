// arcella-broker/src/broker_core/transport/ipc/transport.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Low-level framed IPC transport layer.
//!
//! This module provides the foundational transport primitives for IPC communication
//! in the Arcella broker. It operates at the boundary between raw byte streams
//! (UDS, TCP, Named Pipes) and L2 [`Frame`]s.
//!
//! # Architectural Role
//!
//! According to the L2 Specification:
//! - L2 ensures bidirectional data transfer between two nodes over a single
//!   stream-connection (Client ↔ Server).
//! - L2 does NOT parse the internal structure of L3 Messages — payload is opaque.
//! - L2 provides integrity (CRC32C) and ordering guarantees.
//!
//! This module implements the **transport sub-layer** that handles:
//! 1. Frame encoding/decoding using [`FrameCodec`]
//! 2. Stream fragmentation and reassembly
//! 3. CRC32C verification
//! 4. Full-duplex communication via [`split`](IPCFramedStream::split)
//!
//! # Layer Responsibilities
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │ IPCEndpoint (session state, credits)    │  ← L2 Session Layer
//! ├─────────────────────────────────────────┤
//! │ IPCFramedStream (framing, CRC)          │  ← L2 Transport Layer (this module)
//! ├─────────────────────────────────────────┤
//! │ UDS/TCP/NamedPipe (byte stream)         │  ← Physical Layer
//! └─────────────────────────────────────────┘
//! ```
//!
//! # Key Types
//!
//! - [`IPCFramedStream`]: Bidirectional framed stream (before handshake completion)
//! - [`IPCFrameReader`]: Read-half for receiving frames (after [`split`])
//! - [`IPCFrameWriter`]: Write-half for sending frames (after [`split`])
//!
//! [`split`]: IPCFramedStream::split

use bytes::Buf;
use tokio::io::{split, AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite};
use futures::{SinkExt, StreamExt};

use crate::protocol::{
    {Frame, FrameType},
    codec::FrameCodec,
};
use super::super::TransportError;

// ============================================================================
// IPCFrameReader — read-half of a framed IPC stream
// ============================================================================

/// Read-half of a framed IPC stream.
///
/// Created by [`IPCFramedStream::split`], this type allows receiving frames
/// independently from sending, enabling full-duplex communication without locks.
///
/// # Architectural Role
///
/// After the L2 handshake completes (Hello → Welcome → AuthResponse → AuthResult → CreditGrant),
/// the endpoint is split into reader and writer halves. The reader half runs in a dedicated
/// task that:
/// 1. Receives incoming frames from the peer
/// 2. Processes control frames (Ack, CreditGrant, Ping/Pong)
/// 3. Forwards Data frames to the Ingress Bridge for L3 routing
///
/// # Thread Safety
///
/// `IPCFrameReader` is **not** `Sync` (due to internal buffering in `FramedRead`),
/// but it is `Send`. It should be owned by a single task.
///
/// # Typical Usage
///
/// ```rust,ignore
/// let (mut reader, writer) = transport.split();
///
/// // Run reader in a dedicated task
/// tokio::spawn(async move {
///     while let Some(frame) = reader.recv().await? {
///         // Process incoming frame
///     }
/// });
/// ```
pub struct IPCFrameReader<R> {
    reader: FramedRead<R, FrameCodec>,
}

impl<R> IPCFrameReader<R>
where
    R: AsyncRead + Unpin + Send,
{
    /// Receives the next frame from the stream.
    ///
    /// # Return Values
    ///
    /// - `Ok(Some(Frame))`: A complete frame was successfully received and decoded.
    /// - `Ok(None)`: The stream has been closed by the peer (EOF).
    /// - `Err(TransportError::FrameCodec)`: Decoding failed (e.g., CRC mismatch).
    ///
    /// # Guarantees
    ///
    /// - **Frame integrity**: CRC32C is verified for every frame.
    /// - **Order preservation**: Frames are received in the order they were sent.
    pub async fn recv(&mut self) -> Result<Option<Frame>, TransportError> {
        match self.reader.next().await {
            Some(Ok(frame)) => Ok(Some(frame)),
            Some(Err(e)) => Err(TransportError::FrameCodec(e)),
            None => Ok(None),
        }
    }
}

// ============================================================================
// IPCFrameWriter — write-half of a framed IPC stream
// ============================================================================

/// Write-half of a framed IPC stream.
///
/// Created by [`IPCFramedStream::split`], this type allows sending frames
/// independently from receiving, enabling full-duplex communication without locks.
///
/// # Architectural Role
///
/// After the L2 handshake completes, the endpoint is split into reader and writer halves.
/// The writer half runs in a dedicated task (the "writer actor") that:
/// 1. Receives egress commands from the Egress Bridge (L3 → L2)
/// 2. Assigns `session_sequence` numbers to outgoing Data frames
/// 3. Stores sent frames in `UnackedBuffer` for retransmission
/// 4. Writes frames to the underlying stream
///
/// # Thread Safety
///
/// `IPCFrameWriter` is **not** `Sync` (due to internal buffering in `FramedWrite`),
/// but it is `Send`. It should be owned by a single task (the writer actor).
///
/// # Invariant: Single Writer
///
/// Per L2 Specification §13.2, there must be **exactly one writer** per session direction.
/// This ensures:
/// - `session_sequence` assignment order matches write order
/// - No interleaving of frames from concurrent tasks
/// - Deterministic ordering guarantees
pub struct IPCFrameWriter<W> {
    writer: FramedWrite<W, FrameCodec>,
}

impl<W> IPCFrameWriter<W>
where
    W: AsyncWrite + Unpin + Send,
{
    /// Sends a frame over the underlying byte stream.
    ///
    /// # Serialization Process
    ///
    /// 1. **Frame encoding**: `FrameCodec` encodes the frame (header + payload + CRC32C).
    /// 2. **Stream write**: Encoded bytes are written to the stream via `FramedWrite::send()`.
    ///
    /// # Guarantees
    ///
    /// - **Atomic write**: The entire frame is written as a single unit.
    /// - **Order preservation**: Frames are written in the order they are sent.
    /// - **Backpressure**: Yields if the stream's write buffer is full.
    pub async fn send(&mut self, frame: Frame) -> Result<(), TransportError> {
        self.writer.send(frame).await.map_err(TransportError::FrameCodec)
    }
}

// ============================================================================
// IPCFramedStream — bidirectional framed stream
// ============================================================================

/// Low-level IPC transport that wraps a byte stream with [`FrameCodec`].
///
/// Responsible for:
/// - Serializing/deserializing frames over the stream
/// - Handling stream-level errors (I/O, CRC mismatch)
/// - Providing full-duplex communication via [`split`](IPCFramedStream::split)
///
/// Does **NOT** manage session state or acknowledgments — this is the responsibility
/// of [`IPCEndpoint`].
///
/// # Lifecycle
///
/// ```text
/// 1. Connection established (UDS/TCP/NamedPipe)
/// 2. IPCFramedStream::new(stream) — wraps byte stream
/// 3. Handshake phase: Hello → Welcome → AuthResponse → AuthResult
/// 4. Credit exchange: CreditGrant ↔ CreditGrant
/// 5. IPCEndpoint::split() — creates reader/writer tasks
/// 6. IPCFramedStream consumed, IPCFrameReader/Writer take over
/// ```
pub struct IPCFramedStream<S> {
    reader: FramedRead<ReadHalf<S>, FrameCodec>,
    writer: FramedWrite<WriteHalf<S>, FrameCodec>,    
}

impl<S> IPCFramedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Creates a new `IPCFramedStream` wrapping the given byte stream.
    ///
    /// It takes a raw byte stream and wraps it in `FramedRead`/`FramedWrite`
    /// instances using [`FrameCodec`], enabling frame-based communication.
    ///
    /// # What happens inside
    ///
    /// 1. **Codec instantiation**: A new [`FrameCodec`] is created with default
    ///    settings (max payload = 16 MB, CRC32C verification enabled).
    /// 2. **Stream splitting**: The byte stream is split into read/write halves
    ///    using `tokio::io::split()`, enabling concurrent I/O.
    /// 3. **Framed wrappers**: Each half is wrapped in `FramedRead`/`FramedWrite`,
    ///    which handle encoding/decoding transparently.
    ///
    /// # Arguments
    ///
    /// * `stream` — The underlying byte stream implementing `AsyncRead + AsyncWrite + Unpin + Send`.
    ///   Typical implementations:
    ///   - `tokio::net::UnixStream` (Unix Domain Sockets, for Linux/macOS)
    ///   - `tokio::net::TcpStream` (TCP, for network scenarios)
    ///   - `tokio::net::windows::named_pipe::NamedPipeClient` (Windows Named Pipes)
    ///
    /// # Guarantees
    ///
    /// - **Ready for use**: The returned `IPCFramedStream` is fully initialized.
    /// - **Zero-copy decoding**: `FrameCodec` uses `BytesMut::split_to()` to extract
    ///   payloads without copying data.
    /// - **Atomic writes**: Each call to [`send`](IPCFramedStream::send) writes a complete
    ///   frame as a single unit; frames are never interleaved.
    ///
    /// # Typical Usage
    ///
    /// This method is usually called **indirectly** via [`IPCEndpoint::from_authenticated_transport`],
    /// which additionally sets up the L2 session state (`SessionState`) and unacked buffer
    /// (`UnackedBuffer`) for flow control. Direct usage is recommended only for
    /// low-level testing or when implementing custom transport wrappers.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use arcella_broker::broker_core::transport::ipc::{IPCFramedStream, UdsConnector, IPCConnector};
    /// use arcella_broker::protocol::{Frame, FrameType};
    /// use bytes::Bytes;
    ///
    /// // 1. Establish a connection via UDS
    /// let connector = UdsConnector::new("/tmp/arcella.sock");
    /// let stream = connector.connect().await?;
    ///
    /// // 2. Wrap the stream in IPCFramedStream
    /// let mut transport = IPCFramedStream::new(stream);
    ///
    /// // 3. Send and receive frames
    /// let frame = Frame::new(FrameType::Data as u8, Bytes::from("hello"));
    /// transport.send(frame).await?;
    ///
    /// if let Some(received) = transport.recv().await? {
    ///     println!("Received frame: type={}", received.frame_type);
    /// }
    /// ```
    ///
    /// # See Also
    ///
    /// - [`IPCEndpoint::from_authenticated_transport`] — Higher-level constructor that adds
    ///   session management and flow control.
    /// - [`FrameCodec`] — The underlying codec responsible for binary encoding/decoding.
    /// - [`IPCConnector`] — The trait for establishing the initial byte stream connection.
    pub fn new(stream: S) -> Self {
        let codec = FrameCodec::new();
        let (read_half, write_half) = split(stream);
        Self {
            reader: FramedRead::new(read_half, codec.clone()),
            writer: FramedWrite::new(write_half, codec),
        }
    }

    /// Splits the stream into independent read and write halves.
    ///
    /// This is a **critical operation** that enables full-duplex communication.
    /// After splitting:
    /// - The read-half ([`IPCFrameReader`]) can run in a dedicated "reader task"
    /// - The write-half ([`IPCFrameWriter`]) can run in a dedicated "writer task"
    /// - Both tasks operate concurrently without locks or mutexes
    ///
    /// # Architectural Rationale
    ///
    /// Per L2 Specification §13.5 ("Full-duplex и конкурентная отправка"):
    /// > "Reader and writer halves should be split after handshake completion.
    /// > Each half runs in its own Tokio task, eliminating lock contention."
    ///
    /// # When to Call
    ///
    /// This method should be called **after** the L2 handshake completes:
    /// 1. Hello → Welcome → AuthResponse → AuthResult
    /// 2. CreditGrant ↔ CreditGrant exchange
    /// 3. **Then** split into reader/writer tasks
    ///
    /// # Consumes `self`
    ///
    /// This method consumes the `IPCFramedStream` because the original bidirectional
    /// handle is no longer needed after splitting.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let transport = IPCFramedStream::new(stream);
    /// let endpoint = IPCEndpoint::from_authenticated_transport(transport, &ctx).await?;
    /// let (reader, writer) = endpoint.split();
    ///
    /// // Spawn reader task
    /// tokio::spawn(async move {
    ///     main_loop(reader, ctx, ingress_bridge).await
    /// });
    ///
    /// // Spawn writer task
    /// tokio::spawn(async move {
    ///     writer_task(writer, egress_rx).await
    /// });
    /// ```
    pub fn split(self) -> (IPCFrameReader<ReadHalf<S>>, IPCFrameWriter<WriteHalf<S>>) {
        (
            IPCFrameReader { reader: self.reader },
            IPCFrameWriter { writer: self.writer },
        )
    }

    /// Sends a frame over the underlying byte stream.
    ///
    /// This method serializes the `Frame` using `FrameCodec` and writes it
    /// to the stream. The operation is fully asynchronous and integrates with
    /// the Tokio runtime.
    ///
    /// # Serialization Process
    ///
    /// 1. **Frame encoding**: `FrameCodec` encodes the frame into the binary
    ///    format (header + payload + CRC32C).
    /// 2. **Stream write**: The encoded bytes are written to the underlying stream
    ///    using `FramedWrite::send()`, which handles buffering and flushing automatically.
    ///
    /// # Error Handling
    ///
    /// - `TransportError::FrameCodec(err)`: Frame encoding failed (e.g., payload too large).
    /// - `TransportError::Io(io_error)`: Stream write failed (e.g., connection closed).
    ///
    /// # Guarantees
    ///
    /// - **Atomic write**: The entire frame is written as a single unit.
    /// - **Order preservation**: Frames are written in the order they are sent.
    ///
    /// # Backpressure
    ///
    /// If the underlying stream's write buffer is full, this method will yield
    /// (await) until space becomes available. This provides natural backpressure,
    /// preventing unbounded memory growth.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use arcella_broker::protocol::{Frame, FrameType};
    /// use bytes::Bytes;
    ///
    /// let frame = Frame::new(FrameType::Data as u8, Bytes::from("hello"));
    /// transport.send(frame).await?;
    /// ```
    pub async fn send(&mut self, frame: Frame) -> Result<(), TransportError> {
        self.writer.send(frame).await.map_err(TransportError::FrameCodec)
    }

    /// Receives the next frame from the underlying byte stream.
    ///
    /// This method reads from the stream and decodes the next complete frame
    /// using `FrameCodec`. The operation is fully asynchronous and integrates
    /// with the Tokio runtime.
    ///
    /// # Decoding Process
    ///
    /// 1. **Stream read**: Reads bytes from the underlying stream into an internal
    ///    buffer (`BytesMut`).
    /// 2. **Frame decoding**: `FrameCodec` parses the buffer, extracting the
    ///    frame header, payload, and verifying the CRC32C checksum.
    /// 3. **Frame extraction**: Returns the complete `Frame` if enough data is
    ///    available.
    ///
    /// # Return Values
    ///
    /// - `Ok(Some(Frame))`: A complete frame was successfully received and decoded.
    /// - `Ok(None)`: The stream has been closed by the peer (EOF).
    /// - `Err(TransportError::FrameCodec)`: Decoding failed (e.g., CRC mismatch).
    /// - `Err(TransportError::Io)`: Stream read failed (e.g., connection reset).
    ///
    /// # Error Handling
    ///
    /// - **CRC mismatch**: Fatal error that terminates the connection.
    /// - **Payload too large**: Frame exceeds 16 MB limit.
    ///
    /// # Guarantees
    ///
    /// - **Frame integrity**: CRC32C checksum is verified for every frame.
    /// - **Order preservation**: Frames are received in the order they were sent.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// while let Some(frame) = transport.recv().await? {
    ///     println!("Received frame: type={}, payload={:?}", frame.frame_type, frame.payload);
    /// }
    /// // Stream closed by peer
    /// println!("Connection closed");
    /// ```
    pub async fn recv(&mut self) -> Result<Option<Frame>, TransportError> {
        match self.reader.next().await {
            Some(Ok(frame)) => Ok(Some(frame)),
            Some(Err(e)) => Err(TransportError::FrameCodec(e)),
            None => Ok(None),
        }
    }
}

/// Extracts the sequence number from an Acknowledge frame payload.
pub fn parse_ack_sequence(frame: &Frame) -> Result<u64, TransportError> {
    if frame.frame_type != FrameType::Acknowledge as u8 {
        return Err(TransportError::Session(format!(
            "Expected Acknowledge frame (0x03), got 0x{:02x}",
            frame.frame_type
        )));
    }
    if frame.payload.len() < 8 {
        return Err(TransportError::Session(
            "Acknowledge payload too short".to_string(),
        ));
    }
    let mut buf = frame.payload.as_ref();
    Ok(buf.get_u64_le())
}
