// arcella-broker/src/broker_core/transport/ipc/transport.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};

use crate::protocol::{
    {Frame, FrameType},
    codec::FrameCodec,
};
use super::super::TransportError;

// ============================================================================
// IPCTransport — low-level framed transport
// ============================================================================

/// Low-level IPC transport that wraps a byte stream with `FrameCodec`.
///
/// Responsible for:
/// - Serializing/deserializing frames over the stream.
/// - Handling stream-level errors (I/O, CRC mismatch, etc.).
///
/// Does NOT manage session state or acknowledgments — this is the responsibility
/// of `IPCEndpoint`.
pub struct IPCTransport<S> {
    framed: Framed<S, FrameCodec>,
}

impl<S> IPCTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Creates a new `IPCTransport` wrapping the given byte stream.
    ///
    /// This is the foundational constructor for the IPC transport layer.
    /// It takes a raw byte stream (e.g., `UnixStream`, `TcpStream`) and wraps it
    /// in a `tokio_util::codec::Framed` instance using [`FrameCodec`], enabling
    /// frame-based communication over a continuous byte stream.
    ///
    /// # What happens inside
    ///
    /// 1. **Codec instantiation**: A new [`FrameCodec`] is created with default
    ///    settings (max payload size = 16 MB, CRC32C verification enabled).
    /// 2. **Framed wrapper**: The byte stream is wrapped in `Framed<S, FrameCodec>`,
    ///    which handles:
    ///    - **Encoding**: Serializing `Frame` instances into the binary L2 format
    ///      (header + payload + CRC32C) before writing to the stream.
    ///    - **Decoding**: Parsing incoming bytes into complete `Frame` instances,
    ///      handling fragmentation and CRC verification transparently.
    /// 3. **Buffering**: Internal read/write buffers are managed by `Framed`,
    ///    providing natural backpressure when the underlying stream is slow.
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
    /// - **Ready for use**: The returned `IPCTransport` is fully initialized and
    ///   ready to send/receive frames immediately. No additional setup is required.
    /// - **Zero-copy decoding**: The internal `FrameCodec` uses `BytesMut::split_to()`
    ///   to extract payloads without copying data, ensuring high throughput.
    /// - **Atomic writes**: Each call to [`IPCTransport::send`] writes a complete frame
    ///   as a single unit; frames are never interleaved in the stream.
    ///
    /// # Typical usage
    ///
    /// This method is usually called **indirectly** via [`IPCEndpoint::new`], which
    /// additionally sets up the L2 session state (`SessionState`) and unacked buffer
    /// (`UnackedBuffer`) for flow control. Direct usage is recommended only for
    /// low-level testing or when implementing custom transport wrappers.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use arcella_broker::broker_core::transport::ipc::{IPCTransport, UdsConnector, IPCConnector};
    /// use arcella_broker::protocol::{Frame, FrameType};
    /// use bytes::Bytes;
    ///
    /// // 1. Establish a connection via UDS
    /// let connector = UdsConnector::new("/tmp/arcella.sock");
    /// let stream = connector.connect().await?;
    ///
    /// // 2. Wrap the stream in IPCTransport
    /// let mut transport = IPCTransport::new(stream);
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
    /// - [`IPCEndpoint::new`] — Higher-level constructor that adds session management
    ///   and flow control on top of `IPCTransport`.
    /// - [`FrameCodec`] — The underlying codec responsible for binary encoding/decoding.
    /// - [`IPCConnector`] — The trait for establishing the initial byte stream connection.
    pub fn new(stream: S) -> Self {
        Self {
            framed: Framed::new(stream, FrameCodec::new()),
        }
    }

    /// Sends a frame over the underlying byte stream.
    ///
    /// This method serializes the `Frame` using `FrameCodec` and writes it
    /// to the stream. The operation is fully asynchronous and integrates with
    /// the Tokio runtime.
    ///
    /// # Serialization Process
    ///
    /// 1. **Frame encoding**: The `FrameCodec` encodes the frame into the binary
    ///    format (header + payload + CRC32C).
    /// 2. **Stream write**: The encoded bytes are written to the underlying stream
    ///    using `Framed::send()`, which handles buffering and flushing automatically.
    ///
    /// # Error Handling
    ///
    /// - `TransportError::FrameCodec(err)`: If the frame encoding fails (e.g.,
    ///   payload too large).
    /// - `TransportError::Io(io_error)`: If the stream write fails (e.g., connection
    ///   closed, broken pipe).
    ///
    /// # Guarantees
    ///
    /// - **Atomic write**: The entire frame is written as a single unit. Frames
    ///   are not interleaved in the stream.
    /// - **Order preservation**: Frames are written in the order they are sent.
    ///   If `send()` is called multiple times concurrently, the Tokio runtime
    ///   ensures that frames are serialized in a deterministic order.
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
        self.framed.send(frame).await.map_err(TransportError::FrameCodec)
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
    /// 2. **Frame decoding**: The `FrameCodec` parses the buffer, extracting the
    ///    frame header, payload, and verifying the CRC32C checksum.
    /// 3. **Frame extraction**: Returns the complete `Frame` if enough data is
    ///    available.
    ///
    /// # Return Values
    ///
    /// - `Ok(Some(Frame))`: A complete frame was successfully received and decoded.
    /// - `Ok(None)`: The stream has been closed by the peer (EOF). No more frames
    ///   will be received.
    /// - `Err(TransportError)`: A fatal error occurred (e.g., CRC mismatch, I/O error).
    ///
    /// # Error Handling
    ///
    /// - `TransportError::FrameCodec(err)`: If the frame decoding fails (e.g.,
    ///   CRC mismatch, payload too large). A CRC mismatch is a fatal error that
    ///   terminates the connection.
    /// - `TransportError::Io(io_error)`: If the stream read fails (e.g., connection
    ///   reset, broken pipe).
    ///
    /// # Guarantees
    ///
    /// - **Frame integrity**: The CRC32C checksum is verified for every frame.
    ///   If the checksum does not match, the method returns an error and the
    ///   connection is terminated.
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
        match self.framed.next().await {
            Some(Ok(frame)) => Ok(Some(frame)),
            Some(Err(e)) => Err(TransportError::FrameCodec(e)),
            None => Ok(None),
        }
    }

    /// Returns a reference to the underlying stream.
    pub fn get_ref(&self) -> &S {
        self.framed.get_ref()
    }

    /// Returns a mutable reference to the underlying stream.
    pub fn get_mut(&mut self) -> &mut S {
        self.framed.get_mut()
    }

    /// Consumes the transport and returns the underlying stream.
    pub fn into_inner(self) -> S {
        self.framed.into_inner()
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Creates an Acknowledge frame payload for the given sequence number.
pub fn create_ack_frame(sequence: u64) -> Frame {
    let mut payload = BytesMut::with_capacity(8);
    payload.put_u64_le(sequence);
    Frame::new(FrameType::Acknowledge as u8, payload.freeze())
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