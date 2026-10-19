// arcella-broker/src/broker_core/transport/ipc/endpoint.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::protocol::{
    Message,
    {Frame, FrameType},
    payloads::DataPayload,
};
use super::super::super::l2::{SessionState, UnackedBuffer};
use super::super::TransportError;
use super::transport::IPCTransport;

// ============================================================================
// IPCEndpoint — high-level session-aware endpoint
// ============================================================================

/// High-level IPC endpoint that combines transport, session state, and unacked buffer.
///
/// Responsible for:
/// - Incrementing `session_sequence` for each outgoing Data frame.
/// - Storing sent frames in `UnackedBuffer` until acknowledged.
/// - Providing a clean API for sending Data frames and Acknowledges.
///
/// # Thread safety
///
/// `SessionState` and `UnackedBuffer` are internally synchronized (atomic + mutex),
/// so `IPCEndpoint` can be shared across tasks via `Arc` if needed.
/// However, `send` and `recv` operations require `&mut self` due to the underlying
/// `Framed` stream.
pub struct IPCEndpoint<S> {
    transport: IPCTransport<S>,
    session: Arc<SessionState>,
    unacked_buffer: Arc<UnackedBuffer>,
}

impl<S> IPCEndpoint<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Creates a new endpoint with the given stream and hot credit limit.
    ///
    /// # Arguments
    /// * `stream` — the underlying byte stream.
    /// * `hot_credit` — maximum number of unacknowledged frames (size of `UnackedBuffer`).
    pub fn new(stream: S, hot_credit: usize) -> Self {
        Self {
            transport: IPCTransport::new(stream),
            session: Arc::new(SessionState::new(1)),
            unacked_buffer: Arc::new(UnackedBuffer::new(hot_credit)),
        }
    }

    /// Creates an endpoint with an existing session state (for session resumption).
    pub fn with_session(stream: S, session: Arc<SessionState>, unacked_buffer: Arc<UnackedBuffer>) -> Self {
        Self {
            transport: IPCTransport::new(stream),
            session,
            unacked_buffer,
        }
    }

    /// Sends a Data frame (0xfe) containing one or more L3 messages.
    ///
    /// This method is the primary interface for sending application-level messages
    /// over the IPC transport. It automatically handles session sequencing, payload
    /// encoding, and buffering for retransmission.
    ///
    /// # Automatic Operations
    ///
    /// 1. **Sequence number generation**: Calls `session.next_sequence()` to obtain
    ///    a monotonically increasing sequence number for this frame.
    /// 2. **Payload encoding**: Calls `encode_data_payload(session_sequence, address, messages)`
    ///    to serialize the L3 messages into the Data frame payload format:
    ///    ```text
    ///    - session_sequence: u64 LE
    ///    - address_length: u16 LE
    ///    - address: [u8; address_length]
    ///    - message_count: u16 LE
    ///    - messages: [Message; message_count]
    ///    ```
    /// 3. **Frame creation**: Wraps the encoded payload in a `Frame` with type `0xfe` (Data).
    /// 4. **Buffering**: Stores the frame in `UnackedBuffer` with the mapping
    ///    `session_sequence -> frame` for potential retransmission (if the peer
    ///    does not acknowledge receipt).
    /// 5. **Transmission**: Sends the frame over the underlying stream via `IPCTransport::send()`.
    ///
    /// # Arguments
    ///
    /// * `address` - The destination address (extracted from the L3 messages).
    ///   This is used by the L2 credit system for routing and flow control.
    /// * `messages` - A slice of L3 `Message` instances to be aggregated into
    ///   a single Data frame. This enables coalescing (batching) for efficiency.
    ///
    /// # Returns
    ///
    /// The `session_sequence` (u64) assigned to this frame. This value can be
    /// used to track the frame's status (e.g., waiting for acknowledgment).
    ///
    /// # Error Handling
    ///
    /// - `TransportError::DataFrame(err)`: If the payload encoding fails (e.g.,
    ///   address too long).
    /// - `TransportError::FrameCodec(err)`: If the frame encoding fails (e.g.,
    ///   payload too large).
    /// - `TransportError::Io(io_error)`: If the stream write fails (e.g., connection
    ///   closed, broken pipe).
    ///
    /// # Backpressure and Flow Control
    ///
    /// The method provides natural backpressure: the size of `UnackedBuffer`
    /// is strictly limited by the `hot_credit` value. If the buffer is full (all frames
    /// are in the in-flight status), the `push` method will return a `BufferFull` error,
    /// preventing unbounded memory growth (O(1) guarantee).
    ///
    /// # Guarantees
    ///
    /// - **Atomic operation**: The frame is either fully sent or an error is returned.
    ///   Partial sends are not possible.
    /// - **Sequence ordering**: Each call to `send_data()` increments the sequence
    ///   number, ensuring strict ordering of frames.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use arcella_broker::protocol::Message;
    /// use bytes::Bytes;
    ///
    /// let messages = vec![
    ///     create_message("test.msg", Bytes::from("hello")),
    ///     create_message("test.msg", Bytes::from("world")),
    /// ];
    ///
    /// let sequence = endpoint.send_data(Bytes::from("worker:1"), &messages).await?;
    /// println!("Sent frame with sequence {}", sequence);
    ///
    /// // Later, when acknowledgment is received:
    /// endpoint.acknowledge(sequence);
    /// ```
    pub async fn send_data(
        &mut self,
        address: Bytes,
        messages: Vec<Message>,
    ) -> Result<u64, TransportError> {
        let seq = self.session.next_sequence();
        let payload = DataPayload::new(seq, address, messages);
        let encoded = payload.encode()?;
        let frame = Frame::data(encoded);

        self.transport.send(frame.clone()).await?;
        self.unacked_buffer
            .push(seq, frame)
            .map_err(TransportError::UnackedBuffer)?;

        Ok(seq)
    }

    /// Sends an Acknowledge frame (0x03) for the given sequence number.
    pub async fn send_ack(&mut self, sequence: u64) -> Result<(), TransportError> {
        let mut payload = BytesMut::with_capacity(8);
        payload.put_u64_le(sequence);
        let frame = Frame::new(FrameType::Acknowledge as u8, payload.freeze());
        self.transport.send(frame).await
    }

    /// Receives the next frame from the peer.
    ///
    /// Returns `Ok(None)` if the connection has been closed.
    pub async fn recv(&mut self) -> Result<Option<Frame>, TransportError> {
        self.transport.recv().await
    }

    /// Acknowledges a received frame, removing it from the local `UnackedBuffer`.
    ///
    /// Returns `true` if the frame was found and removed.
    pub fn acknowledge(&self, sequence: u64) -> bool {
        self.unacked_buffer.acknowledge(sequence)
    }

    /// Returns a reference to the session state.
    pub fn session(&self) -> &Arc<SessionState> {
        &self.session
    }

    /// Returns a reference to the unacked buffer.
    pub fn unacked_buffer(&self) -> &Arc<UnackedBuffer> {
        &self.unacked_buffer
    }

    /// Returns the number of frames currently in flight (sent but not acknowledged).
    pub fn in_flight_count(&self) -> usize {
        self.unacked_buffer.len()
    }

    /// Consumes the endpoint and returns the underlying stream.
    pub fn into_inner(self) -> S {
        self.transport.into_inner()
    }
}
