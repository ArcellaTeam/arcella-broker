// arcella-broker/src/broker_core/transport/ipc/endpoint.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::protocol::{
    Message,
    {Frame, FrameType},
    payloads::{PayloadError, CreditGrantPayload, DataPayload},
};
use super::super::super::l2::{SessionState, UnackedBuffer, UnackedBufferError};
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
    S: AsyncRead + AsyncWrite + Unpin + Send,
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

    /// Creates an endpoint with an existing `IPCTransport` (for saving buffer) 
    /// and with an existing session state (for session resumption).
    pub fn with_transport_and_session(
        transport: IPCTransport<S>,
        session: Arc<SessionState>,
        unacked_buffer: Arc<UnackedBuffer>,
    ) -> Self {
        Self {
            transport,
            session,
            unacked_buffer,
        }
    }    

    /// Sends a CreditGrant frame (0x02) to the peer.
    ///
    /// This method is used to grant credits to the peer, allowing it to send
    /// Data frames for the specified address. CreditGrant frames are sent
    /// immediately after successful authentication to initialize the credit
    /// system, and dynamically during the session to replenish credits.
    ///
    /// # Important
    /// - CreditGrant frames consume a `session_sequence` number (shared with Data frames).
    /// - They are NOT stored in `UnackedBuffer` (no acknowledgment required).
    /// - The `session_sequence` is automatically generated via `session.next_sequence()`.
    ///
    /// # Arguments
    /// * `address` — address prefix for which credits are granted
    /// * `credit_type` — `CreditGrantPayload::CREDIT_TYPE_HOT` or `CREDIT_TYPE_COLD`
    /// * `credit_amount` — number of credits being granted
    ///
    /// # Returns
    /// The `session_sequence` assigned to this CreditGrant frame.
    ///
    /// # Errors
    /// - `TransportError::Protocol(PayloadError::CreditGrant(...))` if encoding fails
    /// - `TransportError::Io(...)` if the stream write fails
    pub async fn send_credit_grant(
        &mut self,
        address: Bytes,
        credit_type: u8,
        credit_amount: u32,
    ) -> Result<u64, TransportError> {
        let session_sequence = self.session.next_sequence();
        let payload = CreditGrantPayload::new(session_sequence, address, credit_type, credit_amount);
        let encoded = payload.encode().map_err(|e| TransportError::Protocol(PayloadError::from(e).into()))?;
        let frame = Frame::new(FrameType::CreditGrant as u8, encoded);
        self.transport.send(frame).await?;
        Ok(session_sequence)
    }

    /// Sends an Acknowledge frame (0x03) for the given sequence number.
    pub async fn send_ack(&mut self, sequence: u64) -> Result<(), TransportError> {
        let mut payload = BytesMut::with_capacity(8);
        payload.put_u64_le(sequence);
        let frame = Frame::new(FrameType::Acknowledge as u8, payload.freeze());
        self.transport.send(frame).await
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
    /// 2. **Payload encoding**: Calls `DataPayload::new` to serialize the L3 messages 
    ///    into the Data frame payload format.
    /// 3. **Buffering (with Rollback guarantee)**: Stores the frame in `UnackedBuffer` 
    ///    with the mapping `session_sequence -> frame`. If the subsequent network send 
    ///    fails, the frame is immediately removed from the buffer to prevent phantom 
    ///    slots from blocking the `hot_credit` window.
    /// 4. **Transmission**: Sends the frame over the underlying stream via `IPCTransport::send()`.
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
    /// - `TransportError::UnackedBuffer(err)`: If the buffer is full or sequence is invalid.
    /// - `TransportError::Io(io_error)`: If the stream write fails (e.g., connection
    ///   closed, broken pipe). In this case, the frame is rolled back from the buffer.
    ///
    /// # Backpressure and Flow Control
    ///
    /// The method provides natural backpressure: the size of `UnackedBuffer`
    /// is strictly limited by the `hot_credit` value. If the buffer is full, the `push` 
    /// method will return an error *before* any I/O occurs, preventing unbounded 
    /// memory growth (O(1) guarantee).
    ///
    /// # Guarantees
    ///
    /// - **Atomic operation**: The frame is either fully sent and buffered, or an 
    ///   error is returned and the buffer state is cleanly rolled back.
    /// - **Sequence ordering**: Each call to `send_data()` increments the sequence
    ///   number, ensuring strict ordering of frames.
    pub async fn send_data(
        &mut self,
        address: Bytes,
        messages: &[Message],
    ) -> Result<u64, TransportError> {
        // 1. Quick check WITHOUT allocations
        let next_seq = self.session.peek_next_sequence();
        if self.unacked_buffer.is_full() {
            return Err(UnackedBufferError::SlotOccupied { 
                sequence: next_seq, 
                index: (next_seq % self.unacked_buffer.capacity() as u64) as usize 
            }.into());
        }

        // 2. Only now do we allocate
        let seq = self.session.next_sequence();
        let payload = DataPayload::new(seq, address.clone(), messages);
        let encoded = payload.encode()?;
        let frame = Frame::data(encoded);

        // 3. Reserve slot in UnackedBuffer FIRST (Backpressure guarantee)
        self.unacked_buffer
            .push(seq, frame.clone())
            .map_err(|e| {
                debug_assert!(
                    !matches!(e, UnackedBufferError::SequenceTooOld { .. }),
                    "Protocol violation: attempted to send sequence {} which is <= base_sequence",
                    seq
                );
                TransportError::UnackedBuffer(e)
            })?;

        // 4. Attempt to send over the network
        if let Err(e) = self.transport.send(frame).await {
            // 5. ROLLBACK: If send fails, remove the frame from the buffer.
            // This prevents a "phantom" frame from permanently occupying a slot
            // and blocking the hot_credit window.
            self.unacked_buffer.remove(seq);
            return Err(e);
        }

        Ok(seq)
    }

    /// Sends a raw frame to the peer without any additional processing.
    ///
    /// This is a low-level method intended for:
    /// - Sending control frames (Hello, Welcome, AuthResponse, AuthResult, etc.)
    /// - Testing and debugging
    /// - Future protocol extensions
    ///
    /// # Important
    /// - The frame is sent as-is, without sequence number assignment or buffering.
    /// - The caller is responsible for ensuring the frame is properly formatted.
    /// - For Data frames, prefer `send_data()` which handles sequencing and buffering.
    ///
    /// # Arguments
    /// * `frame` — the complete frame to send
    pub async fn send_raw_frame(&mut self, frame: Frame) -> Result<(), TransportError> {
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

    pub fn get_frames_for_retransmission(&self) -> Vec<(u64, Frame)> {
        self.unacked_buffer.get_unacked_frames()
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
