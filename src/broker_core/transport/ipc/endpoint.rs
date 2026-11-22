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
    ProtocolError,
    payloads::{PayloadError, CreditGrantPayload, DataPayload},
    build_error_frame,
    create_ack_frame,
};
use super::transport::IPCFramedStream;
use super::super::TransportError;
use super::super::super::l2::{CreditPool, UnackedBuffer, UnackedBufferError, SessionState};
use super::super::super::security::SessionContext;

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
    transport: IPCFramedStream<S>,
    session: Arc<SessionState>,
    unacked_buffer: Arc<UnackedBuffer>,
    outgoing_credit_pool: Arc<CreditPool>, 
}

impl<S> IPCEndpoint<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Creates an endpoint from an authenticated transport and completes
    /// the initial credit exchange.
    pub async fn from_authenticated_transport<C>(
        transport: IPCFramedStream<S>,
        session_ctx: &Arc<C>,
    ) -> Result<Self, TransportError>
    where
        C: SessionContext + ?Sized,
    {
        // 1. Bind shared states (Arc cloning, no allocations)
        let mut endpoint = Self {
            transport,
            session: Arc::clone(session_ctx.l2_state()),
            unacked_buffer: Arc::clone(session_ctx.outgoing_unacked_buffer()),
            outgoing_credit_pool: Arc::clone(session_ctx.outgoing_credit_pool()),
        };

        // 2. Perform the symmetric exchange (using the limit from the context)
        let peer_credits = endpoint.complete_credit_exchange(session_ctx.limits().hot_credit).await?;

        // 3. Update the context: the peer granted us these credits
        session_ctx.grant_outgoing_credits(peer_credits);

        Ok(endpoint)
    }

    /// Completes the initial credit exchange after successful authentication.
    ///
    /// This method performs the bidirectional CreditGrant exchange:
    /// 1. Sends our CreditGrant to the peer.
    /// 2. Waits for the peer's CreditGrant.
    ///
    /// # Protocol Order
    ///
    /// Both sides send their CreditGrant immediately after AuthResult,
    /// then wait for the peer's CreditGrant. The order of receipt is not
    /// guaranteed, so this method handles both cases.
    ///
    /// # Errors
    ///
    /// - `TransportError::Protocol`: If the received frame is not a CreditGrant.
    /// - `TransportError::Io`: If the connection is closed.
    async fn complete_credit_exchange(&mut self, hot_credit: u32) -> Result<u32, TransportError> {
        // 1. Send our CreditGrant
        self.send_credit_grant(
            Bytes::new(), // Global credits
            CreditGrantPayload::CREDIT_TYPE_HOT,
            hot_credit,
        ).await?;

        tracing::debug!(
            ptr = %format_args!("{:p}", self),
            credit_amount = hot_credit,
            "Sent initial CreditGrant to peer"
        );

        // 2. Wait for peer's CreditGrant
        let frame = loop {
            let frame = self.recv().await?
                .ok_or_else(|| TransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "Connection closed while waiting for peer CreditGrant",
                )))?;

            if frame.frame_type == FrameType::Ping as u8 {
                let mut pong_payload = BytesMut::with_capacity(frame.payload.len());
                pong_payload.put_slice(&frame.payload);
                let pong_frame = Frame::new(FrameType::Pong as u8, pong_payload.freeze());
                self.send_raw_frame(pong_frame).await?;
                tracing::debug!("Replied with Pong during CreditGrant");
                continue;
            }

            break frame;
        };

        match frame {
            frame if frame.frame_type == FrameType::CreditGrant as u8 => {
                let mut payload_buf = frame.payload.clone();
                let peer_cg = CreditGrantPayload::decode(&mut payload_buf)
                    .map_err(|e| TransportError::Protocol(e.into()))?;
                
                tracing::debug!(
                    ptr = %format_args!("{:p}", self),
                    peer_credit_amount = peer_cg.credit_amount,
                    peer_credit_type = peer_cg.credit_type,
                    "Received initial CreditGrant from peer"
                );
                
                Ok(peer_cg.credit_amount)
            }
            frame => {
                let err_msg = format!(
                    "Expected CreditGrant (0x02), got frame type 0x{:02x}",
                    frame.frame_type
                );
                tracing::warn!(
                    ptr = %format_args!("{:p}", self),
                    frame_type = frame.frame_type,
                    "Unexpected frame during credit exchange"
                );
                
                // Send Error frame before disconnecting
                let err_frame = build_error_frame(400u16, &err_msg);
                let _ = self.send_raw_frame(err_frame).await;
                
                Err(ProtocolError::UnexpectedFrame(err_msg.into()).into())
            }
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
        let frame = create_ack_frame(sequence);
        self.transport.send(frame).await
    }

    /// Sends an Drop frame (0x04).
    pub async fn send_drop(&mut self, sequence: u64, reason: u8) -> Result<(), TransportError> {
        let mut payload = BytesMut::with_capacity(9);
        payload.put_u64_le(sequence);
        payload.put_u8(reason);
        let frame = Frame::new(FrameType::Drop as u8, payload.freeze());
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
    /// 4. **Transmission**: Sends the frame over the underlying stream via `IPCFramedStream::send()`.
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
        // 1. Quick check WITHOUT allocations  (Local backpressure)
        let next_seq = self.session.peek_next_sequence();
        if self.unacked_buffer.is_full() {
            return Err(UnackedBufferError::SlotOccupied { 
                sequence: next_seq, 
                index: (next_seq % self.unacked_buffer.capacity() as u64) as usize 
            }.into());
        }

        // 2. Check and consume outgoing credit
        if !self.outgoing_credit_pool.consume() {
            return Err(ProtocolError::UnexpectedFrame("Client exceeded hot credit limit".to_string()).into());
        }

        // 3. Only now do we allocate
        let seq = self.session.next_sequence();
        let payload = DataPayload::new(seq, address.clone(), messages);
        let encoded = match payload.encode() {
            Ok(bytes) => bytes,
            Err(e) => {
                self.outgoing_credit_pool.replenish();
                return Err(TransportError::Protocol(e.into()));
            }
        };
        let frame = Frame::data(encoded);

        // 4. Reserve slot in UnackedBuffer FIRST (Backpressure guarantee)
        self.unacked_buffer
            .push(seq, frame.clone())
            .map_err(|e| {
                self.outgoing_credit_pool.replenish();
                debug_assert!(
                    !matches!(e, UnackedBufferError::SequenceTooOld { .. }),
                    "Protocol violation: attempted to send sequence {} which is <= base_sequence",
                    seq
                );
                TransportError::UnackedBuffer(e)
            })?;

        // 5. Attempt to send over the network
        if let Err(e) = self.transport.send(frame).await {
            // 5. ROLLBACK: If send fails, remove the frame from the buffer.
            // This prevents a "phantom" frame from permanently occupying a slot
            // and blocking the hot_credit window.
            self.unacked_buffer.remove(seq);
            self.outgoing_credit_pool.replenish();
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
        let result = self.unacked_buffer.acknowledge(sequence);
        if result {
            self.outgoing_credit_pool.replenish();
        }
        result
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

    /// Returns a reference to the outgoing credit pool.
    pub fn outgoing_credit_pool(&self) -> &Arc<CreditPool> {
        &self.outgoing_credit_pool
    }

    /// Returns the number of outgoing credits currently available.
    pub fn outgoing_credits_available(&self) -> u32 {
        self.outgoing_credit_pool.available()
    }

    /// Consumes the endpoint and returns the underlying stream.
    pub fn into_inner(self) -> S {
        self.transport.into_inner()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tokio::net::UnixListener;

    use super::*;
    use super::super::IPCConnector;
    use crate::broker_core::l2::SessionState;
    use crate::broker_core::security::{ServerSessionContext, ClientSessionContext};
    use crate::protocol::ServerLimits;

    #[tokio::test]
    async fn test_complete_credit_exchange() {
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.endpoint.sock");
        let socket_path_clone = socket_path.clone();

        // Create session contexts for client and server
        let limits = ServerLimits {
            hot_credit: 10,
            cold_credit: 5,
            ..ServerLimits::default()
        };
        let limits_clone = limits.clone();

        // === Server side ===
        let server_handle = tokio::spawn(async move {
            let listener = UnixListener::bind(&socket_path_clone).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let transport = IPCFramedStream::new(stream);

            // The server creates the context with 0 outgoing credits (the client hasn't granted any yet)
            let session_ctx = Arc::new(ServerSessionContext::new(
                1,
                [0xAA; 32],
                limits.clone(),
                Arc::new(SessionState::new(1)),
                "".to_string(),
            ));

            let mut endpoint = IPCEndpoint::from_authenticated_transport(transport, &session_ctx).await.expect("Credit synchronization error");

            // Server-side checks
            assert_eq!(endpoint.unacked_buffer().capacity(), limits.hot_credit as usize);
            
            // Verify symmetry: the endpoint and context share the same `Arc<CreditPool>`
            assert!(Arc::ptr_eq(
                endpoint.outgoing_credit_pool(),
                session_ctx.outgoing_credit_pool()
            ));

            (endpoint, session_ctx)

        });  

        // === Client side ===
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let connector = crate::broker_core::transport::UdsConnector::new(&socket_path);
        let stream = connector.connect().await.unwrap();
        let transport = IPCFramedStream::new(stream);               

        // The client creates the context. Outgoing credits = `limits.hot_credit` (what the server will grant)
        let client_ctx = Arc::new(ClientSessionContext::new(
            1,
            [0xBB; 32],
            limits_clone.clone(),
        ));

        let mut client_endpoint = IPCEndpoint::from_authenticated_transport(transport, &client_ctx).await.expect("Credit synchronization error");

        // Client-side checks
        assert_eq!(client_endpoint.unacked_buffer().capacity(), limits_clone.hot_credit as usize);
        assert_eq!(client_ctx.outgoing_credit_pool().available(), limits_clone.hot_credit);

        // Verify symmetry: the endpoint and context share the same `Arc<CreditPool>`
        assert!(Arc::ptr_eq(
            client_endpoint.outgoing_credit_pool(),
            client_ctx.outgoing_credit_pool()
        ));        

        server_handle.await.unwrap();
    }
}
