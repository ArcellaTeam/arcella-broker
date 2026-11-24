// arcella-broker/src/broker_core/transport/ipc/endpoint.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! High-level, session-aware IPC endpoint layer.
//!
//! This module sits on top of the low-level [`IPCFramedStream`] (transport sub-layer)
//! and adds **session semantics**: sequence numbering, flow control (credits),
//! acknowledgment tracking (`UnackedBuffer`), and structured frame construction.
//!
//! # Architectural Role (L2 Specification)
//!
//! According to the L2 specification, an IPC session progresses through two distinct phases:
//!
//! 1. **Handshake Phase** — a single bidirectional handle ([`IPCEndpoint`]) is used to
//!    exchange `Hello`, `Welcome`, `AuthResponse`, `AuthResult`, and the initial
//!    `CreditGrant` pair. During this phase, concurrent reads and writes share the
//!    same underlying stream through `&mut self`.
//!
//! 2. **Data Phase** — after the handshake completes, the endpoint is **split** into
//!    two independent halves:
//!    - [`IPCEndpointReader`] — dedicated to receiving frames (Ingress direction).
//!    - [`IPCEndpointWriter`] — dedicated to sending frames (Egress direction).
//!
//!    Each half runs in its own Tokio task, eliminating lock contention and enabling
//!    true full-duplex communication.
//!
//! # Lifecycle Diagram
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    HANDSHAKE PHASE                              │
//! │                                                                 │
//! │   IPCEndpoint                                                   │
//! │   ├─ IPCFramedStream (bidirectional)                            │
//! │   ├─ SessionState        (Arc, shared)                          │
//! │   ├─ UnackedBuffer       (Arc, shared)                          │
//! │   └─ CreditPool          (Arc, shared)                          │
//! │                                                                 │
//! │   Flow: Hello → Welcome → AuthResponse → AuthResult             │
//! │         → CreditGrant ↔ CreditGrant                             │
//! └───────────────────────────┬─────────────────────────────────────┘
//!                             │
//!                             │ endpoint.split()
//!                             ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    DATA PHASE                                   │
//! │                                                                 │
//! │   ┌─────────────────────────┐     ┌─────────────────────────┐   │
//! │   │  IPCEndpointReader      │     │  IPCEndpointWriter      │   │
//! │   │  ─────────────────      │     │  ─────────────────      │   │
//! │   │  • IPCFrameReader       │     │  • IPCFrameWriter       │   │
//! │   │  • Arc<SessionState>    │     │  • Arc<SessionState>    │   │
//! │   │  • Arc<UnackedBuffer>   │     │  • Arc<UnackedBuffer>   │   │
//! │   │  • Arc<CreditPool>      │     │  • Arc<CreditPool>      │   │
//! │   │                         │     │                         │   │
//! │   │  Runs in: reader_task   │     │  Runs in: writer_task   │   │
//! │   │  Direction: Ingress     │     │  Direction: Egress      │   │
//! │   └─────────────────────────┘     └─────────────────────────┘   │
//! │                                                                 │
//! │   Both tasks run concurrently, sharing state via Arc.           │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Trait Architecture
//!
//! The module uses a layered trait design to avoid code duplication between
//! [`IPCEndpoint`], [`IPCEndpointReader`], and [`IPCEndpointWriter`]:
//!
//! ```text
//! EndpointStateAccess          — Read-only access to session state (shared by all three)
//!    │
//!    ├─► EndpointStateExt      — Convenience methods: acknowledge(), in_flight_count()
//!    │
//!    └─► EndpointWriter        — Adds mutable sender access (IPCEndpoint + Writer)
//!            │
//!            └─► EndpointWriterExt — High-level send: send_data(), send_ack(), send_credit_grant()
//! ```
//!
//! # Thread Safety
//!
//! - [`SessionState`], [`UnackedBuffer`], and [`CreditPool`] are wrapped in `Arc`
//!   and use internal atomic/mutex synchronization, so they can be safely shared
//!   between the reader and writer tasks.
//! - [`IPCEndpointReader`] and [`IPCEndpointWriter`] themselves are **not `Sync`**
//!   (due to `FramedRead`/`FramedWrite` internal buffers) but **are `Send`**,
//!   so each must be owned by a single Tokio task.
//!
//! # Key Invariants
//!
//! | # | Invariant | Spec Reference |
//! |---|-----------|----------------|
//! | 1 | **Single Writer per session direction** — only one `IPCEndpointWriter` exists per session. | L2 §13.2 |
//! | 2 | **`session_sequence` is assigned in monotonic order** inside `send_data`, matching write order. | L2 §13.3 |
//! | 3 | **Credit consumption precedes allocation** — backpressure is applied before `BytesMut` allocation. | L2 §14.2 |
//! | 4 | **Rollback on send failure** — if `FramedWrite::send` fails, the slot is removed from `UnackedBuffer` and the credit is returned. | L2 §14.5 |

use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

use crate::protocol::{
    Message,
    {Frame, FrameType},
    ProtocolError,
    payloads::{PayloadError, CreditGrantPayload, DataPayload},
    build_error_frame,
    create_ack_frame,
};
use super::transport::{IPCFramedStream, IPCFrameReader, IPCFrameWriter};
use super::super::TransportError;
use super::super::super::l2::{CreditPool, UnackedBuffer, UnackedBufferError, SessionState};
use super::super::super::security::SessionContext;

// ============================================================================
// FrameSender trait
// ============================================================================

/// Low-level trait for sending raw [`Frame`]s over an IPC transport.
///
/// # Purpose
///
/// Provides a uniform interface for frame transmission, abstracting over:
/// - [`IPCFramedStream`] (bidirectional, used during handshake phase)
/// - [`IPCFrameWriter`] (write-half, used during data phase)
///
/// This trait is the foundation of the [`EndpointWriter`] abstraction: any type
/// that can send raw frames can be elevated into a full session-aware writer
/// by implementing [`EndpointWriter`] on top of it.
///
/// # Thread Safety
///
/// The `send_frame` method requires `&mut self` because the underlying
/// `FramedWrite`/`FramedRead` implementations maintain internal buffers that
/// are not safe to access concurrently.
///
/// # Error Handling
///
/// Returns [`TransportError`] on I/O or codec failures. The caller is
/// responsible for deciding whether the error is recoverable or fatal.
#[allow(async_fn_in_trait)]
pub trait FrameSender {
    async fn send_frame(&mut self, frame: Frame) -> Result<(), TransportError>;
}

impl<S> FrameSender for IPCFramedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Sends a single raw frame over the transport.
    ///
    /// The frame is encoded by the underlying [`FrameCodec`] (header + payload + CRC32C)
    /// and written atomically to the byte stream.
    async fn send_frame(&mut self, frame: Frame) -> Result<(), TransportError> {
        self.send(frame).await
    }
}

impl<W> FrameSender for IPCFrameWriter<W>
where
    W: AsyncWrite + Unpin + Send,
{
    async fn send_frame(&mut self, frame: Frame) -> Result<(), TransportError> {
        self.send(frame).await
    }
}

// ============================================================================
// EndpointStateAccess 
// ============================================================================

/// Read-only access to the shared session state.
///
/// # Purpose
///
/// This trait is the **base layer** of the endpoint trait hierarchy. It provides
/// immutable access to the three pieces of state that are shared between the
/// reader and writer halves of a session:
///
/// | Component | Role |
/// |-----------|------|
/// | [`SessionState`] | Monotonic `session_sequence` counter and session metadata. |
/// | [`UnackedBuffer`] | Ring buffer of sent-but-unacknowledged frames (for retransmission). |
/// | [`CreditPool`] | Outgoing credit counter (flow control, backpressure). |
///
/// # Implementors
///
/// This trait is implemented by all three endpoint types:
/// - [`IPCEndpoint`] (handshake phase)
/// - [`IPCEndpointReader`] (data phase, read-half)
/// - [`IPCEndpointWriter`] (data phase, write-half)
///
/// This allows generic code to inspect session state regardless of which
/// endpoint variant is in use.
///
/// # Thread Safety
///
/// All returned references are to `Arc`-wrapped state, which is internally
/// synchronized. The references themselves are bound to `&self`, so they
/// cannot outlive the borrow.
pub trait EndpointStateAccess {
    /// Returns a reference to the session state (sequence counter, etc.).
    fn session(&self) -> &Arc<SessionState>;
    /// Returns a reference to the outgoing unacked frame buffer.
    fn unacked_buffer(&self) -> &Arc<UnackedBuffer>;
    /// Returns a reference to the outgoing credit pool.
    fn outgoing_credit_pool(&self) -> &Arc<CreditPool>;
}

// ============================================================================
// EndpointStateExt
// ============================================================================

/// Convenience extension methods built on top of [`EndpointStateAccess`].
///
/// # Purpose
///
/// Provides high-level operations that combine multiple pieces of session state.
/// By implementing this trait as a blanket impl over `T: EndpointStateAccess`,
/// the same methods are available on [`IPCEndpoint`], [`IPCEndpointReader`],
/// and [`IPCEndpointWriter`] without code duplication.
///
/// # Key Operations
///
/// - **`acknowledge(sequence)`** — processes an incoming Ack: removes the frame
///   from `UnackedBuffer` and replenishes one outgoing credit.
/// - **`get_frames_for_retransmission()`** — returns all frames still awaiting
///   acknowledgment (used by the retransmission timer).
/// - **`in_flight_count()`** — returns the number of frames currently in flight.
/// - **`outgoing_credits_available()`** — returns the current credit balance.
pub trait EndpointStateExt: EndpointStateAccess {
    /// Processes an incoming Acknowledge for the given sequence number.
    ///
    /// # Behavior
    ///
    /// 1. Looks up the sequence in the `UnackedBuffer`.
    /// 2. If found, removes the frame and **replenishes one outgoing credit**
    ///    (the peer has successfully received it, so we can send another).
    /// 3. Returns `true` if the sequence was found and processed, `false` otherwise
    ///    (e.g., duplicate ack, old sequence).
    ///
    /// # Side Effects
    ///
    /// - Mutates `UnackedBuffer` (internal lock acquired).
    /// - Mutates `CreditPool` (atomic increment).
    fn acknowledge(&self, sequence: u64) -> bool {
        let result = self.unacked_buffer().acknowledge(sequence);
        if result {
            self.outgoing_credit_pool().replenish();
        }
        result
    }

    /// Returns all frames currently awaiting acknowledgment.
    ///
    /// Used by the retransmission timer to resend frames that the peer
    /// has not acknowledged within the timeout window.
    fn get_frames_for_retransmission(&self) -> Vec<(u64, Frame)> {
        self.unacked_buffer().get_unacked_frames()
    }

    /// Returns the number of frames currently in flight (sent but not acked).
    #[inline]
    fn in_flight_count(&self) -> usize {
        self.unacked_buffer().len()
    }

    /// Returns the number of outgoing credits currently available.
    ///
    /// When this reaches zero, the writer must apply backpressure and wait
    /// for incoming Acks to replenish the pool.
    #[inline]
    fn outgoing_credits_available(&self) -> u32 {
        self.outgoing_credit_pool().available()
    }
}

// Blanket implementation: any type with state access gets the convenience methods.
impl<T: EndpointStateAccess> EndpointStateExt for T {}

// ============================================================================
// EndpointWriter
// ============================================================================

/// Trait for endpoint types capable of sending frames.
///
/// # Purpose
///
/// Extends [`EndpointStateAccess`] with mutable access to a [`FrameSender`].
/// This is the dividing line between "can observe state" and "can transmit":
///
/// - [`IPCEndpointReader`] implements `EndpointStateAccess` but **not** `EndpointWriter`.
/// - [`IPCEndpoint`] and [`IPCEndpointWriter`] implement both.
///
/// # Associated Type
///
/// `Sender` is the concrete [`FrameSender`] implementation used by this endpoint:
/// - `IPCEndpoint::Sender` = `IPCFramedStream<S>` (bidirectional)
/// - `IPCEndpointWriter::Sender` = `IPCFrameWriter<WriteHalf<S>>` (write-half)
///
/// This design allows [`EndpointWriterExt`] to provide the same high-level
/// `send_data()`, `send_ack()` methods regardless of which transport variant
/// is being used.
pub trait EndpointWriter: EndpointStateAccess {
    /// The concrete frame sender type.
    type Sender: FrameSender;

    /// Returns a mutable reference to the underlying frame sender.
    fn sender(&mut self) -> &mut Self::Sender;
}

// ============================================================================
// EndpointWriterExt
// ============================================================================

/// High-level frame construction and transmission methods.
///
/// # Purpose
///
/// Provides the **application-facing API** for sending L2 frames. Each method
/// handles the complete pipeline:
///
/// 1. **Pre-flight checks** (credit availability, buffer capacity) — fast path,
///    no allocations.
/// 2. **Sequence assignment** (atomic increment of `session_sequence`).
/// 3. **Payload encoding** into the binary wire format.
/// 4. **UnackedBuffer reservation** — slot is claimed before any I/O.
/// 5. **Network transmission** via the underlying [`FrameSender`].
/// 6. **Rollback on failure** — if I/O fails, the slot and credit are released.
///
/// # Supported Frame Types
///
/// | Method | Frame Type | Purpose |
/// |--------|------------|---------|
/// | `send_credit_grant` | `CreditGrant` (0x02) | Grant incoming credits to peer |
/// | `send_ack` | `Acknowledge` (0x03) | Confirm receipt of a Data frame |
/// | `send_drop` | `Drop` (0x04) | Signal that a frame will not be acked |
/// | `send_data` | `Data` (0x01) | Transmit one or more L3 messages |
/// | `send_raw_frame` | *any* | Escape hatch for control frames |
///
/// # Backpressure Strategy (L2 §14)
///
/// The `send_data` method implements a **fail-fast backpressure** model:
/// - If the `UnackedBuffer` is full, it returns an error **before** allocating.
/// - If no outgoing credits are available, it returns an error **before** encoding.
///
/// This guarantees O(1) memory per session and prevents the writer from
/// overwhelming a slow peer.
#[allow(async_fn_in_trait)]
pub trait EndpointWriterExt: EndpointWriter {
    /// Sends a CreditGrant frame to the peer.
    ///
    /// # Arguments
    ///
    /// * `address` — Target address (empty `Bytes` for global credits).
    /// * `credit_type` — `CREDIT_TYPE_HOT` or `CREDIT_TYPE_COLD`.
    /// * `credit_amount` — Number of credits being granted.
    ///
    /// # Returns
    ///
    /// The `session_sequence` assigned to this frame, which can be used
    /// to track its acknowledgment.
    async fn send_credit_grant(
        &mut self,
        address: Bytes,
        credit_type: u8,
        credit_amount: u32,
    ) -> Result<u64, TransportError> {
        let session_sequence = self.session().next_sequence();
        let payload = CreditGrantPayload::new(session_sequence, address, credit_type, credit_amount);
        let encoded = payload.encode().map_err(|e| TransportError::Protocol(PayloadError::from(e).into()))?;
        let frame = Frame::new(FrameType::CreditGrant as u8, encoded);
        self.sender().send_frame(frame).await?;
        Ok(session_sequence)
    }
			 
    /// Sends an Acknowledge frame for the given sequence number.
    ///
    /// This is a lightweight operation: Acks do not consume credits and
    /// are not stored in the `UnackedBuffer` (they are fire-and-forget).
    async fn send_ack(&mut self, sequence: u64) -> Result<(), TransportError> {
        let frame = create_ack_frame(sequence);
        self.sender().send_frame(frame).await
    }
								  
    /// Sends a Drop frame, signaling that a previously sent frame
    /// will not be retransmitted.
    ///
    /// # Arguments
    ///
    /// * `sequence` — The sequence number of the dropped frame.
    /// * `reason` — Drop reason code (defined in L2 spec).
    async fn send_drop(&mut self, sequence: u64, reason: u8) -> Result<(), TransportError> {
        let mut payload = BytesMut::with_capacity(9);
        payload.put_u64_le(sequence);
        payload.put_u8(reason);
        let frame = Frame::new(FrameType::Drop as u8, payload.freeze());
        self.sender().send_frame(frame).await
    }
																	 
    /// Sends a Data frame containing one or more L3 messages.
    ///
    /// This is the **primary egress method** and implements the full
    /// send pipeline with backpressure and rollback guarantees.
    ///
    /// # Pipeline
    ///
    /// 1. **Peek next sequence** (no increment yet) for error reporting.
    /// 2. **Check `UnackedBuffer` capacity** — fail fast if full.
    /// 3. **Consume one outgoing credit** — fail fast if exhausted.
    /// 4. **Assign sequence** (atomic increment).
    /// 5. **Encode `DataPayload`** into binary format.
    /// 6. **Reserve slot** in `UnackedBuffer`.
    /// 7. **Transmit frame** over the network.
    /// 8. **Rollback** on any failure: release slot and credit.
    ///
    /// # Arguments
    ///
    /// * `address` — Destination L3 address (opaque to L2).
    /// * `messages` — Slice of L3 [`Message`]s to bundle into this frame.
    ///
    /// # Returns
    ///
    /// The `session_sequence` assigned to this frame on success.
    ///
    /// # Errors
    ///
    /// * `TransportError::UnackedBuffer(SlotOccupied)` — buffer is full (backpressure).
    /// * `TransportError::Protocol(UnexpectedFrame)` — no outgoing credits available.
    /// * `TransportError::Protocol(PayloadError)` — encoding failed (e.g., oversized payload).
    /// * `TransportError::FrameCodec` / `TransportError::Io` — network transmission failed.
    async fn send_data(
        &mut self,
        address: Bytes,
        messages: &[Message],
    ) -> Result<u64, TransportError> {
        // 1. Quick check WITHOUT allocations (Local backpressure)
        let next_seq = self.session().peek_next_sequence();
        if self.unacked_buffer().is_full() {
            return Err(UnackedBufferError::SlotOccupied { 
                sequence: next_seq, 
                index: (next_seq % self.unacked_buffer().capacity() as u64) as usize 
            }.into());
        }

        // 2. Check and consume outgoing credit
        if !self.outgoing_credit_pool().consume() {
            return Err(ProtocolError::UnexpectedFrame("Client exceeded hot credit limit".to_string()).into());
        }

        // 3. Only now do we allocate
        let seq = self.session().next_sequence();
        let payload = DataPayload::new(seq, address.clone(), messages);
        let encoded = match payload.encode() {
            Ok(bytes) => bytes,
            Err(e) => {
                self.outgoing_credit_pool().replenish();
                return Err(TransportError::Protocol(e.into()));
            }
        };
        let frame = Frame::data(encoded);

        // 4. Reserve slot in UnackedBuffer FIRST (Backpressure guarantee)
        self.unacked_buffer()
            .push(seq, frame.clone())
            .map_err(|e| {
                self.outgoing_credit_pool().replenish();
                debug_assert!(
                    !matches!(e, UnackedBufferError::SequenceTooOld { .. }),
                    "Protocol violation: attempted to send sequence {} which is <= base_sequence",
                    seq
                );
                TransportError::UnackedBuffer(e)
            })?;

        // 5. Attempt to send over the network
        if let Err(e) = self.sender().send_frame(frame).await {
            // ROLLBACK
            self.unacked_buffer().remove(seq);
            self.outgoing_credit_pool().replenish();
            return Err(e);
        }

        Ok(seq)
    }
			 
    /// Sends a pre-constructed raw frame.
    ///
    /// This is an **escape hatch** for control frames that don't fit the
    /// standard patterns (e.g., Ping/Pong, Error, custom protocol extensions).
    ///
    /// # Warning
    ///
    /// The caller is responsible for:
    /// - Assigning the correct `session_sequence` (if applicable).
    /// - Managing credits (this method does NOT consume credits).
    /// - Storing the frame in `UnackedBuffer` (if retransmission is required).
    async fn send_raw_frame(&mut self, frame: Frame) -> Result<(), TransportError> {
        self.sender().send_frame(frame).await
    }
}

// Blanket implementation: any EndpointWriter gets the high-level send methods.
impl<T: EndpointWriter> EndpointWriterExt for T {}

// ============================================================================
// IPCEndpointReader
// ============================================================================

/// Read-half of a session-aware IPC endpoint.
///
/// Created by [`IPCEndpoint::split`], this struct owns the read-half of the
/// framed transport plus `Arc` references to the shared session state.
///
/// # Architectural Role
///
/// `IPCEndpointReader` is the **Ingress entry point** for the data phase.
/// It runs in a dedicated Tokio task (the "reader task") that:
///
/// 1. Continuously receives frames from the peer via [`recv`](Self::recv).
/// 2. Dispatches control frames (Ack, CreditGrant, Ping/Pong) locally.
/// 3. Forwards Data frames to the [`IngressBridge`] for L3 routing.
///
/// # What it CAN do
///
/// - Receive frames from the peer.
/// - Read session state (sequence numbers, credits, in-flight count).
/// - Process acknowledgments (via [`EndpointStateExt::acknowledge`]).
///
/// # What it CANNOT do
///
/// - Send frames (does not implement [`EndpointWriter`]).
/// - Modify session state (all state access is read-only through `Arc`).
///
/// # Thread Safety
///
/// - **`Send`**: Yes — can be moved into a spawned Tokio task.
/// - **`Sync`**: No — `IPCFrameReader` contains internal buffers that are
///   not safe to access from multiple threads concurrently.
///
/// # Typical Usage
///
/// ```rust,ignore
/// let (mut reader, writer) = endpoint.split();
///
/// tokio::spawn(async move {
///     loop {
///         match reader.recv().await {
///             Ok(Some(frame)) => handle_frame(&reader, frame).await,
///             Ok(None) => break, // peer closed
///             Err(e) => { tracing::error!(%e, "Reader error"); break; }
///         }
///     }
/// });
/// ```
pub struct IPCEndpointReader<S> {
    transport: IPCFrameReader<ReadHalf<S>>,
    session: Arc<SessionState>,
    unacked_buffer: Arc<UnackedBuffer>,
    outgoing_credit_pool: Arc<CreditPool>,
}

impl<S> EndpointStateAccess for IPCEndpointReader<S> {
    #[inline]
    fn session(&self) -> &Arc<SessionState> { &self.session }

    #[inline]
    fn unacked_buffer(&self) -> &Arc<UnackedBuffer> { &self.unacked_buffer }

    #[inline]
    fn outgoing_credit_pool(&self) -> &Arc<CreditPool> { &self.outgoing_credit_pool }
}

impl<S> IPCEndpointReader<S>
where
    S: AsyncRead + Unpin + Send,
{
    /// Receives the next frame from the peer.
    ///
    /// This is the main entry point for the reader task's event loop.
    /// It decodes the frame, verifies the CRC32C checksum, and returns
    /// the complete [`Frame`].
    ///
    /// # Return Values
    ///
    /// - `Ok(Some(Frame))` — a complete frame was received.
    /// - `Ok(None)` — the peer closed the connection (EOF).
    /// - `Err(TransportError)` — I/O or codec error (usually fatal).
    pub async fn recv(&mut self) -> Result<Option<Frame>, TransportError> {
        self.transport.recv().await
    }
}

// ============================================================================
// IPCEndpointWriter
// ============================================================================

/// Write-half of a session-aware IPC endpoint.
///
/// Created by [`IPCEndpoint::split`], this struct owns the write-half of the
/// framed transport plus `Arc` references to the shared session state.
///
/// # Architectural Role
///
/// `IPCEndpointWriter` is the **Egress exit point** for the data phase.
/// It runs in a dedicated Tokio task (the "writer task" or "writer actor") that:
///
/// 1. Receives egress commands from the [`EgressBridge`] (via an `mpsc` channel).
/// 2. Constructs L2 frames using [`EndpointWriterExt`] methods.
/// 3. Writes frames to the peer over the underlying byte stream.
///
/// # Invariant: Single Writer (L2 §13.2)
///
/// > "There must be exactly one writer per session direction. This ensures
/// > `session_sequence` assignment order matches write order and prevents
/// > frame interleaving."
///
/// Because `IPCEndpointWriter` is **not `Clone`** and requires `&mut self`
/// for all send operations, it is impossible to have concurrent writers
/// for the same session. This invariant is enforced at compile time.
///
/// # What it CAN do
///
/// - Send Data, Ack, Drop, and CreditGrant frames (via [`EndpointWriterExt`]).
/// - Read session state (for credit checks, in-flight count).
/// - Process retransmissions on timeout.
///
/// # What it CANNOT do
///
/// - Receive frames from the peer.
/// - Be cloned (enforces single-writer invariant).
///
/// # Thread Safety
///
/// - **`Send`**: Yes — can be moved into a spawned Tokio task.
/// - **`Sync`**: No — `IPCFrameWriter` contains internal buffers that are
///   not safe to access from multiple threads concurrently.
///
/// # Typical Usage
///
/// ```rust,ignore
/// let (reader, mut writer) = endpoint.split();
///
/// tokio::spawn(async move {
///     while let Some(cmd) = egress_rx.recv().await {
///         match cmd {
///             EgressCommand::SendData { address, messages } => {
///                 if let Err(e) = writer.send_data(address, &messages).await {
///                     tracing::error!(%e, "Writer send failed");
///                     break;
///                 }
///             }
///             EgressCommand::Shutdown => break,
///         }
///     }
/// });
/// ```
pub struct IPCEndpointWriter<S> {
    transport: IPCFrameWriter<WriteHalf<S>>,
    session: Arc<SessionState>,
    unacked_buffer: Arc<UnackedBuffer>,
    outgoing_credit_pool: Arc<CreditPool>,
}

impl<S> EndpointStateAccess for IPCEndpointWriter<S> {
    #[inline]
    fn session(&self) -> &Arc<SessionState> { &self.session }

    #[inline]
    fn unacked_buffer(&self) -> &Arc<UnackedBuffer> { &self.unacked_buffer }

    #[inline]
    fn outgoing_credit_pool(&self) -> &Arc<CreditPool> { &self.outgoing_credit_pool }
}

impl<S> EndpointWriter for IPCEndpointWriter<S>
where
    S: AsyncWrite + Unpin + Send,
{
    type Sender = IPCFrameWriter<WriteHalf<S>>;

    #[inline]
    fn sender(&mut self) -> &mut Self::Sender { &mut self.transport }
}

// ============================================================================
// IPCEndpoint — high-level session-aware endpoint
// ============================================================================

/// High-level, session-aware IPC endpoint.
///
/// Combines the low-level [`IPCFramedStream`] transport with L2 session state
/// ([`SessionState`], [`UnackedBuffer`], [`CreditPool`]) into a single cohesive
/// handle.
///
/// # Architectural Role
///
/// `IPCEndpoint` is the **primary handle during the handshake phase**. It is used to:
///
/// 1. Complete the authentication handshake (Hello ↔ Welcome ↔ Auth ↔ AuthResult).
/// 2. Perform the initial bidirectional `CreditGrant` exchange.
/// 3. Split into independent [`IPCEndpointReader`] and [`IPCEndpointWriter`] halves
///    for the data phase.
///
/// After [`split`](Self::split) is called, the `IPCEndpoint` is **consumed**
/// and its responsibilities are transferred to the two halves.
///
/// # Lifecycle
///
/// ```text
/// Raw byte stream (UDS/TCP/Pipe)
///        │
///        ▼
/// IPCFramedStream::new(stream)
///        │
///        ▼
/// IPCEndpoint::from_authenticated_transport(transport, ctx)
///        │  • Binds session state (Arc cloning)
///        │  • Performs CreditGrant ↔ CreditGrant exchange
///        │  • Updates ctx with peer-granted credits
///        ▼
/// endpoint.split()
///        │
///        ├──► IPCEndpointReader   ──► reader_task (Ingress)
///        │
///        └──► IPCEndpointWriter   ──► writer_task (Egress)
/// ```
///
/// # Thread Safety
///
/// - **`Send`**: Yes — can be moved between tasks.
/// - **`Sync`**: No — the underlying `IPCFramedStream` requires `&mut self`
///   for `send`/`recv` operations.
///
/// # Relationship to Session Context
///
/// The endpoint does **not** own the session state. Instead, it holds `Arc`
/// clones of the state stored in the [`SessionContext`] (e.g., `ServerSessionContext`
/// or `ClientSessionContext`). This means:
///
/// - Changes to `CreditPool` made by the endpoint are visible through the context.
/// - The context can inspect session state (e.g., `in_flight_count()`) without
///   needing access to the endpoint itself.
/// - When the last `Arc` is dropped, the session state is deallocated (RAII).
pub struct IPCEndpoint<S> {
    transport: IPCFramedStream<S>,
    session: Arc<SessionState>,
    unacked_buffer: Arc<UnackedBuffer>,
    outgoing_credit_pool: Arc<CreditPool>, 
}

impl<S> EndpointStateAccess for IPCEndpoint<S> {
    #[inline]
    fn session(&self) -> &Arc<SessionState> { &self.session }

    #[inline]
    fn unacked_buffer(&self) -> &Arc<UnackedBuffer> { &self.unacked_buffer }

    #[inline]
    fn outgoing_credit_pool(&self) -> &Arc<CreditPool> { &self.outgoing_credit_pool }
}

impl<S> EndpointWriter for IPCEndpoint<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    type Sender = IPCFramedStream<S>;
																						  
    #[inline]
    fn sender(&mut self) -> &mut Self::Sender { &mut self.transport }
}

impl<S> IPCEndpoint<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    ///
    /// This is the **primary constructor** for `IPCEndpoint`. It performs
    /// three steps:
    ///
    /// 1. **Bind state** — clones `Arc` handles to the session state from
    ///    the provided [`SessionContext`]. No heap allocations occur here.
    /// 2. **Credit exchange** — sends our `CreditGrant` to the peer and
    ///    waits for the peer's `CreditGrant`. Handles `Ping` frames that
    ///    may arrive during the wait.
    /// 3. **Update context** — stores the peer-granted credit amount in
    ///    the context, making it visible to the rest of the application.
    ///
    /// # Arguments
    ///
    /// * `transport` — A framed stream that has already completed authentication
    ///   (Hello/Welcome/Auth frames). Typically created by a connection handler.
    /// * `session_ctx` — The session context holding the shared state.
    ///
    /// # Errors
    ///
    /// * `TransportError::Protocol` — unexpected frame type during credit exchange.
    /// * `TransportError::Io` — connection closed before credit exchange completed.
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
    /// Performs the bidirectional `CreditGrant` exchange:
    /// 1. Sends our `CreditGrant` to the peer.
    /// 2. Waits for the peer's `CreditGrant`, replying to any `Ping` frames
    ///    that arrive in the interim.
    ///
    /// # Protocol Order
    ///
    /// Both sides send their `CreditGrant` immediately after `AuthResult`,
    /// then wait for the peer's `CreditGrant`. The order of receipt is not
    /// guaranteed by the transport, so this method handles both orderings.
    ///
    /// # Ping Handling
    ///
    /// Per L2 specification, a `Ping` may arrive at any time, including during
    /// the credit exchange. This method transparently replies with `Pong` and
    /// continues waiting for the expected `CreditGrant`.
    ///
    /// # Errors
    ///
    /// * `TransportError::Protocol(UnexpectedFrame)` — received a frame other
    ///   than `CreditGrant` or `Ping`.
    /// * `TransportError::Io` — connection closed before peer's `CreditGrant` arrived.
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

    /// Splits the endpoint into independent reader and writer halves.
    ///
    /// This is the **transition point** from the handshake phase to the
    /// data phase. After this call:
    ///
    /// - The original `IPCEndpoint` is **consumed** and can no longer be used.
    /// - [`IPCEndpointReader`] owns the read-half and runs in a dedicated task.
    /// - [`IPCEndpointWriter`] owns the write-half and runs in a dedicated task.
    /// - Both halves share the session state via `Arc` (zero-cost cloning).
    ///
    /// # When to Call
    ///
    /// Call this method **exactly once**, immediately after:
    /// 1. `from_authenticated_transport` has completed successfully.
    /// 2. The initial `CreditGrant` exchange has finished.
    /// 3. The session is ready to enter the data phase.
    ///
    /// # Architectural Rationale
    ///
    /// Splitting enables **lock-free full-duplex communication** (L2 §13.5):
    /// - The reader task can block on `recv()` without starving the writer.
    /// - The writer task can block on network I/O without blocking the reader.
    /// - No `Mutex` or `RwLock` is needed for the transport itself.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let endpoint = IPCEndpoint::from_authenticated_transport(transport, &ctx).await?;
    /// let (mut reader, mut writer) = endpoint.split();
    ///
    /// tokio::spawn(async move { reader_loop(&mut reader).await });
    /// tokio::spawn(async move { writer_loop(&mut writer).await });
    /// ```
    pub fn split(self) -> (IPCEndpointReader<S>, IPCEndpointWriter<S>) {
        let (reader, writer) = self.transport.split();
        (
            IPCEndpointReader {
                transport: reader,
                session: Arc::clone(&self.session),
                unacked_buffer: Arc::clone(&self.unacked_buffer),
                outgoing_credit_pool: Arc::clone(&self.outgoing_credit_pool),
            },
            IPCEndpointWriter {
                transport: writer,
                session: self.session,
                unacked_buffer: self.unacked_buffer,
                outgoing_credit_pool: self.outgoing_credit_pool,
            },
        )
    }

    /// Receives the next frame from the peer.
    ///
    /// Convenience wrapper around the underlying [`IPCFramedStream::recv`].
    /// Used primarily during the handshake phase before `split()` is called.
    pub async fn recv(&mut self) -> Result<Option<Frame>, TransportError> {
        self.transport.recv().await
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
