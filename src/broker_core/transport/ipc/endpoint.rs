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
//! ```text
//! EndpointStateAccess          — Read-only access to session state (shared by all three)
//!    │
//!    ├─► EndpointStateExt      — Convenience methods: acknowledge(), in_flight_count()
//!    │
//!    └─► EndpointWriter        — Adds mutable sender access + payload ceiling
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
//! | 1 | **Single Writer per session direction** — only one `IPCEndpointWriter` exists per session; `&mut self` makes concurrent writes impossible. | L2 §13.2 |
//! | 2 | **Assignment order == write order** — `assign_sequence()`, `UnackedBuffer::push` and the socket write form one critical section inside a single `&mut self` call. | L2 §13.2 |
//! | 3 | **Everything that can fail happens before assignment** — credits, buffer capacity, address/count validation and the payload size gate are all pre-flight. | L2 §13.3 |
//! | 4 | **Fatal after assignment** — any failure past the point of no return drops the `SequenceGuard`, latching `SessionState::broken`. Rollback of slot/credit is resource hygiene, never recovery. | L2 §13.3 |
//! | 5 | **Only `Data` occupies the credit window** — `UnackedBuffer.len() <= hot_credit` is the O(1) memory guarantee. `CreditGrant` consumes a sequence number but no slot. | L2 §8.4, §13.1 |
//!
//! # Known limitations (tracked separately)
//!
//! - `EndpointWriter::sender()` exposes the raw [`FrameSender`]; calling it directly
//!   bypasses invariants 1–4. It is `#[doc(hidden)]` and must be treated as
//!   crate-internal. Closing this at the type level requires moving the high-level
//!   send methods to inherent impls.
//! - `SessionState::broken` currently blocks further sends but does not itself tear
//!   the connection down; the reader/writer loops must observe it.
//! - Receiver-side adjacency verification (§13.4) is not implemented here.

use bytes::Bytes;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

use crate::protocol::{
    Message,
    {Frame, FrameType},
    ProtocolError,
    payloads::{PayloadError, CreditGrantPayload, DataPayload},
    create_ack_frame,
    create_drop_frame,
    create_error_frame,
    FRAME_MAX_PAYLOAD_LENGTH,
    FRAME_OVERHEAD,
};
use super::control::PongEcho;
use super::transport::{IPCFramedStream, IPCFrameReader, IPCFrameWriter};
use super::super::TransportError;
use super::super::super::l2::{CreditPool, UnackedBuffer, UnackedBufferError, SessionState};
use super::super::super::security::SessionContext;

// ============================================================================
// Send-path helpers (L2 §13.3)
// ============================================================================

/// Smallest payload ceiling that can still carry every fixed-size control frame.
///
/// `Pong` (24 bytes) is the largest of them. A negotiated `max_frame_size` that
/// leaves less room is a configuration error, rejected before any I/O happens.
const MIN_PAYLOAD_CEILING: u32 = 24;

/// Converts the negotiated `max_frame_size` (whole frame incl. header + CRC)
/// into the payload ceiling used by **both** the codec and the pre-flight gate.
///
/// Keeping the two in sync is what makes `FrameCodecError::PayloadTooLarge`
/// unreachable from the endpoint API: an oversized frame is rejected *before*
/// a sequence number is assigned, instead of failing inside `start_send`.
#[inline]
fn payload_ceiling(max_frame_size: u32) -> u32 {
    max_frame_size
        .saturating_sub(FRAME_OVERHEAD as u32)
        .min(FRAME_MAX_PAYLOAD_LENGTH)
}

/// Legal rejection: the sequence space is already broken, nothing was assigned.
#[inline]
fn broken_error() -> TransportError {
    TransportError::Session(
        "L2 session sequence space is broken; connection must be closed".to_string(),
    )
}

/// Rejects a send on a session whose outgoing sequence space may contain a hole.
///
/// Called *before* sequence assignment, therefore it is a legal non-fatal rejection.
///
/// Deliberately a free function, not a trait method: it must be callable from any
/// context that can reach `&SessionState` (endpoint, writer half, reader half)
/// without requiring extra trait bounds on `Self`.
#[inline]
fn ensure_sendable(session: &SessionState) -> Result<(), TransportError> {
    if session.is_broken() {
        return Err(broken_error());
    }
    Ok(())
}

/// Latches the session as fatally broken and returns the original error.
///
/// `FramedWrite::send` = feed + flush, and flush writes bytes incrementally.
/// A failure mid-flush can leave a partial frame on the wire, so the byte stream
/// is unusable even when no sequence number was assigned (Ack/Drop/Pong).
/// Teardown + Resume is the only legal recovery.
#[inline]
fn latch_broken(session: &SessionState, e: TransportError) -> TransportError {
    session.mark_broken();
    e
}

/// Fatal rejection **after** a sequence number was assigned (L2 §13.3).
///
/// The caller drops `SequenceGuard` without `commit()`, which latches `broken`.
/// The returned class MUST NOT be one that L3 interprets as retryable
/// backpressure: the number is burned, and the only legal recovery is
/// teardown + Resume.
#[inline]
fn fatal_after_seq(context: &str) -> TransportError {
    TransportError::Session(format!(
        "fatal after sequence assignment ({context}); connection must be closed and resumed"
    ))
}

/// Pre-flight size rejection. Never consumes a sequence number.
#[inline]
fn frame_too_large(kind: &str, encoded_len: usize, ceiling: u32) -> TransportError {
    ProtocolError::UnexpectedFrame(format!(
        "{kind} payload {encoded_len} B exceeds negotiated payload ceiling {ceiling} B"
    ))
    .into()
}

/// Frame types that consume `session_sequence` (L2 §13.1). They MUST go through
/// `assign_sequence()` and can never be emitted via `send_raw_frame`.
#[inline]
fn consumes_sequence(frame_type: u8) -> bool {
    frame_type == FrameType::Data as u8 || frame_type == FrameType::CreditGrant as u8
}

// ============================================================================
// WriteGuard
// ============================================================================

/// Latches the session as broken when a non-sequenced write is cancelled.
///
/// `Ack`/`Drop`/`Pong` carry no `SequenceGuard`, but `FramedWrite::send` is
/// still feed + flush: dropping the future mid-flush leaves a partial frame
/// on the wire. This guard extends the `latch_broken` contract from the error
/// path to the cancellation path (L2 §13.3).
struct WriteGuard<'a> {
    session: &'a SessionState,
    committed: bool,
}

impl<'a> WriteGuard<'a> {
    #[inline]
    fn new(session: &'a SessionState) -> Self {
        Self { session, committed: false }
    }

    #[inline]
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.session.mark_broken();
        }
    }
}

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
/// # Thread Safety
///
/// The `send_frame` method requires `&mut self` because the underlying
/// `FramedWrite` implementation maintains internal buffers that are not safe
/// to access concurrently.
///
/// # Error Handling
///
/// Returns [`TransportError`] on I/O or codec failures. This trait performs
/// **no** session-level checks: the caller (`EndpointWriterExt`) owns the
/// §13.3 contract.
pub trait FrameSender: Send {
    fn send_frame(
        &mut self,
        frame: Frame,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;
}

impl<S> FrameSender for IPCFramedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Sends a single raw frame over the transport.
    ///
    /// The frame is encoded by the underlying `FrameCodec` (header + payload + CRC32C)
    /// and written to the byte stream. Note that `send` = feed + flush: a failure
    /// or cancellation mid-flush may leave a partial frame on the wire.
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
/// access to the three pieces of state shared between the reader and writer
/// halves of a session:
///
/// | Component | Role |
/// |-----------|------|
/// | [`SessionState`] | Monotonic `session_sequence` counter, incoming watermark, fatal latch. |
/// | [`UnackedBuffer`] | Ring buffer of sent-but-unacknowledged frames (for retransmission). |
/// | [`CreditPool`] | Outgoing credit counter (flow control, backpressure). |
///
/// # Implementors
///
/// - [`IPCEndpoint`] (handshake phase)
/// - [`IPCEndpointReader`] (data phase, read-half)
/// - [`IPCEndpointWriter`] (data phase, write-half)
///
/// # Thread Safety
///
/// All returned references are to `Arc`-wrapped state, which is internally
/// synchronized. "Read-only" refers to the *reference*: the pointed-to state is
/// mutated through atomics and internal locks (e.g. `acknowledge()`).
pub trait EndpointStateAccess {
    /// Returns a reference to the session state (sequence counters, fatal latch).
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
/// # Key Operations
///
/// - **`acknowledge(sequence)`** — processes an incoming Ack: removes the frame
///   from `UnackedBuffer` and replenishes one outgoing credit.
/// - **`get_frames_for_retransmission()`** — returns all frames still awaiting
///   acknowledgment (used by the retransmission timer / Resume replay).
/// - **`in_flight_count()`** — returns the number of frames currently in flight.
/// - **`outgoing_credits_available()`** — returns the current credit balance.
pub trait EndpointStateExt: EndpointStateAccess {
    /// Processes an incoming Acknowledge for the given sequence number.
    ///
    /// # Behavior
    ///
    /// 1. Looks up the sequence in the `UnackedBuffer`.
    /// 2. If found, removes the frame and **replenishes one outgoing credit**.
    /// 3. Returns `true` if the sequence was found and processed, `false` otherwise
    ///    (e.g., duplicate ack, old sequence).
    ///
    /// # Side Effects
    ///
    /// - Mutates `UnackedBuffer` (internal lock acquired).
    /// - Mutates `CreditPool` (atomic increment, capped at the limit).
    ///
    /// Note that the two mutations are not one atomic transaction: a concurrent
    /// sender may observe a free slot before the credit is returned and fail fast.
    /// That is advisory backpressure, not a protocol error.
    fn acknowledge(&self, sequence: u64) -> bool {
        let result = self.unacked_buffer().acknowledge(sequence);
        if result {
            self.outgoing_credit_pool().replenish();
        }
        result
    }

    /// Returns all frames currently awaiting acknowledgment, ordered by sequence.
    ///
    /// Retransmission must go through the writer's serialization point and must
    /// precede any new Data frame (L2 §13.6, §13.7).
    fn get_frames_for_retransmission(&self) -> Vec<(u64, Frame)> {
        self.unacked_buffer().get_unacked_frames()
    }

    /// Returns the number of frames currently in flight (sent but not acked).
    #[inline]
    fn in_flight_count(&self) -> usize {
        self.unacked_buffer().len()
    }

    /// Returns the number of outgoing credits currently available.
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
/// Extends [`EndpointStateAccess`] with mutable access to a [`FrameSender`] and
/// with the negotiated payload ceiling. This is the dividing line between
/// "can observe state" and "can transmit":
///
/// - [`IPCEndpointReader`] implements `EndpointStateAccess` but **not** `EndpointWriter`.
/// - [`IPCEndpoint`] and [`IPCEndpointWriter`] implement both.
///
/// # Associated Type
///
/// - `IPCEndpoint::Sender` = `IPCFramedStream<S>` (bidirectional)
/// - `IPCEndpointWriter::Sender` = `IPCFrameWriter<WriteHalf<S>>` (write-half)
pub trait EndpointWriter: EndpointStateAccess + Send {
    /// The concrete frame sender type.
    type Sender: FrameSender;

    /// Maximum payload size accepted by the peer, derived from `max_frame_size`.
    ///
    /// Checked **before** sequence assignment so that an oversized frame is a
    /// legal rejection rather than a fatal codec error (L2 §13.3).
    fn max_payload_len(&self) -> u32;

    /// Returns a mutable reference to the underlying frame sender.
    ///
    /// # Warning — internal API
    ///
    /// Raw access bypasses every §13.3 guard: no fatal-latch check, no size gate,
    /// no credit accounting, no sequence assignment. Only `EndpointWriterExt`
    /// may use it. Application code must never call it.
    #[doc(hidden)]
    fn sender(&mut self) -> &mut Self::Sender;
}

// ============================================================================
// EndpointWriterExt
// ============================================================================

/// High-level frame construction and transmission methods.
///
/// # Send pipeline (L2 §13.3)
///
/// ```text
///   ┌── legal rejections (no sequence consumed) ──────────────────┐
///   │ 0. fatal latch check                                        │
///   │ 1. UnackedBuffer capacity (advisory, no allocation)         │
///   │ 2. payload validation (address length, message count, size) │
///   │ 3. negotiated payload ceiling                               │
///   │ 4. outgoing credit                                          │
///   └─────────────────────────────────────────────────────────────┘
///                          │
///              point of no return: assign_sequence()
///                          │
///   ┌── fatal outcomes only ──────────────────────────────────────┐
///   │ 5. infallible encoding                                      │
///   │ 6. UnackedBuffer slot reservation                           │
///   │ 7. socket write                                             │
///   │    any failure => guard dropped => SessionState::broken      │
///   └─────────────────────────────────────────────────────────────┘
/// ```
///
/// # Supported Frame Types
///
/// | Method | Frame Type | Consumes seq | Buffered |
/// |--------|------------|:------------:|:--------:|
/// | `send_credit_grant` | `CreditGrant` (0x02) | yes | no |
/// | `send_ack` | `Acknowledge` (0x03) | no | no |
/// | `send_drop` | `Drop` (0x04) | no | no |
/// | `send_data` | `Data` (0xfe) | yes | yes |
/// | `send_raw_frame` | any non-sequenced | no | no |
///
/// # Backpressure Strategy (L2 §8.4, §13.5)
///
/// `send_data` implements a **fail-fast backpressure** model: a full
/// `UnackedBuffer` or an exhausted credit pool returns an error before any
/// allocation and before the counter moves. This guarantees O(1) memory per
/// session and prevents the writer from overwhelming a slow peer.
pub trait EndpointWriterExt: EndpointWriter {
    /// Sends a CreditGrant frame to the peer.
    ///
    /// Consumes a `session_sequence` but is never acknowledged or buffered
    /// (L2 §13.1). It must therefore pass through the same serialization point
    /// as Data — which it does, since both require `&mut self`.
    ///
    /// # Arguments
    ///
    /// * `address` — Target address (empty `Bytes` for global credits).
    /// * `credit_type` — `CREDIT_TYPE_HOT` or `CREDIT_TYPE_COLD`.
    /// * `credit_amount` — Number of credits being granted.
    ///
    /// # Returns
    ///
    /// The `session_sequence` assigned to this frame.
    fn send_credit_grant(
        &mut self,
        address: Bytes,
        credit_type: u8,
        credit_amount: u32,
    ) -> impl Future<Output = Result<u64, TransportError>> + Send {
        async move {
            // 0. Fatal latch (no sequence consumed).
            ensure_sendable(self.session())?;

            // 1. Pre-flight validation BEFORE assignment: address length + exact size.
            //    `session_sequence` is a placeholder here and is patched below.
            let mut payload = CreditGrantPayload::new(0, address, credit_type, credit_amount);
            let encoded_len = payload
                .validate()
                .map_err(|e| TransportError::Protocol(PayloadError::from(e).into()))?;

            // 2. Size gate: keeps FrameCodec::encode infallible past the counter move.
            if encoded_len as u64 > self.max_payload_len() as u64 {
                return Err(frame_too_large(
                    "CreditGrant",
                    encoded_len,
                    self.max_payload_len(),
                ));
            }

            // 3. Point of no return. `assign_sequence` merges the broken-check and the
            //    increment; `None` is a LEGAL rejection (nothing assigned yet).
            let session = Arc::clone(self.session());
            let guard = session.assign_sequence().ok_or_else(broken_error)?;
            payload.session_sequence = guard.sequence();                

            // 4. Encoding is infallible after validate().
            let frame = Frame::new(FrameType::CreditGrant as u8, payload.encode_validated());

            // 5. Transmit. Any failure past this point is fatal: the guard is dropped
            //    without commit, which latches the session as broken.
            self.sender().send_frame(frame).await?;

            Ok(guard.commit())
        }
    }
			 
    /// Sends an Acknowledge frame for the given sequence number.
    ///
    /// Acks do not consume credits, do not consume a `session_sequence` and are
    /// not stored in the `UnackedBuffer` (fire-and-forget). The payload is a
    /// fixed 8 bytes, always below any valid ceiling.
    fn send_ack(
        &mut self,
        sequence: u64,
    ) -> impl Future<Output = Result<(), TransportError>> + Send {
        async move {
            // 0. Fatal latch (no sequence consumed).
            ensure_sendable(self.session())?;
            
            // 1. Create and send ack frame
            let frame = create_ack_frame(sequence);
            
            let session = Arc::clone(self.session());
            // Covers cancellation too: a dropped future mid-flush may leave a
            // partial frame on the wire.
            let guard = WriteGuard::new(&session);
            self.sender()
                .send_frame(frame)
                .await
                .map_err(|e| latch_broken(&session, e))?;
            guard.commit();
            Ok(())
        }
    }
								  
    /// Sends a Drop frame, signaling that a received frame will not be delivered.
    ///
    /// Does not consume a `session_sequence`; the payload carries the **peer's**
    /// sequence. Fixed 9 bytes, always below any valid ceiling.
    ///
    /// # Arguments
    ///
    /// * `sequence` — The sequence number of the dropped frame.
    /// * `reason` — Drop reason code (0 = ttl_expired, 1 = buffer_overflow,
    ///   2 = address_removed).
    fn send_drop(
        &mut self,
        sequence: u64,
        reason: u8,
    ) -> impl Future<Output = Result<(), TransportError>> + Send {
        async move {
            // 0. Fatal latch (no sequence consumed).
            ensure_sendable(self.session())?;
            
            // 1. Create and send drop frame
            let frame = create_drop_frame(sequence, reason);

            let session = Arc::clone(self.session());
            let guard = WriteGuard::new(&session);
            self.sender()
                .send_frame(frame)
                .await
                .map_err(|e| latch_broken(&session, e))?;
            guard.commit();
            Ok(())
        }
    }
																	 
    /// Sends a Data frame containing one or more L3 messages.
    ///
    /// This is the **primary egress method** and implements the full send pipeline
    /// described in [`EndpointWriterExt`].
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
    /// Legal rejections (counter untouched, session stays usable):
    /// * `TransportError::UnackedBuffer(SlotOccupied)` — buffer is full.
    /// * `TransportError::Protocol(UnexpectedFrame)` — no credits, or payload
    ///   exceeds the negotiated ceiling.
    /// * `TransportError::Protocol(PayloadError)` — address/count validation failed.
    ///
    /// Fatal outcomes (sequence burned, `SessionState::broken` latched):
    /// * `TransportError::Session` — slot reservation failed after assignment.
    /// * `TransportError::FrameCodec` / `TransportError::Io` — write failed.
    ///
    /// # Cancellation
    ///
    /// This future is **not cancel-safe**. Dropping it after the point of no
    /// return drops the `SequenceGuard`, which marks the session broken without
    /// rolling the counter back: the frame may already be partially on the wire.
    /// Callers must not wrap it in `select!`/`timeout`; cancellation belongs on
    /// the command-queue boundary of the writer actor (L2 §13.3, §13.5).
    fn send_data(
        &mut self,
        address: Bytes,
        messages: &[Message],
    ) -> impl Future<Output = Result<u64, TransportError>> + Send {
        async move {
            // 0. Fatal latch (no sequence consumed).
            ensure_sendable(self.session())?;
            
            // 1. Advisory backpressure: retransmission buffer capacity. No allocation.
            if self.unacked_buffer().is_full() {
                let next_seq = self.session().peek_next_sequence();
                return Err(UnackedBufferError::SlotOccupied { 
                    sequence: next_seq, 
                    index: (next_seq % self.unacked_buffer().capacity() as u64) as usize, 
                }.into());
            }

            // 2. Pre-flight payload validation (address length, message count, size).
            let mut payload = DataPayload::new(0, address, messages);
            let encoded_len = payload
                .validate()
                .map_err(|e| TransportError::Protocol(PayloadError::from(e).into()))?;

            // 3. Size gate. Without it FrameCodec::encode fails inside start_send,
            //    i.e. AFTER assignment — a forbidden hole (§13.3).
            if encoded_len as u64 > self.max_payload_len() as u64 {
                return Err(frame_too_large("Data", encoded_len, self.max_payload_len()));
            }

            // 4. Check and consume one outgoing credit.
            if !self.outgoing_credit_pool().consume() {
                return Err(ProtocolError::UnexpectedFrame(
                    "outgoing hot credit exhausted".to_string(),
                ).into());
            }

            // 5. Point of no return: from here on only fatal outcomes are allowed.
            let session = Arc::clone(self.session());
            let guard = match session.assign_sequence() {
                Some(g) => g,
                None => {
                    // Legal rejection: nothing was assigned, give the credit back.
                    self.outgoing_credit_pool().replenish();
                    return Err(broken_error());
                }
            };
            let seq = guard.sequence();
            payload.session_sequence = seq;                

            // 6. Infallible encoding.
            let frame = Frame::data(payload.encode_validated());        

            // 7. Reserve the retransmission slot. A rejection here means an internal
            //    invariant is violated (e.g. ring-index collision with a CreditGrant
            //    sequence). It MUST be reported as fatal, never as retryable
            //    backpressure: the number is already burned.
            if let Err(e) = self.unacked_buffer().push(seq, frame.clone()) {
                self.outgoing_credit_pool().replenish(); // resource hygiene only
                debug_assert!(
                    !matches!(e, UnackedBufferError::SequenceTooOld { .. }),
                    "Protocol violation: attempted to send sequence {} which is <= base_sequence",
                    seq
                );
                tracing::error!(
                    seq,
                    error = %e,
                    "UnackedBuffer rejected an assigned sequence; session becomes fatal"
                );
                // guard drops -> broken
                return Err(fatal_after_seq(&format!("slot reservation failed: {e}")));
            }

            // 8. Transmit. `FramedWrite::send` may have written part of the frame
            //    before failing, so the stream is not reusable: rollback is hygiene,
            //    not recovery.
            if let Err(e) = self.sender().send_frame(frame).await {
                self.unacked_buffer().remove(seq);
                self.outgoing_credit_pool().replenish();
                return Err(e); // guard drops -> broken
            }

            Ok(guard.commit())
        }
    }
			 
    /// Sends a pre-constructed raw frame.
    ///
    /// This is an **escape hatch** for control frames that don't fit the standard
    /// patterns (Ping/Pong, Error, Close, protocol extensions).
    ///
    /// # Restrictions
    ///
    /// Sequence-consuming frames (`Data`, `CreditGrant`) are rejected: they must
    /// go through [`send_data`](Self::send_data) /
    /// [`send_credit_grant`](Self::send_credit_grant), which own the counter.
    ///
    /// The caller remains responsible for credit accounting and for storing the
    /// frame in `UnackedBuffer` if retransmission is required.
    fn send_raw_frame(
        &mut self,
        frame: Frame,
    ) -> impl Future<Output = Result<(), TransportError>> + Send {
        async move {
            // Legal rejection: nothing has been assigned or written yet.
            ensure_sendable(self.session())?;

            if consumes_sequence(frame.frame_type) {
                return Err(TransportError::Session(format!(
                    "send_raw_frame must not carry sequence-consuming frame 0x{:02x}; \
                    use send_data/send_credit_grant",
                    frame.frame_type
                )));
            }

            // Size gate: an oversized control frame is a legal rejection here,
            // instead of a fatal codec error that would kill a healthy session.
            if frame.payload.len() as u64 > self.max_payload_len() as u64 {
                return Err(frame_too_large(
                    "raw frame",
                    frame.payload.len(),
                    self.max_payload_len(),
                ));
            }

            let session = Arc::clone(self.session());
            let guard = WriteGuard::new(&session);
            self.sender()
                .send_frame(frame)
                .await
                .map_err(|e| latch_broken(&session, e))?;
            guard.commit();
            Ok(())
        }
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
/// 2. Dispatches control frames (Ack, CreditGrant, Ping/Pong).
/// 3. Forwards Data frames to the `IngressBridge` for L3 routing.
///
/// # What it CAN do
///
/// - Receive frames from the peer.
/// - Inspect session state (sequence numbers, credits, in-flight count).
/// - Process acknowledgments (via [`EndpointStateExt::acknowledge`]), which
///   mutates the shared buffer and credit pool through internal synchronization.
///
/// # What it CANNOT do
///
/// - Send frames (does not implement [`EndpointWriter`]). Outgoing control
///   frames must be enqueued to the writer actor; the reader must never block
///   on the writer (L2 §13.5.2).
///
/// # Thread Safety
///
/// - **`Send`**: Yes — can be moved into a spawned Tokio task.
/// - **`Sync`**: No — `IPCFrameReader` contains internal buffers.
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
    /// Decodes the frame and verifies the CRC32C checksum.
    ///
    /// # Return Values
    ///
    /// - `Ok(Some(Frame))` — a complete frame was received.
    /// - `Ok(None)` — the peer closed the connection (EOF).
    /// - `Err(TransportError)` — I/O or codec error (fatal).
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
/// `IPCEndpointWriter` is the **Egress exit point** for the data phase and the
/// single serialization point of the outgoing direction (L2 §13.2). It runs in
/// a dedicated Tokio task (the "writer actor") that:
///
/// 1. Receives egress commands from the `EgressBridge` (via a bounded channel).
/// 2. Constructs L2 frames using [`EndpointWriterExt`] methods.
/// 3. Writes frames to the peer over the underlying byte stream.
///
/// # Invariant: Single Writer (L2 §13.2)
///
/// Because `IPCEndpointWriter` is **not `Clone`** and requires `&mut self` for
/// all send operations, concurrent writers for the same session are impossible.
/// Sequence assignment, slot reservation and the socket write therefore happen
/// in one uninterrupted critical section.
///
/// # Thread Safety
///
/// - **`Send`**: Yes. **`Sync`**: No.
pub struct IPCEndpointWriter<S> {
    transport: IPCFrameWriter<WriteHalf<S>>,
    max_payload_len: u32,
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
    fn max_payload_len(&self) -> u32 { self.max_payload_len }

    #[inline]
    fn sender(&mut self) -> &mut Self::Sender { &mut self.transport }
}

// ============================================================================
// IPCEndpoint — high-level session-aware endpoint
// ============================================================================

/// High-level, session-aware IPC endpoint.
///
/// Combines the low-level [`IPCFramedStream`] transport with L2 session state
/// ([`SessionState`], [`UnackedBuffer`], [`CreditPool`]) into a single handle.
///
/// # Architectural Role
///
/// `IPCEndpoint` is the **primary handle during the handshake phase**:
///
/// 1. Complete the authentication handshake (Hello ↔ Welcome ↔ Auth ↔ AuthResult).
/// 2. Perform the initial bidirectional `CreditGrant` exchange.
/// 3. Split into independent [`IPCEndpointReader`] / [`IPCEndpointWriter`] halves.
///
/// After [`split`](Self::split) the `IPCEndpoint` is **consumed**.
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
///        │  • Derives payload ceiling from limits, aligns both codec halves
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
/// The endpoint does **not** own the session state: it holds `Arc` clones of the
/// state stored in the [`SessionContext`]. Changes made by the endpoint are
/// visible through the context, and the state is deallocated when the last `Arc`
/// is dropped (RAII).
pub struct IPCEndpoint<S> {
    transport: IPCFramedStream<S>,
    max_payload_len: u32,
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
    fn max_payload_len(&self) -> u32 { self.max_payload_len }
																						  
    #[inline]
    fn sender(&mut self) -> &mut Self::Sender { &mut self.transport }
}

impl<S> IPCEndpoint<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Creates an endpoint from an authenticated transport.
    ///
    /// This is the **primary constructor** for `IPCEndpoint`. It performs:
    ///
    /// 1. **Bind limits** — derives the payload ceiling from the negotiated
    ///    `max_frame_size` and aligns both codec halves with it, so that the
    ///    sender-side gate and the codec can never disagree (§13.3).
    /// 2. **Bind state** — clones `Arc` handles from the [`SessionContext`].
    /// 3. **Credit exchange** — sends our `CreditGrant` and waits for the peer's,
    ///    answering any `Ping` that arrives in the interim.
    /// 4. **Update context** — stores the peer-granted credit amount.
    ///
    /// # Arguments
    ///
    /// * `transport` — A framed stream that has already completed authentication.
    /// * `session_ctx` — The session context holding the shared state.
    ///
    /// # Errors
    ///
    /// * `TransportError::Session` — negotiated `max_frame_size` is too small to
    ///   carry control frames (rejected before any I/O).
    /// * `TransportError::Protocol` — unexpected frame type during credit exchange.
    /// * `TransportError::Io` — connection closed before the exchange completed.
    pub async fn from_authenticated_transport<C>(
        transport: IPCFramedStream<S>,
        session_ctx: &Arc<C>,
    ) -> Result<Self, TransportError>
    where
        C: SessionContext + ?Sized,
    {
        // 1. Derive and validate the payload ceiling before touching the wire.
        let max_frame_size = session_ctx.limits().max_frame_size;
        let ceiling = payload_ceiling(max_frame_size);
        if ceiling < MIN_PAYLOAD_CEILING {
            return Err(TransportError::Session(format!(
                "negotiated max_frame_size {max_frame_size} B leaves a payload ceiling of \
                 {ceiling} B, which cannot carry control frames (min {MIN_PAYLOAD_CEILING} B)"
            )));
        }

        // Align the codec with the gate. The codec is stateless and no byte has
        // been read yet, so replacing it cannot lose partial frame state.
        let mut transport = transport;
        transport.set_max_payload_size(ceiling);

        // 2. Bind shared state (Arc cloning, no allocations).
        let mut endpoint = Self {
            transport,
            max_payload_len: ceiling,
            session: Arc::clone(session_ctx.l2_state()),
            unacked_buffer: Arc::clone(session_ctx.outgoing_unacked_buffer()),
            outgoing_credit_pool: Arc::clone(session_ctx.outgoing_credit_pool()),
        };

        // 3. Perform the symmetric exchange (using the limit from the context).
        let peer_credits = endpoint
            .complete_credit_exchange(session_ctx.limits().hot_credit)
            .await?;

        // 4. Update the context: the peer granted us these credits.
        session_ctx.grant_outgoing_credits(peer_credits);

        Ok(endpoint)
    }

    /// Completes the initial credit exchange after successful authentication.
    ///
    /// 1. Sends our `CreditGrant` to the peer (consumes our first sequence number).
    /// 2. Waits for the peer's `CreditGrant`, replying to any `Ping` that arrives.
    ///
    /// # Protocol Order
    ///
    /// Both sides send their `CreditGrant` immediately after `AuthResult`, then
    /// wait for the peer's. The order of receipt is not guaranteed by the
    /// transport, so both orderings are handled.
    ///
    /// # Errors
    ///
    /// * `TransportError::Protocol(UnexpectedFrame)` — a frame other than
    ///   `CreditGrant` or `Ping` arrived.
    /// * `TransportError::Io` — connection closed before the peer's grant arrived.
    async fn complete_credit_exchange(&mut self, hot_credit: u32) -> Result<u32, TransportError> {
        // 1. Send our CreditGrant.
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

        loop {
            let frame = self.recv().await?.ok_or_else(|| {
                TransportError::Io(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "Connection closed while waiting for peer CreditGrant",
                ))
            })?;

            if frame.frame_type == FrameType::CreditGrant as u8 {
                let mut payload_buf = frame.payload.clone();
                let peer_cg = CreditGrantPayload::decode(&mut payload_buf)
                    .map_err(|e| TransportError::Protocol(e.into()))?;
                
                // According to L2 §13.1, CreditGrant consumes a session_sequence on the sender side.
                // We must track it in the incoming watermark to ensure adjacency checks (§13.4) 
                // work correctly during the data phase.
                let is_new = self.session.update_last_received(peer_cg.session_sequence);
                if !is_new {
                    tracing::warn!(seq = peer_cg.session_sequence, "Duplicate initial CreditGrant, ignoring");
                    continue; // Wait next frame
                }

                tracing::debug!(peer_credit_amount = peer_cg.credit_amount, "Received initial CreditGrant from peer");
                return Ok(peer_cg.credit_amount);
            }

            // Unexpected frame
            let err_msg = format!("Expected CreditGrant (0x02), got frame type 0x{:02x}", frame.frame_type);
            tracing::warn!(frame_type = frame.frame_type, "Unexpected frame during credit exchange");
            let err_frame = create_error_frame(400u16, &err_msg);
            let _ = self.send_raw_frame(err_frame).await;
            return Err(ProtocolError::UnexpectedFrame(err_msg).into());
        }
    }    

    /// Splits the endpoint into independent reader and writer halves.
    ///
    /// This is the **transition point** from the handshake phase to the data phase:
    ///
    /// - The original `IPCEndpoint` is **consumed**.
    /// - [`IPCEndpointReader`] owns the read-half.
    /// - [`IPCEndpointWriter`] owns the write-half and the payload ceiling.
    /// - Both halves share the session state via `Arc`.
    ///
    /// # When to Call
    ///
    /// Exactly once, immediately after `from_authenticated_transport` succeeded
    /// and the initial `CreditGrant` exchange finished.
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
                max_payload_len: self.max_payload_len,
                session: self.session,
                unacked_buffer: self.unacked_buffer,
                outgoing_credit_pool: self.outgoing_credit_pool,
            },
        )
    }

    /// Receives the next frame from the peer.
    ///
    /// Convenience wrapper around [`IPCFramedStream::recv`], used during the
    /// handshake phase before `split()` is called.
    pub async fn recv(&mut self) -> Result<Option<Frame>, TransportError> {
        self.transport.recv().await
    }    
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tokio::io::{duplex, DuplexStream};
    use tokio::net::UnixListener;

    use super::*;
    use super::super::IPCConnector;
											 
    use crate::broker_core::security::{ServerSessionContext, ClientSessionContext};
    use crate::protocol::test_utils::dummy_in_only_message;
    use crate::protocol::ServerLimits;

    /// Builds a connected endpoint pair over an in-memory duplex stream.
    ///
    /// `buf` is the per-direction duplex capacity: a small value makes socket
    /// writes block deterministically, which is required by the cancellation test.
    async fn duplex_endpoints(
        limits: ServerLimits,
        buf: usize,
    ) -> (
        IPCEndpoint<DuplexStream>,
        Arc<ClientSessionContext>,
        IPCEndpoint<DuplexStream>,
        Arc<ServerSessionContext>,
    ) {
        let (client_io, server_io) = duplex(buf);

        let server_limits = limits.clone();
        let server = tokio::spawn(async move {
            let ctx = Arc::new(ServerSessionContext::new(
                1,
                [0xAA; 32],
                server_limits,
                Arc::new(SessionState::new(1)),
                "".to_string(),
            ));
            let ep = IPCEndpoint::from_authenticated_transport(IPCFramedStream::new(server_io), &ctx)
                .await
                .expect("server credit exchange");
            (ep, ctx)
        });

        let client_ctx = Arc::new(ClientSessionContext::new(1, [0xBB; 32], limits));
        let client_ep =
            IPCEndpoint::from_authenticated_transport(IPCFramedStream::new(client_io), &client_ctx)
                .await
                .expect("client credit exchange");

        let (server_ep, server_ctx) = server.await.unwrap();
        (client_ep, client_ctx, server_ep, server_ctx)
    }

    #[tokio::test]
    async fn test_complete_credit_exchange() {
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.endpoint.sock");
        let socket_path_clone = socket_path.clone();

														
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

            // The server starts with 0 outgoing credits (the client has granted none yet).
            let session_ctx = Arc::new(ServerSessionContext::new(
                1,
                [0xAA; 32],
                limits.clone(),
                Arc::new(SessionState::new(1)),
                "".to_string(),
            ));

            let endpoint =
                IPCEndpoint::from_authenticated_transport(transport, &session_ctx)
                    .await
                    .expect("Credit synchronization error");

								 
            assert_eq!(endpoint.unacked_buffer().capacity(), limits.hot_credit as usize);
			
																						 
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

																											  
        let client_ctx = Arc::new(ClientSessionContext::new(1, [0xBB; 32], limits_clone.clone()));
			  
					   
								 
		   

        let client_endpoint =
            IPCEndpoint::from_authenticated_transport(transport, &client_ctx)
                .await
                .expect("Credit synchronization error");

							 
        assert_eq!(client_endpoint.unacked_buffer().capacity(), limits_clone.hot_credit as usize);
        assert_eq!(client_ctx.outgoing_credit_pool().available(), limits_clone.hot_credit);

																					 
        assert!(Arc::ptr_eq(
            client_endpoint.outgoing_credit_pool(),
            client_ctx.outgoing_credit_pool()
        ));

        server_handle.await.unwrap();
    }

    // ------------------------------------------------------------------
    // L2 §13.3 — no holes in the sequence space
    // ------------------------------------------------------------------

    /// An oversized Data frame is rejected BEFORE assignment:
    /// the counter does not move and the session stays usable.
    #[tokio::test]
    async fn oversized_data_is_rejected_before_sequence() {
        let limits = ServerLimits {
            max_frame_size: 4096,
            hot_credit: 4,
            ..ServerLimits::default()
        };
        let (mut ep, ctx, _peer, _peer_ctx) = duplex_endpoints(limits, 64 * 1024).await;

        let before = ctx.l2_state().current_send_sequence();
        let msgs = [dummy_in_only_message(
            Bytes::from("t"),
            Bytes::from("a"),
            Bytes::from(vec![0u8; 8192]),
        )];

        let err = ep.send_data(Bytes::from("a"), &msgs).await.unwrap_err();
        assert!(matches!(err, TransportError::Protocol(_)), "got {err:?}");
        assert_eq!(ctx.l2_state().current_send_sequence(), before);
        assert!(!ctx.l2_state().is_broken());

        // The session must still be able to send a normal frame.
        let ok_msgs = [dummy_in_only_message(
            Bytes::from("t"),
            Bytes::from("a"),
            Bytes::from("small"),
        )];
        assert!(ep.send_data(Bytes::from("a"), &ok_msgs).await.is_ok());
        assert_eq!(ctx.l2_state().current_send_sequence(), before + 1);
    }

    /// Same guarantee for CreditGrant (address longer than 1024 bytes).
    #[tokio::test]
    async fn oversized_credit_grant_is_rejected_before_sequence() {
        let (mut ep, ctx, _peer, _peer_ctx) =
            duplex_endpoints(ServerLimits::default(), 64 * 1024).await;

        let before = ctx.l2_state().current_send_sequence();
        let long_address = Bytes::from(vec![b'a'; 2048]); // > MAX_CREDIT_GRANT_ADDRESS_LEN

        let err = ep
            .send_credit_grant(long_address, CreditGrantPayload::CREDIT_TYPE_HOT, 1)
            .await
            .unwrap_err();

        assert!(matches!(err, TransportError::Protocol(_)), "got {err:?}");
        assert_eq!(ctx.l2_state().current_send_sequence(), before);
        assert!(!ctx.l2_state().is_broken());
    }

    /// Cancellation AFTER assignment is fatal and observable, never a silent hole.
    #[tokio::test]
    async fn cancelled_send_after_assignment_is_fatal() {
        // Tiny duplex buffer + a peer that never reads => flush cannot complete.
        let (mut ep, ctx, _peer, _peer_ctx) =
            duplex_endpoints(ServerLimits::default(), 64).await;

        let before = ctx.l2_state().current_send_sequence();
        let msgs = [dummy_in_only_message(
            Bytes::from("t"),
            Bytes::from("a"),
            Bytes::from(vec![0u8; 4096]),
        )];

        tokio::select! {
            biased;
            r = ep.send_data(Bytes::from("a"), &msgs) => {
                panic!("send must not complete on a saturated socket: {r:?}");
            }
            _ = std::future::ready(()) => {}
        }

        // The guard was dropped without commit.
        assert!(ctx.l2_state().is_broken());
        // The number is burned, NOT rolled back.
        assert_eq!(ctx.l2_state().current_send_sequence(), before + 1);

        // Every subsequent send fails fast without touching the counter.
        assert!(ep.send_data(Bytes::from("a"), &msgs).await.is_err());
        assert!(ep.send_ack(1).await.is_err());
        assert_eq!(ctx.l2_state().current_send_sequence(), before + 1);
    }

    /// `send_raw_frame` must never carry a sequence-consuming frame.
    #[tokio::test]
    async fn raw_frame_rejects_sequenced_types_without_consuming() {
        let (mut ep, ctx, _peer, _peer_ctx) =
            duplex_endpoints(ServerLimits::default(), 64 * 1024).await;

        let before = ctx.l2_state().current_send_sequence();

        for frame in [
            Frame::data(Bytes::from("x")),
            Frame::new(FrameType::CreditGrant as u8, Bytes::from("x")),
        ] {
            let err = ep.send_raw_frame(frame).await.unwrap_err();
            assert!(matches!(err, TransportError::Session(_)), "got {err:?}");
        }

        assert_eq!(ctx.l2_state().current_send_sequence(), before);
        assert!(!ctx.l2_state().is_broken());
    }

    /// Cancellation of a NON-sequenced send must also latch the session:
    /// `FramedWrite::send` may have written part of the frame before the
    /// future was dropped, and there is no `SequenceGuard` to catch it.
    #[tokio::test]
    async fn cancelled_raw_frame_send_is_fatal() {
        // Tiny duplex buffer + a peer that never reads => flush cannot complete.
        let (mut ep, ctx, _peer, _peer_ctx) =
            duplex_endpoints(ServerLimits::default(), 64).await;

        let before = ctx.l2_state().current_send_sequence();
        // Oversized payload only to saturate the socket; the type is non-sequenced.
        let frame = Frame::new(FrameType::Pong as u8, Bytes::from(vec![0u8; 4096]));

        tokio::select! {
            biased;
            r = ep.send_raw_frame(frame) => {
                panic!("send must not complete on a saturated socket: {r:?}");
            }
            _ = std::future::ready(()) => {}
        }

        assert!(ctx.l2_state().is_broken());
        // No sequence number was consumed by a control frame.
        assert_eq!(ctx.l2_state().current_send_sequence(), before);

        // Every subsequent send fails fast.
        assert!(ep.send_ack(1).await.is_err());
    }    
}
