// arcella-broker/src/broker_core/transport/ipc/control.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Non-blocking control command queue: reader task -> writer actor.
//!
//! # Architectural role (L2 spec 13.5)
//!
//! After `IPCEndpoint::split()` the read half cannot transmit: `IPCEndpointReader`
//! deliberately does not implement `EndpointWriter`. Every outgoing frame it
//! needs (`Acknowledge`, `Drop`, `Pong`) is handed to the writer actor through
//! this queue.
//!
//! ```text
//!   reader task                            writer actor
//!   -----------                            ------------
//!   recv() -> Data                         recv() -> ControlCommand
//!   process (Ingress Bridge)                 match cmd {
//!   ctrl.try_send(Ack { seq })                 Ack  => writer.send_ack(seq),
//!   ctrl.try_pong(echo)                        Drop => writer.send_drop(seq, r),
//!                                              Pong => writer.send_raw_frame(..),
//!                                            }
//! ```
//!
//! # Why `try_send` only
//!
//! L2 spec 13.5.2: the reader MUST never await the writer. With both sockets
//! saturated, a reader blocking on a write creates a cyclic wait (both writers
//! blocked on flush, both readers waiting for write capacity) -> deadlock.
//!
//! The underlying [`Sender`] does expose an async `send`. This module wraps it
//! in a newtype with a private field and no `Deref`, so blocking on the queue
//! is not expressible from the reader task.
//!
//! # Capacity and memory (L2 spec 13.5.3, 13.5.4)
//!
//! The reliable lane is bounded by the **incoming** direction's `hot_credit`:
//! the peer may hold at most `hot_credit` unacknowledged `Data` frames, and each
//! produces at most one `Ack`.
//!
//! Commands are `Copy` and fixed-size, so the queue costs
//! `hot_credit * size_of::<ControlCommand>()` bytes (~1.6 KB at the default
//! `hot_credit = 50`), independent of `max_frame_size`. No `Bytes` is queued.
//!
//! # Two internal lanes
//!
//! The 13.5.4 bound accounts only for `Data`-derived `Ack`s. `Ping` is not
//! rate-limited by any credit, so a `Ping` flood sharing one lane could evict
//! `Ack`s. Two lanes sit behind a single API:
//!
//! | Lane | Commands | Capacity | Overflow |
//! |------|----------|----------|----------|
//! | reliable | `Ack`, `Drop` | `incoming hot_credit` | reported to the caller |
//! | best-effort | `Pong` | 1 | silently dropped, counted |
//!
//! `recv()` is `biased` towards the reliable lane (L2 spec 13.5.6). Ordering is
//! unaffected: no `session_sequence` is assigned here or on enqueue.
//!
//! # Overflow policy for the reliable lane
//!
//! Spec 13.5.4 permits dropping an `Ack` on overflow **because the sender
//! retransmits on timeout (13.6)**. Retransmission is not implemented yet, so
//! today a lost `Ack` would pin the peer's `UnackedBuffer` slot forever.
//! Until 13.6 lands, `TrySendError::Full` on `Ack`/`Drop` MUST be treated as
//! fatal by the reader loop (teardown + Resume), not as a legal drop.
//!
//! # What does NOT belong here
//!
//! `CreditGrant` consumes a `session_sequence` and must be coalesced and
//! assigned inside the writer's serialization point (L2 spec 13.2, 13.5.2).
//! It is intentionally not a `ControlCommand`.
//!
//! # Cancellation
//!
//! Both lanes are built on [`Receiver`], which observes the connection's
//! `CancellationToken`. Note that `Receiver::recv` drains already-buffered
//! items before observing cancellation; the writer loop must therefore check
//! the token itself if it needs to stop mid-backlog.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use super::super::channel::{
    create_generic_channel,
    Receiver,
    Sender,
    TrySendError,
};

/// Capacity of the best-effort `Pong` lane.
///
/// A `Pong` is a liveness reply: only the most recent one is useful, and the
/// prober re-sends `Ping` on its own schedule. One slot is enough.
const PONG_LANE_CAPACITY: usize = 1;

// ============================================================================
// PongEcho
// ============================================================================

/// Fixed-size payload of a `Pong` frame (L2 spec 7.12).
///
/// Wire layout: `original_timestamp: u64 LE` + `response_timestamp: u64 LE`
/// + `opaque_data: [u8; 8]` = 24 bytes.
///
/// The `response_timestamp` is supplied by the caller (the reader task, at
/// `Ping` arrival time) so that this module carries no clock dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PongEcho {
    /// Timestamp copied verbatim from the `Ping`.
    pub original_timestamp: u64,
    /// Local timestamp taken when the `Ping` was received.
    pub response_timestamp: u64,
    /// Opaque correlation bytes echoed back to the prober.
    pub opaque_data: [u8; 8],
}

impl PongEcho {
    /// Encoded `Pong` payload size in bytes.
    ///
    /// Matches `MIN_PAYLOAD_CEILING` in `ipc::endpoint`: a negotiated ceiling
    /// below this value cannot carry a `Pong`.
    pub const WIRE_SIZE: usize = 24;

    /// Expected `Ping` payload size (L2 spec 7.11): `u64` + `[u8; 8]`.
    pub const PING_WIRE_SIZE: usize = 16;

    /// Builds the echo from a `Ping` payload.
    ///
    /// Returns `None` if the payload is not exactly [`Self::PING_WIRE_SIZE`]
    /// bytes. A malformed `Ping` is a protocol violation; the caller decides
    /// the reaction (`Error` + teardown per L2 spec 17.7).
    pub fn from_ping_payload(payload: &[u8], response_timestamp: u64) -> Option<Self> {
        if payload.len() != Self::PING_WIRE_SIZE {
            return None;
        }

        let original_timestamp = u64::from_le_bytes(
            payload[0..8]
                .try_into()
                .expect("length checked above to be exactly 16"),
        );

        let mut opaque_data = [0u8; 8];
        opaque_data.copy_from_slice(&payload[8..16]);

        Some(Self {
            original_timestamp,
            response_timestamp,
            opaque_data,
        })
    }

    /// Serializes the payload in Little-Endian order.
    pub fn to_bytes(self) -> [u8; Self::WIRE_SIZE] {
        let mut out = [0u8; Self::WIRE_SIZE];
        out[0..8].copy_from_slice(&self.original_timestamp.to_le_bytes());
        out[8..16].copy_from_slice(&self.response_timestamp.to_le_bytes());
        out[16..24].copy_from_slice(&self.opaque_data);
        out
    }
}

// ============================================================================
// ControlCommand
// ============================================================================

/// A frame the reader needs the writer to emit.
///
/// None of these consume a `session_sequence` (L2 spec 13.1), so enqueueing
/// them never touches the outgoing counter and never creates a hole.
///
/// The enum is `Copy` and fixed-size by construction: queueing a command
/// performs no allocation and cannot inflate memory with peer-controlled
/// payload bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlCommand {
    /// Acknowledge a fully processed incoming `Data` frame (0x03).
    ///
    /// `sequence` is the **peer's** sequence number.
    Ack { sequence: u64 },

    /// Report a discarded incoming `Data` frame (0x04).
    ///
    /// `reason` per L2 spec 7.5: `0 = ttl_expired`, `1 = buffer_overflow`,
    /// `2 = address_removed`.
    Drop { sequence: u64, reason: u8 },

    /// Reply to a `Ping` (0x11). Best-effort lane.
    Pong(PongEcho),
}

// ============================================================================
// ControlTx / ControlRx
// ============================================================================

/// Producer side, owned by the reader task.
///
/// Cheap to clone. The reader is the intended sole producer, but additional
/// producers are memory-safe: control commands carry no sequence number.
///
/// There is deliberately **no** async send method and no access to the inner
/// [`Sender`] (L2 spec 13.5.2).
#[derive(Clone)]
pub struct ControlTx {
    reliable: Sender<ControlCommand>,
    pong: Sender<PongEcho>,
    dropped_pongs: Arc<AtomicU64>,
    cancel_token: CancellationToken,
}

/// Consumer side, owned by the writer actor.
pub struct ControlRx {
    reliable: Receiver<ControlCommand>,
    pong: Receiver<PongEcho>,
    dropped_pongs: Arc<AtomicU64>,
    cancel_token: CancellationToken,
}

/// Creates a bounded control queue bound to the connection's shutdown signal.
///
/// # Arguments
///
/// * `incoming_hot_credit` - `hot_credit` of the **incoming** direction, i.e.
///   the amount this endpoint granted to the peer (L2 spec 13.5.4).
///   `NonZeroUsize` makes a zero capacity unrepresentable.
/// * `cancel_token` - the connection-level token. Both lanes observe it, so
///   [`ControlRx::recv`] wakes up on teardown instead of hanging forever.
///
/// # Panics
///
/// Panics if the capacity exceeds the `tokio` channel limit. `hot_credit`
/// arrives from the peer's `ServerLimits`; validating that value is a separate
/// concern and must happen before the session context is built.
pub fn create_control_channel(
    incoming_hot_credit: NonZeroUsize,
    cancel_token: CancellationToken,
) -> (ControlTx, ControlRx) {
    let (reliable_tx, reliable_rx) = create_generic_channel(incoming_hot_credit.get(), cancel_token.clone());
    let (pong_tx, pong_rx) = create_generic_channel(PONG_LANE_CAPACITY, cancel_token.clone());
    let dropped_pongs = Arc::new(AtomicU64::new(0));

    (
        ControlTx {
            reliable: reliable_tx,
            pong: pong_tx,
            dropped_pongs: Arc::clone(&dropped_pongs),
            cancel_token: cancel_token.clone(),
        },
        ControlRx {
            reliable: reliable_rx,
            pong: pong_rx,
            dropped_pongs,
            cancel_token,
        },
    )
}

impl ControlTx {
    /// Enqueues a command without ever awaiting.
    ///
    /// `Ack`/`Drop` go to the reliable lane, `Pong` to the best-effort lane.
    ///
    /// # Required reaction to failure
    ///
    /// * `Full` on `Pong` - ignorable (see [`Self::try_pong`]).
    /// * `Full` on `Ack`/`Drop` - **fatal until L2 spec 13.6 is implemented**.
    /// * `Disconnected` - the writer actor is gone; always fatal.
    #[must_use = "a rejected Ack/Drop must tear the connection down (L2 13.5.4)"]
    #[inline]
    pub fn try_send(&self, cmd: ControlCommand) -> Result<(), TrySendError<ControlCommand>> {
        match cmd {
            ControlCommand::Pong(echo) => self.pong.try_send(echo).map_err(|e| match e {
                TrySendError::Full(e) => TrySendError::Full(ControlCommand::Pong(e)),
                TrySendError::Disconnected(e) => {
                    TrySendError::Disconnected(ControlCommand::Pong(e))
                }
            }),
            other => self.reliable.try_send(other),
        }
    }

    /// Enqueues a `Pong`, discarding it if the lane is busy.
    ///
    /// Never fails and never blocks. Discards are counted and exposed via
    /// [`Self::dropped_pongs`]. Writer death is detected by the reliable lane
    /// and by the cancellation token, not here.
    #[inline]
    pub fn try_pong(&self, echo: PongEcho) {
        if self.pong.try_send(echo).is_err() {
            self.dropped_pongs.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `true` once the writer actor has dropped its [`ControlRx`].
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.reliable.is_closed()
    }

    /// Number of `Pong` replies discarded since the queue was created.
    #[inline]
    pub fn dropped_pongs(&self) -> u64 {
        self.dropped_pongs.load(Ordering::Relaxed)
    }

    /// The connection shutdown signal this queue is bound to.
    #[inline]
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

impl ControlRx {
    /// Awaits the next command, preferring `Ack`/`Drop` over `Pong`.
    ///
    /// Returns `None` on connection cancellation, or once the producer side is
    /// dropped and both lanes are drained. **Cancel-safe**: dropping this
    /// future never consumes a command.
    ///
    /// `None` means "no more control commands", not "shut down": the writer may
    /// still have egress work. Teardown is driven by the cancellation token.
    pub async fn recv(&mut self) -> Option<ControlCommand> {
        // Split the borrow explicitly: the handler of one branch touches the
        // other lane while its future is still alive.
        let Self { reliable, pong, .. } = self;

        tokio::select! {
            biased;

            cmd = reliable.recv() => match cmd {
                Some(c) => Some(c),
                // Reliable lane closed or cancelled: drain any buffered Pong.
                None => pong.try_recv().ok().map(ControlCommand::Pong),
            },

            echo = pong.recv() => echo.map(ControlCommand::Pong),
        }
    }

    /// Non-blocking drain, for batching several commands before one flush.
    ///
    /// Returns `None` when both lanes are currently empty.
    #[inline]
    pub fn try_recv(&mut self) -> Option<ControlCommand> {
        if let Ok(cmd) = self.reliable.try_recv() {
            return Some(cmd);
        }
        self.pong.try_recv().ok().map(ControlCommand::Pong)
    }

    /// Number of `Pong` replies discarded by the producer.
    #[inline]
    pub fn dropped_pongs(&self) -> u64 {
        self.dropped_pongs.load(Ordering::Relaxed)
    }

    /// The connection shutdown signal this queue is bound to.
    #[inline]
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    fn echo(ts: u64) -> PongEcho {
        PongEcho {
            original_timestamp: ts,
            response_timestamp: ts + 1,
            opaque_data: [0xAB; 8],
        }
    }

    fn queue(capacity: usize) -> (ControlTx, ControlRx, CancellationToken) {
        let token = CancellationToken::new();
        let (tx, rx) = create_control_channel(nz(capacity), token.clone());
        (tx, rx, token)
    }

    // ------------------------------------------------------------------
    // Memory guarantee
    // ------------------------------------------------------------------

    #[test]
    fn command_is_copy_and_fixed_size() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<ControlCommand>();
        // 24-byte PongEcho + discriminant, rounded up.
        assert!(std::mem::size_of::<ControlCommand>() <= 32);
        assert_eq!(PongEcho::WIRE_SIZE, 24);
    }

    // ------------------------------------------------------------------
    // Boundedness
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn reliable_lane_is_bounded_and_reports_full() {
        let (tx, mut rx, _t) = queue(2);

        tx.try_send(ControlCommand::Ack { sequence: 1 }).unwrap();
        tx.try_send(ControlCommand::Ack { sequence: 2 }).unwrap();

        let err = tx
            .try_send(ControlCommand::Ack { sequence: 3 })
            .unwrap_err();
        assert!(matches!(
            err,
            TrySendError::Full(ControlCommand::Ack { sequence: 3 })
        ));

        // FIFO inside the reliable lane.
        assert_eq!(rx.recv().await, Some(ControlCommand::Ack { sequence: 1 }));
        assert_eq!(rx.recv().await, Some(ControlCommand::Ack { sequence: 2 }));
    }

    /// The key invariant: a Ping flood must never consume Ack capacity.
    #[tokio::test]
    async fn pong_flood_never_evicts_acks() {
        let (tx, mut rx, _t) = queue(2);

        for i in 0..1000 {
            tx.try_pong(echo(i));
        }
        assert!(tx.dropped_pongs() >= 999);
        assert_eq!(rx.dropped_pongs(), tx.dropped_pongs());

        // Both reliable slots are still free.
        tx.try_send(ControlCommand::Ack { sequence: 1 }).unwrap();
        tx.try_send(ControlCommand::Drop { sequence: 2, reason: 1 })
            .unwrap();

        assert_eq!(rx.recv().await, Some(ControlCommand::Ack { sequence: 1 }));
        assert_eq!(
            rx.recv().await,
            Some(ControlCommand::Drop { sequence: 2, reason: 1 })
        );
    }

    #[tokio::test]
    async fn reliable_commands_have_priority_over_pong() {
        let (tx, mut rx, _t) = queue(4);

        tx.try_pong(echo(7));
        tx.try_send(ControlCommand::Ack { sequence: 42 }).unwrap();

        assert_eq!(rx.recv().await, Some(ControlCommand::Ack { sequence: 42 }));
        assert_eq!(rx.recv().await, Some(ControlCommand::Pong(echo(7))));
    }

    #[tokio::test]
    async fn try_recv_drains_without_awaiting() {
        let (tx, mut rx, _t) = queue(4);
        assert_eq!(rx.try_recv(), None);

        tx.try_send(ControlCommand::Ack { sequence: 1 }).unwrap();
        tx.try_pong(echo(1));

        assert_eq!(rx.try_recv(), Some(ControlCommand::Ack { sequence: 1 }));
        assert_eq!(rx.try_recv(), Some(ControlCommand::Pong(echo(1))));
        assert_eq!(rx.try_recv(), None);
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn disconnected_when_writer_is_gone() {
        let (tx, rx, _t) = queue(2);
        drop(rx);

        assert!(tx.is_closed());
        let err = tx
            .try_send(ControlCommand::Ack { sequence: 1 })
            .unwrap_err();
        assert!(matches!(err, TrySendError::Disconnected(_)));
    }

    #[tokio::test]
    async fn recv_drains_pong_then_returns_none() {
        let (tx, mut rx, _t) = queue(2);
        tx.try_pong(echo(1));
        drop(tx);

        assert_eq!(rx.recv().await, Some(ControlCommand::Pong(echo(1))));
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn cancellation_wakes_the_writer() {
        let (_tx, mut rx, token) = queue(2);
        token.cancel();
        assert_eq!(rx.recv().await, None);
    }

    // ------------------------------------------------------------------
    // Cancellation safety (the writer selects on recv())
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn recv_is_cancel_safe() {
        let (tx, mut rx, _t) = queue(2);

        tokio::select! {
            biased;
            _ = rx.recv() => panic!("recv must not complete on an empty queue"),
            _ = std::future::ready(()) => {}
        }

        tx.try_send(ControlCommand::Ack { sequence: 9 }).unwrap();
        assert_eq!(rx.recv().await, Some(ControlCommand::Ack { sequence: 9 }));
    }

    // ------------------------------------------------------------------
    // PongEcho
    // ------------------------------------------------------------------

    #[test]
    fn pong_echo_roundtrip() {
        let mut ping = [0u8; 16];
        ping[0..8].copy_from_slice(&1234u64.to_le_bytes());
        ping[8..16].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);

        let e = PongEcho::from_ping_payload(&ping, 5678).unwrap();
        assert_eq!(e.original_timestamp, 1234);
        assert_eq!(e.response_timestamp, 5678);
        assert_eq!(e.opaque_data, [1, 2, 3, 4, 5, 6, 7, 8]);

        let wire = e.to_bytes();
        assert_eq!(&wire[0..8], &1234u64.to_le_bytes());
        assert_eq!(&wire[8..16], &5678u64.to_le_bytes());
        assert_eq!(&wire[16..24], &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn malformed_ping_is_rejected() {
        assert!(PongEcho::from_ping_payload(&[], 0).is_none());
        assert!(PongEcho::from_ping_payload(&[0u8; 15], 0).is_none());
        assert!(PongEcho::from_ping_payload(&[0u8; 17], 0).is_none());
    }
}
