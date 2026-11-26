// arcella-broker/src/l2/session.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// State of the L2 Frame session.
///
/// Manages monotonically increasing sequence numbers (`session_sequence`) for frames
/// sent and received within a single L2 session. This is a core component of the
/// flow control and reliability mechanism in the Arcella protocol.
///
/// # Architectural Role
///
/// Each L2 connection (whether in-process, IPC, or network) maintains its own `SessionState`.
/// The session state serves two critical purposes:
///
/// 1. **Outgoing sequence generation**: Provides a thread-safe, lock-free counter for
///    assigning unique sequence numbers to outgoing frames. These numbers are used by
///    the receiver to detect losses, duplicates, and reorderings.
///
/// 2. **Incoming sequence tracking**: Tracks the highest sequence number received from
///    the peer, enabling the detection of out-of-order or duplicate frames.
///
/// # Thread Safety
///
/// All operations are **lock-free** and use atomic instructions (`AtomicU64`) with
/// appropriate memory ordering:
/// - `Ordering::Relaxed` is used for all operations, which is safe because:
///   - Sequence numbers are monotonically increasing (no need for cross-variable synchronization).
///   - The only requirement is atomicity of individual reads/writes, not ordering relative
///     to other memory operations.
/// - This design allows multiple tasks to concurrently generate sequence numbers or
///   update the last-received counter without contention.
///
/// # Invariants
///
/// The following invariants are maintained throughout the session lifecycle:
/// - `last_received_sequence < next_session_sequence` (the last received is always
///   less than the next to be sent, assuming bidirectional communication).
/// - `next_session_sequence` is monotonically increasing (never decreases).
/// - `last_received_sequence` is monotonically increasing (only updated when a higher
///   sequence is received).
///
/// # Example
///
/// ```rust
/// use arcella_broker::broker_core::l2::SessionState;
///
/// let session = SessionState::new(1);
///
/// // Sender side: generate sequence numbers
/// let seq1 = session.assign_sequence().unwrap().commit(); // Returns 1, internal counter becomes 2
/// let seq2 = session.assign_sequence().unwrap().commit(); // Returns 2, internal counter becomes 3
///
/// // Receiver side: track received sequences
/// assert!(session.update_last_received(1)); // true: new sequence
/// assert!(!session.update_last_received(1)); // false: duplicate
/// assert!(session.update_last_received(2)); // true: new sequence
/// ```
///
/// # See Also
///
/// - [`UnackedBuffer`](super::unacked_buffer::UnackedBuffer) — uses `session_sequence`
///   to track frames awaiting acknowledgment.
/// - [`IPCEndpoint`](crate::broker_core::transport::ipc::endpoint::IPCEndpoint) —
///   maintains a `SessionState` for each IPC connection.
#[derive(Debug, Default)]
pub struct SessionState {
    /// Atomic counter for the next outgoing sequence number.
    /// Incremented by `next_sequence()` using `fetch_add`.
    next_session_sequence: AtomicU64,

    /// Atomic tracker for the highest received sequence number.
    /// Updated by `update_last_received()` using `compare_exchange_weak`.
    /// Initialized to `initial_sequence - 1` to indicate "no frames received yet".
    last_received_sequence: AtomicU64,

    /// Fatal latch for the outgoing direction (L2 spec §13.3).
    ///
    /// Once a sequence number has been assigned, the only legal outcome other than
    /// a successful write is a fatal transport failure. This flag records that the
    /// sequence space may contain a hole, so the connection MUST be torn down and
    /// recovered via Resume, never reused.
    ///
    /// Set by `SequenceGuard::drop` when the guard is not committed.
    broken: AtomicBool,
}

impl SessionState {
    /// Creates a new session state with the specified initial sequence number.
    ///
    /// # Arguments
    ///
    /// * `initial_sequence` - The first sequence number to be assigned to an outgoing frame.
    ///   Typically `1` for a new session, or a restored value when resuming a session
    ///   after a connection break (see `Resume`/`Resumed` frames in the protocol spec).
    ///
    /// # Initialization Semantics
    ///
    /// - `next_session_sequence` is set to `initial_sequence`. The first call to
    ///   `next_sequence()` will return this value and increment the counter.
    /// - `last_received_sequence` is set to `initial_sequence.saturating_sub(1)`,
    ///   which represents "no frames received yet". This ensures that the first
    ///   received frame (with `sequence >= initial_sequence`) will be accepted.
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::broker_core::l2::SessionState;
    ///
    /// // New session starting from sequence 1
    /// let session = SessionState::new(1);
    /// assert_eq!(session.current_send_sequence(), 1);
    /// assert_eq!(session.last_received_sequence(), 0); // 1 - 1 = 0
    ///
    /// // Resumed session starting from sequence 1000
    /// let resumed_session = SessionState::new(1000);
    /// assert_eq!(resumed_session.current_send_sequence(), 1000);
    /// assert_eq!(resumed_session.last_received_sequence(), 999);
    /// ```
    pub fn new(initial_sequence: u64) -> Self {
        Self {
            next_session_sequence: AtomicU64::new(initial_sequence),
            last_received_sequence: AtomicU64::new(initial_sequence.saturating_sub(1)),
            broken: AtomicBool::new(false),
        }
    }

    /// Assigns the next outgoing sequence number and returns a commit guard.
    ///
    /// Returns `None` if the session is already broken: a broken sequence space
    /// must never be extended (L2 §13.3). This merges the "is sendable" check and
    /// the assignment into a single operation, so no caller can slip a number in
    /// between the check and the increment.
    #[inline]
    pub fn assign_sequence(&self) -> Option<SequenceGuard<'_>> {
        if self.is_broken() {
            return None;
        }
        let sequence = self.next_session_sequence.fetch_add(1, Ordering::Relaxed);
        Some(SequenceGuard {
            session: self,
            sequence,
            committed: false,
        })
    }

    /// Returns the next sequence number WITHOUT consuming it.
    ///
    /// This method is used by `IPCEndpoint::send_data` to perform a
    /// **zero-allocation backpressure check** before committing to
    /// frame encoding and transmission.
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::broker_core::l2::SessionState;
    ///
    /// let session = SessionState::new(1);
    ///
    /// // Peek does not increment the counter
    /// assert_eq!(session.peek_next_sequence(), 1);
    /// assert_eq!(session.peek_next_sequence(), 1); // Still 1
    ///
    /// // next_sequence() consumes the number
    /// assert_eq!(session.assign_sequence().unwrap().commit(), 1); // Returns 1, counter becomes 2
    /// assert_eq!(session.assign_sequence().unwrap().commit(), 2); // Now peek returns 2
    /// ```
    #[inline]
    pub fn peek_next_sequence(&self) -> u64 {
        self.next_session_sequence.load(Ordering::Relaxed)
    }    

    /// Updates the last received sequence number if the provided sequence is greater
    /// than the current value.
    ///
    /// This method is called by the L2 receiver upon successfully processing an incoming
    /// frame. It serves two purposes:
    ///
    /// 1. **Duplicate detection**: If the sequence is less than or equal to the current
    ///    `last_received_sequence`, it indicates a duplicate or out-of-order frame, and
    ///    the method returns `false`.
    ///
    /// 2. **Progress tracking**: If the sequence is greater, the method atomically updates
    ///    the counter and returns `true`, indicating that this is a new, valid frame.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The sequence number of the received frame.
    ///
    /// # Returns
    ///
    /// - `true` if the sequence is greater than the current `last_received_sequence`
    ///   (i.e., this is a new, valid frame).
    /// - `false` if the sequence is less than or equal to the current value (i.e.,
    ///   this is a duplicate or out-of-order frame).
    ///
    /// # Thread Safety
    ///
    /// This method is **lock-free** and uses a compare-and-swap loop (`compare_exchange_weak`)
    /// to handle concurrent updates. If multiple tasks attempt to update the counter
    /// simultaneously, the loop ensures that only the highest sequence number is retained.
    ///
    /// # Ordering Guarantees
    ///
    /// The method uses `Ordering::Relaxed`, which is sufficient because:
    /// - We only care about the final value of `last_received_sequence`, not the order
    ///   of updates relative to other memory operations.
    /// - The compare-and-swap loop ensures atomicity of the update.
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::broker_core::l2::SessionState;
    ///
    /// let session = SessionState::new(1);
    ///
    /// // First frame received
    /// assert!(session.update_last_received(1)); // true: new sequence
    ///
    /// // Duplicate frame (e.g., retransmission)
    /// assert!(!session.update_last_received(1)); // false: duplicate
    ///
    /// // Out-of-order frame (older than current)
    /// assert!(!session.update_last_received(0)); // false: outdated
    ///
    /// // Next frame in sequence
    /// assert!(session.update_last_received(2)); // true: new sequence
    /// ```
    pub fn update_last_received(&self, sequence: u64) -> bool {
        let mut current = self.last_received_sequence.load(Ordering::Relaxed);
        loop {
            if sequence <= current {
                return false; // Duplicate or outdated
            }
            match self.last_received_sequence.compare_exchange_weak(
                current,
                sequence,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(new_current) => current = new_current, // Retry with updated value
            }
        }
    }

    /// Returns the current value of the outgoing sequence counter.
    ///
    /// This is the sequence number that will be assigned to the **next** outgoing frame.
    /// It is equivalent to the value returned by the most recent call to `next_sequence()`,
    /// plus one.
    ///
    /// # Use Cases
    ///
    /// - **Diagnostics and logging**: Track the progress of frame transmission.
    /// - **Session resumption**: When reconnecting after a break, the client can report
    ///   `current_send_sequence()` to the server to synchronize state.
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::broker_core::l2::SessionState;
    ///
    /// let session = SessionState::new(1);
    /// assert_eq!(session.current_send_sequence(), 1); // Next frame will be 1
    ///
    /// session.assign_sequence().unwrap().commit(); // Generates 1, counter becomes 2
    /// assert_eq!(session.current_send_sequence(), 2); // Next frame will be 2
    /// ```
    #[inline]
    pub fn current_send_sequence(&self) -> u64 {
        self.next_session_sequence.load(Ordering::Relaxed)
    }

    /// Returns the highest sequence number received from the peer.
    ///
    /// This value represents the last successfully processed incoming frame. It is
    /// updated by `update_last_received()` and is used for:
    ///
    /// - **Acknowledgment**: When sending an `Acknowledge` frame, the receiver includes
    ///   this value to inform the sender which frames have been successfully processed.
    /// - **Loss detection**: The sender can compare its `current_send_sequence()` with
    ///   the peer's `last_received_sequence()` to identify unacknowledged frames.
    ///
    /// # Initial Value
    ///
    /// For a new session (created with `SessionState::new(1)`), this returns `0`,
    /// indicating that no frames have been received yet.
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::broker_core::l2::SessionState;
    ///
    /// let session = SessionState::new(1);
    /// assert_eq!(session.last_received_sequence(), 0); // No frames received
    ///
    /// session.update_last_received(1);
    /// assert_eq!(session.last_received_sequence(), 1);
    ///
    /// session.update_last_received(5);
    /// assert_eq!(session.last_received_sequence(), 5);
    /// ```
    #[inline]
    pub fn last_received_sequence(&self) -> u64 {
        self.last_received_sequence.load(Ordering::Relaxed)
    }

    /// Marks the session as fatally broken: the sequence space may contain a hole.
    ///
    /// After this call every subsequent send MUST fail fast (before assigning a new
    /// sequence number) and the connection MUST be closed.
    #[inline]
    pub fn mark_broken(&self) {
        self.broken.store(true, Ordering::Release);
    }

    /// Returns `true` if the outgoing sequence space is no longer contiguous.
    ///
    /// Checked at the entry of every send path and may also be polled by the reader
    /// task to trigger shutdown.
    #[inline]
    pub fn is_broken(&self) -> bool {
        self.broken.load(Ordering::Acquire)
    }    
}

/// Commit guard for an assigned `session_sequence` (L2 spec §13.3).
///
/// Dropping the guard without calling [`commit`](Self::commit) marks the session as
/// broken. This makes cancellation of a send future *after* sequence assignment a
/// fatal, observable event instead of a silent hole in the sequence space.
///
/// The guard deliberately does **not** roll the counter back: rolling back would
/// allow a later frame to reuse a number that may already be on the wire.
#[must_use = "dropping a SequenceGuard without commit() marks the session as broken"]
#[derive(Debug)]
pub struct SequenceGuard<'a> {
    session: &'a SessionState,
    sequence: u64,
    committed: bool,
}

impl SequenceGuard<'_> {
    /// The assigned sequence number, to be embedded into the frame payload.
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Confirms that the frame has been successfully handed to the transport.
    #[inline]
    pub fn commit(mut self) -> u64 {
        self.committed = true;
        self.sequence
    }
}

impl Drop for SequenceGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // No rollback: the number is burned. The only legal recovery is teardown.
            self.session.mark_broken();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_guard_keeps_session_usable() {
        let s = SessionState::new(1);
        let g = s.assign_sequence().unwrap();
        assert_eq!(g.sequence(), 1);
        assert_eq!(g.commit(), 1);
        assert!(!s.is_broken());
        assert_eq!(s.current_send_sequence(), 2);
    }

    #[test]
    fn dropped_guard_breaks_session_without_rollback() {
        let s = SessionState::new(1);
        {
            let g = s.assign_sequence().unwrap();
            assert_eq!(g.sequence(), 1);
            // Simulates cancellation / early return after assignment.
        }
        assert!(s.is_broken());
        // The number is NOT rolled back.
        assert_eq!(s.current_send_sequence(), 2);
    }
}
