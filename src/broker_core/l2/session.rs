// arcella-broker/src/l2/session.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::atomic::{AtomicU64, Ordering};

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
/// let seq1 = session.next_sequence(); // Returns 1, internal counter becomes 2
/// let seq2 = session.next_sequence(); // Returns 2, internal counter becomes 3
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
        }
    }

    /// Generates and returns the next sequence number for an outgoing frame.
    ///
    /// This method is called by the L2 sender (e.g., `IPCEndpoint::send_data`) before
    /// encoding and transmitting a frame. The returned sequence number is embedded in
    /// the frame payload and used by the receiver for acknowledgment and ordering.
    ///
    /// # Returns
    ///
    /// The next available sequence number (u64). The internal counter is atomically
    /// incremented, so subsequent calls will return strictly increasing values.
    ///
    /// # Thread Safety
    ///
    /// This method is **lock-free** and safe to call concurrently from multiple tasks.
    /// The `fetch_add` operation with `Ordering::Relaxed` guarantees that each call
    /// receives a unique sequence number, even under high contention.
    ///
    /// # Overflow Behavior
    ///
    /// The sequence number is a `u64`, which provides ~18 quintillion unique values.
    /// In practice, overflow is impossible within the lifetime of any realistic session.
    /// If overflow were to occur, the counter would wrap around to `0`, which could
    /// cause protocol errors (duplicate sequence numbers). This is an acceptable
    /// trade-off for the performance benefits of using a simple atomic counter.
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::broker_core::l2::SessionState;
    ///
    /// let session = SessionState::new(1);
    /// assert_eq!(session.next_sequence(), 1);
    /// assert_eq!(session.next_sequence(), 2);
    /// assert_eq!(session.next_sequence(), 3);
    /// ```
    #[inline]
    pub fn next_sequence(&self) -> u64 {
        self.next_session_sequence.fetch_add(1, Ordering::Relaxed)
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
    /// assert_eq!(session.next_sequence(), 1); // Returns 1, counter becomes 2
    /// assert_eq!(session.peek_next_sequence(), 2); // Now peek returns 2
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
    /// session.next_sequence(); // Generates 1, counter becomes 2
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
}
