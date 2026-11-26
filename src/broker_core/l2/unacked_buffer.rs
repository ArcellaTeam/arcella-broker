// arcella-broker/src/l2/unacked_buffer.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Bounded, sequence-ordered buffer of unacknowledged L2 Data frames.
//!
//! Guarantees O(1) memory per session: at most `capacity == hot_credit` frames
//! are stored, which applies natural backpressure to L3 (L2 §8.4).
//!
//! # Why ordered storage instead of `sequence % capacity`
//!
//! `session_sequence` is shared by `Data` and `CreditGrant` (§13.1), but only
//! `Data` occupies a slot. The sequence space is therefore **sparse**: with
//! `hot_credit = 4` a session may legitimately hold `Data(1)` and `Data(6)`.
//! A ring indexed by `sequence % capacity` assumes a dense space and fails in
//! three ways:
//!
//! 1. `push` rejects a frame while the buffer is NOT full (index collision with
//!    an older frame). The writer assigns the sequence number before `push`,
//!    so this rejection lands *after* the point of no return and tears down a
//!    healthy session (§13.3).
//! 2. A cached `base_sequence` can become `None` while frames are still stored.
//! 3. Scanning only `[base, base + capacity)` silently drops frames outside
//!    that window, breaking Resume replay (§13.7).
//!
//! Ascending-order storage removes all three failure modes. `len` and
//! `base_sequence` are **derived** from the queue, never cached.
//!
//! # Contract (L2 §13.3)
//!
//! `push` has exactly one rejection reachable in a correct session: *full*.
//! There is a single pusher (the writer actor, `&mut self`), and
//! `acknowledge`/`remove` only ever decrease the length. Therefore:
//!
//! ```text
//! !is_full() observed before assign_sequence()  =>  push() cannot fail
//! ```
//!
//! The writer performs that check before assigning a sequence number, so no
//! non-fatal rejection can occur past the point of no return.
//!
//! # Complexity and memory
//!
//! | Operation | Cost |
//! |-----------|------|
//! | `push` | O(1), no allocation (capacity reserved once) |
//! | `acknowledge`, in-order (common case on an ordered stream) | O(1) |
//! | `acknowledge`, out-of-order | O(log n) search + O(n) shift, n <= hot_credit |
//! | `base_sequence` | O(1) |
//! | `get_unacked_frames` | O(n), already sorted |
//!
//! Retransmission must NOT go through `push`: a replayed frame keeps its
//! original sequence and its slot (§13.6, §13.7).

use parking_lot::Mutex;
use std::collections::VecDeque;

use crate::protocol::Frame;

// ============================================================================
// Slot
// ============================================================================

/// A sent-but-unacknowledged Data frame.
///
/// Extension point: per-slot send timestamp (§13.6 retransmission timer) and
/// the destination address (§7.4/§7.5 `seq -> address` mapping) belong here.
#[derive(Debug)]
struct Slot {
    sequence: u64,
    frame: Frame,
}

// ============================================================================
// UnackedBufferInner
// ============================================================================

/// Locked state of the buffer.
///
/// # Invariants
///
/// Maintained by the methods below, never by the caller:
///
/// 1. `slots` is sorted by strictly ascending `sequence` — the writer assigns
///    and pushes inside one critical section (§13.2).
/// 2. `slots.len() <= capacity` — enforced by [`Self::try_push`], so the
///    allocation performed in `with_capacity` is never exceeded.
/// 3. The window head is `slots.front()`; the occupancy is `slots.len()`.
///    Neither is cached: cached copies previously diverged from the contents
///    whenever `CreditGrant` made the sequence space sparse.
#[derive(Debug)]
struct UnackedBufferInner {
    slots: VecDeque<Slot>,
}

impl UnackedBufferInner {
    /// Reserves the whole window up front; the hot path never reallocates.
    fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: VecDeque::with_capacity(capacity),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    #[inline]
    fn is_full(&self, capacity: usize) -> bool {
        self.slots.len() >= capacity
    }

    /// Smallest stored sequence: everything below it has been acknowledged.
    #[inline]
    fn base_sequence(&self) -> Option<u64> {
        self.slots.front().map(|s| s.sequence)
    }

    /// Appends a frame under an already assigned sequence number.
    ///
    /// `Full` is the only rejection reachable in a correct session; the writer
    /// observes it via `is_full()` *before* assignment (§13.3). A non-monotonic
    /// sequence is an implementation bug, reported as `SequenceTooOld`.
    fn try_push(
        &mut self,
        sequence: u64,
        frame: Frame,
        capacity: usize,
    ) -> Result<(), UnackedBufferError> {
        if self.is_full(capacity) {
            return Err(UnackedBufferError::SlotOccupied {
                sequence,
                index: capacity,
            });
        }

        // Invariant 1: assignment and push form one critical section, so every
        // pushed sequence strictly exceeds the newest stored one.
        if let Some(newest) = self.slots.back() {
            if sequence <= newest.sequence {
                return Err(UnackedBufferError::SequenceTooOld {
                    sequence,
                    base_sequence: newest.sequence,
                });
            }
        }

        self.slots.push_back(Slot { sequence, frame });
        Ok(())
    }

    /// Removes a frame by sequence number; idempotent.
    fn remove(&mut self, sequence: u64) -> bool {
        // Fast path: on an ordered stream Acks arrive in send order, so the
        // acked frame is almost always at the head of the window.
        if self.slots.front().is_some_and(|s| s.sequence == sequence) {
            self.slots.pop_front();
            return true;
        }

        // Out-of-order Ack (cold path holds an address while later frames are
        // acked) or a duplicate: binary search over the sorted window.
        match self.slots.binary_search_by_key(&sequence, |s| s.sequence) {
            Ok(idx) => {
                // Shifts the shorter side, at most floor(n / 2) slots.
                self.slots.remove(idx);
                true
            }
            Err(_) => false,
        }
    }
}

// ============================================================================
// UnackedBuffer
// ============================================================================

/// Bounded buffer of unacknowledged frames, ordered by ascending `sequence`.
///
/// # Single-pusher invariant
///
/// Only the direction's serialization point (the writer) may call `push`.
/// `acknowledge`/`remove` are safe to call concurrently from the reader task.
///
/// # Performance
///
/// `parking_lot::Mutex` guards a very short critical section. `capacity` is
/// immutable and read without locking.
#[derive(Debug)]
pub struct UnackedBuffer {
    inner: Mutex<UnackedBufferInner>,
    capacity: usize,
}

impl UnackedBuffer {
    /// Creates a buffer with the given capacity.
    ///
    /// * `capacity` — maximum number of stored frames, equal to `hot_credit`
    ///   from `ServerLimits`. Must be greater than 0.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "UnackedBuffer capacity must be > 0");
        
        Self {
            inner: Mutex::new(UnackedBufferInner::with_capacity(capacity)),
            capacity,
        }
    }

    /// Stores a frame under an already assigned sequence number.
    ///
    /// # Contract
    ///
    /// Called strictly after `assign_sequence()` and only by the writer.
    /// Because the writer checks `is_full()` *before* assignment and is the
    /// only pusher, `SlotOccupied` is unreachable here in a correct session
    /// (L2 §13.3). It remains as a defensive guard, and any occurrence must be
    /// treated as fatal by the caller.
    ///
    /// # Errors
    ///
    /// * `SlotOccupied` — buffer is full (backpressure).
    /// * `SequenceTooOld` — non-monotonic push; an implementation bug.
    ///
    /// # Performance
    ///
    /// O(1), no dynamic allocations.
    pub fn push(&self, sequence: u64, frame: Frame) -> Result<(), UnackedBufferError> {
        self.inner.lock().try_push(sequence, frame, self.capacity)
    }

    /// Acknowledges a frame, removing it from the buffer.
    ///
    /// Returns `true` if the frame was found and removed, `false` for a
    /// duplicate or unknown sequence (idempotent).
    #[inline]
    pub fn acknowledge(&self, sequence: u64) -> bool {
        self.remove(sequence)
    }

    /// Removes a frame by sequence number.
    ///
    /// Returns `true` if the frame was found and removed.
    pub fn remove(&self, sequence: u64) -> bool {
        self.inner.lock().remove(sequence)
    } 

    /// Returns all unacknowledged frames in ascending sequence order.
    ///
    /// Used by the retransmission timer and by Resume replay (§13.6, §13.7).
    /// Every stored frame is returned regardless of how sparse the sequence
    /// space is — `CreditGrant` gaps cannot hide a frame.
    ///
    /// # Performance
    ///
    /// O(n) over stored frames; no sorting pass is needed.
    pub fn get_unacked_frames(&self) -> Vec<(u64, Frame)> {
        let inner = self.inner.lock();

        inner
            .slots
            .iter()
            .map(|s| (s.sequence, s.frame.clone()))
            .collect()
    }

    /// Returns the smallest stored sequence number, or `None` if empty.
    ///
    /// Any number below it has been acknowledged and removed.
    pub fn base_sequence(&self) -> Option<u64> {
        self.inner.lock().base_sequence()
    }    

    /// Number of stored frames
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Returns `true` if no frames are stored.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Returns `true` if the buffer holds `capacity` frames.
    ///
    /// This is the authoritative pre-flight backpressure gate: the writer MUST
    /// call it before `assign_sequence()` (L2 §13.3). O(1), no allocations.
    pub fn is_full(&self) -> bool {
        self.inner.lock().is_full(self.capacity)
    }    

    /// Maximum capacity, bound to `hot_credit` from `Welcome`.
    ///
    /// Immutable, therefore lock-free.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

// ============================================================================
// UnackedBufferError
// ============================================================================

/// Errors that can occur when pushing a frame into the buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum UnackedBufferError {
    /// Non-monotonic push: `sequence` does not exceed the newest stored frame.
    ///
    /// The writer assigns and pushes inside one critical section, so this is an
    /// implementation bug (e.g. a replay path calling `push` instead of reusing
    /// the stored frame), never backpressure. Reported after assignment, hence
    /// fatal (§13.3).
    ///
    /// `base_sequence` carries the newest stored sequence — the ordering floor
    /// that `sequence` failed to exceed.
    SequenceTooOld { sequence: u64, base_sequence: u64 },
    
    /// The buffer is full: `capacity` frames await acknowledgment.
    ///
    /// This is the backpressure signal. A correct writer observes it via
    /// `is_full()` BEFORE assigning a sequence number.
    ///
    /// `index` is the number of occupied slots at rejection time (== capacity);
    /// the field is kept for compatibility with the previous ring-based API.
    SlotOccupied { sequence: u64, index: usize },
}

impl std::fmt::Display for UnackedBufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnackedBufferError::SequenceTooOld { sequence, base_sequence } => write!(
                f,
                "Sequence {} is not newer than the newest buffered sequence {}; \
                 push must be strictly monotonic.",
                sequence, base_sequence
            ),
            UnackedBufferError::SlotOccupied { sequence, index } => write!(
                f,
                "UnackedBuffer is full ({} slots occupied); sequence {} rejected (backpressure).",
                index, sequence
            ),
        }
    }
}

impl std::error::Error for UnackedBufferError {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::thread;
        
    use crate::protocol::{Frame, FrameType};

    fn dummy_frame(payload: &[u8]) -> Frame {
        Frame::new(FrameType::Data as u8, Bytes::from(payload.to_vec()))
    }

    #[test]
    fn test_push_and_acknowledge() {
        let buffer = UnackedBuffer::new(4);
        
        for i in 1..=4 {
            buffer.push(i, dummy_frame(b"test")).unwrap();
        }
        
        assert_eq!(buffer.len(), 4);
        
        for i in 1..=4 {
            assert!(buffer.acknowledge(i));
        }
        
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.base_sequence(), None);
    }

    #[test]
    fn credit_grant_gaps_must_not_reject_push() {
        let buffer = UnackedBuffer::new(4);
        
        for i in 1..=4 {
            buffer.push(i, dummy_frame(b"test")).unwrap();
        }
        
        assert!(buffer.acknowledge(2)); // one slot freed, len = 3
        assert!(!buffer.is_full());
        
        // seq 5..8 were CreditGrants: numbers consumed, no slots used.
        // The old ring computed 9 % 4 = 1 and collided with the still-unacked seq 1.
        assert!(buffer.push(9, dummy_frame(b"next")).is_ok());
        assert_eq!(buffer.len(), 4);
        
        assert_eq!(buffer.base_sequence(), Some(1));
    }

    /// Regression: removing the head must not orphan frames whose sequence lies
    /// beyond `base + capacity`.
    #[test]
    fn sparse_window_survives_head_removal() {
        let buffer = UnackedBuffer::new(4);
        buffer.push(1, dummy_frame(b"a")).unwrap();
        buffer.push(6, dummy_frame(b"b")).unwrap();

        assert!(buffer.remove(1));
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.base_sequence(), Some(6));
        assert_eq!(
            buffer.get_unacked_frames().iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![6]
        );
    }

    /// Regression: Resume must see every stored frame, however sparse the space.
    #[test]
    fn get_unacked_frames_returns_all_frames() {
        let buffer = UnackedBuffer::new(4);
        for seq in [1u64, 40, 41, 1000] {
            buffer.push(seq, dummy_frame(b"x")).unwrap();
        }
        let seqs: Vec<u64> = buffer.get_unacked_frames().iter().map(|(s, _)| *s).collect();
        assert_eq!(seqs, vec![1, 40, 41, 1000]);
    }

    #[test]
    fn out_of_order_acknowledge() {
        let buffer = UnackedBuffer::new(4);
        for seq in [1u64, 3, 7, 9] {
            buffer.push(seq, dummy_frame(b"x")).unwrap();
        }
        assert!(buffer.acknowledge(7));
        assert_eq!(buffer.base_sequence(), Some(1));
        assert!(!buffer.acknowledge(7)); // duplicate Ack is idempotent
        assert!(!buffer.acknowledge(2)); // never sent
        assert!(buffer.acknowledge(1));
        assert_eq!(buffer.base_sequence(), Some(3));
    }

    #[test]
    fn non_monotonic_push_is_rejected() {
        let buffer = UnackedBuffer::new(4);
        buffer.push(10, dummy_frame(b"x")).unwrap();
        assert!(matches!(
            buffer.push(9, dummy_frame(b"y")),
            Err(UnackedBufferError::SequenceTooOld { sequence: 9, base_sequence: 10 })
        ));
        assert!(matches!(
            buffer.push(10, dummy_frame(b"y")),
            Err(UnackedBufferError::SequenceTooOld { .. })
        ));
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_backpressure_at_capacity_o1_guarantee() {
        let capacity = 4;
        let buffer = UnackedBuffer::new(capacity);

        for i in 1..=capacity as u64 {
            buffer.push(i, dummy_frame(b"test")).unwrap();
        }
        assert!(buffer.is_full());

        // Full is the only rejection, and the writer sees it BEFORE assignment.
        let err = buffer.push(5, dummy_frame(b"overflow")).unwrap_err();
        assert!(matches!(err, UnackedBufferError::SlotOccupied { sequence: 5, .. }));
        assert_eq!(buffer.len(), capacity); // state unchanged, no allocation

        assert!(buffer.acknowledge(1));
        assert!(!buffer.is_full());
        assert!(buffer.push(5, dummy_frame(b"success")).is_ok());
        assert_eq!(buffer.len(), capacity);
    }

    /// Concurrent Ack processing (reader task) against a single pusher (writer).
    #[test]
    fn test_single_producer_concurrent_acks() {
        let buffer = Arc::new(UnackedBuffer::new(8));
        let acker = {
            let buffer = Arc::clone(&buffer);
            thread::spawn(move || {
                let mut next = 0u64;
                while next < 1000 {
                    if buffer.acknowledge(next) {
                        next += 1;
                    } else {
                        thread::yield_now();
													 
                    }
                }
            })
        };

        for seq in 0..1000u64 {
            while buffer.push(seq, dummy_frame(b"test")).is_err() {
                thread::yield_now();
            }
        }
        acker.join().unwrap();
        assert!(buffer.is_empty());
        assert_eq!(buffer.base_sequence(), None);
    }

    #[test]
    fn test_concurrent_acknowledge_disjoint_ranges() {
        use std::sync::Arc;
        use std::thread;

        let buffer = Arc::new(UnackedBuffer::new(1024));
        for seq in 0..1024u64 {
            buffer.push(seq, dummy_frame(b"test")).unwrap();
        }

        let handles: Vec<_> = (0..4u64)
            .map(|t| {
                let buffer = Arc::clone(&buffer);
                thread::spawn(move || {
                    for i in 0..256u64 {
                        assert!(buffer.acknowledge(t * 256 + i));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(buffer.len(), 0);
    }
}
