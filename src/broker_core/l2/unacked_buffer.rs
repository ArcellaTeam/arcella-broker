// arcella-broker/src/l2/unacked_buffer.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Lock-based ring buffer for storing unacknowledged L2 frames.
//!
//! This module is a key component for implementing the client
//! backpressure guarantee with O(1) memory consumption (Stage 5.2 of the specification). It ensures
//! that the client physically cannot allocate memory beyond the `hot_credit` limit, even if
//! the server "hangs" and does not send acknowledgments (`Acknowledge`).
//!
//! # Architectural features
//!
//! - Uses a direct index (`sequence % capacity`) to access a slot, which
//! provides the acknowledge operation in O(1) time without searching.
//! - Uses `parking_lot::Mutex` to minimize contention. The critical section
//! is extremely short (access to a single slot), so the locking overhead
//! is negligible.
//! - Performs no dynamic allocations (`Vec::with_capacity` is used only during
//! initialization) on the hot path.

use parking_lot::Mutex;
use crate::protocol::Frame;

/// Slot in the ring buffer.
#[derive(Debug)]
struct Slot {
    sequence: u64,
    frame: Frame,
}

#[derive(Debug)]
struct UnackedBufferInner {
    slots: Vec<Option<Slot>>,
    occupied_count: usize,
    base_sequence: Option<u64>,
}

/// Lock-based ring buffer for storing unacknowledged frames.
/// 
/// Uses direct indexing: slot index = `sequence % capacity`.
/// This allows O(1) acknowledge operations without searching.
/// 
/// # Performance
/// 
/// Uses `parking_lot::Mutex` for minimal lock contention.
/// The critical section is very short (single slot access),
/// so lock overhead is negligible in practice.
#[derive(Debug)]
pub struct UnackedBuffer {
    inner: Mutex<UnackedBufferInner>,
    capacity: usize,
}

impl UnackedBuffer {
    /// Creates a new ring buffer with the given capacity.
    ///
    /// # Arguments
    ///
    /// * capacity — the maximum number of frames the buffer can hold.
    /// Must be strictly greater than 0 and is usually equal to the hot_credit value from ServerLimits.
    /// 
    /// # Panics
    /// 
    /// Panics if `capacity` is 0.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "UnackedBuffer capacity must be > 0");
        
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(None);
        }

        let inner = UnackedBufferInner {
            slots,
            occupied_count: 0,
            base_sequence: None,
        };
        
        Self {
            inner: Mutex::new(inner),
            capacity,
        }
    }

    /// Pushes a frame into the buffer with the given sequence number.
    ///
    /// # Arguments
    ///
    /// * `sequence` — monotonically increasing frame number within the session.
    /// * `frame` — the frame itself to store.
    /// 
    /// # Return value
    ///
    /// * `Ok(())` if the frame was successfully placed into a free slot.
    /// * `Err(UnackedBufferError::SlotOccupied)` if the target slot is already occupied.
    /// This signals backpressure: the buffer is full, and the sender must
    /// stop sending until an Acknowledge is received.
    ///
    /// # Performance
    ///
    /// O(1) in time. Performs no dynamic allocations.
    pub fn push(&self, sequence: u64, frame: Frame) -> Result<(), UnackedBufferError> {
        let idx = (sequence % (self.capacity as u64)) as usize;
        let mut inner = self.inner.lock();

        if inner.slots[idx].is_some() {
            return Err(UnackedBufferError::SlotOccupied { sequence, index: idx });
        }
        
        inner.slots[idx] = Some(Slot { sequence, frame });
        inner.occupied_count += 1;

        // Update base_sequence only if the buffer was empty.
        // Otherwise, base_sequence is already the minimum (we checked sequence > base above).
        if inner.base_sequence.is_none() {
            inner.base_sequence = Some(sequence);
        }

        Ok(())
    }

    /// Acknowledges a frame with the given sequence number, removing it from the buffer.
    /// 
    /// # Return value
    /// 
    /// - `true` if the frame was found and removed.
    /// - `false` if the slot was empty or the sequence number did not match.
    #[inline]
    pub fn acknowledge(&self, sequence: u64) -> bool {
        self.remove(sequence)
    }

    /// Removes a frame from the buffer by sequence number.
    ///
    /// # Returns
    /// - `true` if the frame was found and removed.
    /// - `false` if the slot is empty or the sequence does not match.
    ///
    /// # Return value
    ///
    /// * true if the frame was found and removed.
    /// * false if the slot is empty or the sequence number does not match.
    pub fn remove(&self, sequence: u64) -> bool {
        let idx = (sequence % (self.capacity as u64)) as usize;
        let mut inner = self.inner.lock();
        
        if let Some(slot) = &inner.slots[idx] {
            if slot.sequence == sequence {
                inner.slots[idx] = None;
                inner.occupied_count -= 1;
                
                if inner.base_sequence == Some(sequence) {
                    inner.base_sequence = Self::find_next_base_sequence_locked(&inner, sequence);
                }

                return true;
            }
        }

        false
    } 

    /// Returns all unacknowledged frames sorted by sequence
    /// number in ascending order.
    ///
    /// # Usage
    ///
    /// Used primarily during session recovery (the Resume operation),
    /// when the client needs to resend frames that were sent
    /// but did not receive acknowledgment from the server before the connection dropped.
    ///
    /// # Performance
    ///
    /// O(N), where N is the buffer capacity (`capacity`), since the method performs a linear
    /// pass over the ring buffer to collect and sort the active frames.
    pub fn get_unacked_frames(&self) -> Vec<(u64, Frame)> {
        let inner = self.inner.lock();
        
        if inner.occupied_count == 0 {
            return Vec::new();
        }
        let mut unacked_frames = Vec::with_capacity(inner.occupied_count);
        let start_sequence = match inner.base_sequence {
            Some(seq) => seq,
            None => return Vec::new(),
        };

        let capacity = inner.slots.len() as u64;
        let end_sequence = start_sequence + capacity;

        for seq in start_sequence..end_sequence {
            let idx = (seq % capacity) as usize;

            if let Some(slot) = &inner.slots[idx] {
                if slot.sequence == seq {
                    unacked_frames.push((slot.sequence, slot.frame.clone()));
                }
            }
        }

        unacked_frames
    }

    /// Returns the smallest sequence number currently stored in the buffer.
    ///
    /// # Return value
    ///
    /// * `Some(u64)` — the "head" of the unacknowledged frame window. Any number below this
    /// value has already been acknowledged and removed from the buffer.
    /// * `None` — if the buffer is completely empty.
    #[inline]
    pub fn base_sequence(&self) -> Option<u64> {
        let inner = self.inner.lock();
        inner.base_sequence
    }    

    /// Returns the number of occupied slots.
    #[inline]
    pub fn len(&self) -> usize {
        let inner = self.inner.lock();
        inner.occupied_count
    }

    /// Returns `true` if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        let inner = self.inner.lock();
        inner.occupied_count == 0
    }

    /// Returns true if the buffer has reached its maximum capacity.
    ///
    /// # Performance
    ///
    /// O(1) operation, performs no allocations. Used for an instant
    /// check of whether backpressure needs to be applied before attempting to send.
    #[inline]
    pub fn is_full(&self) -> bool {
        let inner = self.inner.lock();
        inner.occupied_count >= self.capacity
    }    

    /// Returns the maximum capacity of the buffer.
    ///
    /// This value is strictly bound to the `hot_credit` limit received from the server
    /// in the `Welcome` packet, which guarantees O(1) memory consumption per session.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Internal helper method for finding the next valid
    /// `base_sequence` number after removing the current one.
    ///
    /// # Arguments
    ///
    /// * `inner` — reference to the locked internal buffer state.
    /// * `after_sequence` — the sequence number that was just removed.
    ///
    /// # Return value
    ///
    /// * `Some(u64)` — the new minimum sequence number in the buffer.
    /// * `None` — if the buffer became completely empty after the removal.
    fn find_next_base_sequence_locked(inner: &UnackedBufferInner, after_sequence: u64) -> Option<u64> {
        if inner.occupied_count == 0 {
            return None;
        }

        let capacity = inner.slots.len() as u64;
        let start_sequence = after_sequence + 1;
        let end_sequence = after_sequence + capacity;

        for seq in start_sequence..end_sequence {
            let idx = (seq % capacity) as usize;

            if let Some(slot) = &inner.slots[idx] {
                if slot.sequence == seq {
                    return Some(seq);
                }
            }
        }

        None   
    }
}

/// Errors that can occur when pushing a frame into the buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum UnackedBufferError {
    /// The sequence number is too old (already acknowledged or in-flight).
    /// 
    /// This error indicates a protocol violation:
    /// - `sequence == base_sequence`: The frame is already in the buffer (duplicate send).
    /// - `sequence < base_sequence`: The frame was already acknowledged, and the buffer
    ///   has moved past it. This typically indicates a bug in session state synchronization
    ///   (e.g., after a failed Resume operation).
    SequenceTooOld { 
        sequence: u64, 
        base_sequence: u64 
    },
    
    /// The target slot is already occupied (buffer full or protocol error).
    SlotOccupied { sequence: u64, index: usize },
}

impl std::fmt::Display for UnackedBufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnackedBufferError::SequenceTooOld { sequence, base_sequence } => {
                write!(
                    f, 
                    "Sequence {} is too old (base_sequence is {}). Frame already acknowledged or in-flight.",
                    sequence, base_sequence
                )
            }
            UnackedBufferError::SlotOccupied { sequence, index } => {
                write!(f, "Slot {} is already occupied for sequence {}", index, sequence)
            }
        }
    }
}

impl std::error::Error for UnackedBufferError {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::protocol::{Frame, FrameType};

    fn dummy_frame(payload: &[u8]) -> Frame {
        Frame::new(FrameType::Data as u8, Bytes::from(payload.to_vec()))
    }

    #[test]
    fn test_push_and_acknowledge() {
        let buffer = UnackedBuffer::new(4);
        
        for i in 0..4 {
            buffer.push(i, dummy_frame(b"test")).unwrap();
        }
        
        assert_eq!(buffer.len(), 4);
        
        for i in 0..4 {
            assert!(buffer.acknowledge(i));
        }
        
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_wrap_around() {
        let buffer = UnackedBuffer::new(4);
        
        for i in 0..4 {
            buffer.push(i, dummy_frame(b"test")).unwrap();
        }
        
        assert!(buffer.acknowledge(0));
        assert!(buffer.acknowledge(1));
        
        buffer.push(4, dummy_frame(b"test")).unwrap();
        buffer.push(5, dummy_frame(b"test")).unwrap();
        
        assert_eq!(buffer.len(), 4);
        
        for i in [2, 3, 4, 5] {
            assert!(buffer.acknowledge(i));
        }
        
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_acknowledge_wrong_sequence() {
        let buffer = UnackedBuffer::new(4);
        
        buffer.push(0, dummy_frame(b"test")).unwrap();
        
        assert!(!buffer.acknowledge(1));
        assert_eq!(buffer.len(), 1);
        assert!(buffer.acknowledge(0));
    }

    #[test]
    fn test_slot_occupied() {
        let buffer = UnackedBuffer::new(4);
        
        buffer.push(0, dummy_frame(b"test1")).unwrap();
        
        let result = buffer.push(4, dummy_frame(b"test2"));
        assert!(matches!(result, Err(UnackedBufferError::SlotOccupied { sequence: 4, index: 0 })));
    }

    #[test]
    fn test_concurrent_push_acknowledge() {
        use std::sync::Arc;
        use std::thread;
        
        let buffer = Arc::new(UnackedBuffer::new(1024));
        let mut handles = vec![];
        
        for t in 0..4 {
            let buffer = Arc::clone(&buffer);
            handles.push(thread::spawn(move || {
                for i in 0..256 {
                    let seq = (t * 256 + i) as u64;
                    buffer.push(seq, dummy_frame(b"test")).unwrap();
                }
                for i in 0..256 {
                    let seq = (t * 256 + i) as u64;
                    assert!(buffer.acknowledge(seq));
                }
            }));
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_backpressure_at_capacity_o1_guarantee() {
        let capacity = 4;
        let buffer = UnackedBuffer::new(capacity);
        
        // 1. Fill the buffer to capacity
        for i in 0..capacity {
            assert!(buffer.push(i as u64, dummy_frame(b"test")).is_ok());
        }
        assert_eq!(buffer.len(), capacity);
        
        // 2. The next send should receive backpressure (SlotOccupied)
        let result = buffer.push(capacity as u64, dummy_frame(b"overflow"));
        assert!(matches!(result, Err(UnackedBufferError::SlotOccupied { .. })));
        assert_eq!(buffer.len(), capacity); // State unchanged, no allocations occurred
        
        // 3. Free one slot (simulating receipt of an Ack)
        assert!(buffer.acknowledge(0));
        assert_eq!(buffer.len(), capacity - 1);
        
        // 4. Now the send should succeed (uses the freed slot)
        assert!(buffer.push(capacity as u64, dummy_frame(b"success")).is_ok());
        assert_eq!(buffer.len(), capacity);
    }
}
