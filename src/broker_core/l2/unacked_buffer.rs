// arcella-broker/src/l2/unacked_buffer.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use parking_lot::Mutex;
use crate::protocol::Frame;

/// Slot in the ring buffer.
#[derive(Debug)]
struct Slot {
    sequence: u64,
    frame: Frame,
}

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
pub struct UnackedBuffer {
    inner: Mutex<UnackedBufferInner>,
    capacity: usize,
}

impl UnackedBuffer {
    /// Creates a new ring buffer with the given capacity.
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
    /// # Returns
    /// 
    /// - `Ok(())` if the frame was successfully pushed.
    /// - `Err(PushError::BufferFull)` if the target slot is already occupied.
    pub fn push(&self, sequence: u64, frame: Frame) -> Result<(), UnackedBufferError> {
        let idx = (sequence % (self.capacity as u64)) as usize;
        let mut inner = self.inner.lock();

        if let Some(base) = inner.base_sequence {
            if sequence <= base {
                return Err(UnackedBufferError::SequenceTooOld { 
                    sequence, 
                    base_sequence: base 
                });
            }
        }
        
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
    /// # Returns
    /// 
    /// - `true` if the frame was found and removed.
    /// - `false` if the slot was empty or the sequence number did not match.
    pub fn acknowledge(&self, sequence: u64) -> bool {
        self.remove(sequence)
    }

    /// Removes a frame from the buffer by sequence number.
    ///
    /// # Returns
    /// - `true` if the frame was found and removed.
    /// - `false` if the slot is empty or the sequence does not match.
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

    /// Returns all unacked frames, sorted by sequence in ascending order.
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

    pub fn base_sequence(&self) -> Option<u64> {
        let inner = self.inner.lock();
        inner.base_sequence
    }    

    /// Returns the number of occupied slots.
    pub fn len(&self) -> usize {
        let inner = self.inner.lock();
        inner.occupied_count
    }

    /// Returns `true` if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        let inner = self.inner.lock();
        inner.occupied_count == 0
    }

    /// Returns the capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

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
    fn test_sequence_too_old_rejected() {
        let buffer = UnackedBuffer::new(4);
        
        // 1. Fill the buffer
        buffer.push(0, dummy_frame(b"0")).unwrap();
        buffer.push(1, dummy_frame(b"1")).unwrap();
        assert_eq!(buffer.base_sequence(), Some(0));
        
        // 2. Attempt to send a duplicate (sequence == base_sequence)
        let result = buffer.push(0, dummy_frame(b"dup"));
        assert!(matches!(
            result,
            Err(UnackedBufferError::SequenceTooOld { sequence: 0, base_sequence: 0 })
        ));
        
        // 3. Acknowledge frame 0, base_sequence advances to 1
        assert!(buffer.acknowledge(0));
        assert_eq!(buffer.base_sequence(), Some(1));
        
        // 4. Attempt to send a stale frame (sequence < base_sequence)
        let result = buffer.push(0, dummy_frame(b"stale"));
        assert!(matches!(
            result,
            Err(UnackedBufferError::SequenceTooOld { sequence: 0, base_sequence: 1 })
        ));
        
        // The buffer state must not change
        assert_eq!(buffer.len(), 1);
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
