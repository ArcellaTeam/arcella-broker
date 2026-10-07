// arcella-broker/src/stream_transport/unacked_buffer.rs
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
    slots: Mutex<Vec<Option<Slot>>>,
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
        
        Self {
            slots: Mutex::new(slots),
            capacity,
        }
    }

    /// Pushes a frame into the buffer with the given sequence number.
    /// 
    /// # Returns
    /// 
    /// - `Ok(())` if the frame was successfully pushed.
    /// - `Err(PushError::BufferFull)` if the target slot is already occupied.
    pub fn push(&self, sequence: u64, frame: Frame) -> Result<(), PushError> {
        let idx = (sequence as usize) % self.capacity;
        let mut slots = self.slots.lock();
        
        if slots[idx].is_some() {
            return Err(PushError::SlotOccupied { sequence, index: idx });
        }
        
        slots[idx] = Some(Slot { sequence, frame });
        Ok(())
    }

    /// Acknowledges a frame with the given sequence number, removing it from the buffer.
    /// 
    /// # Returns
    /// 
    /// - `true` if the frame was found and removed.
    /// - `false` if the slot was empty or the sequence number did not match.
    pub fn acknowledge(&self, sequence: u64) -> bool {
        let idx = (sequence as usize) % self.capacity;
        let mut slots = self.slots.lock();
        
        if let Some(slot) = &slots[idx] {
            if slot.sequence == sequence {
                slots[idx] = None;
                return true;
            }
        }
        
        false
    }

    /// Returns the number of occupied slots.
    pub fn len(&self) -> usize {
        let slots = self.slots.lock();
        slots.iter().filter(|s| s.is_some()).count()
    }

    /// Returns `true` if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Errors that can occur when pushing a frame into the buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum PushError {
    /// The target slot is already occupied (buffer full or protocol error).
    SlotOccupied { sequence: u64, index: usize },
}

impl std::fmt::Display for PushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PushError::SlotOccupied { sequence, index } => {
                write!(f, "Slot {} is already occupied for sequence {}", index, sequence)
            }
        }
    }
}

impl std::error::Error for PushError {}

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
        assert!(matches!(result, Err(PushError::SlotOccupied { sequence: 4, index: 0 })));
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
}
