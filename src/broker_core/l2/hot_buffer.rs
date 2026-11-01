// arcella-broker/src/l2/hot_buffer.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::Bytes;
use std::collections::VecDeque;
use tokio::sync::Mutex;

/// Physical buffer for frames awaiting processing
/// Size is strictly limited by `hot_credit`
#[derive(Debug)]
pub struct HotBuffer {
    frames: Mutex<VecDeque<BufferedFrame>>,
    capacity: usize,
}

#[derive(Debug)]
pub struct BufferedFrame {
    pub session_sequence: u64,
    pub address: Bytes,
    pub messages_raw: Bytes,
}

impl HotBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            frames: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Try to push a frame into the buffer
    /// Returns `true` if successful, `false` if buffer is full
    pub async fn try_push(&self, frame: BufferedFrame) -> bool {
        let mut frames = self.frames.lock().await;
        if frames.len() >= self.capacity {
            return false;
        }
        frames.push_back(frame);
        true
    }

    /// Pop the next frame from the buffer
    pub async fn pop(&self) -> Option<BufferedFrame> {
        let mut frames = self.frames.lock().await;
        frames.pop_front()
    }

    /// Check if buffer is empty
    pub async fn is_empty(&self) -> bool {
        let frames = self.frames.lock().await;
        frames.is_empty()
    }

    /// Current number of frames in buffer
    pub async fn len(&self) -> usize {
        let frames = self.frames.lock().await;
        frames.len()
    }
}