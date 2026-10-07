// arcella-broker/src/stream_transport/session.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::atomic::{AtomicU64, Ordering};

/// State of the Frame session.
/// Responsible for generating monotonically increasing sequence numbers (session_sequence).
#[derive(Debug, Default)]
pub struct SessionState {
    next_session_sequence: AtomicU64,
    last_received_sequence: AtomicU64,
}

impl SessionState {
    pub fn new(initial_sequence: u64) -> Self {
        Self {
            next_session_sequence: AtomicU64::new(initial_sequence),
            last_received_sequence: AtomicU64::new(initial_sequence.saturating_sub(1)),
        }
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_session_sequence.fetch_add(1, Ordering::Relaxed)
    }

    pub fn update_last_received(&self, sequence: u64) -> bool {
        let mut current = self.last_received_sequence.load(Ordering::Relaxed);
        loop {
            if sequence <= current {
                return false;
            }
            match self.last_received_sequence.compare_exchange_weak(
                current,
                sequence,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(new_current) => current = new_current,
            }
        }
    }

    pub fn current_send_sequence(&self) -> u64 {
        self.next_session_sequence.load(Ordering::Relaxed)
    }

    pub fn last_received_sequence(&self) -> u64 {
        self.last_received_sequence.load(Ordering::Relaxed)
    }
}
