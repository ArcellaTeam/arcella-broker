// arcella-broker/src/l2/credit_manager.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::protocol::ServerLimits;
use super::CreditPool;

/// Server-side credit manager for a single client session.
///
/// Tracks the "shadow" credit counter that the server maintains to detect
/// protocol violations by the client. The server issues credits to the client
/// via `CreditGrant` frames, and the client is expected to respect those limits.
///
/// # Invariants
///
/// - `available` ≤ `limits.hot_credit` (never exceeds the issued limit)
/// - `available` decreases by 1 for each `Data` frame received
/// - `available` increases by 1 for each `Acknowledge` sent (replenishment)
/// - If `available == 0` and client sends `Data` → protocol violation
///
/// # Thread Safety
///
/// All operations are **lock-free** using atomic CAS loops, allowing concurrent
/// frame processing from multiple tasks (if needed in future stages).
#[derive(Debug)]
pub struct CreditManager {
    /// Available hot credits (shadow counter).
    /// Represents how many more Data frames the client can send before
    /// the server expects to send an Ack (which replenishes the credit).
    hot: CreditPool,

    /// Available cold credits (for backpressure buffer cells).
    /// Reserved for Stage 10 (full two-tier credit system).
    cold: CreditPool,
}

impl CreditManager {
    /// Creates a new credit manager with full credit allocation from ServerLimits.
    ///
    /// Called immediately after successful handshake. The initial values equal
    /// the credits that were granted to the client via the initial `CreditGrant` frame.
    pub fn new(limits: &ServerLimits) -> Self {
        Self {
            hot: CreditPool::new(limits.hot_credit),
            cold: CreditPool::new(limits.cold_credit),
        }
    }

    // ── Hot credits ──────────────────────────────────────────────

    /// Attempts to consume one hot credit atomically.
    #[inline]
    pub fn consume_hot_credit(&self) -> bool {
        self.hot.consume()
    }

    /// Replenishes one hot credit after sending an Acknowledge.
    #[inline]
    pub fn replenish_hot_credit(&self) {
        self.hot.replenish()
    }

    /// Returns the current number of available hot credits (for metrics/diagnostics).
    #[inline]
    pub fn hot_available(&self) -> u32 {
        self.hot.available()
    }

    /// Returns the maximum hot credit limit (for diagnostics).
    #[inline]
    pub fn hot_limit(&self) -> u32 {
        self.hot.limit()
    }

    // ── Cold credits ─────────────────────────────────────────────

    /// Attempts to consume one cold credit atomically.
    #[inline]
    pub fn consume_cold_credit(&self) -> bool {
        self.cold.consume()
    }

    /// Replenishes one cold credit after sending an Acknowledge.
    #[inline]
    pub fn replenish_cold_credit(&self) {
        self.cold.replenish()
    }

    /// Returns the current number of available cold credits (for metrics/diagnostics).
    #[inline]
    pub fn cold_available(&self) -> u32 {
        self.cold.available()
    }

    /// Returns the maximum cold credit limit (for diagnostics).
    #[inline]
    pub fn cold_limit(&self) -> u32 {
        self.cold.limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_limits(hot: u32, cold: u32) -> ServerLimits {
        ServerLimits {
            hot_credit: hot,
            cold_credit: cold,
            ..ServerLimits::default()
        }
    }

    #[test]
    fn test_consume_and_replenish() {
        let limits = make_limits(3, 1);
        let mgr = CreditManager::new(&limits);
        
        assert_eq!(mgr.hot_available(), 3);
        
        // Consume all 3 credits
        assert!(mgr.consume_hot_credit());
        assert!(mgr.consume_hot_credit());
        assert!(mgr.consume_hot_credit());
        assert_eq!(mgr.hot_available(), 0);
        
        // 4th consume fails (protocol violation)
        assert!(!mgr.consume_hot_credit());
        
        // Replenish 2 credits
        mgr.replenish_hot_credit();
        mgr.replenish_hot_credit();
        assert_eq!(mgr.hot_available(), 2);
        
        // Can consume 2 more
        assert!(mgr.consume_hot_credit());
        assert!(mgr.consume_hot_credit());
        assert_eq!(mgr.hot_available(), 0);
    }

    #[test]
    fn test_replenish_capped_at_limit() {
        let limits = make_limits(2, 1);
        let mgr = CreditManager::new(&limits);
        
        // Consume 1
        assert!(mgr.consume_hot_credit());
        assert_eq!(mgr.hot_available(), 1);
        
        // Replenish 5 times (but capped at limit=2)
        for _ in 0..5 {
            mgr.replenish_hot_credit();
        }
        assert_eq!(mgr.hot_available(), 2); // Capped at limit
    }

    #[test]
    fn test_concurrent_consume() {
        use std::sync::Arc;
        use std::thread;
        
        let limits = make_limits(1000, 10);
        let mgr = Arc::new(CreditManager::new(&limits));
        let mut handles = vec![];
        
        // Spawn 10 threads, each trying to consume 200 credits
        for _ in 0..10 {
            
            let mgr = Arc::clone(&mgr);
            handles.push(thread::spawn(move || {
                let mut consumed = 0;
                for _ in 0..200 {
                    if mgr.consume_hot_credit() {
                        consumed += 1;
                    }
                }
                consumed
            }));
        }
        
        let total_consumed: u32 = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .sum();
        
        // Total consumed must be exactly 1000 (the limit)
        assert_eq!(total_consumed, 1000);
        assert_eq!(mgr.hot_available(), 0);
    }
}
