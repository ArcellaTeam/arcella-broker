// arcella-broker/src/l2/credit_pool.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::atomic::{AtomicU32, Ordering};

/// Universal credit pool with an atomic counter.
///
/// Encapsulates the "consume / replenish / check availability" logic
/// for a single credit type (hot or cold). Used as a building
/// block inside `CreditManager`.
///
/// # Invariants
///
/// - `0 <= available <= limit` — the counter never goes out of bounds.
/// - `consume()` atomically decrements the counter, returning `false` at zero.
/// - `replenish()` atomically increments the counter, but not above `limit`.
///
/// # Thread Safety
///
/// All operations are lock-free (CAS loops on `AtomicU32`).
#[derive(Debug)]
pub struct CreditPool {
    available: AtomicU32,
    limit: AtomicU32,
}

impl CreditPool {
    /// Creates a pool with the initial value equal to the limit.
    #[inline]
    pub fn new(limit: u32) -> Self {
        Self {
            available: AtomicU32::new(limit),
            limit: AtomicU32::new(limit),
        }
    }

    /// Grants updates the limit and additional credits.
    pub fn grant(&self, amount: u32) {
        self.limit.fetch_add(amount, Ordering::AcqRel);
        self.available.fetch_add(amount, Ordering::AcqRel);
    }    

    /// Attempts to atomically consume one credit.
    ///
    /// Returns `true` if the credit was successfully consumed (`available > 0`).
    /// Returns `false` if there are no credits (client protocol violation).
    #[inline]
    pub fn consume(&self) -> bool {
        let mut current = self.available.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return false;
            }
            match self.available.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Replenishes one credit after sending an Ack / freeing a slot.
    ///
    /// The counter does not exceed `limit` (protection against duplicate Acks).
    #[inline]
    pub fn replenish(&self) {
        let limit = self.limit();
        let mut current = self.available.load(Ordering::Acquire);
        loop {
            if current >= limit {
                return;
            }
            match self.available.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Current number of available credits (for metrics and diagnostics).
    #[inline]
    pub fn available(&self) -> u32 {
        self.available.load(Ordering::Acquire)
    }

    /// Maximum credit limit.
    #[inline]
    pub fn limit(&self) -> u32 {
        self.limit.load(Ordering::Acquire)
    }

    /// Usage percentage (for metrics).
    #[inline]
    pub fn utilization_pct(&self) -> f64 {
        let limit = self.limit();
        let available= self.available();
        if limit == 0 {
            return 0.0;
        }
        let used = limit - available;
        (used as f64 / limit as f64) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consume_and_replenish() {
        let pool = CreditPool::new(3);
        assert_eq!(pool.available(), 3);

        assert!(pool.consume());
        assert!(pool.consume());
        assert!(pool.consume());
        assert_eq!(pool.available(), 0);

        // The 4th consume does not succeed
        assert!(!pool.consume());

        pool.replenish();
        pool.replenish();
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_replenish_capped_at_limit() {
        let pool = CreditPool::new(2);
        assert!(pool.consume());

        for _ in 0..10 {
            pool.replenish();
        }
        assert_eq!(pool.available(), 2); // not above the limit
    }

    #[test]
    fn test_zero_limit() {
        let pool = CreditPool::new(0);
        assert!(!pool.consume());
        pool.replenish(); // must not panic
        assert_eq!(pool.available(), 0);
    }

    #[test]
    fn test_concurrent_consume_exact() {
        use std::sync::Arc;
        use std::thread;

        let pool = Arc::new(CreditPool::new(1000));
        let mut handles = vec![];

        for _ in 0..10 {
            let p = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                let mut consumed = 0u32;
                for _ in 0..200 {
                    if p.consume() {
                        consumed += 1;
                    }
                }
                consumed
            }));
        }

        let total: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, 1000);
        assert_eq!(pool.available(), 0);
    }
}
