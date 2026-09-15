// arcella-broker/src/registry/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Local recipient registry for the Arcella broker.
//!
//! This module provides an in-memory registry for routing messages to local recipients
//! within a single process. It supports exact address matching and wildcard patterns
//! under a strict **exclusive binding model**.
//!
//! # Wildcard Rules
//! - `*` matches exactly one segment at its position (e.g., `arcella:*:users`).
//! - `**` matches zero or more trailing segments and **must** be the last segment 
//!   in the pattern (e.g., `arcella:core:**`).
//! - Segments are separated by `:`.
//!
//! # Exclusive Binding
//! Every address can have at most one recipient. Conflicting subscriptions 
//! (e.g., registering a wildcard that covers an already registered exact address, 
//! or vice versa) are rejected at registration time to prevent ambiguous routing.
//! 
//! # Architectural Note (Performance)
//! In the current implementation, wildcard address lookup in the lookup method is 
//! performed via linear scanning (O(N), where N is the number of wildcard rules).
//! To achieve the O(L) performance specified in the requirements (where L is the address length),
//! future versions plan to migrate the wildcards index to a Radix Tree (Trie) data structure.   

use arc_swap::ArcSwap;
use iradix::sync::Radix;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use thiserror::Error;

use crate::transport::channel::MessageSender;

pub struct SubscriptionSlot {
    /// Current sender. `None` means the subscription has been removed.
    /// We use `ArcSwap` for lock-free updates.
    sender: ArcSwap<Option<MessageSender>>,
    
    /// Subscription version. Increments on ANY change:
    /// - register (initial or re-registration)
    /// - unregister
    /// - replacement of the sender
    version: AtomicU64,
}

impl SubscriptionSlot {
    pub fn new(sender: MessageSender) -> Arc<Self> {
        Arc::new(Self {
            sender: ArcSwap::from(Arc::new(Some(sender))),
            version: AtomicU64::new(1),
        })
    }
    
    /// Update the sender and version increment.
    pub fn update(&self, new_sender: MessageSender) {
        self.sender.store(Arc::new(Some(new_sender)));
        self.version.fetch_add(1, Ordering::Release);
    }
    
    /// Marks the slot as deleted and version increment.
    pub fn mark_removed(&self) {
        self.sender.store(Arc::new(None));
        self.version.fetch_add(1, Ordering::Release);
    }
    
    /// Lock-free retrieval of the current state
    pub fn load(&self) -> (Option<MessageSender>, u64) {
        let guard = self.sender.load();        
        let version = self.version.load(Ordering::Acquire);
        (guard.as_ref().as_ref().cloned(), version)
    }
    
    /// Checks if the underlying channel is closed or the slot is marked as removed
    pub fn is_closed(&self) -> bool {
        let guard = self.sender.load();
        match guard.as_ref().as_ref() {
            Some(sender) => sender.is_closed(),
            None => true, // Slot was deleted explicitly with mark_removed
        }
    }
}

impl Drop for SubscriptionSlot {
    fn drop(&mut self) {
        tracing::debug!("drop");
    }
}

/// Internal state of the registry, protected by a `RwLock`.
/// Separates exact matches and wildcards for optimized lookup and conflict detection.
#[derive(Clone)]
struct RegistryInner {
    /// Exact address -> channel. Key is `u8` for zero-allocation `&[u8]` queries.
    exact_tree: Radix<u8, Arc<SubscriptionSlot>>,
    /// Prefix wildcards (ending in `:**`). O(L) lookup via `get_ancestor`.
    prefix_wildcard_tree: Radix<u8, Arc<SubscriptionSlot>>,
    /// Single-segment wildcards (containing `*` but not ending in `**`).
    single_wildcards: Vec<(Vec<u8>, Arc<SubscriptionSlot>)>,
}

/// Registry of local recipients (within a single process).
///
/// Supports two subscription types:
/// - **Exact**: `"arcella:core:users"` - receives only messages to this exact address.
/// - **Wildcard**: patterns containing `*` (single segment) or ending with `**` (multi-segment).
pub struct LocalRegistry {
    /// Lock-free readable snapshot of the registry state.
    inner: ArcSwap<RegistryInner>,
    /// Mutex to serialize all mutations, preventing TOCTOU races during conflict checks
    /// without ever blocking concurrent readers.
    register_mutex: Mutex<()>,
}

/// Errors that can occur during registry operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    /// Returned when attempting to register an address or pattern that is already registered.
    #[error("Address or pattern '{0}' is already occupied")]
    AddressAlreadyOccupied(String),
    
    /// Returned when a new wildcard pattern overlaps with an existing exact address.
    #[error("Wildcard subscription '{0}' conflicts with existing exact address '{1}'")]
    WildcardConflict(String, String),
    
    /// Returned when an exact address is registered that falls under an existing wildcard pattern.
    #[error("Exact address '{0}' conflicts with existing wildcard subscription '{1}'")]
    ConflictsWithWildcard(String, String),

    /// Returned when a wildcard pattern violates syntax rules.
    /// 
    /// Common causes:
    /// - Empty pattern or empty segments (e.g., `a::b`).
    /// - `**` is not the last segment (e.g., `a:**:b`).
    /// - Malformed segments containing `*` alongside other characters (e.g., `a*`, `*b`).
    #[error("Invalid wildcard format: {0}")]
    InvalidWildcardFormat(String),

    #[error("Waiter already exists")]
    WaiterAlreadyExists,
}

impl LocalRegistry {
    /// Creates a new, empty `LocalRegistry`.
    pub(crate) fn new() -> Self {

        Self {
            inner: ArcSwap::from(Arc::new(RegistryInner {
                exact_tree: Radix::new(),
                prefix_wildcard_tree: Radix::new(),
                single_wildcards: Vec::new(),
            })),
            register_mutex: Mutex::new(()),
        }
    }

    /// Returns `true` if the address contains wildcard characters (`*`).
    /// This is a fast, pre-validation check to route to the correct registration logic.
    #[inline]
    fn is_wildcard(address: &str) -> bool {
        address.contains('*')
    }

    /// Validates wildcard pattern syntax according to Arcella routing rules.
    ///
    /// # Rules enforced:
    /// 1. Pattern cannot be empty.
    /// 2. No empty segments allowed (e.g., `a::b` or trailing `:`).
    /// 3. `**` can only appear as the very last segment.
    /// 4. Partial wildcards (e.g., `a*`, `*b`, `a*b`) are forbidden; `*` must be the entire segment.
    fn validate_wildcard_pattern(pattern: &str) -> Result<(), RegistryError> {
        if pattern.is_empty() {
            return Err(RegistryError::InvalidWildcardFormat(
                "Pattern cannot be empty".to_string(),
            ));
        }

        // Prohibit global catch-all "**"
        if pattern == "**" {
            return Err(RegistryError::InvalidWildcardFormat(
                "Global '**' wildcard is not allowed; pattern must have at least one static prefix segment".to_string()
            ));
        }        

        // Prohibit global "*" and patterns starting with "*"
        // This guarantees the presence of a static prefix for O(L) search in the Radix Tree
        if pattern.starts_with('*') {
            return Err(RegistryError::InvalidWildcardFormat(
                "Pattern cannot start with '*' or be a global '*' wildcard; must have a static prefix".to_string()
            ));
        }     

        let mut found_starstar = false;

        for segment in pattern.split(':') {
            // Check for empty segments (e.g., "arcella::*" or "*::users")
            if segment.is_empty() {
                return Err(RegistryError::InvalidWildcardFormat(
                    format!("Empty segment in pattern '{}'", pattern),
                ));
            }

            // If we already found "**", any subsequent segment is invalid
            if found_starstar {
                return Err(RegistryError::InvalidWildcardFormat(
                    format!("'**' must be the last segment in pattern '{}'", pattern),
                ));
            }

            if segment == "**" {
				// Mark if we found "**"
                found_starstar = true;
            } else if segment == "*" {
                // Valid single-segment wildcard, continue checking
            } else if segment.contains('*') {
                // Found incorrect format, e.g., "a*", "*b", "a*b".
                return Err(RegistryError::InvalidWildcardFormat(
                    format!("Invalid wildcard segment '{}' in pattern '{}'", segment, pattern),
                ));
            }
        }

        Ok(())
    }
        
    /// Core segment-by-segment comparison algorithm.
    ///
    /// # Arguments
    /// * `pattern` - The pattern to match against (may contain `*` or `**`).
    /// * `target` - The concrete address or another pattern to compare with.
    /// * `allow_wildcard_both_sides` - If `true`, treats `**` in *either* string as a 
    ///   universal matcher for the remainder of the comparison. Used for detecting 
    ///   conflicts between two wildcard patterns. If `false`, only `pattern` is 
    ///   treated as a wildcard, used for matching a concrete `target` address.
    fn compare_segments_bytes(pattern: &[u8], target: &[u8], allow_wildcard_both_sides: bool) -> bool {
        let mut pat_iter = pattern.split(|&b| b == b':');
        let mut tar_iter = target.split(|&b| b == b':');

        loop {
            let seg1 = pat_iter.next();
            let seg2 = tar_iter.next();

            match (seg1, seg2) {
                (None, None) => return true,
                // Handle ** when checking pattern-to-pattern conflicts
                (Some(b"**"), _) if allow_wildcard_both_sides => return true,
                (_, Some(b"**")) if allow_wildcard_both_sides => return true,
                // Handle ** when matching pattern to concrete address.
                // Since ** must be at the end (enforced by validation), if we see it 
                // in the pattern, it automatically matches the rest of the target.
                (Some(b"**"), _) => return pat_iter.next().is_none(),
                // Length mismatch: one string has more segments than the other
                (Some(_), None) | (None, Some(_)) => return false,
                // Compare individual segments
                (Some(a), Some(b)) => {
                    if a != b"*" && b != b"*" && a != b {
                        return false;
                    }
                }
            }
        }
    }         

    /// Checks whether a concrete address matches a wildcard pattern.
    ///
    /// # Examples
    /// ```text
    /// matches("arcella:*:users",    "arcella:core:users")   - true
    /// matches("arcella:*:*",        "arcella:core:users")   - true
    /// matches("*:core:users",       "arcella:core:users")   - true
    /// matches("arcella:core:**",    "arcella:core:users")   - true
    /// matches("arcella:core:**",    "arcella:core")         - true  (zero extra segments)
    /// matches("arcella:core:**",    "arcella")              - false (too short)
    /// matches("arcella:core:*",     "arcella:core:users")   - true
    /// matches("arcella:core:*",     "arcella:core:a:b")     - false (length mismatch)
    /// matches("arcella:web:*",      "arcella:core:users")   - false
    /// ```
    fn matches(pattern: &[u8], address: &[u8]) -> Result<bool, RegistryError> {
        let p1 = String::from_utf8_lossy(pattern);
        let p2 = String::from_utf8_lossy(address);
        Self::validate_wildcard_pattern(&p1)?;

        Ok(Self::compare_segments_bytes(pattern, address, false))
    } 

    /// Checks whether two wildcard patterns can ever match the same concrete address.
    /// Used during registration to enforce the exclusive binding model.
    fn patterns_conflict_bytes(pattern1: &[u8], pattern2: &[u8]) -> Result<bool, RegistryError> {
        let p1 = String::from_utf8_lossy(pattern1);
        let p2 = String::from_utf8_lossy(pattern2);
        Self::validate_wildcard_pattern(&p1)?;
        Self::validate_wildcard_pattern(&p2)?;
        Ok(Self::compare_segments_bytes(pattern1, pattern2, true))
    }

    fn static_prefix_bytes(pattern: &[u8]) -> &[u8] {
        if let Some(idx) = pattern.iter().position(|&b| b == b'*') {
            &pattern[..idx]
        } else {
            pattern
        }
    }

    fn check_prefix_wildcard_conflicts(
        prefix_wildcard_tree: &Radix<u8, Arc<SubscriptionSlot>>,
        pat_bytes: &[u8],
    ) -> Result<Option<String>, RegistryError> {
        if prefix_wildcard_tree.is_empty() {
            return Ok(None);
        }

        let mut candidate = Vec::with_capacity(pat_bytes.len() + 3);
        for segment in pat_bytes.split(|&b| b == b':') {
            if segment == b"**" {
                break; // Reached the end of the pattern
            }
            if !candidate.is_empty() {
                candidate.push(b':');
            }
            candidate.extend_from_slice(segment);

            // Form the pattern prefix: "a:**", "a:b:**", etc.
            let mut check_bytes = candidate.clone();
            check_bytes.extend_from_slice(b":**"); 

            // Targeted O(L) check
            if prefix_wildcard_tree.get(&check_bytes).is_some() {
                return Ok(Some(String::from_utf8_lossy(&check_bytes).into_owned()));
            }
        }

        let static_pref = Self::static_prefix_bytes(pat_bytes);
        let iter = prefix_wildcard_tree.walk_prefix(static_pref);   

        for (k, _) in iter {
            // Skip the pattern itself (already checked as an exact duplicate)
            if k == pat_bytes {
                continue;
            }
            
            // Semantic conflict check
            if Self::compare_segments_bytes(pat_bytes, &k, true) {
                return Ok(Some(String::from_utf8_lossy(&k).into_owned()));
            }
        }       
        
        Ok(None)
    }        

    /// Registers a recipient at the specified address.
    ///
    /// Automatically routes to `register_exact` or `register_wildcard` based on 
    /// the presence of the `*` character.
    pub fn register(&self, address: String, channel: MessageSender) -> Result<(), RegistryError> {
        let _guard = self.register_mutex.lock().unwrap();
        let current = self.inner.load();
        let mut new_inner = (**current).clone();
        
        tracing::debug!("register: {}", address);

        if Self::is_wildcard(&address) {
            self.register_wildcard(&mut new_inner, address, channel)?;
        } else {
            self.register_exact(&mut new_inner, address, channel)?;
        }

        self.inner.store(Arc::new(new_inner));

        Ok(())
    }

    /// Registers an exact, non-wildcard address.
    fn register_exact(
        &self,
        inner: &mut RegistryInner,
        address: String,
        channel: MessageSender,
    ) -> Result<(), RegistryError> {
        let addr_bytes = address.as_bytes();

        // 1. Exact duplicate check (O(L))
        if inner.exact_tree.get(addr_bytes).is_some() {
            return Err(RegistryError::AddressAlreadyOccupied(address));
        }

        // 2. Check conflicts with prefix wildcards (O(L) via walk_path)
        if let Some((k, _slot)) = inner.prefix_wildcard_tree.walk_path(addr_bytes).next() {
            let wc = String::from_utf8_lossy(&k).into_owned();
            return Err(RegistryError::ConflictsWithWildcard(address, wc));
        }

        // 3. Check conflicts with single wildcards (O(N))
        for (pattern, _slot) in &inner.single_wildcards {
            if Self::compare_segments_bytes(pattern, addr_bytes, false) {
                let wc = String::from_utf8_lossy(pattern).into_owned();
                return Err(RegistryError::ConflictsWithWildcard(address, wc));
            }
        } 

        // 4. Insert (O(L) copy-on-write)
        let slot = SubscriptionSlot::new(channel);
        let mut txn = inner.exact_tree.txn();
        txn.insert(addr_bytes, slot);
        inner.exact_tree = txn.commit();                       

        Ok(())
    }
    
    /// Registers a wildcard pattern.
    fn register_wildcard(
        &self,
        inner: &mut RegistryInner,
        pattern: String,
        channel: MessageSender,
    ) -> Result<(), RegistryError> {
        Self::validate_wildcard_pattern(&pattern)?;
        let pat_bytes = pattern.as_bytes();
        let is_prefix_wildcard = pattern.ends_with("**");

        // 1. Exact duplication (Fast path O(L))
        if is_prefix_wildcard {
            if inner.prefix_wildcard_tree.get(pat_bytes).is_some() {
                return Err(RegistryError::AddressAlreadyOccupied(pattern));
            }
        } else {
            if inner.single_wildcards.iter().any(|(p, _)| p == pat_bytes) {
                return Err(RegistryError::AddressAlreadyOccupied(pattern));
            }
        };

        // 2. Semantic conflict with existing PREFIX wildcards ().
        // This check is universal and works both for new "" and for new "*" patterns.
        if let Some(existing) = Self::check_prefix_wildcard_conflicts(&inner.prefix_wildcard_tree, pat_bytes)? {
            return Err(RegistryError::WildcardConflict(pattern, existing));
        }        

        // 3. Semantic conflict with existing SINGLE wildcards (*).
        // Iterate over the entire list, since they do not form a prefix hierarchy.
        for (existing_pat, _) in &inner.single_wildcards {
            if Self::patterns_conflict_bytes(existing_pat, pat_bytes)? {
                return Err(RegistryError::WildcardConflict(pattern, String::from_utf8_lossy(existing_pat).into_owned()));
            }
        }

        // 4. Check conflicts with exact addresses (Optimized: only scan relevant prefix)
        let static_pref = Self::static_prefix_bytes(pat_bytes);
        let exact_iter = inner.exact_tree.walk_prefix(static_pref);

        for (k, _slot) in exact_iter {
            if Self::compare_segments_bytes(pat_bytes, &k, false) {
                return Err(RegistryError::WildcardConflict(pattern, String::from_utf8_lossy(&k).into_owned()));
            }
        }

        // 6. Insert
        let slot = SubscriptionSlot::new(channel);
        if is_prefix_wildcard {
            let mut txn = inner.prefix_wildcard_tree.txn();
            txn.insert(pat_bytes, slot);
            inner.prefix_wildcard_tree = txn.commit();
        } else {
            inner.single_wildcards.push((pat_bytes.to_vec(), slot));
        }

        Ok(())
    }

    /// Unregisters a recipient by address or pattern.
    ///
    /// Note: This is a silent no-op if the address/pattern is not found, 
    /// which is standard for cleanup operations.
    pub fn unregister(&self, address: &str) -> Result<(), RegistryError>{
        let _guard = self.register_mutex.lock().unwrap();
        let current = self.inner.load();
        let mut new_inner = (**current).clone();
        let addr_bytes = address.as_bytes();

        tracing::debug!("unregister: {}", address);

        if Self::is_wildcard(address) {
            if address.ends_with("**") {
                if let Some(slot) = new_inner.prefix_wildcard_tree.get(addr_bytes) {
                    slot.mark_removed(); // Notify existing endpoints FIRST
                    let mut txn = new_inner.prefix_wildcard_tree.txn();
                    txn.remove(addr_bytes); // THEN remove from tree to prevent false wildcard conflicts
                    new_inner.prefix_wildcard_tree = txn.commit();
                }
            } else {
                if let Some(idx) = new_inner.single_wildcards.iter().position(|(p, _)| p.as_slice() == addr_bytes) {
                    let (_pattern, slot) = new_inner.single_wildcards.remove(idx);
                    slot.mark_removed();
                }
            }
        } else {
            if let Some(slot) = new_inner.exact_tree.get(addr_bytes) {
                slot.mark_removed(); // Notify existing endpoints FIRST
                let mut txn = new_inner.exact_tree.txn();
                txn.remove(addr_bytes); // THEN remove from tree
                new_inner.exact_tree = txn.commit();
            }
        }

        self.inner.store(Arc::new(new_inner));
        Ok(())
    }

    /// Finds a local channel for the given address.
    ///
    /// Returns `Some(channel)` if a recipient exists in this process.
    /// Priority is given to exact matches, followed by wildcard matches.
    pub fn lookup(&self, address: &str) -> Option<Arc<SubscriptionSlot>> {
        let inner = self.inner.load();
        let addr_bytes = address.as_bytes();    

        // 1. Exact match - O(L)
        if let Some(slot) = inner.exact_tree.get(addr_bytes) {
            return Some(slot.clone());
        }

        // 2. Prefix wildcard match - O(L) via get_ancestor
        if let Some(slot) = inner.prefix_wildcard_tree.get_ancestor(addr_bytes) {
            return Some(slot.clone());
        }

        // 3. Single wildcard match - O(N)
        for (pattern, slot) in &inner.single_wildcards {
            if Self::compare_segments_bytes(pattern, addr_bytes, false) {
                return Some(slot.clone());
            }
        }

        None
    }

    /// Checks if a local recipient exists for the given address (exact match only).
    /// Useful for quick negative caching or routing decisions.
    pub fn has_local(&self, address: &str) -> bool {
        let inner = self.inner.load();
        let addr_bytes = address.as_bytes();    

        inner.exact_tree.get(addr_bytes).is_some()
    }

    /// Checks if a local recipient exists for the given address, including wildcard matches.
    pub fn has_route(&self, address: &str) -> bool {
        self.lookup(address).is_some()
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    mod matches {
        use super::*;

        // ============================================================
        // 1. Exact matches
        // ============================================================

        #[test]
        fn exact_match() {
            assert!(LocalRegistry::matches(b"arcella", b"arcella").unwrap());
            assert!(LocalRegistry::matches(b"arcella:core:users", b"arcella:core:users").unwrap());

            assert!(!LocalRegistry::matches(b"arcella:core:users", b"arcella:core:admin").unwrap());
            assert!(!LocalRegistry::matches(b"Arcella", b"arcella").unwrap()); // Case-sensitive
        }

        // ============================================================
        // 2. Length mismatches (without wildcards)
        // ============================================================

        #[test]
        fn length_mismatch() {
            assert!(!LocalRegistry::matches(b"a:b:c", b"a:b").unwrap());
            assert!(!LocalRegistry::matches(b"a:b", b"a:b:c").unwrap());
        }

        // ============================================================
        // 3. Empty strings
        // ============================================================

            #[test]
        fn empty_strings() {
            assert!(LocalRegistry::matches(b"", b"").is_err());
            assert!(LocalRegistry::matches(b"a::b", b"a:b").is_err());
            assert!(LocalRegistry::matches(b"", b"a:b").is_err());

            assert!(!LocalRegistry::matches(b"a:b", b"").unwrap());
            assert!(!LocalRegistry::matches(b"a", b"").unwrap());
            assert!(!LocalRegistry::matches(b"a:b", b"a::b").unwrap());
        }

        // ============================================================
        // 4. Single-segment wildcard (*)
        // ============================================================

        #[test]
        fn single_segment_wildcard() {
            assert!(LocalRegistry::matches(b"a:*:c", b"a:b:c").unwrap());
            assert!(LocalRegistry::matches(b"a:b:*", b"a:b:c").unwrap());
            
            // Forbidden patterns must return an error
            assert!(LocalRegistry::matches(b"*", b"a").is_err());
            assert!(LocalRegistry::matches(b"*:b:c", b"a:b:c").is_err());
            assert!(LocalRegistry::matches(b"*:*:*", b"a:b:c").is_err());

            assert!(!LocalRegistry::matches(b"a:*:d", b"a:b:c").unwrap());
            assert!(!LocalRegistry::matches(b"a:*:d", b"a:b:c:d").unwrap());
            assert!(!LocalRegistry::matches(b"a:*:d", b"a:d").unwrap());
        }

        // ============================================================
        // 5. Multi-segment wildcard (**)
        // ============================================================

        #[test]
        fn multi_segment_wildcard() {
            assert!(LocalRegistry::matches(b"a:**",   b"a").unwrap());
            assert!(LocalRegistry::matches(b"a:b:**", b"a:b").unwrap());
            assert!(LocalRegistry::matches(b"a:b:**", b"a:b:c").unwrap());
            assert!(LocalRegistry::matches(b"a:b:**", b"a:b:c:d:e").unwrap());
            assert!(LocalRegistry::matches(b"a:**",   b"a:b:c:d:e").unwrap());

            // Forbidden patterns must return an error
            assert!(LocalRegistry::matches(b"**",     b"a").is_err());
            assert!(LocalRegistry::matches(b"**",     b"a:b:c:d").is_err());

            assert!(!LocalRegistry::matches(b"a:b:**", b"a").unwrap());
            assert!(!LocalRegistry::matches(b"a:b:**", b"x:b:c").unwrap());
        }

        // ============================================================
        // 6. Combinations * & **
        // ============================================================

        #[test]
        fn star_and_starstar_combined() {
            assert!(LocalRegistry::matches(b"a:*:c:**", b"a:b:c:d:e").unwrap());
            //assert!(LocalRegistry::matches(b"a:*:**", b"x:y:z").unwrap());

            // Forbidden patterns must return an error
            assert!(LocalRegistry::matches(b"*:b:**", b"a:b:c:d:e").is_err());
            assert!(LocalRegistry::matches(b"*:**", b"x:y:z").is_err());

            assert!(LocalRegistry::patterns_conflict_bytes(b"a:b:c:*", b"a:**").unwrap());
        }

        // ============================================================
        // 7. Invalid wildcard patterns
        // ============================================================
        #[test]
        fn invalid_patterns() {
            assert!(LocalRegistry::matches(b"a:**:b", b"a:b").is_err());
            assert!(LocalRegistry::matches(b"a:*:b:", b"a:b").is_err());
            assert!(LocalRegistry::matches(b"**:a:**", b"a:b").is_err());
            assert!(LocalRegistry::matches(b"a:**:**", b"a:b").is_err());
        }
    }    

}
