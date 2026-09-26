// arcella-broker/src/broker_core/registry/mod.rs
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
    Mutex,
};

mod error;
mod load_balanced_group;
mod routing;
mod slot;

pub use error::RegistryError;
pub use slot::SubscriptionSlot;
pub use routing::{RoutingPolicy, RouteTarget};
pub use load_balanced_group::LoadBalancedGroup;

/// Internal state of the registry, protected by a `RwLock`.
/// Separates exact matches and wildcards for optimized lookup and conflict detection.
#[derive(Clone)]
struct RegistryInner {
    /// Exact address -> routing target (Single slot or Group). 
    /// Key is `u8` for zero-allocation `&[u8]` queries.
    exact_tree: Radix<u8, Arc<RouteTarget>>,
    
    /// Prefix wildcards (ending in `:**`). O(L) lookup via `get_ancestor`.
    prefix_wildcard_tree: Radix<u8, Arc<RouteTarget>>,
    
    /// Single-segment wildcards (containing `*` but not ending in `**`).
    single_wildcards: Vec<(Vec<u8>, Arc<RouteTarget>)>,
}

/// The result of checking an address before registration.
/// Used for atomic detection of duplicates and routing conflicts.
enum LookupResult {
    /// An exact duplicate was found (the address is already registered).
    Duplicate(Arc<RouteTarget>),
    /// A conflict with an existing route was found.
    /// Contains the address of the existing route that conflicts.
    Conflict(String),
    /// Nothing was found, the address is free for registration.
    NotFound,
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

/// The result of the atomic "get existing or register new" operation.
pub enum RegisterResult {
    /// The address was free, and the new RouteTarget was successfully registered.
    Registered(Arc<RouteTarget>),
    /// The address was already occupied. Returns the existing RouteTarget to join the group.
    AlreadyExists(Arc<RouteTarget>),
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

    fn static_prefix_bytes(pattern: &[u8]) -> &[u8] {
        if let Some(idx) = pattern.iter().position(|&b| b == b'*') {
            &pattern[..idx]
        } else {
            pattern
        }
    }

    fn check_prefix_wildcard_conflicts(
        prefix_wildcard_tree: &Radix<u8, Arc<RouteTarget>>,
        pat_bytes: &[u8],
    ) -> Option<String> {
        if prefix_wildcard_tree.is_empty() {
            return None;
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
                return Some(String::from_utf8_lossy(&check_bytes).into_owned());
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
                return Some(String::from_utf8_lossy(&k).into_owned());
            }
        }       
        
        None
    }        

    /// Searches for an existing route for an exact address.
    /// Checks for duplicates and conflicts with wildcard patterns.
    fn get_exact(&self, inner: &RegistryInner, address: &str) -> LookupResult {
        let addr_bytes = address.as_bytes();
        
        // 1. Exact duplicate check (O(L))
        if let Some(target) = inner.exact_tree.get(addr_bytes) {
            return LookupResult::Duplicate(target.clone());
        }
        
        // 2. Check conflicts with prefix wildcards (O(L) via walk_path)
        if let Some((k, _)) = inner.prefix_wildcard_tree.walk_path(addr_bytes).next() {
            return LookupResult::Conflict(String::from_utf8_lossy(&k).into_owned());
        }
        
        // 3. Check conflicts with single wildcards (O(N))
        for (pattern, _) in &inner.single_wildcards {
            if Self::compare_segments_bytes(pattern, addr_bytes, false) {
                return LookupResult::Conflict(String::from_utf8_lossy(pattern).into_owned());
            }
        }
        
        LookupResult::NotFound
    }

    /// Searches for an existing route for a wildcard pattern.
    /// Checks for duplicates and conflicts with exact addresses and other wildcards.
    fn get_wildcard(&self, inner: &RegistryInner, pattern: &str) -> LookupResult {
        let pat_bytes = pattern.as_bytes();
        let is_prefix_wildcard = pattern.ends_with("**");
        
        // 1. Exact duplication (Fast path O(L))
        if is_prefix_wildcard {
            if let Some(target) = inner.prefix_wildcard_tree.get(pat_bytes) {
                return LookupResult::Duplicate(target.clone());
            }
        } else {
            if let Some((_, target)) = inner.single_wildcards.iter().find(|(p, _)| p.as_slice() == pat_bytes) {
                return LookupResult::Duplicate(target.clone());
            }
        }
        
        // 2. Semantic conflict with existing PREFIX wildcards.
        // This check is universal and works both for new "**" and for new "*" patterns.
        if let Some(existing) = Self::check_prefix_wildcard_conflicts(&inner.prefix_wildcard_tree, pat_bytes) {
            return LookupResult::Conflict(existing);
        }
        
        // 3. Semantic conflict with existing SINGLE wildcards (*).
        // Iterate over the entire list, since they do not form a prefix hierarchy.
        for (existing_pat, _) in &inner.single_wildcards {
            if Self::compare_segments_bytes(existing_pat, pat_bytes, true) {
                return LookupResult::Conflict(String::from_utf8_lossy(existing_pat).into_owned());
            }
        }

        // 4. Check conflicts with exact addresses (Optimized: only scan relevant prefix)
        let static_pref = Self::static_prefix_bytes(pat_bytes);
        let exact_iter = inner.exact_tree.walk_prefix(static_pref);
        for (k, _) in exact_iter {
            if Self::compare_segments_bytes(pat_bytes, &k, false) {
                return LookupResult::Conflict(String::from_utf8_lossy(&k).into_owned());
            }
        }
        
        LookupResult::NotFound
    }

    /// Registers a recipient at the specified address with the given routing policy.
    ///
    /// # Arguments
    /// * `address` - logical address or wildcard pattern.
    /// * `target` - the routing target to register.
    ///
    /// # Errors
    /// - `AddressAlreadyOccupied` if an Exclusive subscription already exists.
    pub fn register(
        &self,
        address: String, 
        target: Arc<RouteTarget>,
    ) -> Result<(), RegistryError> {
        let _guard = self.register_mutex.lock().unwrap();
        let current = self.inner.load();
        
        tracing::debug!("LocalRegistry register: {}", address);

        let is_wildcard = Self::is_wildcard(&address);

        let lookup_result = if is_wildcard {
            Self::validate_wildcard_pattern(&address)?;
            self.get_wildcard(&current, &address)
        } else {
            self.get_exact(&current, &address) 
        };

        match (lookup_result, is_wildcard) {
            (LookupResult::Duplicate(_), _) => {
                return Err(RegistryError::AddressAlreadyOccupied(address));
            }
            (LookupResult::Conflict(wc), true) => {
                return Err(RegistryError::WildcardConflict(address, wc));    
            }
            (LookupResult::Conflict(wc), false) => {
                return Err(RegistryError::ConflictsWithWildcard(address, wc));    
            }
            (LookupResult::NotFound, _) => {}
        }

        let mut new_inner = (**current).clone();
        let addr_bytes = address.as_bytes();

        if !is_wildcard {
            // Insert (O(L) copy-on-write)
            let mut txn = new_inner.exact_tree.txn();
            txn.insert(addr_bytes, target);
            new_inner.exact_tree = txn.commit(); 
        } else if address.ends_with("**") {
            let mut txn = new_inner.prefix_wildcard_tree.txn();
            txn.insert(addr_bytes, target);
            new_inner.prefix_wildcard_tree = txn.commit();
        } else {
            new_inner.single_wildcards.push((addr_bytes.to_vec(), target));
        }
        self.inner.store(Arc::new(new_inner));

        Ok(())
    }

    /// Atomically checks if an address exists and registers a new RouteTarget
    /// if the address is free. Both operations are performed under a single `register_mutex`,
    /// which eliminates TOCTOU races.
    ///
    /// # Arguments
    /// * `address` - logical address or wildcard pattern.
    /// * `factory` - closure that creates the `RouteTarget`. It is called **strictly under the mutex** 
    ///   and **only** if the address is free and all conflict checks have passed. 
    ///   This guarantees that expensive resources (e.g., `tokio::spawn` in `LoadBalancedGroup`) 
    ///   are not created in vain during a thread race.
    ///
    /// # Errors
    /// - `AddressAlreadyOccupied` / `WildcardConflict` / `ConflictsWithWildcard` - if the address is occupied or conflicts.
    /// - Any error returned by the `factory` closure.
    pub fn get_existing_or_register<F>(
        &self,
        address: String,
        factory: F,
    ) -> Result<RegisterResult, RegistryError>
    where
        F: FnOnce() -> Result<Arc<RouteTarget>, RegistryError>,
    {
        // 1. Lock the mutex for the atomicity of the entire "check + action" operation
        let _guard = self.register_mutex.lock().unwrap();
        let current = self.inner.load();
        
        let is_wildcard = Self::is_wildcard(&address);

        // 2. Perform the check (duplicates and conflicts) on the current snapshot
        let lookup_result = if is_wildcard {
            // Note: validate_wildcard_pattern is called here to ensure validation happens before get_wildcard

            Self::validate_wildcard_pattern(&address)?; 
            self.get_wildcard(&current, &address)
        } else {
            self.get_exact(&current, &address)
        };

        // 3. Process the check result
        match lookup_result {
            LookupResult::Duplicate(target) => {
                // Address is already occupied, return the existing target (e.g., to join a LoadBalanced group)
                Ok(RegisterResult::AlreadyExists(target))
            }
            LookupResult::Conflict(existing) => {
                // Found a semantic conflict with an existing route
                if is_wildcard {
                    Err(RegistryError::WildcardConflict(address, existing))
                } else {
                    Err(RegistryError::ConflictsWithWildcard(address, existing))
                }
            }
            LookupResult::NotFound => {
                // 4. Address is free! Call the factory UNDER the mutex.
                // This is critical: if we called factory() before acquiring the mutex,
                // another thread could have occupied the address, and we would have created a tokio::spawn task in vain.
                let target = factory()?;

                // 5. Prepare the new state for ArcSwap (Copy-on-Write)
                let mut new_inner = (**current).clone();
                let addr_bytes = address.as_bytes();

                // 6. Insert into the corresponding data structure
                if !is_wildcard {
                    let mut txn = new_inner.exact_tree.txn();
                    txn.insert(addr_bytes, target.clone());
                    new_inner.exact_tree = txn.commit();
                } else if address.ends_with("**") {
                    let mut txn = new_inner.prefix_wildcard_tree.txn();
                    txn.insert(addr_bytes, target.clone());
                    new_inner.prefix_wildcard_tree = txn.commit();
                } else {
                    new_inner.single_wildcards.push((addr_bytes.to_vec(), target.clone()));
                }

                // 7. Atomically publish the new state for all readers
                self.inner.store(Arc::new(new_inner));

                Ok(RegisterResult::Registered(target))
            }
        }
    }

    /// Unregisters a specific subscription slot from the given address.
    ///
    /// For **Exclusive** (Single) targets: removes the entire route from the tree.
    /// For **Group** targets (LoadBalanced/Broadcast): removes only the specified 
    /// slot from the group. The route is removed from the tree only when the 
    /// group becomes empty.
    ///
    /// The slot is identified by `Arc::ptr_eq`, ensuring that only the exact 
    /// subscription is removed, even if multiple slots share the same address.
    ///
    /// Note: This is a silent no-op if the address or slot is not found.
    pub fn unregister(
        &self,
        address: &str,
        slot: &Arc<SubscriptionSlot>
    ) -> Result<(), RegistryError>{
        let _guard = self.register_mutex.lock().unwrap();
        let current = self.inner.load();
        let mut new_inner = (**current).clone();
        let addr_bytes = address.as_bytes();

        tracing::debug!("LocalRegistry unregister: {}", address);

        let mut needs_update = false;

        if Self::is_wildcard(address) {
            if address.ends_with("**") {
                if let Some(target) = new_inner.prefix_wildcard_tree.get(addr_bytes) {
                    if target.remove_slot(slot) {
                        let mut txn = new_inner.prefix_wildcard_tree.txn();
                        txn.remove(addr_bytes); // THEN remove from tree to prevent false wildcard conflicts
                        new_inner.prefix_wildcard_tree = txn.commit();
                        needs_update = true;
                    }
                }
            } else {
                if let Some(idx) = new_inner.single_wildcards.iter().position(|(p, _)| p.as_slice() == addr_bytes) {
                    let target = new_inner.single_wildcards[idx].1.clone();
                    if target.remove_slot(slot) {
                        new_inner.single_wildcards.remove(idx);
                        needs_update = true;
                    }
                }
            }
        } else {
            if let Some(target) = new_inner.exact_tree.get(addr_bytes) {
                if target.remove_slot(slot) {
                    let mut txn = new_inner.exact_tree.txn();
                    txn.remove(addr_bytes); // THEN remove from tree
                    new_inner.exact_tree = txn.commit();
                    needs_update = true;
                }
            }
        }

        if needs_update {
            self.inner.store(Arc::new(new_inner));
        }

        Ok(())
    }

    /// Finds a local channel for the given address.
    ///
    /// Returns `Some(channel)` if a recipient exists in this process.
    /// Priority is given to exact matches, followed by wildcard matches.
    pub fn lookup(&self, address: &str) -> Option<Arc<RouteTarget>> {
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
    use super::super::transport::{create_channel, MessageSender};

    fn create_dummy_sender() -> MessageSender {
        let (tx, _rx) = create_channel(10);
        tx
    }

    mod matches {
        use super::*;

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
            LocalRegistry::validate_wildcard_pattern(&p1)?;

            Ok(LocalRegistry::compare_segments_bytes(pattern, address, false))
        } 

        /// Checks whether two wildcard patterns can ever match the same concrete address.
        /// Used during registration to enforce the exclusive binding model.
        fn patterns_conflict_bytes(pattern1: &[u8], pattern2: &[u8]) -> Result<bool, RegistryError> {
            let p1 = String::from_utf8_lossy(pattern1);
            let p2 = String::from_utf8_lossy(pattern2);
            LocalRegistry::validate_wildcard_pattern(&p1)?;
            LocalRegistry::validate_wildcard_pattern(&p2)?;
            Ok(LocalRegistry::compare_segments_bytes(pattern1, pattern2, true))
        }

        // ============================================================
        // 1. Exact matches
        // ============================================================

        #[test]
        fn exact_match() {
            assert!(matches(b"arcella", b"arcella").unwrap());
            assert!(matches(b"arcella:core:users", b"arcella:core:users").unwrap());

            assert!(!matches(b"arcella:core:users", b"arcella:core:admin").unwrap());
            assert!(!matches(b"Arcella", b"arcella").unwrap()); // Case-sensitive
        }

        // ============================================================
        // 2. Length mismatches (without wildcards)
        // ============================================================

        #[test]
        fn length_mismatch() {
            assert!(!matches(b"a:b:c", b"a:b").unwrap());
            assert!(!matches(b"a:b", b"a:b:c").unwrap());
        }

        // ============================================================
        // 3. Empty strings
        // ============================================================

            #[test]
        fn empty_strings() {
            assert!(matches(b"", b"").is_err());
            assert!(matches(b"a::b", b"a:b").is_err());
            assert!(matches(b"", b"a:b").is_err());

            assert!(!matches(b"a:b", b"").unwrap());
            assert!(!matches(b"a", b"").unwrap());
            assert!(!matches(b"a:b", b"a::b").unwrap());
        }

        // ============================================================
        // 4. Single-segment wildcard (*)
        // ============================================================

        #[test]
        fn single_segment_wildcard() {
            assert!(matches(b"a:*:c", b"a:b:c").unwrap());
            assert!(matches(b"a:b:*", b"a:b:c").unwrap());
            
            // Forbidden patterns must return an error
            assert!(matches(b"*", b"a").is_err());
            assert!(matches(b"*:b:c", b"a:b:c").is_err());
            assert!(matches(b"*:*:*", b"a:b:c").is_err());

            assert!(!matches(b"a:*:d", b"a:b:c").unwrap());
            assert!(!matches(b"a:*:d", b"a:b:c:d").unwrap());
            assert!(!matches(b"a:*:d", b"a:d").unwrap());
        }

        // ============================================================
        // 5. Multi-segment wildcard (**)
        // ============================================================

        #[test]
        fn multi_segment_wildcard() {
            assert!(matches(b"a:**",   b"a").unwrap());
            assert!(matches(b"a:b:**", b"a:b").unwrap());
            assert!(matches(b"a:b:**", b"a:b:c").unwrap());
            assert!(matches(b"a:b:**", b"a:b:c:d:e").unwrap());
            assert!(matches(b"a:**",   b"a:b:c:d:e").unwrap());

            // Forbidden patterns must return an error
            assert!(matches(b"**",     b"a").is_err());
            assert!(matches(b"**",     b"a:b:c:d").is_err());

            assert!(!matches(b"a:b:**", b"a").unwrap());
            assert!(!matches(b"a:b:**", b"x:b:c").unwrap());
        }

        // ============================================================
        // 6. Combinations * & **
        // ============================================================

        #[test]
        fn star_and_starstar_combined() {
            assert!(matches(b"a:*:c:**", b"a:b:c:d:e").unwrap());
            //assert!(matches(b"a:*:**", b"x:y:z").unwrap());

            // Forbidden patterns must return an error
            assert!(matches(b"*:b:**", b"a:b:c:d:e").is_err());
            assert!(matches(b"*:**", b"x:y:z").is_err());

            assert!(patterns_conflict_bytes(b"a:b:c:*", b"a:**").unwrap());
        }

        // ============================================================
        // 7. Invalid wildcard patterns
        // ============================================================
        #[test]
        fn invalid_patterns() {
            assert!(matches(b"a:**:b", b"a:b").is_err());
            assert!(matches(b"a:*:b:", b"a:b").is_err());
            assert!(matches(b"**:a:**", b"a:b").is_err());
            assert!(matches(b"a:**:**", b"a:b").is_err());
        }
    }

    mod register {
        use super::*;

        #[test]
        fn test_register_lookup_and_unregister() {
            let registry = LocalRegistry::new();
            let address = "arcella:core:users".to_string();
            let sender = create_dummy_sender();

            // 1. Registration
            let slot = SubscriptionSlot::new(sender);
            let target = RouteTarget::new_exclusive(slot.clone());
            
            assert!(registry.register(address.clone(), target).is_ok());
            
            // 2. Successful lookup
            assert!(registry.lookup(&address).is_some());
            assert!(registry.has_route(&address));
            assert!(registry.has_local(&address));

            // 3. Removal
            // Note: in the current implementation, unregister requires a slot,
            // but for simplicity of the test we can check that unregister by address works
            // if we pass any slot (since remove_slot will return false for someone else's slot,
            // but in the current implementation unregister removes the entire address if the slot matches.
            // For the test we need to get the real slot).
            
            // Get the slot via lookup for a correct unregister
            assert!(registry.unregister(&address, &slot).is_ok());

            // 4. Verification after removal
            assert!(registry.lookup(&address).is_none());
            assert!(!registry.has_route(&address));
        }

        #[test]
        fn test_conflict_exact_vs_wildcard() {
            let registry = LocalRegistry::new();
            let exact_addr = "arcella:core:users".to_string();
            let wildcard_addr = "arcella:core:*".to_string();

            // Register the exact address
            let slot = SubscriptionSlot::new(create_dummy_sender());
            let target = RouteTarget::new_exclusive(slot.clone());
            registry.register(exact_addr.clone(), target).unwrap();

            // Attempting to register an overlapping wildcard must fail
            let slot = SubscriptionSlot::new(create_dummy_sender());
            let target = RouteTarget::new_exclusive(slot.clone());
            let err = registry.register(wildcard_addr.clone(), target).unwrap_err();
            assert!(matches!(err, RegistryError::WildcardConflict(_, _)));

            // And vice versa: first wildcard, then exact address
            let registry2 = LocalRegistry::new();
            let slot = SubscriptionSlot::new(create_dummy_sender());
            let target = RouteTarget::new_exclusive(slot.clone());
            registry2.register(wildcard_addr.clone(), target).unwrap();
            
            let slot = SubscriptionSlot::new(create_dummy_sender());
            let target = RouteTarget::new_exclusive(slot.clone());
            let err2 = registry2.register(exact_addr.clone(), target).unwrap_err();
            assert!(matches!(err2, RegistryError::ConflictsWithWildcard(_, _)));
        }

        #[test]
        fn test_lookup_priority_exact_over_wildcard() {
            let registry = LocalRegistry::new();
            let exact_addr = "arcella:core:users".to_string();
            let wildcard_addr = "arcella:core:*".to_string();

            let slot = SubscriptionSlot::new(create_dummy_sender());
            let target1 = RouteTarget::new_exclusive(slot.clone());
            
            let slot = SubscriptionSlot::new(create_dummy_sender());
            let target2 = RouteTarget::new_exclusive(slot.clone());

            // 1. Register the wildcard first
            registry.register(wildcard_addr.clone(), target1.clone()).unwrap();

            // 2. Attempting to register an exact address that falls under the wildcard MUST fail
            let err = registry.register(exact_addr.clone(), target2.clone()).unwrap_err();
            assert!(
                matches!(err, RegistryError::ConflictsWithWildcard(_, _)),
                "Expected ConflictsWithWildcard error, got: {:?}",
                err
            );

            // 3. The reverse situation: register the exact address first
            let registry2 = LocalRegistry::new();
            registry2.register(exact_addr.clone(), target2.clone()).unwrap();

            // 4. Attempting to register a wildcard that covers the exact address MUST fail
            let err2 = registry2.register(wildcard_addr.clone(), target1).unwrap_err();
            assert!(
                matches!(err2, RegistryError::WildcardConflict(_, _)),
                "Expected WildcardConflict error, got: {:?}",
                err2
            );
        }

        #[test]
        fn test_get_existing_or_register_atomic() {
            let registry = LocalRegistry::new();
            let address = "arcella:lb:group".to_string();
            let mut factory_call_count = 0;

            // 1. First call: the address is free, the factory must be invoked
            let result1 = registry.get_existing_or_register(address.clone(), || {
                factory_call_count += 1;
                Ok(RouteTarget::new_exclusive(SubscriptionSlot::new(create_dummy_sender())))
            }).unwrap();

            assert!(matches!(result1, RegisterResult::Registered(_)));
            assert_eq!(factory_call_count, 1);

            // 2. Second call: the address is taken, the factory must NOT be invoked, Existing is returned
            let result2 = registry.get_existing_or_register(address.clone(), || {
                factory_call_count += 1; // This must not execute
                Ok(RouteTarget::new_exclusive(SubscriptionSlot::new(create_dummy_sender())))
            }).unwrap();

            assert!(matches!(result2, RegisterResult::AlreadyExists(_)));
            assert_eq!(factory_call_count, 1); // The count did not change!
        }

        #[test]
        fn test_unregister_with_wrong_slot_is_noop() {
            let registry = LocalRegistry::new();
            let address = "arcella:test".to_string();
            
            let slot1 = SubscriptionSlot::new(create_dummy_sender());
            let target1 = RouteTarget::new_exclusive(slot1.clone());
            
            let slot2 = SubscriptionSlot::new(create_dummy_sender());

            registry.register(address.clone(), target1.clone()).unwrap();

            // Attempting to remove the address by passing the WRONG slot
            let result = registry.unregister(&address, &slot2);
            assert!(result.is_ok()); // There must be no error (silent no-op)

            // The address must still be in the registry, since the slot did not match
            assert!(registry.lookup(&address).is_some());
            assert!(Arc::ptr_eq(&registry.lookup(&address).unwrap(), &target1));
        } 
    }

    mod multithread {
        use super::*;

        #[test]
        fn test_get_existing_or_register_concurrent_race() {
            use std::sync::atomic::{AtomicUsize, Ordering};
            use std::thread;

            let registry = Arc::new(LocalRegistry::new());
            let address = "arcella:lb:race_test".to_string();
            let factory_calls = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];

            // Spawn 100 threads that simultaneously try to register the same address
            for _ in 0..100 {
                let registry_clone = registry.clone();
                let addr = address.clone();
                let calls = factory_calls.clone();
                
                handles.push(thread::spawn(move || {
                    registry_clone.get_existing_or_register(addr, || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(RouteTarget::new_exclusive(SubscriptionSlot::new(create_dummy_sender())))
                    })
                }));
            }

            // Collect results from all threads
            let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

            // 1. The factory must be called EXACTLY once
            assert_eq!(
                factory_calls.load(Ordering::SeqCst), 
                1, 
                "Factory must be called exactly once under concurrent access"
            );

            // 2. Additional safety check: verify the distribution of results
            let registered_count = results.iter().filter(|r| matches!(r, Ok(RegisterResult::Registered(_)))).count();
            let already_exists_count = results.iter().filter(|r| matches!(r, Ok(RegisterResult::AlreadyExists(_)))).count();

            assert_eq!(registered_count, 1, "Exactly one thread should succeed in registering");
            assert_eq!(already_exists_count, 99, "The remaining 99 threads should receive AlreadyExists");
            
            // 3. Ensure no errors occurred
            assert!(results.iter().all(|r| r.is_ok()), "All threads should complete without RegistryError");
        }        
    }
}
