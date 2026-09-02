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

use parking_lot::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;

mod reply_dispatcher;

use crate::protocol::Message;
use reply_dispatcher::ReplyDispatcher;

/// Channel type for local message delivery.
pub type LocalChannel = mpsc::Sender<Message>;
pub type LocalReceiver = mpsc::Receiver<Message>;

pub const REPLY_WILDCARD_ADDRESS: &str = "arcella:reply:**";
pub const DEFAULT_REPLY_CHANNEL_CAPACITY: usize = 1024;

/// Internal state of the registry, protected by a `RwLock`.
/// Separates exact matches and wildcards for optimized lookup and conflict detection.
struct RegistryInner {
    /// Exact address -> channel
    exact: HashMap<String, LocalChannel>,
    /// Wildcard pattern -> channel
    wildcards: HashMap<String, LocalChannel>,
    /// Dispatcher for InOut (Request/Response) message correlations
    reply_dispatcher: ReplyDispatcher,
}

/// Registry of local recipients (within a single process).
///
/// Supports two subscription types:
/// - **Exact**: `"arcella:core:users"` - receives only messages to this exact address.
/// - **Wildcard**: patterns containing `*` (single segment) or ending with `**` (multi-segment).
pub struct LocalRegistry {
    recipients: RwLock<RegistryInner>,
}

/// Errors that can occur during registry operations.
#[derive(Debug, thiserror::Error)]
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
    pub fn new(reply_channel_capacity: usize) -> Self {
        assert!(
            reply_channel_capacity > 0,
            "reply_channel_capacity must be greater than 0"
        );
                
        // Create channel for the reply wildcard subscription
        let (reply_tx, reply_rx) = mpsc::channel(reply_channel_capacity);
        
        // Initialize ReplyDispatcher (starts background listener task)
        let reply_dispatcher = ReplyDispatcher::new(reply_rx);

        // Pre-register the reply wildcard in the wildcards map
        let mut wildcards = HashMap::new();
        wildcards.insert(REPLY_WILDCARD_ADDRESS.to_string(), reply_tx);        

        Self {
            recipients: RwLock::new(RegistryInner {
                exact: HashMap::new(),
                wildcards,
                reply_dispatcher,
            }),
        }
    }

    /// Returns a clone of the `ReplyDispatcher` for use in transport layers.
    pub fn reply_dispatcher(&self) -> ReplyDispatcher {
        self.recipients.read().reply_dispatcher.clone()
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
    fn compare_segments<'a>(
        pattern: &str,
        target: &str,
        allow_wildcard_both_sides: bool,
    ) -> bool {
        let mut pat_iter = pattern.split(':');
        let mut tar_iter = target.split(':');

        loop {
            let seg1 = pat_iter.next();
            let seg2 = tar_iter.next();

            match (seg1, seg2) {
                (None, None) => return true,
                
                // Handle ** when checking pattern-to-pattern conflicts
                (Some("**"), _) if allow_wildcard_both_sides => return true,
                (_, Some("**")) if allow_wildcard_both_sides => return true,
                // Handle ** when matching pattern to concrete address.
                // Since ** must be at the end (enforced by validation), if we see it 
                // in the pattern, it automatically matches the rest of the target.
                (Some("**"), _) => return pat_iter.next().is_none(),
                
                // Length mismatch: one string has more segments than the other
                (Some(_), None) | (None, Some(_)) => return false,
                
                // Compare individual segments
                (Some(a), Some(b)) => {
                    if a != "*" && b != "*" && a != b {
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
    fn matches(pattern: &str, address: &str) -> Result<bool, RegistryError> {
        Self::validate_wildcard_pattern(pattern)?;

        Ok(Self::compare_segments(pattern, address, false))

    } 

    /// Checks whether two wildcard patterns can ever match the same concrete address.
    /// Used during registration to enforce the exclusive binding model.
    fn patterns_conflict(pattern1: &str, pattern2: &str) -> Result<bool, RegistryError> {
        Self::validate_wildcard_pattern(pattern1)?;
        Self::validate_wildcard_pattern(pattern2)?;

        Ok(Self::compare_segments(pattern1, pattern2, true))
    }

    /// Registers a recipient at the specified address.
    ///
    /// Automatically routes to `register_exact` or `register_wildcard` based on 
    /// the presence of the `*` character.
    pub fn register(&self, address: String, channel: LocalChannel) -> Result<(), RegistryError> {
        if Self::is_wildcard(&address) {
            self.register_wildcard(address, channel)
        } else {
            self.register_exact(address, channel)
        }
    }

    /// Registers an exact, non-wildcard address.
    fn register_exact(
        &self,
        address: String,
        channel: LocalChannel,
    ) -> Result<(), RegistryError> {

        let mut recipients = self.recipients.write();

        // 1. Exact duplicate check
        if recipients.exact.contains_key(&address) {
            return Err(RegistryError::AddressAlreadyOccupied(address.clone()));
        }

        // 2. Check if any existing wildcard covers this new exact address
        for wc in recipients.wildcards.keys() {
            if Self::matches(wc, &address)? {
                return Err(RegistryError::ConflictsWithWildcard (
                    address.clone(),
                    wc.to_string(),
                ));
            }
        }

        // 3. Insert - we hold an exclusive write lock, so no race conditions are possible
        recipients.exact.insert(address, channel);
        Ok(())

    }
    
    /// Registers a wildcard pattern.
    fn register_wildcard(
        &self,
        pattern: String,
        channel: LocalChannel,
    ) -> Result<(), RegistryError> {
        // 1. Validate wildcard format syntax
        Self::validate_wildcard_pattern(&pattern)?;

        let mut recipients = self.recipients.write();

        // 2. Exact duplicate check (same pattern already registered)
        if recipients.wildcards.contains_key(&pattern) {
            return Err(RegistryError::AddressAlreadyOccupied(pattern));
        }

        // 3. Check conflicts with existing WILDCARDS
        for existing_wc in recipients.wildcards.keys() {
            if Self::patterns_conflict(existing_wc, &pattern)? {
                return Err(RegistryError::WildcardConflict(pattern, existing_wc.to_string()));
            }
        }

        // 4. Check conflicts with existing EXACT addresses
        for existing_addr in recipients.exact.keys() {
            if Self::matches(&pattern, existing_addr)? {
                return Err(RegistryError::WildcardConflict(pattern, existing_addr.to_string()));
            }
        }

        // 5. Insert
        recipients.wildcards.insert(pattern, channel);
        Ok(())
    }

    /// Unregisters a recipient by address or pattern.
    ///
    /// Note: This is a silent no-op if the address/pattern is not found, 
    /// which is standard for cleanup operations.
    pub fn unregister(&self, address: &str) {
        if address == REPLY_WILDCARD_ADDRESS {
            return;
        }

        let mut recipients = self.recipients.write();

         if Self::is_wildcard(address) {
            recipients.wildcards.remove(address);
        } else {
            recipients.exact.remove(address);
        }
    }

    /// Finds a local channel for the given address.
    ///
    /// Returns `Some(channel)` if a recipient exists in this process.
    /// Priority is given to exact matches, followed by wildcard matches.
    pub fn lookup(&self, address: &str) -> Option<LocalChannel> {
        let recipients = self.recipients.read();

        // 1. Exact match - highest priority and fastest path (O(1))
        if let Some(channel) = recipients.exact.get(address) {
            return Some(channel.clone());
        }

        // 2. Scan wildcard index
        let mut best: Option<(usize, LocalChannel)> = None;
        for (pattern, channel) in &recipients.wildcards {
            if Self::compare_segments(pattern, address, false) {
                let specificity = pattern
                    .split(':')
                    .filter(|s| *s != "**")
                    .count();

                let should_update = match &best {
                    None => true,
                    Some((best_spec, _)) => specificity > *best_spec,
                };

                if should_update {
                   best = Some((specificity, channel.clone()));
                }
            }
        }

        best.map(|(_, ch)| ch)

    }

    /// Checks if a local recipient exists for the given address (exact match only).
    /// Useful for quick negative caching or routing decisions.
    pub fn has_local(&self, address: &str) -> bool {
        let recipients = self.recipients.read();
        recipients.exact.contains_key(address)
    }

    /// Checks if a local recipient exists for the given address, including wildcard matches.
    pub fn has_route(&self, address: &str) -> bool {
        self.lookup(address).is_some()
    }

}

impl Default for LocalRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_REPLY_CHANNEL_CAPACITY)
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
            assert!(LocalRegistry::matches("arcella", "arcella").unwrap());
            assert!(LocalRegistry::matches("arcella:core:users", "arcella:core:users").unwrap());

            assert!(!LocalRegistry::matches("arcella:core:users", "arcella:core:admin").unwrap());
            assert!(!LocalRegistry::matches("Arcella", "arcella").unwrap()); // Case-sensitive
        }

        // ============================================================
        // 2. Length mismatches (without wildcards)
        // ============================================================

        #[test]
        fn length_mismatch() {
            assert!(!LocalRegistry::matches("a:b:c", "a:b").unwrap());
            assert!(!LocalRegistry::matches("a:b", "a:b:c").unwrap());
        }

        // ============================================================
        // 3. Empty strings
        // ============================================================

            #[test]
        fn empty_strings() {
            assert!(LocalRegistry::matches("", "").is_err());
            assert!(LocalRegistry::matches("a::b", "a:b").is_err());
            assert!(LocalRegistry::matches("", "a:b").is_err());

            assert!(!LocalRegistry::matches("a:b", "").unwrap());
            assert!(!LocalRegistry::matches("a", "").unwrap());
            assert!(!LocalRegistry::matches("a:b", "a::b").unwrap());
        }

        // ============================================================
        // 4. Single-segment wildcard (*)
        // ============================================================

        #[test]
        fn single_segment_wildcard() {
            assert!(LocalRegistry::matches("*:b:c", "a:b:c").unwrap());
            assert!(LocalRegistry::matches("a:*:c", "a:b:c").unwrap());
            assert!(LocalRegistry::matches("a:b:*", "a:b:c").unwrap());
            assert!(LocalRegistry::matches("*:*:*", "a:b:c").unwrap());

            assert!(!LocalRegistry::matches("a:*:d", "a:b:c").unwrap());
            assert!(!LocalRegistry::matches("a:*:d", "a:b:c:d").unwrap());
            assert!(!LocalRegistry::matches("a:*:d", "a:d").unwrap());
        }

        // ============================================================
        // 5. Multi-segment wildcard (**)
        // ============================================================

        #[test]
        fn multi_segment_wildcard() {
            assert!(LocalRegistry::matches("**",     "a").unwrap());
            assert!(LocalRegistry::matches("a:**",   "a").unwrap());
            assert!(LocalRegistry::matches("a:b:**", "a:b").unwrap());
            assert!(LocalRegistry::matches("a:b:**", "a:b:c").unwrap());
            assert!(LocalRegistry::matches("**",     "a:b:c:d").unwrap());
            assert!(LocalRegistry::matches("a:b:**", "a:b:c:d:e").unwrap());
            assert!(LocalRegistry::matches("a:**",   "a:b:c:d:e").unwrap());

            assert!(!LocalRegistry::matches("a:b:**", "a").unwrap());
            assert!(!LocalRegistry::matches("a:b:**", "x:b:c").unwrap());
        }

        // ============================================================
        // 6. Combinations * & **
        // ============================================================

        #[test]
        fn star_and_starstar_combined() {
            assert!(LocalRegistry::matches("*:b:**", "a:b:c:d:e").unwrap());
            assert!(LocalRegistry::matches("*:**", "x:y:z").unwrap());
            
            assert!(LocalRegistry::patterns_conflict("a:b:c:*", "a:**").unwrap());
        }

        // ============================================================
        // 7. Invalid wildcard patterns
        // ============================================================
        #[test]
        fn invalid_patterns() {
            assert!(LocalRegistry::matches("a:**:b", "a:b").is_err());
            assert!(LocalRegistry::matches("a:*:b:", "a:b").is_err());
            assert!(LocalRegistry::matches("**:a:**", "a:b").is_err());
            assert!(LocalRegistry::matches("a:**:**", "a:b").is_err());
        }
    }    

}
