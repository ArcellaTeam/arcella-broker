// arcella-broker/src/l2/ingress_bridge.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! # Ingress Bridge (L2 → L3 Boundary)
//!
//! This module defines the `IngressBridge` trait, which serves as the strict boundary 
//! between the L2 transport layer (Frame Receiver) and the L3 application/routing layer.
//! 
//! According to the L2 Specification (Section 10), the Ingress Bridge is responsible for:
//! 1. Deserializing raw L3 message bytes.
//! 2. Validating the L3 message format.
//! 3. Enforcing routing security: verifying that the authenticated `source` (from the 
//!    session context) is authorized to send to the `destination` (from the message), 
//!    preventing address spoofing.
//! 4. Performing route lookup in the `LocalRegistry` and queuing the message for L3 delivery.

use bytes::Bytes;

use super::super::security::SessionContext;

/// Result of processing an incoming frame by the Ingress Bridge.
/// 
/// This enum dictates the flow control actions that the L2 Frame Receiver must take 
/// in response to the L3 processing outcome.
#[derive(Debug, PartialEq)]
pub enum IngressResult {
    /// The frame was successfully validated, authorized, and queued in L3.
    /// 
    /// # L2 Action Required:
    /// The L2 receiver must immediately send an `Acknowledge` (0x03) frame to the sender 
    /// and replenish the consumed hot credit.
    Accepted,
    /// The destination address is currently blocked due to L3 queue backpressure.
    /// 
    /// # L2 Action Required:
    /// The L2 receiver must NOT send an Acknowledge. Instead, it must allocate a cell 
    /// from the Cold Pool (if available) and aggregate (coalesce) this frame into the 
    /// cold buffer for this specific address. If the Cold Pool is exhausted, L2 must 
    /// send a `Drop` (0x04) frame with reason `buffer_overflow`.
    AddressBlocked,
    /// Processing failed due to validation errors, routing rule violations, or 
    /// hierarchical domain isolation breaches (e.g., unauthorized parent-child routing).
    /// 
    /// # L2 Action Required:
    /// The frame is rejected. The consumed credit is NOT returned to the sender 
    /// (acting as a penalty/backpressure mechanism). Depending on the severity, 
    /// L2 may send an `Error` (0xff) frame or terminate the connection.
    Rejected(String),
}

/// Trait defining the contract for the L2 → L3 Ingress Bridge.
/// 
/// # Architectural Role
/// The `IngressBridge` is invoked by the L2 Frame Receiver *after* a `Data` (0xfe) 
/// frame has been parsed and its destination `address` has been extracted for initial 
/// credit checking. 
/// 
/// Implementations of this trait are responsible for the heavy lifting of L3 integration:
/// deserialization, strict format validation, and cryptographic/identity-based routing 
/// authorization. By keeping this logic behind a trait, the L2 core remains agnostic to 
/// L3 payload structures and routing policies.
/// 
/// # Thread Safety & Performance
/// Implementations must be `Send + Sync` to allow concurrent processing of frames 
/// across multiple Tokio tasks. The `process` method is asynchronous to accommodate 
/// potential I/O operations during route lookup or external authorization checks.
pub trait IngressBridge: Send + Sync {
    /// Attempts to process a raw L3 payload extracted from an L2 Data frame.
    ///
    /// # Arguments
    /// * `address` - The destination address extracted directly from the L2 Data frame payload.
    /// * `messages_raw` - The raw, serialized bytes of the L3 message(s). The implementation 
    ///   is responsible for deserializing this into one or more L3 `Message` structs.
    /// * `session_ctx` - The authenticated session context. This is critically important: 
    ///   the implementation **must** derive the `source` identity from this context 
    ///   (e.g., `session_ctx.source_prefix()`), **never** from the `messages_raw` payload, 
    ///   to guarantee protection against source address spoofing.
    /// 
    /// # Returns
    /// An `IngressResult` indicating the outcome of the L3 processing, which dictates 
    /// the subsequent L2 flow control actions (Acknowledge, Cold Pool aggregation, or Drop).
    fn process<'a, C>(
        &'a self,
        address: &'a Bytes,
        messages_raw: &'a Bytes,
        session_ctx: &'a C,
    ) -> impl Future<Output = IngressResult> + Send + 'a
    where
        C: SessionContext + ?Sized;        
}
