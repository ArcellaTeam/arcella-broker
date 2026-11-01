// arcella-broker/src/l2/ingress_bridge.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::Bytes;

use super::super::security::ServerSessionContext;

/// Result of processing an Ingress Bridge frame
#[derive(Debug, PartialEq)]
pub enum IngressResult {
    /// The frame was successfully received and queued
    Accepted,
    /// Address is blocked (backpressure) - the frame must be moved to the cold buffer
    AddressBlocked,
    /// Processing error (validation, routing) - the frame is rejected
    Rejected(String),
}

pub trait IngressBridge: Send + Sync {
    // Attempts to process a frame
    ///
    /// # Arguments
    /// * `address` - destination address (extracted from the Data frame)
    /// * `messages_raw` - raw bytes of the L3 messages
    /// * `session_ctx` - session context (for permission checks)
    /// 
    /// # Returns
    /// * `Accepted` - the frame is accepted; an Ack can be sent and the credit returned
    /// * `AddressBlocked` - the address is blocked; a cold buffer is needed
    /// * `Rejected` - an error; the credit is NOT returned (backpressure)
    fn process<'a>(
        &'a self,
        address: &Bytes,
        messages_raw: &Bytes,
        session_ctx: &ServerSessionContext,
    ) -> impl Future<Output = IngressResult> + Send + 'a;        
}
