// arcella-broker/src/protocol/payloads/subscribe_ack.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! SubscribeAck (0x22), Server → Client.
//!
//! Response to both `Subscribe` (0x20) and `Unsubscribe` (0x21).
//!
//! # Queue capacity is informational
//!
//! The queue belongs to the address and is created by the control plane.
//! `queue_capacity` reports the **actual** capacity of that queue; the client
//! MUST use it (not any locally assumed value) for metrics and watermark
//! logic. `queue_capacity == 0` means "no queue is associated with this
//! response": either the operation failed, or it was an `Unsubscribe`.
//!
//! # Routing policy is informational
//!
//! `routing_policy` tells the client which delivery model the address uses.
//! From the IPC client's perspective all policies look the same (incoming
//! Data frames flow-controlled by L2 credits); the field exists for
//! observability and client-side diagnostics.
//!
//! # Symmetric validation
//!
//! Semantic consistency of the fields is enforced by a single private
//! helper (`check_consistency`) invoked from **both** `encode` and `decode`.
//! A malformed or hostile peer cannot smuggle an inconsistent payload
//! (e.g. a failure carrying a queue capacity, or a success with a policy
//! but zero capacity) past the decoder.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{PayloadError, SubscribePayloadError};

// ============================================================================
// Status codes
// ============================================================================

/// Subscription attached / unsubscription completed successfully.
pub const SUB_STATUS_SUCCESS: u8 = 0;
/// No address with the given name is registered.
pub const SUB_STATUS_ADDRESS_NOT_FOUND: u8 = 1;
/// Exclusive address already has an active consumer (another session).
pub const SUB_STATUS_EXCLUSIVE_OCCUPIED: u8 = 2;
/// This session is already subscribed to the given address.
pub const SUB_STATUS_ALREADY_SUBSCRIBED: u8 = 3;
/// The session is not allowed to consume this address (ACL / hierarchy).
pub const SUB_STATUS_PERMISSION_DENIED: u8 = 4;
/// The address policy is not supported yet (e.g. Broadcast before Stage N).
pub const SUB_STATUS_POLICY_UNSUPPORTED: u8 = 5;
/// Internal broker error (registry, channel, task spawn, ...).
pub const SUB_STATUS_INTERNAL_ERROR: u8 = 6;
/// Unsubscribe failed: this session has no subscription for the address.
pub const SUB_STATUS_NOT_SUBSCRIBED: u8 = 7;
/// The session already holds `ServerLimits::max_addresses_per_session`
/// consumer bindings and cannot attach one more.
pub const SUB_STATUS_BINDING_LIMIT: u8 = 8;

// ============================================================================
// Routing policy codes (mirror of `RoutingPolicy`, wire-stable)
// ============================================================================

pub const SUB_POLICY_EXCLUSIVE: u8 = 0;
pub const SUB_POLICY_LOAD_BALANCED: u8 = 1;
/// Reserved: fan-out delivery (future stage).
pub const SUB_POLICY_BROADCAST: u8 = 2;
/// No policy associated with this response (failure or Unsubscribe).
pub const SUB_POLICY_NONE: u8 = 0xFF;

/// Fixed part of SubscribeAck:
/// request_id(8) + status(1) + policy(1) + queue_capacity(4) + msg_len(2).
pub const ACK_HEADER_LEN: usize = 16;

/// Upper bound for the human-readable error message.
/// Keeps the frame small and prevents u16 length overflow by construction.
pub const MAX_SUB_ERROR_MSG_LEN: usize = 1024;

/// Returns `true` if the status code is defined by this protocol version.
///
/// Uses an explicit match rather than a range check so that retiring a
/// status code in a future version cannot accidentally re-admit it.
#[inline]
pub fn is_known_status(status: u8) -> bool {
    matches!(
        status,
        SUB_STATUS_SUCCESS
            | SUB_STATUS_ADDRESS_NOT_FOUND
            | SUB_STATUS_EXCLUSIVE_OCCUPIED
            | SUB_STATUS_ALREADY_SUBSCRIBED
            | SUB_STATUS_PERMISSION_DENIED
            | SUB_STATUS_POLICY_UNSUPPORTED
            | SUB_STATUS_INTERNAL_ERROR
            | SUB_STATUS_NOT_SUBSCRIBED
            | SUB_STATUS_BINDING_LIMIT
    )
}

/// Returns `true` if the policy code is defined by this protocol version.
#[inline]
pub fn is_known_policy(policy: u8) -> bool {
    matches!(
        policy,
        SUB_POLICY_EXCLUSIVE | SUB_POLICY_LOAD_BALANCED | SUB_POLICY_BROADCAST | SUB_POLICY_NONE
    )
}

/// Human-readable status name for logging / metrics.
pub fn status_name(status: u8) -> &'static str {
    match status {
        SUB_STATUS_SUCCESS => "success",
        SUB_STATUS_ADDRESS_NOT_FOUND => "address_not_found",
        SUB_STATUS_EXCLUSIVE_OCCUPIED => "exclusive_occupied",
        SUB_STATUS_ALREADY_SUBSCRIBED => "already_subscribed",
        SUB_STATUS_PERMISSION_DENIED => "permission_denied",
        SUB_STATUS_POLICY_UNSUPPORTED => "policy_unsupported",
        SUB_STATUS_INTERNAL_ERROR => "internal_error",
        SUB_STATUS_NOT_SUBSCRIBED => "not_subscribed",
        SUB_STATUS_BINDING_LIMIT => "binding_limit",
        _ => "unknown",
    }
}

/// Truncates `msg` to at most `MAX_SUB_ERROR_MSG_LEN` bytes,
/// cutting only at a UTF-8 character boundary.
fn truncate_message(msg: &str) -> Bytes {
    if msg.len() <= MAX_SUB_ERROR_MSG_LEN {
        return Bytes::from(msg.to_owned());
    }
    let mut end = MAX_SUB_ERROR_MSG_LEN;
    while end > 0 && !msg.is_char_boundary(end) {
        end -= 1;
    }
    Bytes::from(msg[..end].to_owned())
}

/// SubscribeAck (0x22), Server → Client.
///
/// Wire format (all integers Little-Endian):
/// ```text
/// request_id     : u64
/// status         : u8
/// routing_policy : u8
/// queue_capacity : u32
/// msg_len        : u16
/// error_message  : [u8; msg_len]   // UTF-8, empty on success
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeAckPayload {
    /// Echoes `request_id` from `Subscribe` / `Unsubscribe`.
    pub request_id: u64,

    /// One of the `SUB_STATUS_*` codes.
    pub status: u8,

    /// One of the `SUB_POLICY_*` codes (informational).
    pub routing_policy: u8,

    /// Actual queue capacity of the address; 0 when no queue is attached.
    pub queue_capacity: u32,

    /// Human-readable error description; empty on success.
    pub error_message: Bytes,
}

impl SubscribeAckPayload {
    /// Successful `Subscribe` response carrying the effective queue capacity
    /// and the address routing policy.
    ///
    /// # Panics (debug builds only)
    /// Panics if `routing_policy` is unknown or `SUB_POLICY_NONE`, or if
    /// `queue_capacity` is zero — a Subscribe-ack must always describe a
    /// real queue. Use [`Self::success_unsubscribed`] for the no-queue case.
    pub fn success(request_id: u64, queue_capacity: u32, routing_policy: u8) -> Self {
        debug_assert!(is_known_policy(routing_policy));
        debug_assert_ne!(routing_policy, SUB_POLICY_NONE);
        debug_assert!(queue_capacity > 0);
        Self {
            request_id,
            status: SUB_STATUS_SUCCESS,
            routing_policy,
            queue_capacity,
            error_message: Bytes::new(),
        }
    }

    /// Successful `Unsubscribe` response — no queue is attached.
    pub fn success_unsubscribed(request_id: u64) -> Self {
        Self {
            request_id,
            status: SUB_STATUS_SUCCESS,
            routing_policy: SUB_POLICY_NONE,
            queue_capacity: 0,
            error_message: Bytes::new(),
        }
    }

    /// Failure response. The message is safely truncated to
    /// `MAX_SUB_ERROR_MSG_LEN` bytes at a UTF-8 boundary.
    pub fn failure(request_id: u64, status: u8, msg: &str) -> Self {
        debug_assert!(is_known_status(status));
        debug_assert_ne!(status, SUB_STATUS_SUCCESS);
        Self {
            request_id,
            status,
            routing_policy: SUB_POLICY_NONE,
            queue_capacity: 0,
            error_message: truncate_message(msg),
        }
    }

    #[inline]
    pub fn is_success(&self) -> bool {
        self.status == SUB_STATUS_SUCCESS
    }

    /// Semantic consistency of the payload, enforced symmetrically
    /// by both `encode` and `decode`.
    ///
    /// # Invariants
    /// - `success` -> empty error message;
    /// - `success` -> `(policy == NONE) <=> (queue_capacity == 0)`:
    ///   a Subscribe-ack carries a real policy and a positive capacity,
    ///   an Unsubscribe-ack carries `NONE` and zero;
    /// - `failure` -> `queue_capacity == 0` and `policy == NONE`.
    fn check_consistency(&self) -> Result<(), PayloadError> {
        let consistent = if self.is_success() {
            self.error_message.is_empty()
                && (self.routing_policy == SUB_POLICY_NONE) == (self.queue_capacity == 0)
        } else {
            self.queue_capacity == 0 && self.routing_policy == SUB_POLICY_NONE
        };
        if consistent {
            Ok(())
        } else {
            Err(SubscribePayloadError::InconsistentFields.into())
        }
    }

    /// Encodes the payload, enforcing wire invariants:
    /// - status and policy codes must be known;
    /// - success -> empty error message and `(policy == NONE) <=> (capacity == 0)`;
    /// - failure -> zero queue capacity and `SUB_POLICY_NONE`;
    /// - error message must fit `MAX_SUB_ERROR_MSG_LEN`.
    pub fn encode(&self) -> Result<Bytes, PayloadError> {
        if !is_known_status(self.status) {
            return Err(SubscribePayloadError::InvalidStatus(self.status).into());
        }
        if !is_known_policy(self.routing_policy) {
            return Err(SubscribePayloadError::InvalidPolicy(self.routing_policy).into());
        }
        if self.error_message.len() > MAX_SUB_ERROR_MSG_LEN {
            return Err(SubscribePayloadError::MessageTooLong.into());
        }
        self.check_consistency()?;

        let mut buf = BytesMut::with_capacity(ACK_HEADER_LEN + self.error_message.len());
        buf.put_u64_le(self.request_id);
        buf.put_u8(self.status);
        buf.put_u8(self.routing_policy);
        buf.put_u32_le(self.queue_capacity);
        buf.put_u16_le(self.error_message.len() as u16);
        buf.put_slice(&self.error_message);
        Ok(buf.freeze())
    }

    /// Decodes the payload, applying the same structural and semantic
    /// checks as `encode` (known codes, message bound, field consistency).
    /// Trailing bytes after the payload are tolerated (forward compatibility).
    pub fn decode(buf: &mut impl Buf) -> Result<Self, PayloadError> {
        if buf.remaining() < ACK_HEADER_LEN {
            return Err(SubscribePayloadError::IncompleteHeader.into());
        }
        let request_id = buf.get_u64_le();
        let status = buf.get_u8();
        if !is_known_status(status) {
            return Err(SubscribePayloadError::InvalidStatus(status).into());
        }
        let routing_policy = buf.get_u8();
        if !is_known_policy(routing_policy) {
            return Err(SubscribePayloadError::InvalidPolicy(routing_policy).into());
        }
        let queue_capacity = buf.get_u32_le();
        let len = buf.get_u16_le() as usize;
        if len > MAX_SUB_ERROR_MSG_LEN {
            return Err(SubscribePayloadError::MessageTooLong.into());
        }
        if buf.remaining() < len {
            return Err(SubscribePayloadError::IncompleteMessage.into());
        }
        let error_message = buf.copy_to_bytes(len);
        
        let payload = Self {
            request_id,
            status,
            routing_policy,
            queue_capacity,
            error_message,
        };
        payload.check_consistency()?;
        Ok(payload)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_roundtrip() {
        let p = SubscribeAckPayload::success(42, 1024, SUB_POLICY_EXCLUSIVE);
        let mut bytes = p.encode().unwrap();
        assert_eq!(bytes.len(), ACK_HEADER_LEN);
        assert_eq!(SubscribeAckPayload::decode(&mut bytes).unwrap(), p);
        assert!(p.is_success());
    }

    #[test]
    fn success_load_balanced_roundtrip() {
        let p = SubscribeAckPayload::success(43, 4096, SUB_POLICY_LOAD_BALANCED);
        let mut bytes = p.encode().unwrap();
        assert_eq!(SubscribeAckPayload::decode(&mut bytes).unwrap(), p);
    }

    #[test]
    fn unsubscribed_roundtrip() {
        let p = SubscribeAckPayload::success_unsubscribed(77);
        assert_eq!(p.queue_capacity, 0);
        assert_eq!(p.routing_policy, SUB_POLICY_NONE);
        let mut bytes = p.encode().unwrap();
        assert_eq!(SubscribeAckPayload::decode(&mut bytes).unwrap(), p);
    }

    #[test]
    fn failure_roundtrip_all_statuses() {
        for status in [
            SUB_STATUS_ADDRESS_NOT_FOUND,
            SUB_STATUS_EXCLUSIVE_OCCUPIED,
            SUB_STATUS_ALREADY_SUBSCRIBED,
            SUB_STATUS_PERMISSION_DENIED,
            SUB_STATUS_POLICY_UNSUPPORTED,
            SUB_STATUS_INTERNAL_ERROR,
            SUB_STATUS_NOT_SUBSCRIBED,
            SUB_STATUS_BINDING_LIMIT,
        ] {
            let p = SubscribeAckPayload::failure(9, status, "reason");
            let mut bytes = p.encode().unwrap();
            let decoded = SubscribeAckPayload::decode(&mut bytes).unwrap();
            assert_eq!(decoded, p);
            assert_eq!(decoded.queue_capacity, 0);
            assert_eq!(decoded.routing_policy, SUB_POLICY_NONE);
        }
    }

    #[test]
    fn failure_truncates_long_message() {
        let long = "x".repeat(MAX_SUB_ERROR_MSG_LEN + 500);
        let p = SubscribeAckPayload::failure(1, SUB_STATUS_INTERNAL_ERROR, &long);
        assert_eq!(p.error_message.len(), MAX_SUB_ERROR_MSG_LEN);
        let mut bytes = p.encode().unwrap();
        assert_eq!(SubscribeAckPayload::decode(&mut bytes).unwrap(), p);
    }

    #[test]
    fn failure_truncates_at_utf8_boundary() {
        // 'я' is 2 bytes in UTF-8; 513 copies = 1026 bytes > 1024.
        let long = "я".repeat(513);
        let p = SubscribeAckPayload::failure(1, SUB_STATUS_INTERNAL_ERROR, &long);
        assert!(p.error_message.len() <= MAX_SUB_ERROR_MSG_LEN);
        assert_eq!(p.error_message.len() % 2, 0); // no split character
        assert!(std::str::from_utf8(&p.error_message).is_ok());
    }

    #[test]
    fn encode_rejects_success_policy_without_capacity() {
        // Subscribe-ack обязан нести положительную ёмкость.
        // Конструктор не используем: debug_assert(queue_capacity > 0)
        // сработал бы раньше, чем encode вернул бы Err.
        let p = SubscribeAckPayload {
            request_id: 1,
            status: SUB_STATUS_SUCCESS,
            routing_policy: SUB_POLICY_EXCLUSIVE,
            queue_capacity: 0,
            error_message: Bytes::new(),
        };
        assert_eq!(
            p.encode().unwrap_err(),
            PayloadError::from(SubscribePayloadError::InconsistentFields)
        );
    }

    #[test]
    fn encode_rejects_success_none_policy_with_capacity() {
        let mut p = SubscribeAckPayload::success_unsubscribed(1);
        p.queue_capacity = 10; // NONE не может нести ёмкость
        assert_eq!(
            p.encode().unwrap_err(),
            PayloadError::from(SubscribePayloadError::InconsistentFields)
        );
    }

    #[test]
    fn encode_rejects_unknown_status() {
        let mut p = SubscribeAckPayload::success_unsubscribed(1);
        p.status = 200;
        assert_eq!(
            p.encode().unwrap_err(),
            PayloadError::from(SubscribePayloadError::InvalidStatus(200))
        );
    }

    #[test]
    fn encode_rejects_unknown_policy() {
        let mut p = SubscribeAckPayload::success_unsubscribed(1);
        p.routing_policy = 0x7F;
        assert_eq!(
            p.encode().unwrap_err(),
            PayloadError::from(SubscribePayloadError::InvalidPolicy(0x7F))
        );
    }

    #[test]
    fn encode_rejects_inconsistent_success() {
        let mut p = SubscribeAckPayload::success(1, 10, SUB_POLICY_EXCLUSIVE);
        p.error_message = Bytes::from_static(b"should be empty");
        assert_eq!(
            p.encode().unwrap_err(),
            PayloadError::from(SubscribePayloadError::InconsistentFields)
        );
    }

    #[test]
    fn encode_rejects_inconsistent_failure() {
        let mut p = SubscribeAckPayload::failure(1, SUB_STATUS_INTERNAL_ERROR, "x");
        p.queue_capacity = 10; // failure must not carry a queue
        assert_eq!(
            p.encode().unwrap_err(),
            PayloadError::from(SubscribePayloadError::InconsistentFields)
        );
    }

    #[test]
    fn encode_rejects_oversized_message() {
        let mut p = SubscribeAckPayload::failure(1, SUB_STATUS_INTERNAL_ERROR, "x");
        p.error_message = Bytes::from(vec![b'x'; MAX_SUB_ERROR_MSG_LEN + 1]);
        assert_eq!(
            p.encode().unwrap_err(),
            PayloadError::from(SubscribePayloadError::MessageTooLong)
        );
    }

    #[test]
    fn decode_incomplete_header() {
        let mut bytes = Bytes::from_static(&[0u8; ACK_HEADER_LEN - 1]);
        assert_eq!(
            SubscribeAckPayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::IncompleteHeader)
        );
    }

    #[test]
    fn decode_incomplete_message() {
        let p = SubscribeAckPayload::failure(1, SUB_STATUS_INTERNAL_ERROR, "reason");
        let bytes = p.encode().unwrap();
        let mut truncated = bytes.slice(..bytes.len() - 1);
        assert_eq!(
            SubscribeAckPayload::decode(&mut truncated).unwrap_err(),
            PayloadError::from(SubscribePayloadError::IncompleteMessage)
        );
    }

    #[test]
    fn decode_rejects_unknown_status() {
        let p = SubscribeAckPayload::success_unsubscribed(1);
        let mut bytes = BytesMut::from(p.encode().unwrap().as_ref());
        bytes[8] = 200; // status byte offset
        let mut bytes = bytes.freeze();
        assert_eq!(
            SubscribeAckPayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::InvalidStatus(200))
        );
    }

    #[test]
    fn decode_rejects_oversized_declared_message() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1);
        buf.put_u8(SUB_STATUS_INTERNAL_ERROR);
        buf.put_u8(SUB_POLICY_NONE);
        buf.put_u32_le(0);
        buf.put_u16_le((MAX_SUB_ERROR_MSG_LEN + 1) as u16);
        let mut bytes = buf.freeze();
        assert_eq!(
            SubscribeAckPayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::MessageTooLong)
        );
    }


    #[test]
    fn decode_rejects_success_policy_without_capacity() {
        let p = SubscribeAckPayload::success(1, 1024, SUB_POLICY_EXCLUSIVE);
        let mut bytes = BytesMut::from(p.encode().unwrap().as_ref());
        // Обнуляем queue_capacity (смещение 10..14)
        bytes[10..14].copy_from_slice(&0u32.to_le_bytes());
        let mut bytes = bytes.freeze();
        assert_eq!(
            SubscribeAckPayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::InconsistentFields)
        );
    }

    #[test]
    fn decode_rejects_success_with_nonempty_message() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1);
        buf.put_u8(SUB_STATUS_SUCCESS);
        buf.put_u8(SUB_POLICY_EXCLUSIVE);
        buf.put_u32_le(1024);
        buf.put_u16_le(1);
        buf.put_u8(b'x');
        let mut bytes = buf.freeze();
        assert_eq!(
            SubscribeAckPayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::InconsistentFields)
        );
    }

    #[test]
    fn decode_rejects_failure_with_capacity() {
        let p = SubscribeAckPayload::failure(1, SUB_STATUS_INTERNAL_ERROR, "x");
        let mut bytes = BytesMut::from(p.encode().unwrap().as_ref());
        bytes[10] = 1; // queue_capacity != 0 у failure
        let mut bytes = bytes.freeze();
        assert_eq!(
            SubscribeAckPayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::InconsistentFields)
        );
    }

    #[test]
    fn decode_rejects_failure_with_policy() {
        let p = SubscribeAckPayload::failure(1, SUB_STATUS_INTERNAL_ERROR, "x");
        let mut bytes = BytesMut::from(p.encode().unwrap().as_ref());
        bytes[9] = SUB_POLICY_EXCLUSIVE; // policy != NONE у failure
        let mut bytes = bytes.freeze();
        assert_eq!(
            SubscribeAckPayload::decode(&mut bytes).unwrap_err(),
            PayloadError::from(SubscribePayloadError::InconsistentFields)
        );
    }

    #[test]
    fn status_names_are_defined() {
        assert_eq!(status_name(SUB_STATUS_SUCCESS), "success");
        assert_eq!(status_name(SUB_STATUS_EXCLUSIVE_OCCUPIED), "exclusive_occupied");
        assert_eq!(status_name(SUB_STATUS_BINDING_LIMIT), "binding_limit");
        assert_eq!(status_name(200), "unknown");
    }
}
