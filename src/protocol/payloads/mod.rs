// arcella-broker/src/protocol/payloads/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Binary payload codecs for the L2 protocol.
//!
//! Each submodule implements `encode` / `decode` for the payload of one
//! frame type. Codecs are pure functions over byte buffers: they perform
//! **structural** validation only (lengths, declared sizes, known enum
//! codes). **Semantic** validation (address syntax, routing rights,
//! session state) is the responsibility of the server handlers.
//!
//! # Wire conventions
//!
//! - All multi-byte integers are **Little-Endian**.
//! - Variable-length fields are length-prefixed (`u16` unless stated otherwise).
//! - Addresses are UTF-8 strings bounded by [`crate::protocol::MAX_ADDRESS_LEN`].
//! - Decoders tolerate trailing bytes after the payload (forward compatibility):
//!   a newer peer may append fields that an older decoder simply ignores.
//! - Encoders enforce the invariants of their payload and return
//!   [`PayloadError`] on violation; they never silently truncate data.
//!
//! # Payload catalogue
//!
//! | Frame          | Code   | Direction        | Module          |
//! |----------------|--------|------------------|-----------------|
//! | `Hello`        | `0x00` | Client -> Server  | [`hello`]       |
//! | `Welcome`      | `0x01` | Server -> Client  | [`welcome`]     |
//! | `CreditGrant`  | `0x02` | Bidirectional    | [`credit_grant`]|
//! | `AuthResponse` | `0x05` | Client -> Server  | [`auth_response`]|
//! | `AuthResult`   | `0x06` | Server -> Client  | [`auth_result`] |
//! | `Subscribe`    | `0x20` | Client -> Server  | [`subscribe`]   |
//! | `Unsubscribe`  | `0x21` | Client -> Server  | [`unsubscribe`] |
//! | `SubscribeAck` | `0x22` | Server -> Client  | [`subscribe_ack`]|
//! | `Data`         | `0xfe` | Bidirectional    | [`data`]        |
//!
//! Payloads of the remaining service frames (`Acknowledge`, `Drop`, `Close`,
//! `Ping`/`Pong`, `Error`, `Resume`/`Resumed`) are added here as the
//! corresponding roadmap stages are implemented.
//!
//! # Control plane / data plane separation
//!
//! The subscription family (`0x20`–`0x22`) belongs strictly to the **data
//! plane**: `Subscribe` attaches the session as a consumer to an **already
//! existing** address, `Unsubscribe` detaches it. Addresses themselves —
//! together with their routing policy and queue capacity — are created,
//! configured and deleted exclusively through the broker **control plane**
//! (`BrokerControl`), never through the client channel.
//!
//! Consequently `Subscribe` carries neither `routing_policy` nor
//! `channel_capacity`. The effective values are reported back in
//! `SubscribeAck` (`queue_capacity`, `routing_policy`) as **informational**
//! fields for client-side metrics and diagnostics.
//!
//! # Error handling
//!
//! Every payload module defines its own error enum. All of them are
//! aggregated into the unified [`PayloadError`] via `#[from]`, so callers
//! can handle payload failures uniformly. `PayloadError` is further
//! converted into `TransportError::Protocol` (through `ProtocolError`),
//! which lets the connection handler treat any malformed payload as a
//! protocol violation without knowing its concrete type.
//!
//! Note: [`SubscribePayloadError`] is a **shared** error family used by all
//! three subscription codecs (`Subscribe`, `Unsubscribe`, `SubscribeAck`);
//! it is therefore represented by a single [`PayloadError::Subscribe`]
//! variant.

use thiserror::Error;

mod hello;
mod welcome;
mod credit_grant;
mod auth_response;
mod auth_result;
mod subscribe;
mod subscribe_ack;
mod unsubscribe;
mod data;

pub use hello::*;
pub use welcome::*;
pub use credit_grant::*;
pub use auth_response::*;
pub use auth_result::*;
pub use subscribe::*;
pub use subscribe_ack::*;
pub use unsubscribe::*;
pub use data::*;

/// Current L2 protocol version, exchanged in `Hello` / `Welcome`.
///
/// Version `1` includes the revised subscription wire format:
/// `Subscribe` = `request_id + address` (no policy/capacity),
/// `SubscribeAck` = `request_id + status + policy + queue_capacity + message`.
///
/// The protocol is pre-release; incompatible wire changes are allowed
/// without a version bump. Once released, any breaking change to a payload
/// layout MUST increment this constant.
pub const L2_PROTOCOL_VERSION: u16 = 1;

/// Unified error type for all L2 payload operations.
///
/// Aggregates the per-module error enums into a single type so that callers
/// can handle payload errors uniformly without knowing the concrete payload
/// type. The `#[error(...)]` attributes provide human-readable context about
/// which frame family failed.
///
/// # Variants
///
/// Each variant corresponds to a payload **module**. Most modules map to a
/// single frame type; the [`Subscribe`](PayloadError::Subscribe) variant
/// covers the whole subscription family (`0x20`–`0x22`), which shares the
/// [`SubscribePayloadError`] error type.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PayloadError {
    /// Error parsing/encoding a `Hello` payload (`0x00`).
    ///
    /// Typically indicates malformed client identification data,
    /// an unsupported protocol version, or truncated auth data.
    #[error("Hello payload (0x00): {0}")]
    Hello(#[from] HelloPayloadError),

    /// Error parsing/encoding a `Welcome` payload (`0x01`).
    ///
    /// Typically indicates truncated server response data
    #[error("Welcome payload (0x01): {0}")]
    Welcome(#[from] WelcomePayloadError),

    /// Error parsing/encoding a `CreditGrant` payload (`0x02`).
    ///
    /// Occurs when the binary format is violated: insufficient buffer size,
    /// address exceeding [`crate::protocol::MAX_ADDRESS_LEN`], or an
    /// incorrect frame structure. Automatically converted to
    /// `TransportError::Protocol` via the `From` mechanism.
    #[error("CreditGrant payload (0x02): {0}")]
    CreditGrant(#[from] CreditGrantPayloadError),

    /// Error parsing/encoding an `AuthResponse` payload (`0x05`).
    ///
    /// Typically indicates a malformed client authentication response,
    #[error("AuthResponse payload (0x05): {0}")]
    AuthResponse(#[from] AuthResponsePayloadError),

    /// Error parsing/encoding an `AuthResult` payload (`0x06`).
    ///
    /// Typically indicates a truncated server authentication result
    /// or a malformed error message.
    #[error("AuthResult payload (0x06): {0}")]
    AuthResult(#[from] AuthResultPayloadError),

    /// Error parsing/encoding a subscription-family payload
    /// (`Subscribe` `0x20`, `Unsubscribe` `0x21`, `SubscribeAck` `0x22`).
    ///
    /// All three codecs share [`SubscribePayloadError`]. Typical causes:
    /// truncated header/address/message, address or error message exceeding
    /// its length limit, unknown status/policy code, or inconsistent
    /// `SubscribeAck` fields (e.g., a failure carrying a queue capacity).
    ///
    /// The variant name is kept for compatibility with existing call sites;
    /// a rename to `Subscription` can be done as a separate refactor.
    #[error("Subscribe payload: {0}")]
    Subscribe(#[from] SubscribePayloadError), 

    /// Error parsing/encoding a `Data` payload (`0xfe`).
    ///
    /// Typically indicates an address length violation, truncated message
    /// data, or a message count mismatch in the L3 aggregation.
    #[error("Data payload (0xfe): {0}")]
    Data(#[from] DataPayloadError),
}

pub type PayloadResult<T> = Result<T, PayloadError>;
