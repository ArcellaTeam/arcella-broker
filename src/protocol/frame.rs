// arcella-broker/src/protocol/frame.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Maximum payload size in bytes(16).
/// Corresponds to the maximum value of u24 (0x00FF_FFFF).
pub const FRAME_MAX_PAYLOAD_LENGTH: u32 = 0x00FF_FFFF;

/// Frame header size in bytes: 1 byte (type) + 3 bytes (length).
pub const FRAME_HEADER_SIZE: usize = 4;

/// Size of the CRC32C field in bytes.
pub const CRC_SIZE: usize = 4;

/// Size of the full frame excluding the payload: header (4) + CRC (4).
pub const FRAME_OVERHEAD: usize = FRAME_HEADER_SIZE + CRC_SIZE;

/// Errors that can occur when parsing or constructing an L2 [`FrameHeader`].
///
/// These errors are detected **before** the full frame is read, i.e., at the
/// very first stage of the L2 FSM (waiting for the 4-byte header). They indicate
/// either a malformed stream or a protocol violation by the peer.
///
/// # Design notes
/// - `InsufficientData` is a *soft* error - it simply means the parser needs
///   more bytes from the stream and should wait (return `None` in `Decoder`).
/// - `PayloadTooLarge` is a *hard* error - it violates the protocol's absolute
///   limit of [`FRAME_MAX_PAYLOAD_LENGTH`] (16 MiB, the maximum value of u24)
///   and must result in immediate connection termination.
///
/// # See also
/// - [`crate::protocol::codec::FrameCodecError`] for errors that occur later in the FSM (CRC mismatch, I/O).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameHeaderError {
    /// The input buffer contains fewer than [`FRAME_HEADER_SIZE`] (4) bytes,
    /// so the header cannot be read yet.
    ///
    /// Contains the number of bytes actually available.
    ///
    /// # Recovery
    /// This is a normal condition during stream fragmentation - the caller
    /// should wait for more data rather than treating it as a failure.
    #[error("Insufficient data to read the header: expected {FRAME_HEADER_SIZE} bytes, received {0}")]
    InsufficientData(usize),
    
    /// The declared payload length exceeds the protocol limit [`FRAME_MAX_PAYLOAD_LENGTH`].
    ///
    /// Contains the offending length value. Since the length field is u24,
    /// this error can only occur if a custom (lower) limit was configured
    /// via [`crate::protocol::codec::FrameCodec::with_max_payload_size`], or if the raw bytes are corrupted.
    #[error("Payload size limit exceeded:{0} > {FRAME_MAX_PAYLOAD_LENGTH}")]
    PayloadTooLarge(u32),
}

/// Typed discriminator of an L2 frame — the `type: u8` field, i.e. the **first byte**
/// of the frame header.
///
/// # Wire format
///
/// ```text
/// ┌──────────┬──────────┬──────────────────┬──────────┐
/// │ type:u8  │ len:u24  │ payload: [u8; N] │ CRC32C   │
/// │ (1 byte) │ (3 bytes)│ (0..16 MB)       │ (4 bytes)│
/// └──────────┴──────────┴──────────────────┴──────────┘
///      ▲
///      └── this enum
/// ```
///
/// All multi-byte values are Little-Endian. `len` covers the payload only.
/// See [`FrameHeader`] for header (de)serialization and
/// [`FrameCodec`](super::codec::FrameCodec) for the stream FSM parser.
///
/// # Design: why `Frame::frame_type` is `u8`, not `FrameType`
///
/// [`Frame`] deliberately stores the raw byte instead of this enum. `FrameType` is
/// an **opt-in view** obtained via [`Frame::typed_frame_type`] or [`FrameType::from_u8`],
/// both returning `Option<Self>`:
///
/// - **Forward compatibility**: a peer running a newer protocol version may send
///   frame types this build does not know. Such frames parse successfully and are
///   ignored (or logged) by the dispatcher instead of killing the connection.
/// - **Zero-cost hot path**: no discriminant validation is performed per frame.
///
/// **Important**: [`FrameCodec`](super::codec::FrameCodec) validates only `length`
/// and CRC32C — it does **not** validate `type`. Type-level validation (direction,
/// connection phase) is the responsibility of the dispatcher
/// (`server::connection_handler`, `IPCEndpoint`).
///
/// # Complete frame registry
///
/// | Code | Variant | Direction | Payload | Phase |
/// |------|---------|-----------|---------|-------|
/// | `0x00` | [`Hello`](Self::Hello) | C -> S | [`HelloPayload`](super::payloads::HelloPayload) | pre-auth |
/// | `0x01` | [`Welcome`](Self::Welcome) | S -> C | [`WelcomePayload`](super::payloads::WelcomePayload) | pre-auth |
/// | `0x02` | [`CreditGrant`](Self::CreditGrant) | both | [`CreditGrantPayload`](super::payloads::CreditGrantPayload) | active |
/// | `0x03` | [`Acknowledge`](Self::Acknowledge) | both | `u64` sequence | active |
/// | `0x04` | [`Drop`](Self::Drop) | both | `u64` sequence + `u8` reason | active |
/// | `0x05` | [`AuthResponse`](Self::AuthResponse) | C -> S | [`AuthResponsePayload`](super::payloads::AuthResponsePayload) | pre-auth |
/// | `0x06` | [`AuthResult`](Self::AuthResult) | S -> C | [`AuthResultPayload`](super::payloads::AuthResultPayload) | pre-auth |
/// | `0x07` | [`Close`](Self::Close) | both | `u8` reason + `u16` len + msg | any |
/// | `0x08` | [`Resume`](Self::Resume) | C -> S | see L2 spec §7.9 | reconnect |
/// | `0x09` | [`Resumed`](Self::Resumed) | S -> C | see L2 spec §7.10 | reconnect |
/// | `0x10` | [`Ping`](Self::Ping) | both | `u64` ts + `[u8; 8]` opaque | **any** |
/// | `0x11` | [`Pong`](Self::Pong) | both | `u64` ts + `u64` ts + `[u8; 8]` | **any** |
/// | `0x20` | [`Subscribe`](Self::Subscribe) | C -> S | [`SubscribePayload`](super::payloads::SubscribePayload) | active |
/// | `0x21` | [`Unsubscribe`](Self::Unsubscribe) | C -> S | [`UnsubscribePayload`](super::payloads::UnsubscribePayload) | active |
/// | `0x22` | [`SubscribeAck`](Self::SubscribeAck) | S -> C | [`SubscribeAckPayload`](super::payloads::SubscribeAckPayload) | active |
/// | `0xfe` | [`Data`](Self::Data) | both | [`DataPayload`](super::payloads::DataPayload) | active |
/// | `0xff` | [`Error`](Self::Error) | both | `u16` code + `u16` len + msg | any |
///
/// `C -> S` = client -> server, `S -> C` = server -> client, `both` = bidirectional
/// (every L2 endpoint is simultaneously frame sender and frame receiver).
///
/// # Code space allocation
///
/// | Range | Purpose |
/// |-------|---------|
/// | `0x00..=0x11` | Session control: handshake, flow control, liveness, recovery |
/// | `0x0a..=0x0f` | *Reserved* — future session control |
/// | `0x12..=0x1f` | *Reserved* — future session control |
/// | `0x20..=0x22` | Subscription management (address registration / egress) |
/// | `0x23..=0xfd` | *Reserved* — future extensions |
/// | `0xfe` | `Data` — the only carrier of L3 messages |
/// | `0xff` | `Error` — fatal protocol error |
///
/// `Data` and `Error` are pinned to the top of the range so that the entire
/// low range can grow without touching the hot-path discriminator.
///
/// # Sequence number & acknowledgment semantics
///
/// **This is the most important invariant to preserve when adding frame types.**
///
/// | Frame | Consumes `session_sequence` | Stored in `UnackedBuffer` | Peer must reply |
/// |-------|:---------------------------:|:-------------------------:|-----------------|
/// | [`Data`](Self::Data) | **yes** | **yes** | `Acknowledge` or `Drop` |
/// | [`CreditGrant`](Self::CreditGrant) | **yes** | no | — | — |
/// | [`Ping`](Self::Ping) | no | no | `opaque_data` | `Pong` |
/// | [`Subscribe`](Self::Subscribe) | no | no | `request_id` | `SubscribeAck` |
/// | [`Unsubscribe`](Self::Unsubscribe) | **yes** | no | `request_id` | `SubscribeAck` |
/// | [`Acknowledge`](Self::Acknowledge)  | no | no | peer's id | — |
/// | [`Drop`](Self::Drop)  | no | no | peer's id | — |
/// | [`Pong`](Self::Pong)  | no | no | peer's id | — |
/// | [`SubscribeAck`](Self::SubscribeAck)  | no | no | peer's id | — |
/// | all others | no | no | — |
///
/// Consequences:
///
/// 1. **`Data` sequence numbers are not contiguous.** `CreditGrant` consumes numbers
///    from the same monotonic counter, so a receiver may legitimately observe
///    `Data(seq=1), Data(seq=3), Data(seq=7)`. Duplicate detection via
///    `SessionState::update_last_received` therefore accepts *any strictly greater*
///    sequence — it must **not** require `last + 1`.
/// 2. **Only `Data` occupies the credit window.** `UnackedBuffer.len() <= hot_credit`
///    is the O(1) memory guarantee; adding another buffered frame type would break it.
/// 3. **`Acknowledge`/`Drop` carry the *peer's* sequence**, not their own. They are
///    built by [`create_ack_frame`] / `IPCEndpoint::send_drop` and never allocate
///    a sequence number.
/// 4. **Two independent correlation spaces exist.** `session_sequence` is a
///    per-direction monotonic counter owned by L2 flow control. `request_id` is
///    allocated by the client purely to match `Subscribe`/`Unsubscribe` with
///    `SubscribeAck`; it is opaque to the server, need not be monotonic, and is
///    never mixed into `SessionState`. A `SubscribeAck` must never be interpreted
///    as acknowledging a `Data` frame.
/// 
/// # Connection phase constraints
///
/// The server enforces a strict state machine; violations produce
/// [`Error`](Self::Error) with `auth_failed` followed by immediate disconnect.
///
/// | Phase | Accepted from client |
/// |-------|----------------------|
/// | Awaiting `Hello` | `Hello`, `Ping` |
/// | Awaiting `AuthResponse` | `AuthResponse`, `Ping` |
/// | Credit exchange | `CreditGrant`, `Ping` |
/// | Active | `Data`, `CreditGrant`, `Acknowledge`, `Drop`, `Subscribe`, `Unsubscribe`, `Close`, `Ping`, `Pong` |
/// | Reconnecting | `Resume`, `Ping` |
///
/// [`Error`](Self::Error) is accepted in **every** phase and is always terminal.
/// 
/// [`Ping`](Self::Ping) is accepted in **every** phase, including before `Hello`,
/// and is answered with [`Pong`](Self::Pong) by `recv_handshake_frame`. This keeps
/// keep-alive working during a slow or stalled handshake.
///
/// Direction violations (e.g. a server receiving [`Welcome`](Self::Welcome) or
/// [`SubscribeAck`](Self::SubscribeAck)) are protocol errors, not ignorable frames.
///
/// # Unknown frame types
///
/// [`from_u8`](Self::from_u8) returns `None`. Recommended handling:
///
/// - **Active phase, unknown code** -> log at `debug` and skip (forward compatibility).
/// - **Pre-auth phase, unknown code** -> reject: the peer has not been authenticated yet.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    /// `0x00` — **Client -> Server.** Opens a connection and selects the authentication
    /// algorithm. Carries `client_id`, which becomes the session's authenticated
    /// `source_prefix` after [`AuthResult`](Self::AuthResult) succeeds.
    ///
    /// Must be the first non-`Ping` frame on a fresh connection.
    /// Payload: [`HelloPayload`](super::payloads::HelloPayload).
    Hello = 0x00,

    /// `0x01` — **Server -> Client.** Accepts the connection: issues `session_id`,
    /// `session_token`, `server_challenge` and the negotiated [`ServerLimits`](super::ServerLimits).
    ///
    /// The client sizes its `UnackedBuffer` from `limits.hot_credit` carried here.
    /// Payload: [`WelcomePayload`](super::payloads::WelcomePayload).
    Welcome = 0x01,

    /// `0x02` — **Bidirectional.** Grants flow-control credit for an address
    /// (`hot` or `cold`). Sent by both sides immediately after authentication and
    /// again whenever credit is returned.
    ///
    /// **Consumes a `session_sequence` but is never acknowledged or buffered.**
    /// Payload: [`CreditGrantPayload`](super::payloads::CreditGrantPayload).
    CreditGrant = 0x02,

    /// `0x03` — **Bidirectional.** Confirms that a `Data` frame was fully processed
    /// (delivered to L3 or durably queued). Releases the peer's `UnackedBuffer` slot
    /// and returns one hot credit.
    ///
    /// Payload: `session_sequence: u64 LE` of the **acknowledged `Data` frame**.
    /// Built by [`create_ack_frame`].
    Acknowledge = 0x03,

    /// `0x04` — **Bidirectional.** Reports that a `Data` frame was discarded.
    /// Releases the peer's buffer slot exactly like `Acknowledge`, but additionally
    /// starts `drop_cooldown_ms` for the affected address.
    ///
    /// Payload: `session_sequence: u64 LE` + `reason: u8`
    /// (`0` = ttl_expired, `1` = buffer_overflow, `2` = address_removed).
    Drop = 0x04,

    /// `0x05` — **Client -> Server.** Challenge-response proof of secret possession:
    /// `HMAC(client_secret, server_challenge || client_nonce)`.
    ///
    /// Payload: [`AuthResponsePayload`](super::payloads::AuthResponsePayload).
    AuthResponse = 0x05,

    /// `0x06` — **Server -> Client.** Authentication verdict. `status = 0` authorizes
    /// the credit exchange and `Data` traffic; any other status closes the connection.
    ///
    /// Payload: [`AuthResultPayload`](super::payloads::AuthResultPayload).
    AuthResult = 0x06,

    /// `0x07` — **Bidirectional.** Graceful shutdown of the session.
    ///
    /// Payload: `reason: u8` (`0` = normal, `1` = error, `2` = timeout, `3` = auth_failed)
    /// + `error_message_length: u16 LE` + UTF-8 message.
    ///
    /// Distinct from a bare socket close: it lets the peer detach subscriptions
    /// deterministically instead of relying on EOF detection.
    Close = 0x07,

    /// `0x08` — **Client -> Server.** Requests session resumption after a transport
    /// break, presenting `session_token` and the last known sequence numbers.
    ///
    /// *Not yet implemented (roadmap Stage 11).* Payload: L2 spec §7.9.
    Resume = 0x08,

    /// `0x09` — **Server -> Client.** Resumption verdict plus a fresh challenge and
    /// the server's sequence watermarks.
    ///
    /// `status = 3 (state_lost_reset)` obliges the client to notify L3 of a **full**
    /// session loss rather than attempting incremental replay.
    ///
    /// *Not yet implemented (roadmap Stage 11).* Payload: L2 spec §7.10.
    Resumed = 0x09,

    /// `0x10` — **Bidirectional.** Liveness probe.
    ///
    /// Accepted in **every** connection phase, including before `Hello`; always
    /// answered with [`Pong`](Self::Pong). Never consumes a sequence number.
    ///
    /// Payload: `timestamp: u64 LE` + `opaque_data: [u8; 8]`.
    Ping = 0x10,

    /// `0x11` — **Bidirectional.** Reply to [`Ping`](Self::Ping), echoing the original
    /// timestamp and opaque data so the prober can compute RTT without local state.
    ///
    /// Payload: `original_timestamp: u64` + `response_timestamp: u64` + `opaque_data: [u8; 8]`.
    Pong = 0x11,

    /// `0x20` — **Client -> Server.** Registers a logical address (or wildcard pattern)
    /// in the broker's `LocalRegistry` and attaches an egress pump to this session.
    ///
    /// Payload: `request_id: u64 LE` + `routing_policy: u8` + `capacity: u32 LE`
    /// + `address_length: u16 LE` + UTF-8 address.
    ///
    /// `routing_policy`: `0` = Exclusive, `1` = LoadBalanced. A LoadBalanced request
    /// for an already registered group **joins** it; an Exclusive request for an
    /// occupied address fails with `address_occupied`.
    ///
    /// `capacity` is a **request, not a command**: `0` means "use the server default",
    /// any other value is clamped to the server's configured bounds and to the
    /// remaining per-session queue budget. The granted value is reported back in
    /// [`SubscribeAck`](Self::SubscribeAck).
    ///
    /// The broker creates the `MessageSender` at registration time; the queue lives
    /// inside the `SubscriptionSlot` and survives disconnects until the ownership
    /// timeout expires. Authorization uses the session's `source_prefix`, never a
    /// field from the payload.
    ///
    /// Bounded by `ServerLimits::max_addresses_per_session`.
    /// Consumes neither a `session_sequence` nor a hot credit.
    ///
    /// Answered with [`SubscribeAck`](Self::SubscribeAck).
    Subscribe = 0x20,

    /// `0x21` — **Client -> Server.** Explicitly destroys a client-owned address:
    /// detaches the egress pump and unregisters the route.
    ///
    /// Payload: `request_id: u64 LE` + `address_length: u16 LE` + UTF-8 address.
    ///
    /// Idempotent: unsubscribing an unknown address returns `success`.
    /// Rejected with `access_denied` for addresses owned by another session.
    ///
    /// *Not yet implemented:* draining queued messages into a dead-letter queue,
    /// and rejection of broker-owned (predefined) addresses.
    ///
    /// Answered with [`SubscribeAck`](Self::SubscribeAck) with `queue_capacity = 0`.
    Unsubscribe = 0x21,

    /// `0x22` — **Server -> Client.** Result of [`Subscribe`](Self::Subscribe) **or**
    /// [`Unsubscribe`](Self::Unsubscribe), correlated by `request_id`.
    ///
    /// Payload: `request_id: u64 LE` + `status: u8` + `queue_capacity: u32 LE`
    /// + `error_message_length: u16 LE` + UTF-8 message. Fixed part: **15 bytes**.
    ///
    /// `status`: `0` = success, `1` = address_occupied, `2` = policy_mismatch,
    /// `3` = access_denied, `4` = invalid_address, `5` = internal.
    ///
    /// `queue_capacity` is the **granted** capacity, which may differ from the
    /// requested one: the server clamps it to its own bounds and to the remaining
    /// per-session budget. When joining an existing LoadBalanced group the capacity
    /// of *that* group is returned, because `mpsc` capacity is fixed at creation
    /// time and cannot be resized. Always `0` for `Unsubscribe` and for any failure.
    ///
    /// Clamping is **not** an error: `status` stays `success`. The client must use
    /// this value, not its own request, for local accounting and watermarks.
    SubscribeAck = 0x22,

    /// `0xfe` — **Bidirectional.** The only carrier of L3 messages.
    ///
    /// L2 extracts `address` for credit accounting and hands `messages_raw` to the
    /// Ingress Bridge **without parsing L3 internals**. One frame may coalesce
    /// several messages for the *same* address.
    ///
    /// **Consumes a `session_sequence`, occupies an `UnackedBuffer` slot, and must be
    /// acknowledged** with [`Acknowledge`](Self::Acknowledge) or [`Drop`](Self::Drop).
    ///
    /// Payload: [`DataPayload`](super::payloads::DataPayload) (send side) /
    /// [`ParsedDataPayload`](super::payloads::ParsedDataPayload) (receive side).
    Data = 0xfe,

    /// `0xff` — **Bidirectional.** Fatal protocol error (bad frame for the current
    /// phase, malformed payload, unsupported algorithm).
    ///
    /// The sender closes the connection immediately after emitting it; the receiver
    /// must treat it as terminal, not as a recoverable per-frame rejection.
    ///
    /// Payload: `error_code: u16 LE` + `message_length: u16 LE` + UTF-8 message.
    /// Built by [`build_error_frame`].
    Error = 0xff,
}

impl FrameType {
    /// Maps a raw wire discriminant to its typed variant.
    ///
    /// Returns `None` for reserved or not-yet-known codes (`0x0a..=0x0f`,
    /// `0x12..=0x1f`, `0x23..=0xfd`, …). This is **not an error**: it is the
    /// forward-compatibility hook that lets an older build coexist with a newer peer.
    ///
    /// The caller decides the policy:
    /// - unknown code in the **active** phase → log and skip;
    /// - unknown code **before authentication** → reject with [`FrameType::Error`].
    ///
    /// # Complexity
    ///
    /// O(1). The match compiles to a jump table; no allocation, no validation of
    /// the accompanying payload.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::Hello),
            0x01 => Some(Self::Welcome),
            0x02 => Some(Self::CreditGrant),
            0x03 => Some(Self::Acknowledge),
            0x04 => Some(Self::Drop),
            0x05 => Some(Self::AuthResponse),
            0x06 => Some(Self::AuthResult),            
            0x07 => Some(Self::Close),
            0x08 => Some(Self::Resume),
            0x09 => Some(Self::Resumed),
            0x10 => Some(Self::Ping),
            0x11 => Some(Self::Pong),
            0x20 => Some(Self::Subscribe),
            0x21 => Some(Self::Unsubscribe),
            0x22 => Some(Self::SubscribeAck),
            0xfe => Some(Self::Data),
            0xff => Some(Self::Error),
            _ => None,
        }
    }
}

/// L2 frame header.
/// Stores the frame type as u8 to support forward-compatibility
/// (the ability to ignore unknown types without a header parsing error).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub frame_type: u8,
    /// Payload length in bytes.
    /// Guaranteed to be <= FRAME_MAX_PAYLOAD_LENGTH.
    pub length: u32,
}

impl FrameHeader {
    /// Creates a new header with length validation.
    pub fn new(frame_type: u8, length: u32) -> Result<Self, FrameHeaderError> {
        if length > FRAME_MAX_PAYLOAD_LENGTH {
            return Err(FrameHeaderError::PayloadTooLarge(length));
        }
        Ok(Self { frame_type, length })
    }

    /// Deserializes the header from a byte slice (Little-Endian).
    /// Expects exactly 4 bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FrameHeaderError> {
        if bytes.len() < FRAME_HEADER_SIZE {
            return Err(FrameHeaderError::InsufficientData(bytes.len()));
        }

        let frame_type = bytes[0];
        // Read 3 bytes of length in Little-Endian order.
        // Bytes: [length_0, length_1, length_2, 0x00]
        // The 4th byte is forced to 0x00, therefore `length` is bounded by
        // u24::MAX == FRAME_MAX_PAYLOAD_LENGTH and cannot exceed it
        let length_bytes = [bytes[1], bytes[2], bytes[3], 0x00];
        let length = u32::from_le_bytes(length_bytes);

        // Additional check in case the most significant byte was non-zero
        // (although the mask above already guarantees this, but for explicitness)
        if length > FRAME_MAX_PAYLOAD_LENGTH {
            return Err(FrameHeaderError::PayloadTooLarge(length));
        }

        Ok(Self { frame_type, length })
    }

    /// Serializes the header into a 4-byte array (Little-Endian).
    pub fn to_bytes(&self) -> [u8; FRAME_HEADER_SIZE] {
        let length_bytes = self.length.to_le_bytes();
        [
            self.frame_type,
            length_bytes[0],
            length_bytes[1],
            length_bytes[2], // 3rd byte (most significant byte of u24)
        ]
    }

    /// Returns a typed variant of the frame if it is known.
    pub fn typed_frame_type(&self) -> Option<FrameType> {
        FrameType::from_u8(self.frame_type)
    }
}

/// L2 Frame.
/// 
/// payload uses Bytes for zero-copy semantics: when extracted from BytesMut,
/// no data is copied, only the reference count is incremented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub frame_type: u8,
    pub payload: Bytes,
}

impl Frame {
    /// Creates a new L2 frame with the specified type and payload.
    ///
    /// # Design Note: Why `frame_type` is `u8` instead of `FrameType` enum
    ///
    /// The `frame_type` field is intentionally stored as a raw `u8` rather than the
    /// strongly-typed `FrameType` enum. This is a deliberate architectural decision
    /// to support **forward-compatibility** (protocol extensibility):
    ///
    /// - **Unknown frame types are not errors**: When a newer protocol version introduces
    ///   new frame types, older implementations can safely ignore them (or log a warning)
    ///   instead of failing to parse the entire stream.
    /// - **Graceful degradation**: The [`Frame::typed_frame_type`] method returns
    ///   `Option<FrameType>`, allowing the caller to decide how to handle unknown types.
    /// - **Zero-cost abstraction**: Storing `u8` avoids the overhead of enum validation
    ///   on every frame parse, which is critical for the hot path.
    ///
    /// # Zero-Copy Semantics
    ///
    /// The `payload` parameter uses [`Bytes`] to enable **zero-copy** data handling:
    /// - When a frame is extracted from a `BytesMut` buffer (e.g., in `FrameCodec::decode`),
    ///   the payload is not copied; only the reference count is incremented.
    /// - This is essential for high-throughput scenarios where frames may be large
    ///   (up to 16 MB) and copying would introduce significant latency.
    ///
    /// # Arguments
    ///
    /// * `frame_type` - The frame type as a raw byte. Known types are defined in
    ///   [`FrameType`] (e.g., `0xfe` for Data, `0x03` for Acknowledge). Unknown types
    ///   are preserved as-is for forward-compatibility.
    /// * `payload` - The frame payload as a `Bytes` instance. The payload can be empty
    ///   (for control frames like `Close`) or up to 16 MB (for `Data` frames).
    ///
    /// # Guarantees
    ///
    /// - **No validation**: This method does not validate the `frame_type` or `payload` size.
    ///   Validation is performed by the transport layer (e.g., `FrameCodec`) or the caller.
    /// - **Ownership transfer**: The `payload` is moved into the frame; the caller loses
    ///   access to the original `Bytes` instance.
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::protocol::{Frame, FrameType};
    /// use bytes::Bytes;
    ///
    /// // Create a Data frame (0xfe)
    /// let payload = Bytes::from("hello, world");
    /// let frame = Frame::new(FrameType::Data as u8, payload);
    /// assert_eq!(frame.frame_type, 0xfe);
    /// ```
    ///
    /// # See Also
    ///
    /// - [`Frame::data`] - A convenience constructor for Data frames.
    /// - [`Frame::typed_frame_type`] - Convert the raw `u8` to a typed `FrameType`.
    pub fn new(frame_type: u8, payload: Bytes) -> Self {
        Self { frame_type, payload }
    }

    /// Creates a Data frame (type `0xfe`) containing the specified payload.
    ///
    /// This is a convenience constructor equivalent to:
    /// ```rust,ignore
    /// Frame::new(FrameType::Data as u8, payload)
    /// ```
    ///
    /// # Purpose
    ///
    /// Data frames are the primary carrier of L3 messages (application-level data)
    /// in the Arcella protocol. Each Data frame contains:
    /// - A `session_sequence` (u64) for ordering and acknowledgment.
    /// - A destination `address` (extracted by L2 for the credit system).
    /// - One or more serialized L3 `Message` instances (aggregated for efficiency).
    ///
    /// The payload is typically constructed using [`DataPayload`](super::payloads::DataPayload), which
    /// handles the serialization of the Data frame's internal structure.
    ///
    /// # Zero-Copy Semantics
    ///
    /// Like [`Frame::new`], this method uses [`Bytes`] for zero-copy payload handling.
    /// The payload is not copied when the frame is created or transmitted; only
    /// reference counts are adjusted.
    ///
    /// # Arguments
    ///
    /// * `payload` - The serialized Data frame payload (including `session_sequence`,
    ///   `address`, and `messages`). Use [`DataPayload::encode`](super::payloads::DataPayload::encode) to construct this.
    ///
    /// # Guarantees
    ///
    /// - **Frame type is fixed**: The `frame_type` is always `0xfe` (`FrameType::Data`).
    /// - **No validation**: The payload is not validated here; validation occurs during
    ///   encoding (in `FrameCodec`) or decoding (in `ParsedDataPayload::decode`).
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::protocol::Frame;
    /// use bytes::Bytes;
    ///
    /// // In a real application, the payload is constructed using `DataPayload`:
    /// // use arcella_broker::protocol::payloads::DataPayload;
    /// // let payload = DataPayload::new(session_seq, address, &[msg]).encode().unwrap();
    ///
    /// // For this example, we use a dummy payload to demonstrate Frame::data.
    /// let dummy_payload = Bytes::from("dummy serialized L3 data");
    /// let frame = Frame::data(dummy_payload);
    ///
    /// assert_eq!(frame.frame_type, 0xfe);
    /// ```
    ///
    /// # See Also
    ///
    /// - [`DataPayload`](super::payloads::DataPayload) - Serialize L3 messages into a Data frame payload.
    /// - [`ParsedDataPayload`](super::payloads::ParsedDataPayload) - Parse a Data frame payload back into L2 metadata and L3 messages.
    /// - [`Frame::new`] - Generic frame constructor for other frame types.
    pub fn data(payload: Bytes) -> Self {
        Self {
            frame_type: FrameType::Data as u8,
            payload,
        }
    }

    /// Returns the typed variant of the frame type if it is known.
    ///
    /// This method attempts to convert the raw `frame_type` byte into a strongly-typed
    /// [`FrameType`] enum variant. If the frame type is unknown (e.g., introduced in a
    /// newer protocol version), this method returns `None`.
    ///
    /// # Forward-Compatibility Strategy
    ///
    /// Returning `Option<FrameType>` instead of `Result` or panicking allows the caller
    /// to implement graceful handling of unknown frame types:
    /// - **Ignore**: Skip the frame and continue processing (for non-critical extensions).
    /// - **Log**: Record a warning for debugging or monitoring.
    /// - **Error**: Terminate the connection (for critical protocol violations).
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::protocol::{Frame, FrameType};
    /// use bytes::Bytes;
    ///
    /// let frame = Frame::new(0xfe, Bytes::from("data"));
    /// assert_eq!(frame.typed_frame_type(), Some(FrameType::Data));
    ///
    /// let unknown_frame = Frame::new(0x55, Bytes::new());
    /// assert_eq!(unknown_frame.typed_frame_type(), None);
    /// ```
    ///
    /// # See Also
    ///
    /// - [`FrameType::from_u8`] - The underlying conversion logic.
    pub fn typed_frame_type(&self) -> Option<FrameType> {
        FrameType::from_u8(self.frame_type)
    }

    /// Returns the total size of the frame in bytes, including the header and CRC.
    ///
    /// The total size is calculated as:
    /// ```text
    /// total_size = FRAME_OVERHEAD + payload.len()
    ///            = (FRAME_HEADER_SIZE + CRC_SIZE) + payload.len()
    ///            = (4 + 4) + payload.len()
    ///            = 8 + payload.len()
    /// ```
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::protocol::{Frame, FRAME_OVERHEAD};
    /// use bytes::Bytes;
    ///
    /// let frame = Frame::new(0xfe, Bytes::from("hello"));
    /// assert_eq!(frame.total_size(), FRAME_OVERHEAD + 5); // 8 + 5 = 13 bytes
    /// ```
    pub fn total_size(&self) -> usize {
        FRAME_OVERHEAD + self.payload.len()
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Helper to build a standardized Error frame.
/// Format: error_code (u16 LE) + message_length (u16 LE) + message (UTF-8)
pub fn build_error_frame(code: u16, message: &str) -> Frame {
    let mut payload = BytesMut::with_capacity(4 + message.len());
    payload.put_u16_le(code);
    payload.put_u16_le(message.len() as u16);
    payload.put_slice(message.as_bytes());
    Frame::new(FrameType::Error as u8, payload.freeze())
}

/// Creates an Acknowledge frame payload for the given sequence number.
pub fn create_ack_frame(sequence: u64) -> Frame {
    let mut payload = BytesMut::with_capacity(8);
    payload.put_u64_le(sequence);
    Frame::new(FrameType::Acknowledge as u8, payload.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_encoding_decoding() {
        // Test for the Data type (0xfe) and a length of 1,000,000 bytes
        let header = FrameHeader::new(0xfe, 1_000_000).unwrap();
        let bytes = header.to_bytes();

        assert_eq!(bytes[0], 0xfe);
        assert_eq!(bytes[1], 0x40); // 1_000_000 = 0x0F4240 -> LE: 40 42 0F
        assert_eq!(bytes[2], 0x42);
        assert_eq!(bytes[3], 0x0F);

        let decoded = FrameHeader::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(decoded.typed_frame_type(), Some(FrameType::Data));
    }

    #[test]
    fn test_max_payload_length() {
        // Boundary value: exactly 16 MB
        let header = FrameHeader::new(0x00, FRAME_MAX_PAYLOAD_LENGTH).unwrap();
        assert_eq!(header.length, FRAME_MAX_PAYLOAD_LENGTH);
        
        let bytes = header.to_bytes();
        assert_eq!(bytes[3], 0xFF); // The most significant byte of u24 must be 0xFF

        // Exceeding the maximum value
        let err = FrameHeader::new(0x00, FRAME_MAX_PAYLOAD_LENGTH + 1).unwrap_err();
        assert_eq!(err, FrameHeaderError::PayloadTooLarge(FRAME_MAX_PAYLOAD_LENGTH + 1));
    }

    #[test]
    fn test_insufficient_data() {
        let short_bytes = [0xfe, 0x40, 0x42];
        let err = FrameHeader::from_bytes(&short_bytes).unwrap_err();
        assert_eq!(err, FrameHeaderError::InsufficientData(3));
    }

    #[test]
    fn test_unknown_frame_type() {
        // Bytes: type 0x55 (unknown), length 10
        let bytes = [0x55, 0x0A, 0x00, 0x00];
        let header = FrameHeader::from_bytes(&bytes).unwrap();
        
        assert_eq!(header.frame_type, 0x55);
        assert_eq!(header.length, 10);
        assert_eq!(header.typed_frame_type(), None); // Correctly returns None
    }
}
