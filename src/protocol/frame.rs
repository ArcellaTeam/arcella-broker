// arcella-broker/src/protocol/frame.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;

/// Maximum payload size in bytes(16 МБ).
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
/// - `InsufficientData` is a *soft* error — it simply means the parser needs
///   more bytes from the stream and should wait (return `None` in `Decoder`).
/// - `PayloadTooLarge` is a *hard* error — it violates the protocol's absolute
///   limit of [`FRAME_MAX_PAYLOAD_LENGTH`] (16 MiB, the maximum value of u24)
///   and must result in immediate connection termination.
///
/// # See also
/// - [`FrameCodecError`] for errors that occur later in the FSM (CRC mismatch, I/O).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameHeaderError {
    /// The input buffer contains fewer than [`FRAME_HEADER_SIZE`] (4) bytes,
    /// so the header cannot be read yet.
    ///
    /// Contains the number of bytes actually available.
    ///
    /// # Recovery
    /// This is a normal condition during stream fragmentation — the caller
    /// should wait for more data rather than treating it as a failure.
    #[error("Insufficient data to read the header: expected {FRAME_HEADER_SIZE} bytes, received {0}")]
    InsufficientData(usize),
    
    /// The declared payload length exceeds the protocol limit [`FRAME_MAX_PAYLOAD_LENGTH`].
    ///
    /// Contains the offending length value. Since the length field is u24,
    /// this error can only occur if a custom (lower) limit was configured
    /// via [`FrameCodec::with_max_payload_size`], or if the raw bytes are corrupted.
    #[error("Payload size limit exceeded:{0} > {FRAME_MAX_PAYLOAD_LENGTH}")]
    PayloadTooLarge(u32),
}

/// L2 protocol frame types.
/// Using #[repr(u8)] guarantees that the enum occupies exactly 1 byte
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Hello       = 0x00,
    Welcome     = 0x01,
    CreditGrant = 0x02,
    Acknowledge = 0x03,
    Drop        = 0x04,
    Close       = 0x07,
    Resume      = 0x08,
    Resumed     = 0x09,
    Ping        = 0x10,
    Pong        = 0x11,
    Data        = 0xfe,
    Error       = 0xff,
}

impl FrameType {
    /// Attempts to convert a raw byte into a known frame type.
    /// Returns None for unknown/reserved types,
    /// which allows the parser to decide how to handle future protocol extensions.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::Hello),
            0x01 => Some(Self::Welcome),
            0x02 => Some(Self::CreditGrant),
            0x03 => Some(Self::Acknowledge),
            0x04 => Some(Self::Drop),
            0x07 => Some(Self::Close),
            0x08 => Some(Self::Resume),
            0x09 => Some(Self::Resumed),
            0x10 => Some(Self::Ping),
            0x11 => Some(Self::Pong),
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
    /// The payload is typically constructed using [`encode_data_payload`], which
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
    ///   `address`, and `messages`). Use [`encode_data_payload`] to construct this.
    ///
    /// # Guarantees
    ///
    /// - **Frame type is fixed**: The `frame_type` is always `0xfe` (`FrameType::Data`).
    /// - **No validation**: The payload is not validated here; validation occurs during
    ///   encoding (in `FrameCodec`) or decoding (in `decode_data_payload`).
    ///
    /// # Example
    ///
    /// ```rust
    /// use arcella_broker::protocol::{Frame, Message, TransferMode};
    /// use arcella_broker::protocol::payloads::encode_data_payload;
    /// use bytes::Bytes;
    ///
    /// // Create a sample L3 message
    /// let msg = Message::new(
    ///     TransferMode::InOnly,
    ///     [0u8; 32], // session_token
    ///     [1u8; 16], // message_id
    ///     [0u8; 4],  // sub_message_id
    ///     0,         // priority
    ///     64,        // ttl
    ///     Bytes::from("test.msg"),
    ///     Bytes::from("worker:1"),
    ///     Bytes::new(), // reply_to (empty for InOnly)
    ///     Bytes::from("payload data"),
    /// ).unwrap();
    ///
    /// // Encode into a Data frame payload
    /// let session_seq = 42;
    /// let address = Bytes::from("worker:1");
    /// let payload = encode_data_payload(session_seq, &address, &[msg]).unwrap();
    ///
    /// // Create the Data frame
    /// let frame = Frame::data(payload);
    /// assert_eq!(frame.frame_type, 0xfe);
    /// ```
    ///
    /// # See Also
    ///
    /// - [`encode_data_payload`] - Serialize L3 messages into a Data frame payload.
    /// - [`decode_data_payload`] - Parse a Data frame payload back into L2 metadata and L3 messages.
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
        let short_bytes = [0xfe, 0x40, 0x42]; // Всего 3 байта
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
