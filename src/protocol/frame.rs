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

/// Errors that occur when working with frame headers.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameHeaderError {
    #[error("Insufficient data to read the header: expected {FRAME_HEADER_SIZE} bytes, received {0}")]
    InsufficientData(usize),
    
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
    /// Create new frame.
    pub fn new(frame_type: u8, payload: Bytes) -> Self {
        Self { frame_type, payload }
    }

    /// Create frame Data (0xfe)
    pub fn data(payload: Bytes) -> Self {
        Self {
            frame_type: FrameType::Data as u8,
            payload,
        }
    }

    /// Returns a typed variant of the frame type if it is known.
    pub fn typed_frame_type(&self) -> Option<FrameType> {
        FrameType::from_u8(self.frame_type)
    }

    /// Total frame size in bytes (header + payload + CRC).
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
