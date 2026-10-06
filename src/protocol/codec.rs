// arcella-broker/src/protocol/codec.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{BufMut, BytesMut};
use crc32fast::Hasher;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

use super::frame::{
    Frame,
    CRC_SIZE,
    FRAME_HEADER_SIZE, 
    FRAME_MAX_PAYLOAD_LENGTH, 
    FRAME_OVERHEAD,
};

/// L2 codec errors.
#[derive(Debug, Error)]
pub enum FrameCodecError {
    #[error("Payload size limit exceeded: {0} > {FRAME_MAX_PAYLOAD_LENGTH}")]
    PayloadTooLarge(u32),

    #[error("CRC32C mismatch: expected {expected:08x}, received {received:08x}. Connection terminated")]
    CrcMismatch { expected: u32, received: u32 },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// L2 codec that implements frame parsing and serialization over stream channels.
///
/// # Correspondence to the FSM from the specification
///
/// Although states are not stored explicitly in the struct, the logic of `Decoder::decode`
/// implements three FSM states:
/// 1. **Waiting for header** — if `src.len() < FRAME_HEADER_SIZE`, return None.
/// 2. **Waiting for payload** + CRC — if `src.len() < FRAME_HEADER_SIZE + length + CRC_SIZE`,
/// return `None` (with a preliminary `reserve` for efficiency).
/// 3. **Extracting the frame** — when there is enough data, verify the CRC and return Frame.
///
/// If the CRC does not match, an error is returned, which leads to an immediate termination
/// of the connection at the `FramedRead` level (according to the L2 specification).
#[derive(Debug, Clone)]
pub struct FrameCodec {
    /// Maximum allowed payload size. Defaults to FRAME_MAX_PAYLOAD_LENGTH (16 MB).
    max_payload_size: u32,
}

impl FrameCodec {
    pub fn new() -> Self {
        Self {
            max_payload_size: FRAME_MAX_PAYLOAD_LENGTH,
        }
    }

    /// Creates a codec with a custom payload size limit.
    pub fn with_max_payload_size(max_payload_size: u32) -> Self {
        assert!(
            max_payload_size <= FRAME_MAX_PAYLOAD_LENGTH,
            "max_payload_size не может превышать MAX_PAYLOAD_LENGTH ({FRAME_MAX_PAYLOAD_LENGTH})"
        );
        Self { max_payload_size }
    }

    /// Computes CRC32C for the given buffer (type + length + payload).
    #[inline]
    fn compute_crc(data: &[u8]) -> u32 {
        // crc32fast uses hardware acceleration (SSE 4.2 / CLMUL) when available.
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// DECODER: stream bytes -> Frame
// ============================================================================

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = FrameCodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // State 1: Waiting for header (4 bytes)
        if src.len() < FRAME_HEADER_SIZE {
            return Ok(None);
        }

        // Reading the header WITHOUT consuming the buffer (peek).
        let frame_type = src[0];
        let length_bytes = [src[1], src[2], src[3], 0];
        let length = u32::from_le_bytes(length_bytes);

        if length > self.max_payload_size {
            return Err(FrameCodecError::PayloadTooLarge(length));
        }

        let total_frame_size = FRAME_HEADER_SIZE + length as usize + CRC_SIZE;

        // State 2: Waiting for payload + CRC
        if src.len() < total_frame_size {
            // Reserving space to minimize reallocations during fragmentation.
            src.reserve(total_frame_size - src.len());
            return Ok(None);
        }

        // State 3: Extracting the frame
        // split_to zero-copy separates exactly the required number of bytes.
        let frame_bytes = src.split_to(total_frame_size).freeze();

        let payload_end = FRAME_HEADER_SIZE + length as usize;
        let crc_slice = &frame_bytes[payload_end..];

        let received_crc = u32::from_le_bytes([crc_slice[0], crc_slice[1], crc_slice[2], crc_slice[3]]);

        // CRC is computed over type + length + payload (everything up to the CRC).
        let computed_crc = Self::compute_crc(&frame_bytes[..payload_end]);

        if received_crc != computed_crc {
            return Err(FrameCodecError::CrcMismatch {
                expected: computed_crc,
                received: received_crc,
            });
        }

        // Zero-copy: the payload becomes a separate Bytes that shares memory with frame_bytes.
        let payload = frame_bytes.slice(FRAME_HEADER_SIZE..payload_end);

        Ok(Some(Frame { frame_type, payload }))
    }
}

// ============================================================================
// ENCODER: Frame -> stream bytes
// ============================================================================

impl Encoder<Frame> for FrameCodec {
    type Error = FrameCodecError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload_len = item.payload.len() as u32;

        if payload_len > self.max_payload_size {
            return Err(FrameCodecError::PayloadTooLarge(payload_len));
        }

        // Reserve space with a single call to avoid multiple reallocations.
        dst.reserve(FRAME_OVERHEAD + item.payload.len());

        // Remember the start position of the frame for subsequent CRC computation.
        let frame_start = dst.len();

        // Write the header.
        dst.put_u8(item.frame_type);
        dst.put_uint_le(payload_len as u64, 3); // u24 LE

        // Writing the payload (zero-copy if the payload is Bytes).
        dst.put_slice(&item.payload);

        // Computing the CRC over the written bytes (type + length + payload).
        let computed_crc = Self::compute_crc(&dst[frame_start..]);
        dst.put_u32_le(computed_crc);

        Ok(())
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    
    use super::{
        *,
        super::{
            frame::FrameType,
        }
    };

    /// Helper function: assembles a "raw" frame into BytesMut for tests.
    fn build_raw_frame(frame_type: u8, payload: &[u8]) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(frame_type);
        buf.put_uint_le(payload.len() as u64, 3);
        buf.put_slice(payload);

        let mut hasher = Hasher::new();
        hasher.update(&buf);
        buf.put_u32_le(hasher.finalize());

        buf
    }

    #[test]
    fn test_decode_single_complete_frame() {
        let mut codec = FrameCodec::new();
        let payload = b"hello, L2";
        let mut src = build_raw_frame(FrameType::Data as u8, payload);

        let frame = codec.decode(&mut src).unwrap().expect("should return the frame");

        assert_eq!(frame.frame_type, FrameType::Data as u8);
        assert_eq!(frame.payload.as_ref(), payload);
        assert!(src.is_empty(), "the buffer must be fully consumed");
    }

    #[test]
    fn test_decode_fragmented_header() {
        let mut codec = FrameCodec::new();
        let payload = b"fragmented";
        let raw = build_raw_frame(FrameType::Ping as u8, payload);

        // Simulate fragmentation: first, only 2 bytes of the header.
        let mut src = BytesMut::from(&raw[..2]);
        assert!(codec.decode(&mut src).unwrap().is_none());

        // Add the remaining 2 bytes of the header.
        src.extend_from_slice(&raw[2..FRAME_HEADER_SIZE]);
        assert!(codec.decode(&mut src).unwrap().is_none());

        // Add the payload + CRC.
        src.extend_from_slice(&raw[FRAME_HEADER_SIZE..]);
        let frame = codec.decode(&mut src).unwrap().expect("should return the frame");

        assert_eq!(frame.frame_type, FrameType::Ping as u8);
        assert_eq!(frame.payload.as_ref(), payload);
    }

    #[test]
    fn test_decode_fragmented_payload() {
        let mut codec = FrameCodec::new();
        let payload = b"this is a longer payload for fragmentation test";
        let raw = build_raw_frame(FrameType::Data as u8, payload);

        // Give the entire header, but only half of the payload.
        let split_point = FRAME_HEADER_SIZE + payload.len() / 2;
        let mut src = BytesMut::from(&raw[..split_point]);
        assert!(codec.decode(&mut src).unwrap().is_none());

        // Add the remainder.
        src.extend_from_slice(&raw[split_point..]);
        let frame = codec.decode(&mut src).unwrap().expect("should return the frame");

        assert_eq!(frame.payload.as_ref(), payload);
    }

    #[test]
    fn test_decode_multiple_frames_in_buffer() {
        let mut codec = FrameCodec::new();
        let mut src = BytesMut::new();

        // Склеиваем три кадра в один буфер.
        src.extend_from_slice(&build_raw_frame(FrameType::Ping as u8, b"ping1"));
        src.extend_from_slice(&build_raw_frame(FrameType::Pong as u8, b"pong1"));
        src.extend_from_slice(&build_raw_frame(FrameType::Data as u8, b"data1"));

        let f1 = codec.decode(&mut src).unwrap().unwrap();
        assert_eq!(f1.frame_type, FrameType::Ping as u8);
        assert_eq!(f1.payload.as_ref(), b"ping1");

        let f2 = codec.decode(&mut src).unwrap().unwrap();
        assert_eq!(f2.frame_type, FrameType::Pong as u8);
        assert_eq!(f2.payload.as_ref(), b"pong1");

        let f3 = codec.decode(&mut src).unwrap().unwrap();
        assert_eq!(f3.frame_type, FrameType::Data as u8);
        assert_eq!(f3.payload.as_ref(), b"data1");

        assert!(src.is_empty());
        assert!(codec.decode(&mut src).unwrap().is_none());
    }

    #[test]
    fn test_decode_crc_mismatch() {
        let mut codec = FrameCodec::new();
        let mut src = build_raw_frame(FrameType::Data as u8, b"payload");

        // Corrupting the last byte of the CRC.
        let last_idx = src.len() - 1;
        src[last_idx] ^= 0xFF;

        let err = codec.decode(&mut src).unwrap_err();
        match err {
            FrameCodecError::CrcMismatch { .. } => {} // as expected
            other => panic!("Expected CrcMismatch, got: {other:?}"),
        }
    }

    #[test]
    fn test_decode_payload_too_large() {
        let mut codec = FrameCodec::with_max_payload_size(100);
        let mut src = BytesMut::new();
        src.put_u8(FrameType::Data as u8);
        src.put_uint_le(200, 3); // length 200 > limit 100

        let err = codec.decode(&mut src).unwrap_err();
        match err {
            FrameCodecError::PayloadTooLarge(200) => {}
            other => panic!("Expected PayloadTooLarge(200), got: {other:?}"),
        }
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut codec = FrameCodec::new();
        let original = Frame::data(Bytes::from("roundtrip payload"));

        let mut buf = BytesMut::new();
        codec.encode(original.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().expect("should be decoded");

        assert_eq!(decoded.frame_type, original.frame_type);
        assert_eq!(decoded.payload, original.payload);
    }

    #[test]
    fn test_encode_payload_too_large() {
        let mut codec = FrameCodec::with_max_payload_size(10);
        let frame = Frame::data(Bytes::from("this payload is way too large"));

        let mut buf = BytesMut::new();
        let err = codec.encode(frame, &mut buf).unwrap_err();
        match err {
            FrameCodecError::PayloadTooLarge(_) => {}
            other => panic!("Expected PayloadTooLarge, got: {other:?}"),
        }
    }

    #[test]
    fn test_empty_payload() {
        let mut codec = FrameCodec::new();
        let frame = Frame::new(FrameType::Close as u8, Bytes::new());

        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.frame_type, FrameType::Close as u8);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn test_zero_copy_payload() {
        // Verify that the payload in the Frame shares memory with the original buffer.
        let mut codec = FrameCodec::new();
        let mut src = build_raw_frame(FrameType::Data as u8, b"zero-copy test");
        let original_ptr = src.as_ptr() as usize;

        let frame = codec.decode(&mut src).unwrap().unwrap();

        // The payload must start exactly after the header.
        let expected_payload_ptr = original_ptr + FRAME_HEADER_SIZE;
        assert_eq!(frame.payload.as_ptr() as usize, expected_payload_ptr);
    }
}
