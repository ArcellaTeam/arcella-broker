// arcella-broker/src/protocol/message.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;

use super::{
    ProtocolError,
    utils::validate_address,
    MAX_ADDRESS_LEN,
    TRANSFER_MODE_MASK,
    TRANSFER_MODE_SHIFT,
};

// ============================================================================
// Protocol constants
// ============================================================================

/// Current protocol version
pub const MESSAGE_PROTOCOL_VERSION: u16 = 1;

/// Maximum message type size (bytes)
pub const MESSAGE_MAX_TYPE_LEN: usize = 255;

/// Maximum payload size (1 MB)
pub const MESSAGE_MAX_PAYLOAD_LEN: u32 = 1_048_576;

/// Token size (32 bytes)
pub const MESSAGE_SESSION_TOKEN_LEN: usize = 32;

/// Message ID size (16 bytes)
pub const MESSAGE_ID_LEN: usize = 16;

/// Submessage ID size (4 bytes)
pub const MESSAGE_SUB_ID_LEN: usize = 4;

/// Size of the fixed header in bytes
/// 2 (version) + 1 (flags) + 1 (priority) + 32 (token) + 16 (guid) + 4 (sub_id)
/// + 1 (ttl) + 1 (type_len) + 2 (addr_len) + 4 (payload_len) = 64 bytes
///
/// Note: reply_to is stored in the variable part of the message and 
/// is not included in the fixed header size.
pub const MESSAGE_FIXED_HEADER_SIZE: usize = 64;

// ============================================================================
// Message errors
// ============================================================================

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MessageError {
    #[error("Insufficient data to read header")]
    IncompleteHeader,

    #[error("Unsupported protocol version: {0}")]
    UnsupportedVersion(u16),

    #[error("Unknown transfer mode: {0}")]
    UnknownTransferMode(u8),

    #[error("Message type length ({0}) exceeds limit {MESSAGE_MAX_TYPE_LEN}")]
    MsgTypeTooLong(u8),

    #[error("ReplyTo length ({0}) exceeds limit {MAX_ADDRESS_LEN}")]
    ReplyToTooLong(u16),

    #[error("Payload size ({0}) exceeds limit {MESSAGE_MAX_PAYLOAD_LEN}")]
    PayloadTooLarge(u32),

    #[error("Insufficient data to read message type")]
    IncompleteMsgType,

    #[error("Insufficient data to read address")]
    IncompleteAddress,

    #[error("Insufficient data to read reply_to address")]
    IncompleteReplyTo,

    #[error("Insufficient data to read payload")]
    IncompletePayload,

    #[error("Invalid UTF-8 in message type")]
    InvalidMsgTypeUtf8,

    #[error("Invalid UTF-8 in reply_to address")]
    InvalidReplyToUtf8,

    #[error("InOut mode requires a non-empty reply_to address")]
    MissingReplyToForInOut,

    #[error("InOnly mode must have an empty reply_to address")]
    UnexpectedReplyToForInOnly,
}

// ============================================================================
// Transfer mode (Flags)
// ============================================================================

/// Message transfer mode, defining the interaction semantics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransferMode {
    /// Asynchronous send without waiting for a response (Tell, fire-and-forget)
    InOnly = 0,
    /// Send with waiting for a response  (Ask, request/response)
    InOut = 1,
}

impl TransferMode {

    /// Extracts the transfer mode the from flags field (uses only bit 0)
    pub fn from_flags(flags: u8) -> Result<Self, ProtocolError> {
        let mode_bits = flags & TRANSFER_MODE_MASK;
        
        match mode_bits {
            0 => Ok(Self::InOnly),
            1 => Ok(Self::InOut),
            // Protection against possible future changes
            _ => Err(MessageError::UnknownTransferMode(mode_bits).into()),
        }
    }

    /// Converts the transfer mode to the flags field value
    pub fn to_flags(self) -> u8 {
        (self as u8) << TRANSFER_MODE_SHIFT
    }
}

// ============================================================================
// Fixed header
// ============================================================================

/// Fixed part of the message header (64 bytes)
/// 
/// Structure (all numbers in Little-Endian):
/// - version: u16 — protocol version
/// - flags: u8 — flags (transfer mode)
/// - priority: u8 — message priority
/// - session_token: [u8; MESSAGE_SESSION_TOKEN_LEN] — session token (SHA-256/BLAKE3)
/// - message_id: [u8; MESSAGE_ID_LEN] — unique message identifier (GUID)
/// - sub_message_id: [u8; MESSAGE_SUB_ID_LEN] — unique submessage identifier
/// - ttl: u8 — routing counter (Time To Live)
/// - msg_type_len: u8 — message type length
/// - address_len: u16 — recipient address length
/// - payload_len: u32 — payload length
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedHeader {
    pub version: u16,
    pub flags: u8,
    pub priority: u8,
    pub session_token: [u8; MESSAGE_SESSION_TOKEN_LEN],
    pub message_id: [u8; MESSAGE_ID_LEN],
    pub sub_message_id: [u8; MESSAGE_SUB_ID_LEN],
    pub ttl: u8,
    pub msg_type_len: u8,
    pub address_len: u16,
    pub payload_len: u32,
}

impl FixedHeader {
    /// Creates a new fixed header with validation
    pub fn new(
        mode: TransferMode,
        session_token: [u8; MESSAGE_SESSION_TOKEN_LEN],
        message_id: [u8; MESSAGE_ID_LEN],
        sub_message_id: [u8; MESSAGE_SUB_ID_LEN],
        priority: u8,
        ttl: u8,
        msg_type_len: u8,
        address_len: u16,
        payload_len: u32,
    ) -> Result<Self, ProtocolError> {
        // Length validation
        if msg_type_len as usize > MESSAGE_MAX_TYPE_LEN {
            return Err(MessageError::MsgTypeTooLong(msg_type_len).into());
        }
        if address_len as usize > MAX_ADDRESS_LEN {
            return Err(ProtocolError::AddressTooLong(address_len));
        }
        if payload_len > MESSAGE_MAX_PAYLOAD_LEN {
            return Err(MessageError::PayloadTooLarge(payload_len).into());
        }
        
        let flags = mode.to_flags();

        Ok(Self {
            version: MESSAGE_PROTOCOL_VERSION,
            flags,
            priority,
            session_token,
            message_id,
            sub_message_id,
            ttl,
            msg_type_len,
            address_len,
            payload_len,
        })
    }

    /// Decodes the fixed header from a buffer
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, ProtocolError> {
        if buf.remaining() < MESSAGE_FIXED_HEADER_SIZE {
            return Err(MessageError::IncompleteHeader.into());
        }

        let version = buf.get_u16_le();
        if version != MESSAGE_PROTOCOL_VERSION {
            return Err(MessageError::UnsupportedVersion(version).into());
        }

        let flags = buf.get_u8();
        // Validate transfer mode immediately during parsing
        TransferMode::from_flags(flags)?;

        let priority = buf.get_u8();

        let mut session_token = [0u8; MESSAGE_SESSION_TOKEN_LEN];
        buf.copy_to_slice(&mut session_token);

        let mut message_id = [0u8; MESSAGE_ID_LEN];
        buf.copy_to_slice(&mut message_id);

        let mut sub_message_id = [0u8; MESSAGE_SUB_ID_LEN];
        buf.copy_to_slice(&mut sub_message_id);

        let ttl = buf.get_u8();
        let msg_type_len = buf.get_u8();
        let address_len = buf.get_u16_le();
        let payload_len = buf.get_u32_le();

        // Length validation after reading
        if msg_type_len as usize > MESSAGE_MAX_TYPE_LEN {
            return Err(MessageError::MsgTypeTooLong(msg_type_len).into());
        }
        if address_len as usize > MAX_ADDRESS_LEN {
            return Err(ProtocolError::AddressTooLong(address_len));
        }
        if payload_len > MESSAGE_MAX_PAYLOAD_LEN {
            return Err(MessageError::PayloadTooLarge(payload_len).into());
        }

        Ok(Self {
            version,
            flags,
            priority,
            session_token,
            message_id,
            sub_message_id,
            ttl,
            msg_type_len,
            address_len,
            payload_len,
        })
    }

    /// Encodes the fixed header into a buffer
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put_u16_le(self.version);
        buf.put_u8(self.flags);
        buf.put_u8(self.priority);
        buf.put_slice(&self.session_token);
        buf.put_slice(&self.message_id);
        buf.put_slice(&self.sub_message_id);
        buf.put_u8(self.ttl);
        buf.put_u8(self.msg_type_len);
        buf.put_u16_le(self.address_len);
        buf.put_u32_le(self.payload_len);
    }

    /// Returns the transfer mode
    pub fn transfer_mode(&self) -> Result<TransferMode, ProtocolError> {
        TransferMode::from_flags(self.flags)
    }
}

// ============================================================================
// Full message
// ============================================================================

/// Full microbroker message
/// 
/// Consists of:
/// 1. Fixed header (64 bytes)
/// 2. Message type (UTF-8 string, < 255 bytes)
/// 3. Recipient address (UTF-8 string, < 1024 bytes)
/// 4. Reply to address (UTF-8 string, < 1024 bytes)
/// 5. Payload (bincode, transparent to the broker)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub header: FixedHeader,
    pub msg_type: Bytes,
    pub address: Bytes,
    pub reply_to: Bytes,
    pub payload: Bytes,
}

impl Message {
    /// Creates a new message with validation
    pub fn new(
        mode: TransferMode,
        session_token: [u8; MESSAGE_SESSION_TOKEN_LEN],
        message_id: [u8; MESSAGE_ID_LEN],
        sub_message_id: [u8; MESSAGE_SUB_ID_LEN],
        priority: u8,
        ttl: u8,
        msg_type: Bytes,
        address: Bytes,
        reply_to: Bytes,
        payload: Bytes,
    ) -> Result<Self, ProtocolError> {
        // Strict check: reply_to is mandatory for InOut, forbidden for InOnly
        match mode {
            TransferMode::InOut => {
                if reply_to.is_empty() {
                    return Err(MessageError::MissingReplyToForInOut.into());
                }
            }
            TransferMode::InOnly => {
                if !reply_to.is_empty() {
                    return Err(MessageError::UnexpectedReplyToForInOnly.into());
                }
            }
        }

        // Address validation
        let address_str = str::from_utf8(&address)
            .map_err(|_| ProtocolError::InvalidAddressUtf8)?;
        validate_address(address_str)?;

        // Validate reply_to (if not empty)
        if !reply_to.is_empty() {
            let reply_to_str = str::from_utf8(&reply_to).map_err(|_| MessageError::InvalidReplyToUtf8)?;
            validate_address(reply_to_str)?;
        }

        // Length validation
        if msg_type.len() > MESSAGE_MAX_TYPE_LEN {
            return Err(MessageError::MsgTypeTooLong(msg_type.len() as u8).into());
        }

        if address.len() > MAX_ADDRESS_LEN {
            return Err(ProtocolError::AddressTooLong(address.len() as u16));
        }

        if reply_to.len() > MAX_ADDRESS_LEN {
            return Err(MessageError::ReplyToTooLong(reply_to.len() as u16).into());
        }

        let payload_len = payload.len() as u32;
        if payload_len > MESSAGE_MAX_PAYLOAD_LEN {
            return Err(MessageError::PayloadTooLarge(payload_len).into());
        }

        let header = FixedHeader::new(
            mode,
            session_token,
            message_id,
            sub_message_id,
            priority,
            ttl,
            msg_type.len() as u8,
            address.len() as u16,
            payload_len,
        )?;

        Ok(Self {
            header,
            msg_type,
            address,
            reply_to,
            payload,
        })
    }

    /// Decodes the full message from a buffer
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, ProtocolError> {
        // 1. Read the fixed header
        let header = FixedHeader::decode(buf)?;
        let mode = header.transfer_mode()?;

        // 2. Read the message type
        let msg_type_len = header.msg_type_len as usize;
        if buf.remaining() < msg_type_len {
            return Err(MessageError::IncompleteMsgType.into());
        }
        let msg_type = buf.copy_to_bytes(msg_type_len);
        let _msg_type = str::from_utf8(&msg_type)
            .map_err(|_| MessageError::InvalidMsgTypeUtf8)?;

        // 3. Read the recipient address
        let address_len = header.address_len as usize;
        if buf.remaining() < address_len {
            return Err(MessageError::IncompleteAddress.into());
        }
        let address = buf.copy_to_bytes(address_len);
        let address_str = str::from_utf8(&address)
            .map_err(|_| ProtocolError::InvalidAddressUtf8)?;
        validate_address(address_str)?;

        // 4. Reply-to (only for InOut)
        let reply_to = if mode == TransferMode::InOut {
            // Read length reply_to from next 2 byte
            if buf.remaining() < 2 {
                return Err(MessageError::IncompleteReplyTo.into());
            }
            let reply_to_len = buf.get_u16_le() as usize;
            
            if reply_to_len == 0 {
                return Err(MessageError::MissingReplyToForInOut.into());
            }
            if reply_to_len > MAX_ADDRESS_LEN {
                return Err(MessageError::ReplyToTooLong(reply_to_len as u16).into());
            }
            
            if buf.remaining() < reply_to_len {
                return Err(MessageError::IncompleteReplyTo.into());
            }
            let reply_to_bytes = buf.copy_to_bytes(reply_to_len);
            let reply_to_str = str::from_utf8(&reply_to_bytes).map_err(|_| MessageError::InvalidReplyToUtf8)?;
            validate_address(reply_to_str)?;
            reply_to_bytes
        } else {
            Bytes::new() // For InOnly, reply_to is empty
        };        

        // 5. Read payload (zero-copy via Bytes)
        let payload_len = header.payload_len as usize;
        if buf.remaining() < payload_len {
            return Err(MessageError::IncompletePayload.into());
        }
        let payload = buf.copy_to_bytes(payload_len);

        Ok(Self {
            header,
            msg_type,
            address,
            reply_to,
            payload,
        })
    }

    /// Encodes the full message into a buffer
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        // 1. Fixed header
        self.header.encode(buf);

        // 2. Message type
        buf.put_slice(&self.msg_type);

        // 3. Recipient address
        buf.put_slice(&self.address);

        // 4. Reply-to (only for InOut)
        if self.header.transfer_mode() == Ok(TransferMode::InOut) {
            buf.put_u16_le(self.reply_to.len() as u16);
            buf.put_slice(&self.reply_to);
        }        

        // 5. Payload
        buf.put_slice(&self.payload);
    }

    /// Returns the transfer mode
    pub fn transfer_mode(&self) -> Result<TransferMode, ProtocolError> {
        self.header.transfer_mode()
    }

    /// Returns the total size of the variable part of the header
    pub fn variable_header_size(&self) -> usize {
        let reply_to_overhead = if self.transfer_mode() == Ok(TransferMode::InOut) {
            2 + self.reply_to.len()
        } else {
            0
        };
        self.header.msg_type_len as usize + self.header.address_len as usize + reply_to_overhead
    }

    /// Returns the total message size in bytes
    pub fn size(&self) -> usize {
        MESSAGE_FIXED_HEADER_SIZE + self.variable_header_size() + self.header.payload_len as usize
    }
}
