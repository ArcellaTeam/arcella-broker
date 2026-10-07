// arcella-broker/src/stream_transport/integration_test.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

#[cfg(test)]
mod integration_tests {
    use bytes::{Bytes, Buf, BufMut};
    use std::sync::Arc;

    use crate::stream_transport::{SessionState, UnackedBuffer};
    use crate::protocol::{Frame, FrameType};
    use crate::protocol::payloads::{decode_data_payload, encode_data_payload};
    use crate::protocol::test_utils;

    fn create_data_frame(sequence: u64, address: Bytes, msg_payload: Bytes) -> Frame {

        let msg = test_utils::dummy_in_only_message(
            Bytes::from("test.msg"),
            address.clone(),
            msg_payload,
        );
        
        let data_payload = encode_data_payload(sequence, &address, &[msg]).unwrap();
        Frame::data(data_payload)
    }

    fn create_ack_frame(sequence: u64) -> Frame {
        let mut payload = bytes::BytesMut::with_capacity(8);
        payload.put_u64_le(sequence);
        Frame::new(FrameType::Acknowledge as u8, payload.freeze())
    }

    #[tokio::test]
    async fn test_stream_transport_simple_flow_control_1000_frames() {
        let session = Arc::new(SessionState::new(1));
        let unacked_buffer = UnackedBuffer::new(1024);

        let address = "test:worker:1";
        let total_frames = 1000;

        for i in 0..total_frames {
            let seq = session.next_sequence();
            let payload_str = format!("payload_{}", i);
            let payload_bytes = Bytes::from(payload_str);
            let frame = create_data_frame(seq, Bytes::from(address), payload_bytes);
            
            unacked_buffer.push(seq, frame).unwrap();
        }

        assert_eq!(unacked_buffer.len(), total_frames);

        for i in 0..total_frames {
            let seq = (i + 1) as u64;
            let payload_str = format!("payload_{}", i);
            let payload_bytes = Bytes::from(payload_str);
            let frame = create_data_frame(seq, Bytes::from(address), payload_bytes);
            
            let parsed = decode_data_payload(&frame.payload).unwrap();
            
            assert!(session.update_last_received(parsed.session_sequence));

            let ack_frame = create_ack_frame(parsed.session_sequence);
            
            let ack_seq = {
                let mut buf = ack_frame.payload.as_ref();
                buf.get_u64_le()
            };
            
            assert!(unacked_buffer.acknowledge(ack_seq));
        }

        assert!(unacked_buffer.is_empty());
        assert_eq!(session.last_received_sequence(), total_frames as u64);
        assert_eq!(session.current_send_sequence(), (total_frames + 1) as u64);
    }
}