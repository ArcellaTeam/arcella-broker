// arcella-broker/src/broker_core/transport/ipc/integration_test.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Integration tests for IPC Transport Layer (Stage 3).
//!
//! This module verifies that two independent Tokio tasks can exchange
//! L2 frames over a Unix Domain Socket without data loss, correctly
//! managing session sequences and the unacked buffer.

#[cfg(test)]
mod tests {
    use bytes::{Buf, Bytes};
    use tempfile::tempdir;
    use tokio::net::UnixListener;
    use tokio::time::{ Duration, sleep, };

    use crate::protocol::{ FrameType, Message };
    use crate::protocol::test_utils::dummy_in_only_message;
    use super::super::{IPCConnector, IPCEndpoint, UdsConnector};

    /// Helper to create a dummy L3 message for testing
    fn create_test_msg(id: u32) -> Message {
        dummy_in_only_message(
            Bytes::from("test.msg"),
            Bytes::from("worker:1"),
            Bytes::from(format!("payload_{}", id)),
        )
    }

    #[tokio::test]
    async fn test_ipc_transport_end_to_end_1000_frames() {
        // 1. Setup UDS path
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let socket_path = temp_dir.path().join("integration_test.sock");

        // 2. Start Server Task (Receiver)
        let server_path = socket_path.clone();
        let server_handle = tokio::spawn(async move {
            let listener = UnixListener::bind(&server_path).expect("Failed to bind UDS");
            let (stream, _addr) = listener.accept().await.expect("Failed to accept connection");
            
            // Server endpoint with hot_credit = 100
            let mut server_endpoint = IPCEndpoint::new(stream, 100);
            let mut received_count = 0u64;

            while let Some(frame) = server_endpoint.recv().await.expect("Server recv failed") {
                if frame.frame_type == FrameType::Data as u8 {
                    // In a real scenario, Ingress Bridge would process this.
                    // Here, we just immediately send an Acknowledge.
                    // We can extract sequence from the payload or just use a mock for this test.
                    // For simplicity in this specific test, we'll parse it.
                    // (Using the helper from transport.rs would be ideal, but let's keep it self-contained)
                    // Actually, let's use the public helper if available, or just mock the ack logic.
                    // Since parse_ack_sequence is for incoming acks, we need to extract seq from Data.
                    // For this test, we'll trust the endpoint's internal state or just send a dummy ack.
                    // Better: let's assume the test validates the client side unacked buffer clearing.
                    
                    // To properly ack, we need the sequence. Let's decode it minimally:
                    // payload format: session_sequence (8) + address_len (2) + address + msg_count (2) + msgs
                    let seq = {
                        let mut buf = frame.payload.as_ref();
                        buf.get_u64_le() // session_sequence
                    };
                    
                    server_endpoint.send_ack(seq).await.expect("Server send_ack failed");
                    received_count += 1;
                }
            }
            received_count
        });

        // Give the server a moment to start listening
        sleep(Duration::from_millis(50)).await;

        // 3. Start Client Task (Sender)
        let client_handle = tokio::spawn(async move {
            let connector = UdsConnector::new(&socket_path);
            let stream = connector.connect().await.expect("Client connect failed");
            
            // Client endpoint with hot_credit = 100
            let mut client_endpoint = IPCEndpoint::new(stream, 100);
            
            let total_frames = 1000u64;

            for i in 0..total_frames {
                let msg = create_test_msg(i as u32);
                let _seq = client_endpoint
                    .send_data(Bytes::from("worker:1"), &[msg])
                    .await
                    .expect("Client send_data failed");
                
                // Simulate receiving the ack and clearing the buffer
                // In a real full-duplex setup, this would be a separate recv loop.
                // For this test, we'll do a quick recv to clear the buffer.
                if let Some(ack_frame) = client_endpoint.recv().await.expect("Client recv ack failed") {
                    if ack_frame.frame_type == FrameType::Acknowledge as u8 {
                        // Extract seq from ack payload (8 bytes, u64 LE)
                        let mut buf = ack_frame.payload.as_ref();
                        let acked_seq = buf.get_u64_le();
                        client_endpoint.acknowledge(acked_seq);
                    }
                }
            }

            // Verify UnackedBuffer is empty after all acks
            assert!(
                client_endpoint.unacked_buffer().is_empty(),
                "UnackedBuffer should be empty after all acknowledgments"
            );
            
            // Verify session state progressed correctly
            assert_eq!(
                client_endpoint.session().current_send_sequence(),
                total_frames + 1
            );
        });

        // 4. Await both tasks
        let (client_res, server_res) = tokio::join!(client_handle, server_handle);
        
        client_res.expect("Client task panicked");
        let received_count = server_res.expect("Server task panicked");
        
        assert_eq!(received_count, 1000, "Server should have received exactly 1000 frames");
    }
}