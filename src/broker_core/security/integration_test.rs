// arcella-broker/src/broker_core/security/integration_test.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

// Integration test for the full handshake protocol with CreditGrant exchange.
// This test verifies the complete flow:
// 1. Client connects via UDS
// 2. Hello → Welcome → AuthResponse → AuthResult
// 3. Server sends initial CreditGrant
// 4. Client sends initial CreditGrant
// 5. Client sends a Data frame (verifying credits are initialized)

#[cfg(test)]
mod handshake_tests {
    use bytes::{Buf, Bytes};
    use tempfile::tempdir;
    use tokio::net::UnixListener;

    use crate::broker_core::{
        security::handshake::HandshakeHandler,
        transport::{IPCConnector, IPCEndpoint, UdsConnector},
    };
    use crate::protocol::{
        Frame, FrameType,
        payloads::{
            AuthResponsePayload, AuthResultPayload, CreditGrantPayload,
            HelloPayload, WelcomePayload, L2_PROTOCOL_VERSION,
        },
        test_utils::dummy_in_only_message,
    };

    /// Helper: create a Hello frame
    fn create_hello_frame() -> Frame {
        let hello = HelloPayload {
            protocol_version: L2_PROTOCOL_VERSION,
            client_id: Bytes::from("test-client"),
            algorithm: 0x00, // None
            auth_data: Bytes::new(),
            capabilities: 0,
        };
        Frame::new(FrameType::Hello as u8, hello.encode())
    }

    /// Helper: create an AuthResponse frame for the given session_id
    fn create_auth_response_frame(session_id: u64) -> Frame {
        let auth_response = AuthResponsePayload {
            session_id,
            client_nonce: [0xAA; 32],
            response: Bytes::new(), // Empty for algorithm None
        };
        Frame::new(FrameType::AuthResponse as u8, auth_response.encode())
    }

    /// Helper: parse a Welcome payload from a frame
    fn parse_welcome(frame: &Frame) -> WelcomePayload {
        assert_eq!(frame.frame_type, FrameType::Welcome as u8);
        let mut payload = frame.payload.clone();
        WelcomePayload::decode(&mut payload).expect("Failed to decode Welcome")
    }

    /// Helper: parse an AuthResult payload from a frame
    fn parse_auth_result(frame: &Frame) -> AuthResultPayload {
        assert_eq!(frame.frame_type, FrameType::AuthResult as u8);
        let mut payload = frame.payload.clone();
        AuthResultPayload::decode(&mut payload).expect("Failed to decode AuthResult")
    }

    /// Helper: parse a CreditGrant payload from a frame
    fn parse_credit_grant(frame: &Frame) -> CreditGrantPayload {
        assert_eq!(frame.frame_type, FrameType::CreditGrant as u8);
        CreditGrantPayload::decode(&frame.payload).expect("Failed to decode CreditGrant")
    }

    /// Integration test: full handshake with CreditGrant exchange
    #[tokio::test]
    async fn test_full_handshake_with_credit_grant() {
        // Setup: create a temporary UDS path
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock");
        let socket_path_clone = socket_path.clone();

        // Server task
        let server_handle = tokio::spawn(async move {
            let listener = UnixListener::bind(&socket_path_clone).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let mut server_endpoint = IPCEndpoint::new(stream, 50);
            let handshake_handler = HandshakeHandler::new();

            // 1. Receive Hello
            let hello_frame = server_endpoint.recv().await.unwrap().unwrap();
            assert_eq!(hello_frame.frame_type, FrameType::Hello as u8);
            let mut hello_payload = hello_frame.payload.clone();
            let hello = HelloPayload::decode(&mut hello_payload).unwrap();

            // 2. Process Hello → Welcome
            let welcome = handshake_handler.process_hello(&hello).unwrap();
            let welcome_frame = Frame::new(FrameType::Welcome as u8, welcome.encode());
            server_endpoint.send_raw_frame(welcome_frame).await.unwrap();

            // 3. Receive AuthResponse
            let auth_response_frame = server_endpoint.recv().await.unwrap().unwrap();
            assert_eq!(auth_response_frame.frame_type, FrameType::AuthResponse as u8);
            let mut auth_response_payload = auth_response_frame.payload.clone();
            let auth_response = AuthResponsePayload::decode(&mut auth_response_payload).unwrap();

            // 4. Process AuthResponse → AuthResult
            let auth_result = handshake_handler.process_auth_response(&auth_response).unwrap();
            assert_eq!(auth_result.status, 0); // success
            let auth_result_frame = Frame::new(FrameType::AuthResult as u8, auth_result.encode());
            server_endpoint.send_raw_frame(auth_result_frame).await.unwrap();

            // 5. Send initial CreditGrant (server grants hot credits to client)
            let credit_grant_seq = server_endpoint
                .send_credit_grant(
                    Bytes::new(), // Global credits
                    CreditGrantPayload::CREDIT_TYPE_HOT,
                    welcome.limits.hot_credit,
                )
                .await
                .unwrap();
            assert_eq!(credit_grant_seq, 1); // First frame after handshake

            // 6. Receive CreditGrant from client
            let client_credit_grant_frame = server_endpoint.recv().await.unwrap().unwrap();
            let client_credit_grant = parse_credit_grant(&client_credit_grant_frame);
            assert_eq!(client_credit_grant.credit_type, CreditGrantPayload::CREDIT_TYPE_HOT);
            assert!(client_credit_grant.credit_amount > 0);

            // 7. Receive Data frame from client (verifying credits work)
            let data_frame = server_endpoint.recv().await.unwrap().unwrap();
            assert_eq!(data_frame.frame_type, FrameType::Data as u8);

            // 8. Send Ack for the Data frame
            let mut data_payload = data_frame.payload.clone();
            let data_seq = data_payload.get_u64_le();
            server_endpoint.send_ack(data_seq).await.unwrap();
        });

        // Client task
        let client_handle = tokio::spawn(async move {
            // Wait for server to start listening
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let connector = UdsConnector::new(&socket_path);
            let stream = connector.connect().await.unwrap();
            let mut client_endpoint = IPCEndpoint::new(stream, 50);

            // 1. Send Hello
            let hello_frame = create_hello_frame();
            client_endpoint.send_raw_frame(hello_frame).await.unwrap();

            // 2. Receive Welcome
            let welcome_frame = client_endpoint.recv().await.unwrap().unwrap();
            let welcome = parse_welcome(&welcome_frame);
            assert_eq!(welcome.protocol_version, L2_PROTOCOL_VERSION);
            assert_eq!(welcome.session_id, 1);
            let session_id = welcome.session_id;
            let hot_credit = welcome.limits.hot_credit;

            // 3. Send AuthResponse
            let auth_response_frame = create_auth_response_frame(session_id);
            client_endpoint.send_raw_frame(auth_response_frame).await.unwrap();

            // 4. Receive AuthResult
            let auth_result_frame = client_endpoint.recv().await.unwrap().unwrap();
            let auth_result = parse_auth_result(&auth_result_frame);
            assert_eq!(auth_result.status, 0); // success
            assert_eq!(auth_result.session_id, session_id);

            // 5. Send initial CreditGrant (client grants hot credits to server)
            let credit_grant_seq = client_endpoint
                .send_credit_grant(
                    Bytes::new(), // Global credits
                    CreditGrantPayload::CREDIT_TYPE_HOT,
                    hot_credit,
                )
                .await
                .unwrap();
            assert_eq!(credit_grant_seq, 1); // First frame after handshake

            // 6. Receive CreditGrant from server
            let server_credit_grant_frame = client_endpoint.recv().await.unwrap().unwrap();
            let server_credit_grant = parse_credit_grant(&server_credit_grant_frame);
            assert_eq!(server_credit_grant.credit_type, CreditGrantPayload::CREDIT_TYPE_HOT);
            assert_eq!(server_credit_grant.credit_amount, hot_credit);

            // 7. Send a Data frame (verifying credits are initialized)
            let msg = dummy_in_only_message(
                Bytes::from("test.msg"),
                Bytes::from("worker:1"),
                Bytes::from("hello"),
            );
            let messages = [msg];
            let data_seq = client_endpoint
                .send_data(Bytes::from("worker:1"), &messages)
                .await
                .unwrap();
            assert_eq!(data_seq, 2); // Second frame (after CreditGrant)

            // 8. Receive Ack for the Data frame
            let ack_frame = client_endpoint.recv().await.unwrap().unwrap();
            assert_eq!(ack_frame.frame_type, FrameType::Acknowledge as u8);
            let mut ack_payload = ack_frame.payload.clone();
            let ack_seq = ack_payload.get_u64_le();
            assert_eq!(ack_seq, data_seq);
        });

        // Wait for both tasks to complete
        let (server_result, client_result) = tokio::join!(server_handle, client_handle);
        server_result.unwrap();
        client_result.unwrap();
    }

    /// Test: CreditGrant with specific address prefix
    #[tokio::test]
    async fn test_credit_grant_with_address_prefix() {
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test_prefix.sock");
        let socket_path_clone = socket_path.clone();

        let server_handle = tokio::spawn(async move {
            let listener = UnixListener::bind(&socket_path_clone).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let mut server_endpoint = IPCEndpoint::new(stream, 50);
            let handshake_handler = HandshakeHandler::new();

            // Handshake (abbreviated)
            let hello_frame = server_endpoint.recv().await.unwrap().unwrap();
            let mut hello_payload = hello_frame.payload.clone();
            let hello = HelloPayload::decode(&mut hello_payload).unwrap();
            let welcome = handshake_handler.process_hello(&hello).unwrap();
            server_endpoint
                .send_raw_frame(Frame::new(FrameType::Welcome as u8, welcome.encode()))
                .await
                .unwrap();

            let auth_response_frame = server_endpoint.recv().await.unwrap().unwrap();
            let mut auth_response_payload = auth_response_frame.payload.clone();
            let auth_response = AuthResponsePayload::decode(&mut auth_response_payload).unwrap();
            let auth_result = handshake_handler.process_auth_response(&auth_response).unwrap();
            server_endpoint
                .send_raw_frame(Frame::new(FrameType::AuthResult as u8, auth_result.encode()))
                .await
                .unwrap();

            // Send CreditGrant for specific address prefix
            server_endpoint
                .send_credit_grant(
                    Bytes::from("worker:1"),
                    CreditGrantPayload::CREDIT_TYPE_HOT,
                    25,
                )
                .await
                .unwrap();

            // Receive CreditGrant from client
            let client_cg_frame = server_endpoint.recv().await.unwrap().unwrap();
            let client_cg = parse_credit_grant(&client_cg_frame);
            assert_eq!(client_cg.address, Bytes::from("client:1"));
        });

        let client_handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let connector = UdsConnector::new(&socket_path);
            let stream = connector.connect().await.unwrap();
            let mut client_endpoint = IPCEndpoint::new(stream, 50);

            // Handshake (abbreviated)
            client_endpoint.send_raw_frame(create_hello_frame()).await.unwrap();
            let welcome_frame = client_endpoint.recv().await.unwrap().unwrap();
            let welcome = parse_welcome(&welcome_frame);
            client_endpoint
                .send_raw_frame(create_auth_response_frame(welcome.session_id))
                .await
                .unwrap();
            let auth_result_frame = client_endpoint.recv().await.unwrap().unwrap();
            let auth_result = parse_auth_result(&auth_result_frame);
            assert_eq!(auth_result.status, 0);

            // Send CreditGrant for specific address prefix
            client_endpoint
                .send_credit_grant(
                    Bytes::from("client:1"),
                    CreditGrantPayload::CREDIT_TYPE_HOT,
                    30,
                )
                .await
                .unwrap();

            // Receive CreditGrant from server
            let server_cg_frame = client_endpoint.recv().await.unwrap().unwrap();
            let server_cg = parse_credit_grant(&server_cg_frame);
            assert_eq!(server_cg.address, Bytes::from("worker:1"));
            assert_eq!(server_cg.credit_amount, 25);
        });

        let (server_result, client_result) = tokio::join!(server_handle, client_handle);
        server_result.unwrap();
        client_result.unwrap();
    }
}
