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
    use std::sync::Arc;    
    use bytes::Bytes;
    use tempfile::tempdir;
    use tokio::net::UnixListener;

    use crate::protocol::{
        Frame, FrameType, ServerLimits,
        payloads::{
            HelloPayload, 
            WelcomePayload, 
            AuthResponsePayload,
            AuthResultPayload,
            L2_PROTOCOL_VERSION,
        },
        test_utils::dummy_in_only_message,
    };
    use super::super::super::{
        l2::SessionState,
        transport::{IPCConnector, IPCEndpoint, IPCTransport, UdsConnector},
    };
    use super::super::{
        SessionContext,
        ServerSessionContext,
        ClientSessionContext, 
        handshake::HandshakeHandler,
    };

    use crate::test_utils::init_tracing;    

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

    /// Integration test: full handshake with CreditGrant exchange
    #[tokio::test]
    async fn test_full_handshake_with_credit_grant() {
        init_tracing("error");
        // Setup: create a temporary UDS path
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock");
        let socket_path_clone = socket_path.clone();

        let limits = ServerLimits {
            hot_credit: 10,
            cold_credit: 5,
            ..ServerLimits::default()
        };
        let limits_clone = limits.clone();        

        // === Server side ===
        let server_handle = tokio::spawn(async move {
            let listener = UnixListener::bind(&socket_path_clone).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let mut transport = IPCTransport::new(stream);
            let handshake_handler = HandshakeHandler::with_limits(limits.clone());

            // 1. Receive Hello
            let hello_frame = transport.recv().await.unwrap().unwrap();
            assert_eq!(hello_frame.frame_type, FrameType::Hello as u8);
            let mut hello_payload = hello_frame.payload.clone();
            let hello = HelloPayload::decode(&mut hello_payload).unwrap();
            tracing::debug!("Received Hello from client"); 

            // 2. Process Hello → Welcome
            let welcome = handshake_handler.process_hello(&hello).unwrap();
            let welcome_frame = Frame::new(FrameType::Welcome as u8, welcome.encode());
            transport.send(welcome_frame).await.unwrap();
            tracing::debug!(
                session_id = welcome.session_id,
                "Sent Welcome"
            );            

            // 3. Receive AuthResponse
            let auth_response_frame = transport.recv().await.unwrap().unwrap();
            assert_eq!(auth_response_frame.frame_type, FrameType::AuthResponse as u8);
            let mut auth_response_payload = auth_response_frame.payload.clone();
            let auth_response = AuthResponsePayload::decode(&mut auth_response_payload).unwrap();
            tracing::debug!(
                session_id = welcome.session_id,
                "Received AuthResponse from client"
            );

            // 4. Process AuthResponse → AuthResult
            let auth_result = handshake_handler.process_auth_response(welcome.session_id, &auth_response).unwrap();
            assert_eq!(auth_result.status, 0); // success
            let auth_result_frame = Frame::new(FrameType::AuthResult as u8, auth_result.encode());
            transport.send(auth_result_frame).await.unwrap();
            tracing::debug!(
                session_id = welcome.session_id,
                "Sent AuthResult"
            );            

            // 5. Send initial CreditGrant (server grants hot credits to client)
            // Initial outgoing credits = 0, will be updated by complete_credit_exchange
            let session_ctx = Arc::new(ServerSessionContext::new(
                welcome.session_id,
                welcome.session_token,
                limits.clone(),
                Arc::new(SessionState::new(1)),
            ));            

            let mut endpoint = IPCEndpoint::from_authenticated_transport(transport, &session_ctx).await.expect("Credit synchronization error");
            tracing::debug!(
                ptr = ?(&endpoint as *const _),
                session_id = welcome.session_id,
                "Created server endpoint"
            );            

            // ASSERTIONS: Verify symmetry and correct state
            assert_eq!(endpoint.unacked_buffer().capacity(), limits.hot_credit as usize);

            // CRITICAL INVARIANT: Endpoint and Context must share the SAME Arc<CreditPool>
            assert!(Arc::ptr_eq(
                endpoint.outgoing_credit_pool(),
                session_ctx.outgoing_credit_pool()
            ));            

            // 7. Verify we can send data (credits are available)
            let msg = dummy_in_only_message(Bytes::from("test.msg"), Bytes::from("worker:1"), Bytes::from("hello"));
            let seq = endpoint.send_data(Bytes::from("worker:1"), &[msg]).await.unwrap();
            tracing::debug!(
                session_id = welcome.session_id,
                "Server sent Data"
            ); 

            // Receive Ack and Data from client to complete the test cleanly
            let mut received_ack = false;
            let mut received_data = false;

            for _ in 0..2 {
                let frame = endpoint.recv().await.unwrap().unwrap();
                match FrameType::from_u8(frame.frame_type) {
                    Some(FrameType::Acknowledge) => {
                        tracing::debug!(
                            session_id = welcome.session_id,
                            "Received Acknowledge from client"
                        ); 
                        endpoint.acknowledge(seq);
                        received_ack = true;    
                    }    
                    Some(FrameType::Data) => {
                        tracing::debug!(
                            session_id = welcome.session_id,
                            "Received Data from client"
                        ); 
                        received_data = true;    
                        endpoint.send_ack(1).await.unwrap();
                        tracing::debug!(
                            session_id = welcome.session_id,
                            "Sent Acknowledge to client"
                        ); 
                    }
                    _ => {
                        panic!("Unexpected frame type: {}", frame.frame_type);
                    }
                }
            }

            assert!(received_ack);
            assert!(received_data);
        });

        // === Client side ===
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let connector = UdsConnector::new(&socket_path);
        let stream = connector.connect().await.unwrap();
        let mut transport = IPCTransport::new(stream);
        tracing::info!("Connected to server");

        // 1. Send Hello
        transport.send(create_hello_frame()).await.unwrap();
        tracing::debug!("Sent Hello to server");        

        // 2. Receive Welcome
        let welcome_frame = transport.recv().await.unwrap().unwrap();
        let welcome = parse_welcome(&welcome_frame);
        let session_id = welcome.session_id;
        tracing::debug!(
            session_id = welcome.session_id,
            "Received Welcome from server"
        );        

        // 3. Send AuthResponse
        transport.send(create_auth_response_frame(session_id)).await.unwrap();
        tracing::debug!(
            session_id = welcome.session_id,
            "Sent AuthResponse to server"
        );        

        // 4. Receive AuthResult
        let auth_result_frame = transport.recv().await.unwrap().unwrap();
        let auth_result = parse_auth_result(&auth_result_frame);
        assert_eq!(auth_result.status, 0); // success
        tracing::debug!(
            session_id = welcome.session_id,
            "Received AuthResult from server"
        );        

        // 5. Create ClientSessionContext
        // Initial outgoing credits = limits.hot_credit (what server just granted us in Welcome limits)
        let client_ctx = Arc::new(ClientSessionContext::new(
            session_id,
            welcome.session_token,
            limits_clone.clone(),
        ));
        let mut client_endpoint = IPCEndpoint::from_authenticated_transport(transport, &client_ctx).await.expect("Credit synchronization error");
        tracing::debug!(
            ptr = ?(&client_endpoint as *const _),
            session_id = welcome.session_id,
            "Created client endpoint"
        );            

        // ASSERTIONS: Verify symmetry and correct state on client
        assert_eq!(client_endpoint.unacked_buffer().capacity(), limits_clone.hot_credit as usize);
        assert_eq!(client_ctx.outgoing_credit_pool().available(), limits_clone.hot_credit);
        
        // CRITICAL INVARIANT: Endpoint and Context must share the SAME Arc<CreditPool>
        assert!(Arc::ptr_eq(
            client_endpoint.outgoing_credit_pool(),
            client_ctx.outgoing_credit_pool()
        ));

        // 7. Verify we can send data and receive Ack
        let msg = dummy_in_only_message(Bytes::from("test.msg"), Bytes::from("worker:1"), Bytes::from("hello"));
        let seq = client_endpoint.send_data(Bytes::from("worker:1"), &[msg]).await.unwrap();
        tracing::debug!(
            session_id = welcome.session_id,
            "Sent Data to server"
        );            

        let mut received_ack = false;
        let mut received_data = false;

        for _ in 0..2 {
            let frame = client_endpoint.recv().await.unwrap().unwrap();
            tracing::debug!(
                session_id = welcome.session_id,
                frame_type = frame.frame_type,
                "Received frame from client"
            ); 
            match FrameType::from_u8(frame.frame_type) {
                Some(FrameType::Acknowledge) => {
                    tracing::debug!(
                        session_id = welcome.session_id,
                        "Received Acknowledge from client"
                    ); 
                    client_endpoint.acknowledge(seq);
                    received_ack = true;    
                }    
                Some(FrameType::Data) => {
                    tracing::debug!(
                        session_id = welcome.session_id,
                        "Received Data from client"
                    ); 
                    received_data = true;    
                    client_endpoint.send_ack(1).await.unwrap();
                    tracing::debug!(
                        session_id = welcome.session_id,
                        "Sent Acknowledge to client"
                    ); 
                }
                _ => {
                    panic!("Unexpected frame type: {}", frame.frame_type);
                }
            }
        }

        assert!(received_ack);
        assert!(received_data);

        server_handle.await.unwrap();
    }
}
