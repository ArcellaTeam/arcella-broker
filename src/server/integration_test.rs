// arcella-broker/src/server/integration_test.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::net::UnixStream;
    use tokio::time::{sleep, Duration};
    use tokio::sync::Mutex;

    use crate::broker_core::{
        l2::{SessionState, UnackedBuffer, IngressBridge, IngressResult},
        registry::LocalRegistry,
        security::{HandshakeHandler, SessionManager, ClientSessionContext, ServerSessionContext},
        transport::{IPCConnector, IPCTransport, IPCEndpoint, UdsConnector, TransportError},
    };

    use crate::protocol::{
        payloads::{
            HelloPayload,
            WelcomePayload,
            AuthResponsePayload,
            AuthResultPayload,
            CreditGrantPayload,
            DataPayload,
            L2_PROTOCOL_VERSION,
        },
        Frame, 
        FrameType, 
        ServerLimits,
    };

    use crate::test_utils::{StubIngressBridge, init_tracing, dummy_in_only_message};
    
    use super::super::{config::ServerConfig, BrokerServer};

    fn create_data_frame(sequence: u64, address: Bytes, msg_payload: Bytes) -> Frame {

        let msg = dummy_in_only_message(
            Bytes::from("test.msg"),
            address.clone(),
            msg_payload,
        );
        
        let messages = [msg.clone()];
        let data_payload = DataPayload::new(sequence, address, &messages);
        Frame::data(data_payload.encode().unwrap())
    }       

    /// Creates a `ServerConfig` with short timeouts to speed up tests.
    fn fast_test_config(socket_path: std::path::PathBuf) -> ServerConfig {
        ServerConfig {
            socket_path,
            limits: ServerLimits {
                hot_credit: 5, // Hot limit for testing backpressure
                cold_credit: 2,
                session_ttl_ms: 60_000,
                ..ServerLimits::default()
            },
            session_timeout_ms: 2000, // 2000ms for the timeout test
            gc_interval_ms: 50,      // GC every 50ms
        }
    }

    /// Starts `BrokerServer` in a background tokio task.
    ///
    /// Returns `Arc<BrokerServer> (for calling `shutdown()` and accessing
    /// `session_manager`) and the server task's `JoinHandle`.
    async fn spawn_test_server(
        config: ServerConfig,
    ) -> (Arc<BrokerServer<StubIngressBridge>>, tokio::task::JoinHandle<anyhow::Result<()>>) {

        let ingress_bridge = Arc::new(StubIngressBridge::new());
        let (server, handle) = spawn_test_server_with_bridge(config, ingress_bridge).await;

        (server, handle)
    }

    /// Starts `BrokerServer` with a custom IngressBridge in a background tokio task.
    async fn spawn_test_server_with_bridge<B>(
        config: ServerConfig,
        ingress_bridge: Arc<B>,
    ) -> (Arc<BrokerServer<B>>, tokio::task::JoinHandle<anyhow::Result<()>>)
    where
        B: IngressBridge + 'static,
    {
        let session_manager = Arc::new(SessionManager::new());
        let handshake_handler = Arc::new(HandshakeHandler::with_limits(config.limits.clone()));
        let local_registry = Arc::new(LocalRegistry::new());


        let server = Arc::new(BrokerServer::new_with_bridge(
            config,
            session_manager,
            handshake_handler,
            local_registry,
            ingress_bridge,
        ));

        let server_clone = Arc::clone(&server);
        let handle = tokio::spawn(async move { server_clone.run().await });

        sleep(Duration::from_millis(50)).await;

        (server, handle)
    }    

    /// Performs the client side of the full handshake:
    /// `Hello → Welcome → AuthResponse → AuthResult → CreditGrant (client) → CreditGrant (server)`.
    ///
    /// Returns `IPCEndpoint` and the received `WelcomePayload` (with `session_id` and `limits`).
    async fn client_handshake(
        socket_path: &Path,
    ) -> anyhow::Result<(IPCEndpoint<UnixStream>, WelcomePayload)> {
        let connector = UdsConnector::new(socket_path);
        let stream = connector.connect().await?;

        // Phase 1: use the bare transport for the handshake
        let mut transport = IPCTransport::new(stream);
        tracing::info!("Connected to server");

        // 1. Send Hello
        let hello = HelloPayload {
            protocol_version: L2_PROTOCOL_VERSION,
            client_id: Bytes::from("test-client"),
            algorithm: 0x00,
            auth_data: Bytes::new(),
            capabilities: 0,
        };
        transport.send(Frame::new(FrameType::Hello as u8, hello.encode())).await?;
        tracing::debug!("Sent Hello to server");        

        // 2. Receive Welcome
        let welcome_frame = transport.recv().await?
            .ok_or_else(|| anyhow::anyhow!("Connection closed before Welcome"))?;
        assert_eq!(welcome_frame.frame_type, FrameType::Welcome as u8);
        let welcome = WelcomePayload::decode(&mut welcome_frame.payload.clone())?;
        tracing::debug!(
            session_id = welcome.session_id,
            "Received Welcome from server"
        );        

        // 3. Send AuthResponse
        let auth_response = AuthResponsePayload {
            session_id: welcome.session_id,
            client_nonce: [0xAA; 32],
            response: Bytes::new(), // algorithm None
        };
        transport.send(Frame::new(FrameType::AuthResponse as u8, auth_response.encode())).await?;
        tracing::debug!(
            session_id = welcome.session_id,
            "Sent AuthResponse to server"
        );        

        // 4. Receive AuthResult
        let auth_result_frame = transport.recv().await?
            .ok_or_else(|| anyhow::anyhow!("Connection closed before AuthResult"))?;
        assert_eq!(auth_result_frame.frame_type, FrameType::AuthResult as u8);
        let auth_result = AuthResultPayload::decode(&mut auth_result_frame.payload.clone())?;
        assert_eq!(auth_result.status, 0, "Authentication must succeed");
        tracing::debug!(
            session_id = welcome.session_id,
            "Received AuthResult from server"
        );        

        // Phase 2: create the endpoint AFTER successful authentication
        let client_ctx = Arc::new(ClientSessionContext::new(
            welcome.session_id,
            welcome.session_token,
            welcome.limits.clone(),
        ));


        let endpoint = IPCEndpoint::from_authenticated_transport(
            transport,
            &client_ctx,
        ).await.expect("Credit synchronization error");

        Ok((endpoint, welcome))
    }

    // =========================================================================
    // Tests
    // =========================================================================

    /// **Test 1:** Full handshake + session registration.
    ///
    /// Verifies that:
    /// - After the handshake `active_sessions_count() == 1`and `get_session(id)` returns `Some`.
    /// - After the client closes the connection, the session is removed from `SessionManager`.
    #[tokio::test]
    async fn test_full_handshake_session_registered() {
        init_tracing("info");

        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock.1");
        let config = fast_test_config(socket_path.clone());

        let (server, server_handle) = spawn_test_server(config).await;
        let session_manager = server.session_manager();

        // Initially there are no active sessions
        assert_eq!(session_manager.active_sessions_count(), 0);

        // The client connects and completes the handshake
        let (endpoint, welcome) = client_handshake(&socket_path).await.unwrap();
        tracing::info!(
            session_id = welcome.session_id,
            "Client handshake completed"
        );          

        let session_id = welcome.session_id;

        // Give the server time to register the session in the main loop
        sleep(Duration::from_millis(50)).await;

        // The session must be registered
        assert_eq!(
            session_manager.active_sessions_count(),
            1,
            "Session must be registered after handshake"
        );
        assert!(
            session_manager.get_session(session_id).is_some(),
            "Session must be retrievable by id"
        );

        // The client closes the connection gracefully
        drop(endpoint);
        tracing::info!(
            session_id = welcome.session_id,
            "Droped endpoint"
        );         

        // Waiting for session removal with polling
        for _ in 0..10 {
            sleep(Duration::from_millis(50)).await;
            if server.session_manager().active_sessions_count() == 0 {
                break;
            }
        }

        // The session must be removed
        assert_eq!(
            session_manager.active_sessions_count(),
            0,
            "Session must be removed after client disconnect"
        );

        server.shutdown();
        let _ = server_handle.await;
    }

    /// **Test 2:** Data before handshake → Error → disconnect
    #[tokio::test]
    async fn test_data_before_handshake_rejected() {
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock.2");
        let config = fast_test_config(socket_path.clone());

        let (server, server_handle) = spawn_test_server(config).await;
        let connector = UdsConnector::new(&socket_path);
        let stream = connector.connect().await.unwrap();
        let mut transport = IPCTransport::new(stream);

        // Send Data WITHOUT Hello
        let msg = dummy_in_only_message(
            Bytes::from("test"),
            Bytes::from("arcella:unknown:address"),
            Bytes::from("")
        );
        let messages = &[msg];
        let payload = DataPayload::new(1, Bytes::from("test"), messages);
        transport.send(Frame::new(FrameType::Data as u8, payload.encode().unwrap())).await.unwrap();

        // Expect an Error frame
        let err_frame = transport.recv().await.unwrap().unwrap();
        assert_eq!(err_frame.frame_type, FrameType::Error as u8);

        // Check the contents of the Error payload
        let mut err_payload = err_frame.payload.clone();
        let error_code = err_payload.get_u16_le();
        let msg_len = err_payload.get_u16_le() as usize;
        let err_split = err_payload.split_to(msg_len);
        let error_msg = String::from_utf8_lossy(&err_split);

        assert_eq!(error_code, 400); // Bad Protocol
        assert!(error_msg.contains("Hello") || error_msg.contains("handshake"));                

        // The connection must be closed
        assert!(transport.recv().await.unwrap().is_none());

        server.shutdown();
        let _ = server_handle.await;
    }    

    /// **Test 3:** Session timeout → forced disconnect
    #[tokio::test]
    async fn test_session_timeout_force_close() {
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock.3");
        let config = fast_test_config(socket_path.clone());

        let (server, server_handle) = spawn_test_server(config).await;
        let (mut endpoint, welcome) = client_handshake(&socket_path).await.unwrap();
        let session_id = welcome.session_id;

        // The session is active
        assert_eq!(server.session_manager().active_sessions_count(), 1);

        // Wait for the session timeout to definitely expire
        // session_timeout_ms = 2000, add a 500ms margin
        sleep(Duration::from_millis(2500)).await;

        // Waiting for session removal with polling
        for _ in 0..10 {
            sleep(Duration::from_millis(50)).await;
            if server.session_manager().active_sessions_count() == 0 {
                break;
            }
        }

        // GC should have removed the session
        assert_eq!(server.session_manager().active_sessions_count(), 0);
        assert!(server.session_manager().get_session(session_id).is_none());

        // The connection must be closed by the server
        assert!(endpoint.recv().await.unwrap().is_none());

        server.shutdown();
        let _ = server_handle.await;
    }

    /// **Test 4:** Reconnect → new session_id
    #[tokio::test]
    async fn test_reconnect_creates_new_session() {
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock.4");
        let config = fast_test_config(socket_path.clone());

        let (server, server_handle) = spawn_test_server(config).await;

        // First connection
        let (endpoint1, welcome1) = client_handshake(&socket_path).await.unwrap();
        let session_id_1 = welcome1.session_id;

        // Explicitly close the first connection
        drop(endpoint1);

        sleep(Duration::from_millis(150)).await;

        // Second connection
        let (_, welcome2) = client_handshake(&socket_path).await.unwrap();
        let session_id_2 = welcome2.session_id;

        // session_ids must be different and monotonically increasing
        assert_ne!(session_id_1, session_id_2);
        assert!(session_id_2 > session_id_1);

        server.shutdown();
        let _ = server_handle.await;
    }

    /// **Test 5:** Bidirectional backpressure stress test (Backpressure O(1) guarantee).
    ///
    /// Verifies that:
    /// - When attempting to send more frames than hot_credit allows,
    /// the client receives a TransportError::UnackedBuffer error.
    /// - The error occurs BEFORE network I/O (natural backpressure).
    /// - The buffer size is strictly bounded by the limit (O(1) memory consumption guarantee).
    #[tokio::test]
    async fn test_backpressure_o1_guarantee_client_server() {
        init_tracing("info");

        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock.5");
        
        // Set an extremely low hot_credit to quickly test backpressure
        let config = ServerConfig {
            socket_path: socket_path.clone(),
            limits: ServerLimits {
                hot_credit: 2, // Hot limit for testing backpressure
                cold_credit: 1,
                session_ttl_ms: 60_000,
                ..ServerLimits::default()
            },
            session_timeout_ms: 5000,
            gc_interval_ms: 1000,
        };

        let (server, server_handle) = spawn_test_server(config).await;

        // 1. The client connects and completes the handshake
        let (mut endpoint, welcome) = client_handshake(&socket_path).await.unwrap();
        
        // Verify that the client received the correct limit from the server
        assert_eq!(welcome.limits.hot_credit, 2);
        assert_eq!(endpoint.unacked_buffer().capacity(), 2);

        let address = Bytes::from("test:backpressure");
        let msg = crate::protocol::test_utils::dummy_in_only_message(
            Bytes::from("test.type"),
            address.clone(),
            Bytes::from("payload"),
        );

        // 2. Send the 1st frame (should succeed, buffer len = 1)
        let seq1 = endpoint.send_data(address.clone(), &[msg.clone()]).await.unwrap();
        assert_eq!(endpoint.in_flight_count(), 1);

        // 3. Send the 2nd frame (should succeed, buffer len = 2, limit reached)
        let seq2 = endpoint.send_data(address.clone(), &[msg.clone()]).await.unwrap();
        assert_eq!(endpoint.in_flight_count(), 2);

        // 4. Send the 3rd frame (must FAIL with a backpressure error)
        // The server intentionally does NOT send an Ack, so the buffer is not freed.
        let result = endpoint.send_data(address.clone(), &[msg.clone()]).await;
        
        // Verify that the returned error is specifically a buffer overflow error
        assert!(matches!(
            result,
            Err(crate::broker_core::transport::TransportError::UnackedBuffer(
                crate::broker_core::l2::UnackedBufferError::SlotOccupied { .. }
            ))
        ), "Expected UnackedBuffer::SlotOccupied error, got: {:?}", result);

        // 5. CRITICAL CHECK: O(1) guarantee.
        // Despite 3 send attempts, the buffer must contain exactly 2 elements.
        // No additional allocations beyond the limit occurred
        assert_eq!(
            endpoint.in_flight_count(), 
            2, 
            "UnackedBuffer size must strictly equal hot_credit limit (O(1) guarantee violated)"
        );

        // 6. Cleanup: free the client's local buffer (do NOT send an Ack to the server).
        // This is needed so the endpoint drops correctly and there are no memory leaks.
        endpoint.acknowledge(seq1);
        endpoint.acknowledge(seq2);
        
        server.shutdown();
        let _ = server_handle.await;
    }

    /// **Test 6:** Server-side credit enforcement.
    ///
    /// Verifies that:
    /// - Server sends initial CreditGrant with hot_credit = 2 after handshake.
    /// - Client sends 2 Data frames (consuming all credits).
    /// - Server processes both and sends Ack (replenishing credits).
    /// - Client sends 3rd frame (credits replenished, so it succeeds).
    /// - If client tries to send without waiting for Ack (violating protocol),
    ///   server sends Drop and closes connection.
    #[tokio::test]
    async fn test_server_credit_enforcement() {
        init_tracing("info");

        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock.6");
        
        let config = ServerConfig {
            socket_path: socket_path.clone(),
            limits: ServerLimits {
                hot_credit: 2,  // Very low limit for testing
                cold_credit: 1,
                session_ttl_ms: 60_000,
                ..ServerLimits::default()
            },
            session_timeout_ms: 5000,
            gc_interval_ms: 1000,
        };

        let (server, server_handle) = spawn_test_server(config).await;
        let (mut endpoint, welcome) = client_handshake(&socket_path).await.unwrap();
        
        assert_eq!(welcome.limits.hot_credit, 2);
        assert_eq!(endpoint.unacked_buffer().capacity(), 2);

        let address = Bytes::from("test:credit");
        let msg = dummy_in_only_message(
            Bytes::from("test.type"),
            address.clone(),
            Bytes::from("payload"),
        );

        // === Phase 1: Send 2 frames (within credit limit) ===
        let seq1 = endpoint.send_data(address.clone(), &[msg.clone()]).await.unwrap();
        let seq2 = endpoint.send_data(address.clone(), &[msg.clone()]).await.unwrap();
        
        // Client buffer is now full (2 frames in-flight)
        assert_eq!(endpoint.in_flight_count(), 2);

        // === Phase 2: Server processes and sends Ack ===
        // Server should process both frames and send Ack for each
        // This replenishes the credits on the server side
        
        // Receive Ack for seq1
        let ack1_frame = endpoint.recv().await.unwrap().unwrap();
        assert_eq!(ack1_frame.frame_type, FrameType::Acknowledge as u8);
        let mut ack1_payload = ack1_frame.payload.clone();
        let ack1_seq = ack1_payload.get_u64_le();
        assert_eq!(ack1_seq, seq1);
        
        // Client acknowledges the Ack (frees local buffer slot)
        endpoint.acknowledge(seq1);
        assert_eq!(endpoint.in_flight_count(), 1);

        // Receive Ack for seq2
        let ack2_frame = endpoint.recv().await.unwrap().unwrap();
        assert_eq!(ack2_frame.frame_type, FrameType::Acknowledge as u8);
        let mut ack2_payload = ack2_frame.payload.clone();
        let ack2_seq = ack2_payload.get_u64_le();
        assert_eq!(ack2_seq, seq2);
        
        endpoint.acknowledge(seq2);
        assert_eq!(endpoint.in_flight_count(), 0);

        // === Phase 3: Send 3rd frame (credits replenished, should succeed) ===
        let seq3 = endpoint.send_data(address.clone(), &[msg.clone()]).await.unwrap();
        assert_eq!(endpoint.in_flight_count(), 1);

        // Receive Ack for seq3
        let ack3_frame = endpoint.recv().await.unwrap().unwrap();
        assert_eq!(ack3_frame.frame_type, FrameType::Acknowledge as u8);
        endpoint.acknowledge(seq3);
        assert_eq!(endpoint.in_flight_count(), 0);

        tracing::info!("Server credit enforcement test passed");

        server.shutdown();
        let _ = server_handle.await;
    }

    /// **Test 7:** Server detects credit protocol violation.
    ///
    /// Verifies that:
    /// - Client sends more frames than hot_credit allows (by ignoring backpressure).
    /// - Server detects the violation and sends Drop frame.
    /// - Server closes the connection.
    ///
    /// Note: This test requires a "malicious" client that bypasses the local
    /// UnackedBuffer backpressure check. We simulate this by using raw transport.
    #[tokio::test]
    async fn test_server_detects_credit_violation() {
        init_tracing("info");

        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock.7");
        
        let config = ServerConfig {
            socket_path: socket_path.clone(),
            limits: ServerLimits {
                hot_credit: 2,
                cold_credit: 1,
                session_ttl_ms: 60_000,
                ..ServerLimits::default()
            },
            session_timeout_ms: 5000,
            gc_interval_ms: 1000,
        };

        let ingress_bridge = Arc::new(StubIngressBridge::new()
            .with_blocked_after(2)
        );

        let (server, server_handle) = spawn_test_server_with_bridge(config, ingress_bridge).await;

        // Perform handshake using raw transport (to bypass client-side backpressure)
        let connector = UdsConnector::new(&socket_path);
        let stream = connector.connect().await.unwrap();
        let mut transport = IPCTransport::new(stream);

        // === Handshake ===
        let hello = HelloPayload {
            protocol_version: L2_PROTOCOL_VERSION,
            client_id: Bytes::from("malicious-client"),
            algorithm: 0x00,
            auth_data: Bytes::new(),
            capabilities: 0,
        };
        transport.send(Frame::new(FrameType::Hello as u8, hello.encode())).await.unwrap();

        let welcome_frame = transport.recv().await.unwrap().unwrap();
        let welcome = WelcomePayload::decode(&mut welcome_frame.payload.clone()).unwrap();

        let auth_response = AuthResponsePayload {
            session_id: welcome.session_id,
            client_nonce: [0xCC; 32],
            response: Bytes::new(),
        };
        transport.send(Frame::new(FrameType::AuthResponse as u8, auth_response.encode())).await.unwrap();

        let auth_result_frame = transport.recv().await.unwrap().unwrap();
        let auth_result = AuthResultPayload::decode(&mut auth_result_frame.payload.clone()).unwrap();
        assert_eq!(auth_result.status, 0);

        // Send CreditGrant (required by protocol to initialize server-side credit tracking)
        let cg = CreditGrantPayload::hot(1, Bytes::new(), welcome.limits.hot_credit);
        let cg_encoded = cg.encode().unwrap();
        transport.send(Frame::new(FrameType::CreditGrant as u8, cg_encoded)).await.unwrap();

        // Receive server's CreditGrant
        let server_cg_frame = transport.recv().await.unwrap().unwrap();
        assert_eq!(server_cg_frame.frame_type, FrameType::CreditGrant as u8);

        // === Malicious behavior: send 3 Data frames (limit is 2) ===
        let address = Bytes::from("test:violation");
        let msg = dummy_in_only_message(
            Bytes::from("test.type"),
            address.clone(),
            Bytes::from("payload"),
        );

        // Send all 5 frames in quick succession
        for seq in 2..=6 {
            let messages = [msg.clone()];
            let payload = DataPayload::new(seq, address.clone(), &messages);
            transport.send(Frame::new(FrameType::Data as u8, payload.encode().unwrap())).await.unwrap();
            tracing::debug!("Sended Data (seq: {})", seq);
        }

        // === Expect server to process first two and Drop the third ===
        // We must drain Acks until we find the Drop frame, as they may arrive first.
        let mut received_drop = false;
        let mut ack_count = 0;

        while let Some(frame) = transport.recv().await.unwrap() {
            match frame.frame_type {
                t if t == FrameType::Acknowledge as u8 => {
                    ack_count += 1;
                    tracing::debug!("Received expected Acknowledge (total: {})", ack_count);
                }
                t if t == FrameType::Drop as u8 => {
                    received_drop = true;
                    tracing::debug!("Received expected Drop");

                    // Parse Drop payload: session_sequence (u64) + reason (u8)
                    let mut drop_payload = frame.payload.clone();
                    let drop_seq = drop_payload.get_u64_le();
                    let drop_reason = drop_payload.get_u8();

                    assert_eq!(drop_seq, 6, "Dropped sequence should be 6 (the violation frame)");
                    assert_eq!(drop_reason, 1, "Reason should be 1 (buffer_overflow)");
                    break; // Found what we were looking for
                }
                _ => {
                    panic!("Unexpected frame type: 0x{:02x}", frame.frame_type);
                }    
            }

        }

        // === 4. Assertions ===
        assert!(received_drop, "Server failed to send Drop frame after credit exhaustion");
        assert_eq!(ack_count, 2, "Server should have sent exactly 2 Acks before the Drop");

        // Verify connection is closed by server immediately after sending Drop
        let next_frame = transport.recv().await.unwrap();
        assert!(next_frame.is_none(), "Server must close connection immediately after sending Drop");

        tracing::info!("Server credit violation detection test passed");

        server.shutdown();
        let _ = server_handle.await;
    }        

    /// **Test 8:** End-to-End Stress Test & Recovery
    ///
    /// Verifies deterministic backpressure and recovery without memory leaks:
    /// 1. Server is configured with extremely low limits (hot_credit = 5).
    /// 2. Server intentionally delays processing (simulating slow L3 / blocked queue).
    /// 3. Client aggressively tries to send 10,000 frames.
    /// 4. Phase 1 (Backpressure): Client hits O(1) limit, stops allocating, gets backpressure errors.
    /// 5. Phase 2 (Recovery): Server "unfreezes", starts processing and sending Acks.
    /// 6. Phase 3 (Resume): Client receives Acks, frees slots, completes all 10,000 sends.
    /// 7. Final check: No frames left in-flight (all session_sequences acknowledged).
    #[tokio::test]
    async fn test_backpressure_stress_and_recovery() {
        init_tracing("info");

        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock.8");
        
        let config = ServerConfig {
            socket_path: socket_path.clone(),
            limits: ServerLimits {
                hot_credit: 5,
                cold_credit: 2,
                session_ttl_ms: 60_000,
                ..ServerLimits::default()
            },
            session_timeout_ms: 30_000,
            gc_interval_ms: 1000,
        };

        // Phase 1 setup: Server processes very slowly (500ms per frame) -> no Acks sent quickly
        let ingress_bridge = Arc::new(StubIngressBridge::new().with_delay(500));
        let bridge_clone = Arc::clone(&ingress_bridge);
        
        let (server, server_handle) = spawn_test_server_with_bridge(config, ingress_bridge).await;
        let (endpoint, welcome) = client_handshake(&socket_path).await.unwrap();
        
        assert_eq!(welcome.limits.hot_credit, 5);
        assert_eq!(endpoint.unacked_buffer().capacity(), 5);

        // Wrap endpoint in Arc<Mutex> to share between sender and receiver tasks
        let endpoint = Arc::new(Mutex::new(endpoint));
        let address = Bytes::from("test:stress");
        let msg = dummy_in_only_message(
            Bytes::from("test.type"),
            address.clone(),
            Bytes::from("payload"),
        );

        let total_messages: u64 = 10_000;
        let sent_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ack_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        
        let sent_count_clone = Arc::clone(&sent_count);
        let ack_count_clone = Arc::clone(&ack_count);
        let endpoint_sender = Arc::clone(&endpoint);
        let msg_clone = msg.clone();
        let address_clone = address.clone();

        // Sender task: aggressively tries to send 10,000 frames
        let sender_task = tokio::spawn(async move {
            let mut current_seq = 0;
            while current_seq < total_messages {
                let mut ep = endpoint_sender.lock().await;
                match ep.send_data(address_clone.clone(), &[msg_clone.clone()]).await {
                    Ok(_seq) => {
                        current_seq += 1;
                        sent_count_clone.store(current_seq, std::sync::atomic::Ordering::SeqCst);
                    }
                    Err(TransportError::UnackedBuffer(_)) 
                    | Err(TransportError::Protocol(_)) => {
                        // Backpressure: buffer full or no credits. Release lock and wait.
                        drop(ep);
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(e) => {
                        panic!("Unexpected error during send: {:?}", e);
                    }
                }
            }
        });

        let endpoint_recv = Arc::clone(&endpoint);
        
        // Receiver task: reads Acks and frees slots in UnackedBuffer
        let receiver_task = tokio::spawn(async move {
            loop {
                let mut ep = endpoint_recv.lock().await;
                let recv_result = tokio::time::timeout(
                    Duration::from_millis(100),
                    ep.recv()
                ).await;
                
                match recv_result {
                    Ok(Ok(Some(frame))) => {
                        if frame.frame_type == FrameType::Acknowledge as u8 {
                            let mut payload = frame.payload.clone();
                            let seq = payload.get_u64_le();
                            ep.acknowledge(seq);
                            let current_acks = ack_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                            if current_acks == total_messages {
                                break;
                            }
                        } else if frame.frame_type == FrameType::Ping as u8 {
                            // Handle server keep-alive
                            let mut pong_payload = BytesMut::with_capacity(frame.payload.len());
                            pong_payload.put_slice(&frame.payload);
                            let pong_frame = Frame::new(FrameType::Pong as u8, pong_payload.freeze());
                            let _ = ep.send_raw_frame(pong_frame).await;
                        }
                    }
                    Ok(Ok(None)) => break, // Connection closed
                    Ok(Err(_)) => break,    // Transport error
                    Err(_) => continue,     // Timeout
                }
            }
        });

        // Give client time to hit the backpressure limit
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        let sent_so_far = sent_count.load(std::sync::atomic::Ordering::SeqCst);
        // Client should be blocked by hot_credit limit (5), server is too slow to send Acks
        assert!(sent_so_far <= 5, "Client should be blocked by hot_credit limit (5), but sent {}", sent_so_far);
        
        let in_flight = endpoint.lock().await.in_flight_count();
        assert_eq!(in_flight, sent_so_far as usize, "All sent frames should be in-flight");

        // Phase 2: Recovery - server "unfreezes"
        bridge_clone.switch_to_accept();

        // Phase 3: Wait for completion
        let _ = tokio::join!(sender_task, receiver_task);

        let final_sent = sent_count.load(std::sync::atomic::Ordering::SeqCst);
        let final_acked = ack_count.load(std::sync::atomic::Ordering::SeqCst);

        assert_eq!(final_sent, total_messages, "All 10,000 frames must be sent");
        assert_eq!(final_acked, total_messages, "All 10,000 frames must be acknowledged");
        assert_eq!(endpoint.lock().await.in_flight_count(), 0, "No frames should be left in-flight (O(1) guarantee)");

        server.shutdown();
        let _ = server_handle.await;
    }
}
