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
    use bytes::{Buf, Bytes};
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::net::UnixStream;
    use tokio::time::{sleep, Duration};

    use crate::broker_core::{
        l2::{SessionState, UnackedBuffer},
        registry::LocalRegistry,
        security::{HandshakeHandler, SessionManager},
        transport::{IPCConnector, IPCTransport, IPCEndpoint, UdsConnector},
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
        Frame, FrameType, ServerLimits,
    };

    use crate::test_utils::{init_tracing, dummy_in_only_message};
    
    use super::super::{config::ServerConfig, BrokerServer};

    // =========================================================================
    // Helpers
    // =========================================================================

    /// Creates a `ServerConfig` with short timeouts to speed up tests.
    fn fast_test_config(socket_path: std::path::PathBuf) -> ServerConfig {
        ServerConfig {
            socket_path,
            limits: ServerLimits {
                hot_credit: 5, // Малый лимит для теста backpressure
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
    ) -> (Arc<BrokerServer>, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let session_manager = Arc::new(SessionManager::new());
        let handshake_handler = Arc::new(HandshakeHandler::with_limits(config.limits.clone()));
        let local_registry = Arc::new(LocalRegistry::new());

        let server = Arc::new(BrokerServer::new(
            config,
            session_manager,
            handshake_handler,
            local_registry,
        ));

        let server_clone = Arc::clone(&server);
        let handle = tokio::spawn(async move { server_clone.run().await });

        // Give the server time to bind the socket
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
        tracing::debug!("Sended Hello to server");        

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
            "Sended AuthResponse to server"
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
        let l2_state = Arc::new(SessionState::new(1));
        let unacked_buffer = Arc::new(UnackedBuffer::new(welcome.limits.hot_credit as usize));
        
        let mut endpoint = IPCEndpoint::with_transport_and_session(transport, l2_state, unacked_buffer);

        // 5. Send initial CreditGrant (client → server)
        endpoint
            .send_credit_grant(
                Bytes::new(),
                CreditGrantPayload::CREDIT_TYPE_HOT,
                welcome.limits.hot_credit,
            )
            .await?;
        tracing::debug!(
            session_id = welcome.session_id,
            credit_amount = welcome.limits.hot_credit,
            "Sended CreditGrant to server"
        );        

        // 6. Receive initial CreditGrant (server → client)
        let cg_frame = endpoint
            .recv()
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection closed before server CreditGrant"))?;
        assert_eq!(cg_frame.frame_type, FrameType::CreditGrant as u8);
        tracing::debug!(
            session_id = welcome.session_id,
            "Received CreditGrant from server"
        );        

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
        init_tracing("trace");

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
        init_tracing("trace");

        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock.5");
        
        // Set an extremely low hot_credit to quickly test backpressure
        let config = ServerConfig {
            socket_path: socket_path.clone(),
            limits: ServerLimits {
                hot_credit: 2, // Лимит всего 2 кадра
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
}
