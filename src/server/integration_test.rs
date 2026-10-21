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
    use bytes::Bytes;
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
            AuthResponsePayload, AuthResultPayload, CreditGrantPayload, HelloPayload,
            WelcomePayload, L2_PROTOCOL_VERSION,
        },
        Frame, FrameType, ServerLimits,
    };

    use crate::test_utils::init_tracing;
    
    use super::super::{config::ServerConfig, BrokerServer};

    // =========================================================================
    // Helpers
    // =========================================================================

    /// Создаёт `ServerConfig` с короткими таймаутами для ускорения тестов.
    fn fast_test_config(socket_path: std::path::PathBuf) -> ServerConfig {
        ServerConfig {
            socket_path,
            limits: ServerLimits {
                hot_credit: 5, // Малый лимит для теста backpressure
                cold_credit: 2,
                session_ttl_ms: 60_000,
                ..ServerLimits::default()
            },
            session_timeout_ms: 2000, // 200ms для теста таймаута
            gc_interval_ms: 50,      // GC каждые 50ms
        }
    }

    /// Запускает `BrokerServer` в фоновой tokio-задаче.
    ///
    /// Возвращает `Arc<BrokerServer>` (для вызова `shutdown()` и доступа к
    /// `session_manager`) и `JoinHandle` задачи сервера.
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

        // Даём серверу время забиндить сокет
        sleep(Duration::from_millis(50)).await;

        (server, handle)
    }

    /// Выполняет **клиентскую сторону** полного рукопожатия:
    /// `Hello → Welcome → AuthResponse → AuthResult → CreditGrant (client) → CreditGrant (server)`.
    ///
    /// Возвращает `IPCEndpoint` и полученный `WelcomePayload` (с `session_id` и `limits`).
    async fn client_handshake(
        socket_path: &Path,
    ) -> anyhow::Result<(IPCEndpoint<UnixStream>, WelcomePayload)> {
        let connector = UdsConnector::new(socket_path);
        let stream = connector.connect().await?;

        // Фаза 1: используем голый транспорт для рукопожатия
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

        // Фаза 2: создаём endpoint ПОСЛЕ успешной аутентификации
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

    /// **Test 1:** Полное рукопожатие + регистрация сессии.
    ///
    /// Проверяет, что:
    /// - После handshake `active_sessions_count() == 1` и `get_session(id)` возвращает `Some`.
    /// - После закрытия соединения клиентом сессия удаляется из `SessionManager`.
    #[tokio::test]
    async fn test_full_handshake_session_registered() {
        init_tracing("trace");

        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock");
        let config = fast_test_config(socket_path.clone());

        let (server, server_handle) = spawn_test_server(config).await;
        let session_manager = server.session_manager();

        // Изначально нет активных сессий
        assert_eq!(session_manager.active_sessions_count(), 0);

        // Клиент подключается и проходит handshake
        let (endpoint, welcome) = client_handshake(&socket_path).await.unwrap();
        tracing::info!(
            session_id = welcome.session_id,
            "Client handshake completed"
        );          

        let session_id = welcome.session_id;

        // Даём серверу время зарегистрировать сессию в main loop
        sleep(Duration::from_millis(50)).await;

        // Сессия должна быть зарегистрирована
        assert_eq!(
            session_manager.active_sessions_count(),
            1,
            "Session must be registered after handshake"
        );
        assert!(
            session_manager.get_session(session_id).is_some(),
            "Session must be retrievable by id"
        );

        // Клиент корректно закрывает соединение
        drop(endpoint);
        tracing::info!(
            session_id = welcome.session_id,
            "Droped endpoint"
        );         

        // Даём серверу время обнаружить разрыв и удалить сессию
        sleep(Duration::from_millis(150)).await;

        // Сессия должна быть удалена
        assert_eq!(
            session_manager.active_sessions_count(),
            0,
            "Session must be removed after client disconnect"
        );

        server.shutdown();
        let _ = server_handle.await;
    }

}