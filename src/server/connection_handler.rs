// arcella-broker/src/server/connection_handler.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;
use tokio::net::UnixStream;
use bytes::{Bytes, BytesMut, BufMut};

use crate::broker_core::l2::{SessionState, UnackedBuffer};
use crate::broker_core::security::{HandshakeError, HandshakeHandler, SessionContext, SessionManager};
use crate::broker_core::registry::LocalRegistry;
use crate::broker_core::transport::{IPCEndpoint, IPCTransport, TransportError};
use crate::protocol::{
    Frame, FrameType,
    payloads::{
        HelloPayload,
        AuthResponsePayload,
        CreditGrantPayload,
    },
};

/// Handles a single IPC connection from accept to disconnect.
pub async fn handle_connection(
    stream: UnixStream,
    session_manager: Arc<SessionManager>,
    handshake_handler: Arc<HandshakeHandler>,
    _local_registry: Arc<LocalRegistry>, // Will be needed in Stage 6 for the Ingress Bridge
) -> Result<(), TransportError> {

    let peer_addr = stream.peer_addr().ok()
        .and_then(|a| a.as_pathname().map(|p| p.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "unix:unnamed".to_string());

    tracing::info!(peer = %peer_addr, "New IPC connection accepted");

    // === Phase 1: Handshake ===
    let (mut endpoint, session_ctx) = match perform_handshake(stream, &handshake_handler, &session_manager).await {
        Ok(ctx) => ctx,
        Err(e) => {
            tracing::warn!(peer = %peer_addr, error = %e, "Handshake failed, closing connection");
            return Err(e);
        }
    }; 

    tracing::info!(
        session_id = session_ctx.session_id,
        peer = %peer_addr,
        "Handshake completed, session registered"
    );

    // === Phase 2: Main frame processing loop ===
    let result = main_loop(&mut endpoint, &session_ctx).await; 

    // === Phase 3: Mandatory cleanup ===
    // Remove the session from the manager on ANY exit from the function
    // (connection drop, timeout, transport error)
    session_manager.remove_session(session_ctx.session_id);

    tracing::info!(
        session_id = session_ctx.session_id,
        peer = %peer_addr,
        "Connection closed, session removed"
    );             

    Ok(())
}

/// Performs the handshake protocol: Hello → Welcome → AuthResponse → AuthResult.
///
/// Returns `Arc<SessionContext>` on successful authentication.
/// On any error, the connection must be closed by the caller.
async fn perform_handshake(
    stream: UnixStream,
    handshake_handler: &Arc<HandshakeHandler>,
    session_manager: &Arc<SessionManager>,
) -> Result<(IPCEndpoint<UnixStream>, Arc<SessionContext>), TransportError> {
    // 1. Create the transport for exchanging control frames.
    let mut transport = IPCTransport::new(stream);

    // 2. Receive Hello from the client (with Ping support for keep-alive)
    let hello = loop {
        let frame = transport.recv().await?
            .ok_or_else(|| TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "Connection closed during Hello wait",
            )))?;

        match frame.frame_type {
            t if t == FrameType::Hello as u8 => {
                let mut payload = frame.payload.clone();
                break HelloPayload::decode(&mut payload)?;
            }
            t if t == FrameType::Ping as u8 => {
                let mut pong_payload = BytesMut::with_capacity(frame.payload.len());
                pong_payload.put_slice(&frame.payload); // Copy the timestamp and opaque_data
                let pong_frame = Frame::new(FrameType::Pong as u8, pong_payload.freeze());
                transport.send(pong_frame).await?;
                tracing::debug!("Replied with Pong during handshake");
                continue;
            }
            _ => {
                tracing::warn!(
                    frame_type = frame.frame_type,
                    "Unexpected frame during handshake"
                );

                let error_msg = format!("Expected Hello or Ping, got frame type 0x{:02x}", frame.frame_type);

                let mut err_payload = bytes::BytesMut::with_capacity(2 + 2 + error_msg.len());
                err_payload.put_u16_le(400u16); // Conditional error code (e.g., 400 Bad Protocol)
                err_payload.put_u16_le(error_msg.len() as u16);
                err_payload.put_slice(error_msg.as_bytes());

                let err_frame = Frame::new(FrameType::Error as u8, err_payload.freeze());

                let _ = transport.send(err_frame).await;
            
                return Err(HandshakeError::UnexpectedFrame(format!("0x{:02x}", frame.frame_type)).into());
            }
        }
    };
    tracing::debug!("Received Hello from client");

    // 3. Process Hello → Welcome
    let welcome = handshake_handler.process_hello(&hello)?;

    let welcome_frame = Frame::new(FrameType::Welcome as u8, welcome.encode());
    transport.send(welcome_frame).await?;
    tracing::debug!(
        session_id = welcome.session_id,
        "Sended Welcome"
    );

    let limits = welcome.limits.clone();

    // 4. Receive AuthResponse from the client
    let auth_response_frame = transport.recv().await?
        .ok_or_else(|| TransportError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "Connection closed during AuthResponse wait",
        )))?; 

    if auth_response_frame.frame_type != FrameType::AuthResponse as u8 {
        return Err(HandshakeError::UnexpectedFrame(
            format!("Expected AuthResponse (0x05), got frame type 0x{:02x}", auth_response_frame.frame_type)
        ).into());
    } 
    tracing::debug!(
        session_id = welcome.session_id,
        "Received AuthResponse from client"
    );

    let mut auth_response_bytes = auth_response_frame.payload.clone();
    let auth_response = AuthResponsePayload::decode(&mut auth_response_bytes)
        .map_err(|e| TransportError::Protocol(e.into()))?; 

    // 5. Process AuthResponse → AuthResult
    let auth_result = handshake_handler.process_auth_response(welcome.session_id, &auth_response)
        .map_err(|e| TransportError::Session(e.to_string()))?; 

    let auth_result_frame = Frame::new(FrameType::AuthResult as u8, auth_result.encode());
    transport.send(auth_result_frame).await?; 
    tracing::debug!(
        session_id = welcome.session_id,
        "Sended AuthResult"
    );

    // 6. Checking the authentication status
    if auth_result.status != 0 {
        return Err(TransportError::Session(
            format!("Authentication failed with status: {}", auth_result.status)
        ));
    }

    // 7. Register the session in SessionManager
    // Create SessionState with initial sequence = 1
    let l2_state = Arc::new(SessionState::new(1));                      
    let session_ctx = session_manager.register_session(
        welcome.session_id,
        welcome.session_token,
        limits,
        Arc::clone(&l2_state),
    );
    tracing::debug!(
        session_id = welcome.session_id,
        "Registered new session"
    );

    // 8. Creating a session endpoint with the correct limits
    let hot_credit = session_ctx.limits.hot_credit as usize;
    let unacked_buffer = Arc::new(UnackedBuffer::new(hot_credit));

    let mut endpoint = IPCEndpoint::with_transport_and_session(
        transport,
        Arc::clone(&session_ctx.l2_state), // Тот же `SessionState`, что в `SessionManager`!
        unacked_buffer,
    );    

    // 9. Sending the initial CreditGrant (hot credits) to the client
    // This allows the client to start sending Data frames
    endpoint.send_credit_grant(
        Bytes::new(), // Global credits (empty address)
        CreditGrantPayload::CREDIT_TYPE_HOT,
        session_ctx.limits.hot_credit,
    ).await?;
    tracing::debug!(
        session_id = welcome.session_id,
        credit_amount = session_ctx.limits.hot_credit,
        "Sended CreditGrant to client"
    );     

    // 10. Waiting for the CreditGrant from the client (non-blocking attempt)
    // The client must send its CreditGrant immediately after AuthResult
    match endpoint.recv().await? {
        Some(frame) if frame.frame_type == FrameType::CreditGrant as u8 => {
            let _client_cg = CreditGrantPayload::decode(&frame.payload)
                .map_err(|e| TransportError::Protocol(e.into()))?;
            tracing::debug!(
                session_id = session_ctx.session_id,
                credit_amount = _client_cg.credit_amount,
                "Received initial CreditGrant from client"
            );
        }
        Some(frame) => {
            tracing::warn!(
                session_id = session_ctx.session_id,
                frame_type = frame.frame_type,
                "Unexpected frame during CreditGrant wait"
            );
            
            // Send an Error frame before disconnecting
            let err_msg = "Expected CreditGrant";

            let mut err_payload = BytesMut::with_capacity(2 + 2 + err_msg.len());
            err_payload.put_u16_le(400u16);
            err_payload.put_u16_le(err_msg.len() as u16);
            err_payload.put_slice(err_msg.as_bytes());

            let err_frame = Frame::new(FrameType::Error as u8, err_payload.freeze());

            let _ = endpoint.send_raw_frame(err_frame).await;

            return Err(HandshakeError::UnexpectedFrame(format!("0x{:02x}", frame.frame_type)).into());
        }
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "Connection closed while waiting for client CreditGrant",
            ).into());
        }
    }

    Ok((endpoint, session_ctx))
}

/// Main frame processing loop after a successful handshake.
///
/// Terminates when:
/// - The client drops the connection (`recv()` returns `None`)
/// - Cancelled via `CancellationToken` (inactivity timeout from `SessionManager`)
/// - A transport error occurs (corrupted CRC, socket drop, etc.)
async fn main_loop(
    endpoint: &mut IPCEndpoint<UnixStream>,
    session_ctx: &Arc<SessionContext>,
) -> Result<(), TransportError> {
    loop {
        tokio::select! {
            // Forced cancellation from SessionManager (inactivity timeout)
            _ = session_ctx.cancellation_token.cancelled() => {
                tracing::info!(
                    session_id = session_ctx.session_id,
                    "Session cancelled by SessionManager (timeout)"
                );
                return Ok(());
            }

            // Reading frames from the client
            result = endpoint.recv() => {
                match result {
                    Ok(Some(frame)) => {
                        // Updating the last activity time
                        session_ctx.touch();

                        // TODO: full frame processing will be implemented in later stages.
                        // For now, just log the frame type.
                        tracing::debug!(
                            session_id = session_ctx.session_id,
                            frame_type = frame.frame_type,
                            "Received frame in main loop"
                        );

                        // TODO (Task 5 of Stage 5): handling Data, Ping, Close
                        // match frame.frame_type {
                        //     t if t == FrameType::Data as u8 => { /* Ingress Bridge */ },
                        //     t if t == FrameType::Ping as u8 => { /* send Pong */ },
                        //     t if t == FrameType::Close as u8 => { return Ok(()); },
                        //     _ => { /* Error frame */ },
                        // }
                    }
                    Ok(None) => {
                        // The client closed the connection gracefully
                        tracing::info!(
                            session_id = session_ctx.session_id,
                            "Client disconnected gracefully"
                        );
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::error!(
                            session_id = session_ctx.session_id,
                            error = %e,
                            "Transport error in main loop"
                        );
                        return Err(e);
                    }
                }
            }
        }
    }
}
