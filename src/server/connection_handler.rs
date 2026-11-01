// arcella-broker/src/server/connection_handler.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;
//use tokio::net::UnixStream;
use tokio::io::{AsyncRead, AsyncWrite};
use bytes::{Bytes, BytesMut, Buf, BufMut};

use crate::broker_core::l2::{SessionState, IngressBridge, IngressResult, BufferedFrame};
use crate::broker_core::registry::LocalRegistry;
use crate::broker_core::security::{
    HandshakeError, 
    HandshakeHandler, 
    SessionContext, 
    ServerSessionContext, 
    SessionManager
};
use crate::broker_core::transport::{IPCEndpoint, IPCTransport, TransportError};
use crate::protocol::{
    Frame, FrameType,
    ProtocolError,
    payloads::{
        HelloPayload,
        AuthResponsePayload,
        CreditGrantPayload,
        ParsedDataPayload,
    },
    build_error_frame,
};

/// Handles a single IPC connection from accept to disconnect.
pub async fn handle_connection<S, B>(
    stream: S,
    peer_addr: String,
    session_manager: Arc<SessionManager>,
    handshake_handler: Arc<HandshakeHandler>,
    _local_registry: Arc<LocalRegistry>, // Will be needed in Stage 6 for the Ingress Bridge
    ingress_bridge: Arc<B>,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    B: IngressBridge,
{
    // === Phase 1: Handshake ===
    let (mut endpoint, session_ctx) = match perform_handshake(stream, &handshake_handler, &session_manager).await {
        Ok(ctx) => ctx,
        Err(e) => {
            tracing::warn!(peer = %peer_addr, error = %e, "Handshake failed, closing connection");
            return Err(e);
        }
    }; 

    tracing::info!(
        session_id = session_ctx.session_id(),
        peer = %peer_addr,
        "Handshake completed, session registered"
    );

    // === Phase 2: Main frame processing loop ===
    let result = main_loop(&mut endpoint, &session_ctx, ingress_bridge.as_ref()).await; 

    // === Phase 3: Mandatory cleanup ===
    // Remove the session from the manager on ANY exit from the function
    // (connection drop, timeout, transport error)
    session_manager.remove_session(session_ctx.session_id());

    tracing::info!(
        session_id = session_ctx.session_id(),
        peer = %peer_addr,
        "Connection closed, session removed"
    );             

    result
}

/// Performs the handshake protocol: Hello → Welcome → AuthResponse → AuthResult.
///
/// Returns `Arc<SessionContext>` on successful authentication.
/// On any error, the connection must be closed by the caller.
async fn perform_handshake<S>(
    stream: S,
    handshake_handler: &HandshakeHandler,
    session_manager: &SessionManager,
) -> Result<(IPCEndpoint<S>, Arc<ServerSessionContext>), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // 1. Create the transport for exchanging control frames.
    let mut transport = IPCTransport::new(stream);

    // 2. Receive Hello from the client (with Ping support for keep-alive)
    let hello_frame = recv_handshake_frame(&mut transport).await?;

    if hello_frame.frame_type != FrameType::Hello as u8 {
        tracing::warn!(
            frame_type = hello_frame.frame_type,
            "Unexpected frame during handshake (expected Hello)"
        );

        let err_msg = format!("Expected Hello, got frame type 0x{:02x}", hello_frame.frame_type);
        let err_frame = build_error_frame(400u16, &err_msg);
        let _ = transport.send(err_frame).await;
    
        return Err(HandshakeError::UnexpectedFrame(err_msg).into());
    }
    let mut payload = hello_frame.payload.clone();
    let hello =  HelloPayload::decode(&mut payload)?;    
    tracing::debug!("Received Hello from client");

    // 3. Process Hello → Welcome
    let welcome = handshake_handler.process_hello(&hello)?;

    let welcome_frame = Frame::new(FrameType::Welcome as u8, welcome.encode());
    transport.send(welcome_frame).await?;
    tracing::debug!(
        session_id = welcome.session_id,
        "Sent Welcome"
    );

    let limits = welcome.limits.clone();

    // 4. Receive AuthResponse from the client
    let auth_response_frame = recv_handshake_frame(&mut transport).await?;
    if auth_response_frame.frame_type != FrameType::AuthResponse as u8 {
        let err_msg = format!("Expected AuthResponse (0x05), got frame type 0x{:02x}", auth_response_frame.frame_type);
        
        tracing::warn!(
            frame_type = auth_response_frame.frame_type,
            "Unexpected frame during handshake (expected AuthResponse)"
        );

        let err_frame = build_error_frame(400u16, &err_msg);
        let _ = transport.send(err_frame).await;

        return Err(HandshakeError::UnexpectedFrame(err_msg).into());
    }
    let mut auth_response_bytes = auth_response_frame.payload.clone();
    let auth_response = AuthResponsePayload::decode(&mut auth_response_bytes)
        .map_err(|e| TransportError::Protocol(e.into()))?; 
    tracing::debug!(
        session_id = welcome.session_id,
        "Received AuthResponse from client"
    );

    // 5. Process AuthResponse → AuthResult
    let auth_result = handshake_handler.process_auth_response(welcome.session_id, &auth_response)
        .map_err(|e| TransportError::Session(e.to_string()))?; 

    let auth_result_frame = Frame::new(FrameType::AuthResult as u8, auth_result.encode());
    transport.send(auth_result_frame).await?; 
    tracing::debug!(
        session_id = welcome.session_id,
        "Sent AuthResult"
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

    // 8. Creating an endpoint bound to ServerSessionContext
    let endpoint = IPCEndpoint::from_authenticated_transport(transport, &session_ctx).await?;
    tracing::debug!(
        session_id = welcome.session_id,
        "Credit exchange completed"
    );    

    Ok((endpoint, session_ctx))
}

/// Helper function for reading frames during the handshake.
/// Automatically handles Ping frames (responding with Pong) and returns
/// the next meaningful frame (Hello, AuthResponse, etc.)
async fn recv_handshake_frame<S>(
    transport: &mut IPCTransport<S>
) -> Result<Frame, TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    loop {
        let frame = transport.recv().await?
            .ok_or_else(|| TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "Connection closed during handshake",
            )))?;

        if frame.frame_type == FrameType::Ping as u8 {
            let mut pong_payload = BytesMut::with_capacity(frame.payload.len());
            pong_payload.put_slice(&frame.payload);
            let pong_frame = Frame::new(FrameType::Pong as u8, pong_payload.freeze());
            transport.send(pong_frame).await?;
            tracing::debug!("Replied with Pong during handshake");
            continue;
        }

        return Ok(frame);
    }
}

/// Main frame processing loop after a successful handshake.
///
/// Terminates when:
/// - The client drops the connection (`recv()` returns `None`)
/// - Cancelled via `CancellationToken` (inactivity timeout from `SessionManager`)
/// - A transport error occurs (corrupted CRC, socket drop, etc.)
async fn main_loop<S, B>(
    endpoint: &mut IPCEndpoint<S>,
    session_ctx: &ServerSessionContext,
    ingress_bridge: &B,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    B: IngressBridge,
{
    loop {
        tokio::select! {
            // Forced cancellation from SessionManager (inactivity timeout)
            _ = session_ctx.cancellation_token.cancelled() => {
                tracing::info!(
                    session_id = session_ctx.session_id(),
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

                        match frame.frame_type {
                            // === CreditGrant frame (0x02) - client granting credits to server ===
                            t if t == FrameType::CreditGrant as u8 => {
                                let mut payload_buf = frame.payload.clone();
                                match CreditGrantPayload::decode(&mut payload_buf) {
                                    Ok(cg) => {
                                        match cg.credit_type {
                                            CreditGrantPayload::CREDIT_TYPE_HOT => {
                                                tracing::debug!(
                                                    session_id = session_ctx.session_id(),
                                                    credit_amount = cg.credit_amount,
                                                    address = %String::from_utf8_lossy(&cg.address),
                                                    "Received hot CreditGrant from client (server-side sending not yet implemented)"
                                                );
                                            }
                                            CreditGrantPayload::CREDIT_TYPE_COLD => {
                                                tracing::debug!(
                                                    session_id = session_ctx.session_id(),
                                                    credit_amount = cg.credit_amount,
                                                    address = %String::from_utf8_lossy(&cg.address),
                                                    "Received cold CreditGrant from client (server-side sending not yet implemented)"
                                                );
                                            }
                                            _ => {
                                                tracing::warn!(
                                                    session_id = session_ctx.session_id(),
                                                    credit_type = cg.credit_type,
                                                    "Unknown credit_type in CreditGrant frame"
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            session_id = session_ctx.session_id(),
                                            error = %e,
                                            "Failed to decode CreditGrant frame"
                                        );
                                        // Optionally send Error frame and close connection
                                        let err_msg = format!("Invalid CreditGrant: {}", e);
                                        let err_frame = build_error_frame(400u16, &err_msg);
                                        let _ = endpoint.send_raw_frame(err_frame).await;
                                        return Err(TransportError::Protocol(e.into()));
                                    }
                                }
                            }

                            // === Acknowledge frame (0x03) - client acknowledging server's frames ===
                            t if t == FrameType::Acknowledge as u8 => {
                                if frame.payload.len() < 8 {
                                    tracing::warn!(
                                        session_id = session_ctx.session_id(),
                                        "Malformed Acknowledge frame (too short)"
                                    );
                                } else {
                                    let mut payload = frame.payload.clone();
                                    let ack_seq = payload.get_u64_le();
                                    if endpoint.acknowledge(ack_seq) {
                                        tracing::debug!(
                                            session_id = session_ctx.session_id(),
                                            ack_seq,
                                            "Acknowledged server frame"
                                        );
                                    }
                                }
                            }
                            
                            // === Close frame (0x07) ===
                            t if t == FrameType::Close as u8 => {
                                tracing::info!(
                                    session_id = session_ctx.session_id(),
                                    "Client requested graceful close"
                                );
                                return Ok(());
                            }

                            // === Ping frame (0x10) ===
                            t if t == FrameType::Ping as u8 => {
                                let mut pong_payload = BytesMut::with_capacity(frame.payload.len());
                                pong_payload.put_slice(&frame.payload);
                                let pong_frame = Frame::new(FrameType::Pong as u8, pong_payload.freeze());
                                endpoint.send_raw_frame(pong_frame).await?;
                                tracing::debug!(
                                    session_id = session_ctx.session_id(),
                                    "Responded with Pong"
														 
                                );
                            }

                            // === Data frame (0xfe) ===
                            t if t == FrameType::Data as u8 => {
                                if let Err(e) = handle_data_frame(
                                    endpoint, 
                                    session_ctx, 
                                    &frame,
                                    ingress_bridge,
                                ).await {
                                    tracing::error!(
                                        session_id = session_ctx.session_id(),
                                        error = %e,
                                        "Data frame handling failed"
                                    );
                                    return Err(e);
                                }
                            }                            

                            // === Unknown or unexpected frame ===
                            _ => {
                                tracing::warn!(
                                    session_id = session_ctx.session_id(),
                                    frame_type = frame.frame_type,
                                    "Unexpected frame type in main loop"
                                );
                                // Send Error and close
                                let err_msg = format!(
                                    "Unexpected frame type 0x{:02x}", 
                                    frame.frame_type
                                );
                                let err_frame = build_error_frame(400u16, &err_msg);
                                let _ = endpoint.send_raw_frame(err_frame).await;
                                return Err(ProtocolError::UnexpectedFrame(format!("0x{:02x}", frame.frame_type)).into());
                            }
                        }
                    }
                    Ok(None) => {
                        // The client closed the connection gracefully
                        tracing::info!(
                            session_id = session_ctx.session_id(),
                            "Client disconnected gracefully"
                        );
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::error!(
                            session_id = session_ctx.session_id(),
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


/// Handles an incoming Data frame with credit enforcement.
///
/// # Algorithm
///
/// 1. Parse the Data payload (zero-copy extraction of address and messages_raw).
/// 2. Attempt to consume one hot credit from `CreditManager`.
/// 3. If no credits available → send `Drop` with reason `buffer_overflow` and close.
/// 4. If credits available → process the frame (TODO: Ingress Bridge in Stage 6).
/// 5. Send `Acknowledge` to the client.
/// 6. Replenish the hot credit (credit is returned after processing).
///
/// # Thread Safety
///
/// `CreditManager` uses atomic CAS operations, so this function is safe to call
/// concurrently if needed (though currently only one task processes frames per session).
pub async fn handle_data_frame<S, B>(
    endpoint: &mut IPCEndpoint<S>,
    session_ctx: &ServerSessionContext,
    frame: &Frame,
    ingress_bridge: &B,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    B: IngressBridge,
{
    // 1. Parse the Data payload (zero-copy)
    let mut payload_buf = frame.payload.clone();
    let parsed = ParsedDataPayload::decode(&mut payload_buf)
        .map_err(|e| TransportError::Protocol(e.into()))?;

    let session_sequence = parsed.session_sequence;
    let address = parsed.address.clone();

    tracing::debug!(
        session_id = session_ctx.session_id(),
        session_sequence,
        address = %String::from_utf8_lossy(&address),
        messages_raw_len = parsed.messages_raw.len(),
        "Received Data frame"
    );

    // 2. Duplicate check
    let is_new_frame = session_ctx.l2_state().update_last_received(session_sequence);
    if !is_new_frame {
        tracing::debug!(
            session_id = session_ctx.session_id(),
            session_sequence,
            "Duplicate or out-of-order Data frame received. Sending Ack without processing."
        );
        // We MUST ack it so the client can free its UnackedBuffer slot,
        // but we DO NOT consume a credit and DO NOT pass it to the Ingress Bridge.
        endpoint.send_ack(session_sequence).await?;
        return Ok(());
    }

    // 3. Attempt to consume hot credit
    let credit_manager = session_ctx.incoming_credit_manager();
    if !credit_manager.consume_hot_credit(){
        // Protocol violation: client sent Data without available credit
        tracing::warn!(
            session_id = session_ctx.session_id(),
            session_sequence,
            "Client violated credit protocol (no hot credits available)"
        );

        // Send Drop frame with reason = buffer_overflow (1)
        let _ = endpoint.send_drop(session_sequence, 1).await;
        tracing::warn!(
            session_id = session_ctx.session_id(),
            session_sequence,
            "Sended Drop"
        );

        // Close the connection
        return Err(ProtocolError::UnexpectedFrame("Client exceeded hot credit limit".into()).into());
    }

    // 4. Try to push to hot buffer
    let buffered_frame = BufferedFrame {
        session_sequence,
        address: address.clone(),
        messages_raw: parsed.messages_raw.clone(),
    };  

    let hot_buffer = session_ctx.incoming_hot_buffer();
    if !hot_buffer.try_push(buffered_frame).await {
        // Hot buffer full — backpressure    
        credit_manager.replenish_hot_credit(); // Вернуть кредит
        tracing::warn!(
            session_id = session_ctx.session_id(),
            session_sequence,
            "Hot buffer full"
        );
        let _ = endpoint.send_drop(session_sequence, 1).await;
        return Err(ProtocolError::UnexpectedFrame("Hot buffer full".into()).into());
    }

    // 5. Frame accepted into hot buffer
    // Credit remains consumed (will be replenished by background processor)
    let hot_buffer_len = session_ctx.incoming_hot_buffer().len().await;
    tracing::debug!(
        session_id = session_ctx.session_id(),
        session_sequence,
        hot_buffer_len,
        "Frame pushed to hot buffer, credit remains consumed"
    );     

   
    // 6. Process the frame (TODO: Stage 6 - Ingress Bridge)
    // For now, just log that we would process it
    let ingress_result = ingress_bridge.process(&address, &parsed.messages_raw, session_ctx).await;

    match ingress_result {
        IngressResult::Accepted => {
            credit_manager.replenish_hot_credit();
            
            // TODO: get frame from hot buffer
            let _msg = hot_buffer.pop().await;

            let hot_buffer_len = session_ctx.incoming_hot_buffer().len().await;
            endpoint.send_ack(session_sequence).await?;
            tracing::debug!(
                session_id = session_ctx.session_id(),
                session_sequence,
                hot_available = credit_manager.hot_available(),
                hot_buffer_len,
                "Data frame processed successfully, credit replenished"
            );
            Ok(())
        }
        IngressResult::AddressBlocked => {
            tracing::warn!(
                session_id = session_ctx.session_id(),
                session_sequence,
                "Address blocked, credit remains consumed (TODO: Stage 10 Cold Buffer)"
            );
            // TODO: In Stage 10, moving to the Cold Buffer will happen here.
            // For now, we don't send an Ack and don't return the credit.
            Ok(())
        }
        IngressResult::Rejected(reason) => {
            tracing::warn!(
                session_id = session_ctx.session_id(),
                session_sequence,
                reason = %reason,
                "Ingress Bridge rejected frame, credit remains consumed"
            );
            let _ = endpoint.send_drop(session_sequence, 3).await; // 3 = processing_error
            Err(ProtocolError::UnexpectedFrame(reason).into())
        }
    }
}
