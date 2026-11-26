// arcella-broker/src/server/connection_handler.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Bytes, Buf};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use crate::broker_core::{IngressBridge, IngressResult};
use crate::broker_core::l2::{SessionState, BufferedFrame};
use crate::broker_core::registry::LocalRegistry;
use crate::broker_core::security::{
    HandshakeError, 
    HandshakeHandler, 
    SessionContext, 
    ServerSessionContext, 
    SessionManager
};
use crate::broker_core::transport::{
    EndpointWriterExt,
    IPCEndpoint, 
    IPCEndpointReader,
    IPCEndpointWriter,
    ControlRx,
    ControlTx,
    ControlCommand,
    PongEcho,
    IPCFramedStream, 
    TransportError,
    create_control_channel, 
};
use crate::protocol::{
    Frame, FrameType,
    ProtocolError,
    payloads::{
        HelloPayload,
        AuthResponsePayload,
        CreditGrantPayload,
        ParsedDataPayload,
    },
    create_ack_frame,
    create_drop_frame,
    create_error_frame,
    utils::validate_address,
};

use crate::broker_core::registry::{SubscriptionSlot, RouteTarget};

/// Handles a single IPC connection from accept to disconnect.
///
/// # Phases
///
/// 1. **Handshake** — synchronous exchange of Hello/Welcome/AuthResponse/AuthResult.
///    On any error the function returns immediately and the caller drops the socket.
/// 2. **Registration** — the session is stored in `SessionManager` and a routing
///    binding is created in `LocalRegistry`.
/// 3. **Supervisor** — spawns the writer actor as a dedicated task and runs the
///    reader loop inline. On any fatal error or graceful Close, the connection
///    token is cancelled, both halves are joined, and cleanup runs.
/// 4. **Cleanup** — `SessionManager::remove_session` runs exactly once, on any
///    exit path (graceful close, transport error, timeout).
pub async fn handle_connection<S, B>(
    stream: S,
    peer_addr: String,
    session_manager: Arc<SessionManager>,
    handshake_handler: Arc<HandshakeHandler>,
    local_registry: Arc<LocalRegistry>,
    ingress_bridge: Arc<B>,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    B: IngressBridge + 'static,
{
    // === Phase 1: Handshake ===
    let (endpoint, session_ctx) = match perform_handshake(
        stream, 
        &handshake_handler, 
        &session_manager
    ).await {
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

    // === Phase 1.5: Connection-level cancellation signal ===
    //
    // Hierarchy:
    //   SessionManager's GC
    //      │  (calls session_ctx.force_close())
    //      ▼
    //   session_ctx.cancellation_token        <- external trigger
    //      │  (child)
    //      ▼
    //   connection_token                      <- observed by reader + writer
    //
    // This gives us both directions of shutdown:
    //   * GC -> session token -> connection token -> both tasks wake up.
    //   * Fatal error inside reader/writer -> connection_token.cancel()
    //     wakes the other half AND signals the `select!` in supervisor.
    let connection_token = session_ctx.cancellation_token.child_token();

    // Local routing binding
    let source_prefix = session_ctx.source_prefix().to_string();
    let slot_token = CancellationToken::new();

    let slot = SubscriptionSlot::new(10, slot_token); // TODO: need to remove magic
    let target = RouteTarget::new_exclusive(slot.clone());

    if let Err(e) = local_registry.register(source_prefix, target) {
        // Failed before Phase 2 — remove the session now so we don't
        // accumulate phantom sessions on repeated handshake failures.
        session_manager.remove_session(session_ctx.session_id());
        return Err(TransportError::Registry(e));
    }

    // === Phase 2: Supervisor ===
    let result = run_connection_supervisor(
        endpoint,
        session_ctx.clone(),
        ingress_bridge,
        connection_token,
    )
    .await;

    // === Phase 3: Mandatory cleanup ===
    // Runs exactly once on ANY exit path (close frame, EOF, transport error,
    // cancellation). The supervisor is responsible for cancelling its own
    // connection_token before returning, so by the time we get here both
    // halves (future) are already stopped.
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
    let mut transport = IPCFramedStream::new(stream);

    // 2. Receive Hello from the client (with Ping support for keep-alive)
    let hello_frame = recv_handshake_frame(&mut transport).await?;

    if hello_frame.frame_type != FrameType::Hello as u8 {
        tracing::warn!(
            frame_type = hello_frame.frame_type,
            "Unexpected frame during handshake (expected Hello)"
        );

        let err_msg = format!("Expected Hello, got frame type 0x{:02x}", hello_frame.frame_type);
        let err_frame = create_error_frame(400u16, &err_msg);
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

        let err_frame = create_error_frame(400u16, &err_msg);
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
    let prefix_str = std::str::from_utf8(&hello.auth_data)
        .map_err(|e| HandshakeError::InvalidSourcePrefixUtf8(e))?;

    validate_address(prefix_str)
        .map_err(|e| HandshakeError::InvalidSourcePrefixFormat(e))?;

    let l2_state = Arc::new(SessionState::new(1));
    let session_ctx = session_manager.register_session(
        welcome.session_id,
        welcome.session_token,
        limits,
        Arc::clone(&l2_state),
        prefix_str.to_string(),
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
    transport: &mut IPCFramedStream<S>
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
            let response_timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let echo = PongEcho::from_ping_payload(&frame.payload, response_timestamp)
                .ok_or_else(|| {
                    TransportError::Session(
                        "malformed Ping payload during handshake".to_string(),
                    )
                })?;
            transport.send(build_pong_frame(echo)).await?;
            tracing::debug!("Replied with Pong during handshake");
            continue;
        }

        return Ok(frame);
    }
}

// ============================================================================
// Supervisor
// ============================================================================

/// Runs the read and write sides of the connection concurrently.
///
/// # Architecture
///
/// ```text
///                          ┌──────────────────────┐
///                          │ connection_token     │
///                          └───────┬──────────────┘
///                                  │
///                    ┌─────────────┼─────────────┐
///                    ▼             ▼             ▼
///               reader_loop   writer_actor   supervisor select
///                    │             ▲             │
///                    │  ControlTx  │             │
///                    └─────────────┘             │
///                                                │
///                       on any fatal: cancel token, join both
/// ```
///
/// 1. Create a bounded control channel (capacity = incoming `hot_credit`).
/// 2. Split the endpoint into `(reader, writer)`.
/// 3. Spawn the writer actor as a dedicated task.
/// 4. Run the reader loop on the supervisor's task.
/// 5. On reader exit (normal or error), cancel the connection token.
/// 6. Await the writer handle. The first error wins.
///
/// # Cancellation safety
///
/// Both tasks observe the same `connection_token`, so a fatal error in
/// either one tears the whole connection down deterministically. The
/// supervisor is itself cancel-safe: dropping it drops both halves, which
/// drops the underlying socket.
async fn run_connection_supervisor<S, B>(
    endpoint: IPCEndpoint<S>,
    session_ctx: Arc<ServerSessionContext>,
    ingress_bridge: Arc<B>,
    connection_token: CancellationToken,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    B: IngressBridge + 'static,
{
    // 1. Create the control channel.
    // Capacity is bounded by the incoming direction's hot_credit: the peer
    // may hold at most that many unacknowledged Data frames, each producing
    // at most one Ack command.
    let incoming_hot_credit = NonZeroUsize::new(session_ctx.limits().hot_credit as usize)
        .expect("hot_credit > 0 validated at handshake");

    let (ctrl_tx, ctrl_rx) =
        create_control_channel(incoming_hot_credit, connection_token.clone());

    // 2. Split endpoint into reader + writer halves.
    let (reader, writer) = endpoint.split();

    // 3. Spawn the writer actor.
    // The writer owns the sole serialization point of the outgoing
    // direction (L2 §13.2). All outbound frames — control replies from the
    // reader and (later) egress Data from the Egress Bridge — flow through
    // it, which guarantees that `session_sequence` assignment order matches
    // write order.
    let writer_ctx = Arc::clone(&session_ctx);

    let writer_token = connection_token.clone();
    let writer_handle = tokio::spawn(async move {
        writer_actor(writer, ctrl_rx, writer_ctx, writer_token).await
    });

    // 4. Run the reader loop inline on the supervisor task.
    // This keeps one task count lower than spawning both halves. The reader
    // is the more latency-sensitive side (it drives credit return and Ack
    // timing) so it gets the supervisor's own scheduler slot.
    let reader_result = reader_loop(
        reader,
        ctrl_tx,
        ingress_bridge,
        Arc::clone(&session_ctx),
        connection_token.clone(),
    ).await;

    // 5. Reader has exited. Cancel the connection token to wake the writer.
    // This is idempotent: if the reader exited because the token was
    // already cancelled (GC, peer Close), this is a no-op. If the reader
    // exited on a transport error, this is how the writer finds out.
    connection_token.cancel();

    // 6. Await the writer handle and combine outcomes.
    // Rule: a fatal transport error in either half is reported; an `Ok(())`
    // from one half never masks an error from the other.
    match writer_handle.await {
        Ok(writer_result) => match (&reader_result, &writer_result) {
            (Ok(()), Err(_)) => {
                tracing::error!(
                    session_id = session_ctx.session_id(),
                    "Writer task failed while reader exited cleanly"
                );
                writer_result
            }
            (Err(_), Ok(())) => reader_result,
            (Err(_), Err(_)) => {
                // Both failed — reader's error was the trigger, report it.
                reader_result
            }
            (Ok(()), Ok(())) => Ok(()),
        }
        Err(join_err) => {
            // Writer panicked or was cancelled by the runtime. This is a fatal
            // outcome: a clean reader exit must not mask it.
            tracing::error!(
                session_id = session_ctx.session_id(),
                error = %join_err,
                "Writer task panicked or was cancelled by the runtime"
            );
            match reader_result {
                Ok(()) => Err(TransportError::Internal(format!(
                    "writer task failed: {join_err}"
                ))),
                err => err,
            }
        }
    }
}

// ============================================================================
// Writer actor
// ============================================================================

/// The single writer task for the outgoing direction (L2 §13.2).
///
/// # Inputs
///
/// * `writer` — write-half of the split endpoint. Only this task ever touches
///   it, so `session_sequence` assignment order matches write order by
///   construction.
/// * `ctrl_rx` — non-blocking queue of control commands from the reader
///   (Ack, Drop, Pong). See `ipc::control` for capacity and overflow rules.
/// * `session_ctx` — for logging and, later, for egress Data dispatch.
/// * `connection_token` — shared shutdown signal.
///
/// # Loop
///
/// Awaits `ctrl_rx.recv()` with a biased select against the cancellation
/// token. On each command it builds the matching frame and transmits it via
/// `EndpointWriterExt::send_raw_frame`, which enforces the negotiated size
/// ceiling and the fatal-latch check.
///
/// # Fatal outcomes
///
/// Any transport error (I/O, codec, post-latch broken session) cancels the
/// connection token and returns the error to the supervisor. No retry —
/// §13.3 mandates teardown + Resume for post-assignment failures.
async fn writer_actor<S>(
    mut writer: IPCEndpointWriter<S>,
    mut ctrl_rx: ControlRx,
    session_ctx: Arc<ServerSessionContext>,
    connection_token: CancellationToken,
) -> Result<(), TransportError>
where
    S: AsyncWrite + Unpin + Send + 'static,
{
    loop {
        tokio::select! {
            biased;

            // Shutdown: GC-driven, reader fatal, or explicit cancel.
            //
            // Biased: checked FIRST so that a cancelled connection does not
            // dispatch one more command on its way out.
            _ = connection_token.cancelled() => {
                tracing::info!(
                    session_id = session_ctx.session_id(),
                    "Writer task cancelled"
                );
                return Ok(());
            }

            // Dispatch next control command from the reader.
            cmd = ctrl_rx.recv() => {
                match cmd {
                    Some(command) => {
                        let dispatch_fut = dispatch_control_command(
                            &mut writer,
                            command,
                            session_ctx.as_ref(),
                        );
                        tokio::select! {
                            biased;
                            _ = connection_token.cancelled() => {
                                tracing::info!(
                                    session_id = session_ctx.session_id(),
                                    "Writer task cancelled during flush"
                                );
                                return Ok(());
                            }
                            result = dispatch_fut => {
                                if let Err(e) = result {
                                    tracing::error!(
                                        session_id = session_ctx.session_id(),
                                        error = %e,
                                        "Writer transport error"
                                    );
                                    connection_token.cancel();
                                    return Err(e);
                                }
                            }    
                        }
                    }
                    None => {
                        // Control channel closed: the reader dropped its
                        // ControlTx, which only happens when the reader is
                        // itself exiting. Drain anything left in the Pong
                        // lane and exit cleanly.
                        tracing::info!(
                            session_id = session_ctx.session_id(),
                            "Control channel closed, writer exiting"
                        );
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Translates a single `ControlCommand` into a wire frame and sends it.
///
/// All three command types are non-sequenced (L2 §13.1), so they go through
/// `send_raw_frame`: no `session_sequence` is consumed, no slot in
/// `UnackedBuffer` is reserved.
async fn dispatch_control_command<S>(
    writer: &mut IPCEndpointWriter<S>,
    command: ControlCommand,
    session_ctx: &ServerSessionContext,
) -> Result<(), TransportError>
where
    S: AsyncWrite + Unpin + Send,
{
    let frame = match command {
        ControlCommand::Ack { sequence } => {
            tracing::debug!(
                session_id = session_ctx.session_id(),
                sequence,
                "Sending Ack"
            );
            create_ack_frame(sequence)
        }
        ControlCommand::Drop { sequence, reason } => {
            tracing::debug!(
                session_id = session_ctx.session_id(),
                sequence,
                reason,
                "Sending Drop"
            );
            create_drop_frame(sequence, reason)
        }
        ControlCommand::Pong(echo) => {
            tracing::debug!(
                session_id = session_ctx.session_id(),
                original_ts = echo.original_timestamp,
                response_ts = echo.response_timestamp,
                "Sending Pong"
            );
            build_pong_frame(echo)
        }
    };
    writer.send_raw_frame(frame).await
}

/// Builds a `Pong` frame (0x11) from a [`PongEcho`].
///
/// Layout per L2 spec §7.12: `original_timestamp: u64 LE` +
/// `response_timestamp: u64 LE` + `opaque_data: [u8; 8]` = 24 bytes.
fn build_pong_frame(echo: PongEcho) -> Frame {
    Frame::new(FrameType::Pong as u8, Bytes::from(echo.to_bytes().to_vec()))
}

// ============================================================================
// Reader task
// ============================================================================

/// Reader loop: receives frames from the peer and dispatches them.
///
/// # Shutdown signal
///
/// The caller passes the **connection-level** cancellation token. This is
/// the only shutdown observer; it covers:
///
/// * GC-driven shutdown (via `session_ctx.cancellation_token` → child),
/// * fatal error in the writer actor,
/// * explicit cancellation from tests / admin.
///
/// On a local fatal outcome the reader cancels the token itself so the
/// writer wakes up promptly.
///
/// # Termination conditions
///
/// * Peer drops the connection (`recv()` returns `None`).
/// * A `Close` frame is received.
/// * A transport error occurs (CRC mismatch, socket error).
/// * A protocol violation is detected (credit overflow, malformed Ping).
/// * `connection_token` is cancelled externally.
async fn reader_loop<S, B>(
    mut reader: IPCEndpointReader<S>,
    ctrl: ControlTx,
    ingress_bridge: Arc<B>,
    session_ctx: Arc<ServerSessionContext>,
    connection_token: CancellationToken,
) -> Result<(), TransportError>
where
    S: AsyncRead + Unpin + Send + 'static,
    B: IngressBridge + 'static,
{
    loop {
        tokio::select! {
            biased;
            
            // Shutdown: GC-driven, peer Close, or fatal in writer task.
            _ = connection_token.cancelled() => {
                tracing::info!(
                    session_id = session_ctx.session_id(),
                    "Reader task cancelled"
                );
                return Ok(());
            }
            
            // Read next frame from peer.
            result = reader.recv() => {
                match result {
                    Ok(Some(frame)) => {
                        session_ctx.touch();

                        // Close (0x07) is a terminal signal: cancel the connection
                        // token and exit. The writer will observe the cancellation
                        // via its biased select and stop. The supervisor joins both
                        // halves and performs cleanup.
                        if frame.frame_type == FrameType::Close as u8 {
                            tracing::info!(
                                session_id = session_ctx.session_id(),
                                "Peer requested graceful close"
                            );
                            connection_token.cancel();
                            return Ok(());
                        }
                        
                        if let Err(e) = dispatch_incoming_frame(
                            &frame,
                            &ctrl,
                            ingress_bridge.as_ref(),
                            session_ctx.as_ref(),
                        ).await {
                            tracing::error!(
                                session_id = session_ctx.session_id(),
                                error = %e,
                                "Frame dispatch failed"
                            );
                            // Fatal error: tear down the connection.
                            connection_token.cancel();
                            return Err(e);
                        }
                    }
                    Ok(None) => {
                        // Peer closed the connection gracefully.
                        tracing::info!(
                            session_id = session_ctx.session_id(),
                            "Peer closed connection (EOF)"
                        );
                        connection_token.cancel();
                        return Ok(());
                    }
                    Err(e) => {
                        // Transport error: CRC mismatch, I/O failure, etc.
                        tracing::error!(
                            session_id = session_ctx.session_id(),
                            error = %e,
                            "Reader transport error"
                        );
                        connection_token.cancel();
                        return Err(e);
                    }
                }
            }
        }
    }
}

async fn dispatch_incoming_frame<B>(
    frame: &Frame,
    ctrl: &ControlTx,
    ingress_bridge: &B,
    session_ctx: &ServerSessionContext,
) -> Result<(), TransportError>
where
    B: IngressBridge,
{
    match frame.frame_type {
        t if t == FrameType::Data as u8 => {
            handle_data_frame_reader(frame, ctrl, ingress_bridge, session_ctx).await
        }
        t if t == FrameType::Acknowledge as u8 => {
            handle_ack_frame(frame, session_ctx)
        }
        t if t == FrameType::Drop as u8 => {
            handle_drop_frame(frame, session_ctx)
        }
        t if t == FrameType::CreditGrant as u8 => {
            handle_credit_grant_frame(frame, session_ctx)
        }
        t if t == FrameType::Ping as u8 => {
            handle_ping_frame(frame, ctrl)
        }
        t if t == FrameType::Pong as u8 => {
            // Pong is a reply to our Ping; no action needed.
            tracing::debug!(
                session_id = session_ctx.session_id(),
                "Received Pong (liveness confirmed)"
            );
            Ok(())
        }
        _ => {
            if frame.typed_frame_type().is_none() {
                tracing::warn!(
                    session_id = session_ctx.session_id(),
                    frame_type = frame.frame_type,
                    "Ignoring unknown frame type in reader loop"
                );
                return Ok(());
            }

            tracing::warn!(
                session_id = session_ctx.session_id(),
                frame_type = frame.frame_type,
                "Unexpected frame type in reader loop"
            );
            Err(ProtocolError::UnexpectedFrame(
                format!("0x{:02x}", frame.frame_type),
            ).into())
        }
    }
}

// ============================================================================
// Per-frame handlers
// ============================================================================

/// Handles an incoming `Data` frame with credit enforcement.
///
/// # Algorithm
///
/// 1. Parse the Data payload (zero-copy extraction of `address` and
///    `messages_raw`).
/// 2. Duplicate check via `SessionState::update_last_received`. Duplicates
///    are Ack'd without consuming a credit or calling the Ingress Bridge.
/// 3. Attempt to consume one hot credit from `CreditManager`.
/// 4. If no credits are available → send `Drop` (reason `buffer_overflow`)
///    via the control queue and return a protocol error.
/// 5. Pass `(address, messages_raw)` to the Ingress Bridge.
/// 6. Replenish the hot credit and send `Ack` on `Accepted`; send `Drop`
///    with the matching reason on `Rejected`.
///
/// # Thread safety
///
/// `CreditManager` uses atomic CAS operations, so this function is safe to
/// call from the reader task while the writer task concurrently observes
/// the same counters.
async fn handle_data_frame_reader<B>(
    frame: &Frame,
    ctrl: &ControlTx,
    ingress_bridge: &B,
    session_ctx: &ServerSessionContext,
) -> Result<(), TransportError>
where
    B: IngressBridge,
{
    // 1. Parse the Data payload (zero-copy).
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

    // 2. Duplicate check.
    let is_new_frame = session_ctx.l2_state().update_last_received(session_sequence);
    if !is_new_frame {
        tracing::debug!(
            session_id = session_ctx.session_id(),
            session_sequence,
            "Duplicate or out-of-order Data frame received. Sending Ack without processing."
        );
        // We MUST ack it so the client can free its UnackedBuffer slot,
        // but we DO NOT consume a credit and DO NOT pass it to the Ingress Bridge.
        ctrl.try_send(ControlCommand::Ack { sequence: session_sequence })
            .map_err(|e| TransportError::Session(
                format!("Control queue error on Ack (fatal until §13.6): {:?}", e)
            ))?;
        return Ok(());
    }

    // 3. Attempt to consume hot credit.
    let credit_manager = session_ctx.incoming_credit_manager();
    if !credit_manager.consume_hot_credit() {
        // Protocol violation: client sent Data without available credit.
        tracing::warn!(
            session_id = session_ctx.session_id(),
            session_sequence,
            "Client violated credit protocol (no hot credits available)"
        );

        // Send Drop frame with reason = buffer_overflow (1).
        ctrl.try_send(ControlCommand::Drop { sequence: session_sequence, reason: 1 })
            .map_err(|e| TransportError::Session(
                format!("Control queue error on Drop (fatal until §13.6): {:?}", e)
            ))?;

        return Err(ProtocolError::UnexpectedFrame("Client exceeded hot credit limit".into()).into());
    }

    // 4. Try to push to hot buffer.
    let buffered_frame = BufferedFrame {
        session_sequence,
        address: address.clone(),
        messages_raw: parsed.messages_raw.clone(),
    };

    let hot_buffer = session_ctx.incoming_hot_buffer();
    if !hot_buffer.try_push(buffered_frame).await {
        // Hot buffer full — backpressure.
        credit_manager.replenish_hot_credit(); // Return the credit.
        tracing::warn!(
            session_id = session_ctx.session_id(),
            session_sequence,
            "Hot buffer full"
        );
        let _ = ctrl.try_send(ControlCommand::Drop {
            sequence: session_sequence,
            reason: 1,
        });
        return Err(ProtocolError::UnexpectedFrame("Hot buffer full".into()).into());
    }

    // 5. Frame accepted into hot buffer.
    // Credit remains consumed (will be replenished after processing).
    let hot_buffer_len = session_ctx.incoming_hot_buffer().len().await;
    tracing::debug!(
        session_id = session_ctx.session_id(),
        session_sequence,
        hot_buffer_len,
        "Frame pushed to hot buffer, credit remains consumed"
    );

    // 6. Process the frame via the Ingress Bridge.
    let ingress_result = ingress_bridge
        .process(&address, &parsed.messages_raw, session_ctx)
        .await;

    match ingress_result {
        IngressResult::Accepted => {
            credit_manager.replenish_hot_credit();

            // TODO: get frame from hot buffer by session_sequence, not FIFO
            let _msg = hot_buffer.pop().await;

            let hot_buffer_len = session_ctx.incoming_hot_buffer().len().await;
            ctrl.try_send(ControlCommand::Ack { sequence: session_sequence })
                .map_err(|e| TransportError::Session(
                    format!("Control queue error on Ack (fatal until §13.6): {:?}", e)
                ))?;

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
            credit_manager.replenish_hot_credit();
            let _ = hot_buffer.pop().await;

            tracing::warn!(
                session_id = session_ctx.session_id(),
                session_sequence,
                "Address blocked, cold pool not implemented; frame dropped"
            );

            ctrl.try_send(ControlCommand::Drop { sequence: session_sequence, reason: 1 })
                .map_err(|e| TransportError::Session(
                    format!("Control queue error on Drop (fatal until §13.6): {:?}", e)
                ))?;

            Ok(())
        }
        IngressResult::Rejected(reason) => {
            tracing::warn!(
                session_id = session_ctx.session_id(),
                session_sequence,
                reason = %reason,
                "Ingress Bridge rejected frame, credit remains consumed"
            );
            ctrl.try_send(ControlCommand::Drop { sequence: session_sequence, reason: 2 })
                .map_err(|e| TransportError::Session(
                    format!("Control queue error on Drop (fatal until §13.6): {:?}", e)
                ))?;
            Err(ProtocolError::UnexpectedFrame(reason).into())
        }
    }
}

/// Handles an incoming `Acknowledge` frame (0x03).
///
/// Removes the matching frame from our `UnackedBuffer` and replenishes one
/// outgoing credit. Idempotent: a duplicate Ack for an already-acknowledged
/// sequence is silently ignored.
fn handle_ack_frame(
    frame: &Frame,
    session_ctx: &ServerSessionContext,
) -> Result<(), TransportError> {
    if frame.payload.len() < 8 {
        return Err(TransportError::Session(
            "Malformed Acknowledge frame (too short)".to_string(),
        ));
    }

    let mut payload = frame.payload.clone();
    let ack_seq = payload.get_u64_le();

    // `acknowledge` removes the frame from UnackedBuffer and replenishes
    // one outgoing credit.
    if session_ctx.outgoing_unacked_buffer().acknowledge(ack_seq) {
        session_ctx.outgoing_credit_pool().replenish();
        tracing::debug!(
            session_id = session_ctx.session_id(),
            ack_seq,
            "Acknowledged outgoing frame"
        );
    } else {
        tracing::warn!(
            session_id = session_ctx.session_id(),
            ack_seq,
            "Received Ack for unknown or already acknowledged sequence"
        );
    }

    Ok(())
}

/// Handles an incoming `Drop` frame (0x04).
///
/// Removes the matching frame from our `UnackedBuffer` and replenishes one
/// outgoing credit. TODO (Stage 10): start `drop_cooldown_ms` for the
/// affected address.
fn handle_drop_frame(
    frame: &Frame,
    session_ctx: &ServerSessionContext,
) -> Result<(), TransportError> {
    if frame.payload.len() < 9 {
        return Err(TransportError::Session(
            "Malformed Drop frame (too short)".to_string(),
        ));
    }

    let mut payload = frame.payload.clone();
    let drop_seq = payload.get_u64_le();
    let reason = payload.get_u8();

    // `remove` frees the slot; credit replenishment is done here for
    // consistency with Ack handling.
    if session_ctx.outgoing_unacked_buffer().remove(drop_seq) {
        session_ctx.outgoing_credit_pool().replenish();
        tracing::debug!(
            session_id = session_ctx.session_id(),
            drop_seq,
            reason,
            "Peer dropped our frame"
        );
        // TODO: start drop_cooldown_ms for the affected address (L2 §8.4)
    } else {
        tracing::warn!(
            session_id = session_ctx.session_id(),
            drop_seq,
            "Received Drop for unknown or already removed sequence"
        );
    }

    Ok(())
}

/// Handles an incoming `CreditGrant` frame (0x02).
///
/// Hot grants are applied to `outgoing_credit_pool` via
/// `SessionContext::grant_outgoing_credits`. Cold grants are logged for now
/// (Stage 10 will route them into the cold pool).
fn handle_credit_grant_frame(
    frame: &Frame,
    session_ctx: &ServerSessionContext,
) -> Result<(), TransportError> {
    let mut payload_buf = frame.payload.clone();
    match CreditGrantPayload::decode(&mut payload_buf) {
        Ok(cg) => {
            let is_new = session_ctx.l2_state().update_last_received(cg.session_sequence);
            if !is_new {
                tracing::debug!(
                    session_id = session_ctx.session_id(),
                    seq = cg.session_sequence,
                    "Duplicate CreditGrant received, ignoring"
                );
                return Ok(());
            }

            match cg.credit_type {
                CreditGrantPayload::CREDIT_TYPE_HOT => {
                    session_ctx.grant_outgoing_credits(cg.credit_amount);
                    tracing::debug!(
                        session_id = session_ctx.session_id(),
                        credit_amount = cg.credit_amount,
                        address = %String::from_utf8_lossy(&cg.address),
                        "Received hot CreditGrant from peer"
                    );
                }
                CreditGrantPayload::CREDIT_TYPE_COLD => {
                    tracing::debug!(
                        session_id = session_ctx.session_id(),
                        credit_amount = cg.credit_amount,
                        address = %String::from_utf8_lossy(&cg.address),
                        "Received cold CreditGrant from peer (not yet implemented)"
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
            Ok(())
        }
        Err(e) => {
            tracing::error!(
                session_id = session_ctx.session_id(),
                error = %e,
                "Failed to decode CreditGrant frame"
            );
            Err(TransportError::Protocol(e.into()))
        }
    }
}

/// Handles an incoming `Ping` frame (0x10) by enqueuing a `Pong` reply.
///
/// The reply is dispatched via `ControlTx::try_pong`, which is
/// non-blocking and silently drops the reply if the pong lane is full
/// (the prober will re-ping on its own schedule).
///
/// A malformed Ping (payload size != 16) is reported as a session error
/// and tears the connection down.
fn handle_ping_frame(
    frame: &Frame,
    ctrl: &ControlTx,
) -> Result<(), TransportError> {
    let response_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    match PongEcho::from_ping_payload(&frame.payload, response_timestamp) {
        Some(echo) => {
            ctrl.try_pong(echo);
            tracing::debug!("Enqueued Pong reply");
            Ok(())
        }
        None => {
            tracing::warn!("Malformed Ping frame (payload size != 16)");
            Err(TransportError::Session(
                "Malformed Ping frame".to_string(),
            ))
        }
    }
}
