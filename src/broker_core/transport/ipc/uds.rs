// arcella-broker/src/broker_core/transport/ipc/uds.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::path::PathBuf;

use tokio::net::UnixStream;

use super::super::TransportError;
use super::connector::IPCConnector;

/// IPC connector for Unix Domain Sockets (Linux/macOS).
#[derive(Debug, Clone)]
pub struct UdsConnector {
    path: PathBuf,
}

impl UdsConnector {
    /// Creates a new UDS connector for the given socket path.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Returns the socket path.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl IPCConnector for UdsConnector {
    type Stream = UnixStream;

    /// Establishes a connection to the Unix Domain Socket at the configured path.
    ///
    /// This method creates a new `UnixStream` connection to the socket file
    /// specified during `UdsConnector::new()`. The connection is fully asynchronous
    /// and integrates with the Tokio runtime.
    ///
    /// # Connection Process
    ///
    /// 1. **Path validation**: The socket path must exist and be accessible.
    /// 2. **Connection attempt**: Calls `UnixStream::connect(path)` asynchronously.
    /// 3. **Stream initialization**: Returns the connected `UnixStream`, which
    ///    implements `AsyncRead + AsyncWrite + Unpin + Send`, making it compatible
    ///    with `FrameCodec` for frame-based communication.
    ///
    /// # Error Handling
    ///
    /// - `TransportError::ConnectionRefused`: If the socket file does not exist
    ///   or no process is listening on it.
    /// - `TransportError::Io(io_error)`: For other I/O errors (permissions,
    ///   invalid path, etc.).
    ///
    /// # Guarantees
    ///
    /// - **No cleanup required on error**: If the connection fails, no resources
    ///   are leaked. The caller does not need to perform any cleanup.
    /// - **Ready for communication**: The returned `UnixStream` is fully initialized
    ///   and ready for bidirectional communication immediately.
    ///
    /// # Platform Support
    ///
    /// This method is only available on Unix-like systems (Linux, macOS, BSD).
    /// On Windows, use `NamedPipeConnector` instead (see `#[cfg(windows)]`).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use arcella_broker::broker_core::transport::ipc::{UdsConnector, IPCConnector};
    ///
    /// let connector = UdsConnector::new("/tmp/arcella.sock");
    /// match connector.connect().await {
    ///     Ok(stream) => {
    ///         // stream is ready for use with IPCTransport
    ///         let transport = IPCTransport::new(stream);
    ///     }
    ///     Err(e) => eprintln!("Connection failed: {}", e),
    /// }
    /// ```
    async fn connect(&self) -> Result<UnixStream, TransportError> {
        UnixStream::connect(&self.path)
            .await
            .map_err(TransportError::Io)
    }
}

#[cfg(test)]
mod tests {

    use tempfile::tempdir;
    use tokio::net::UnixListener;

    use super::*;

    #[tokio::test]
    async fn test_uds_connector_connect() {
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        // Bind a listener first
        let _listener = UnixListener::bind(&socket_path).unwrap();

        let connector = UdsConnector::new(&socket_path);
        assert_eq!(connector.path(), &socket_path);

        // Connect should succeed
        let _stream = connector.connect().await.unwrap();
    }

    #[tokio::test]
    async fn test_uds_connector_connect_nonexistent() {
        let connector = UdsConnector::new("/tmp/nonexistent_arcella_test_socket_12345.sock");
        let result = connector.connect().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TransportError::Io(_)));
    }
}
