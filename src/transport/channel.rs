// arcella-broker/src/transport/channel.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use tokio::sync::mpsc;

use crate::protocol::Message;
use crate::error::BrokerError;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TryRecvError {
    #[error("Channel is empty")]
    Empty,
    #[error("Channel is disconnected")]
    Disconnected,
}

impl From<mpsc::error::TryRecvError> for TryRecvError {
    fn from(err: mpsc::error::TryRecvError) -> Self {
        match err {
            mpsc::error::TryRecvError::Empty => TryRecvError::Empty,
            mpsc::error::TryRecvError::Disconnected => TryRecvError::Disconnected,
        }
    }
}

/// Type-safe message sender.
#[derive(Clone)]
pub struct MessageSender {
    inner: mpsc::Sender<Message>,
}

impl MessageSender {
    pub(crate) fn new(inner: mpsc::Sender<Message>) -> Self {
        Self { inner }
    }

    /// Asynchronous send with natural backpressure.
    /// If the receiver's queue is full, the calling task will be suspended.
    pub async fn send(&self, message: Message) -> Result<(), BrokerError> {
        self.inner
            .send(message)
            .await
            .map_err(|e| match e {
                mpsc::error::SendError(msg) => BrokerError::ChannelClosed(msg),
            })
    }

    /// Non-blocking send attempt.
    /// Returns the message back if the queue is full.
    pub fn try_send(&self, message: Message) -> Result<(), BrokerError> {
        self.inner
            .try_send(message)
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(msg) => BrokerError::ChannelFull(msg),
                mpsc::error::TrySendError::Closed(msg) => BrokerError::ChannelClosed(msg),
            })
    }

    /// Checks whether the receiver is alive.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Current channel utilization (for metrics/telemetry).
    pub fn load(&self) -> ChannelLoad {
        ChannelLoad {
            capacity: self.inner.capacity(),
            max_capacity: self.inner.max_capacity(),
        }
    }
}

/// Type-safe message receiver.
pub struct MessageReceiver {
    inner: mpsc::Receiver<Message>,
}

impl MessageReceiver {
    pub(crate) fn new(inner: mpsc::Receiver<Message>) -> Self {
        Self { inner }
    }

    /// Asynchronously receives the next message.
    /// Returns `None` if all senders have been dropped (channel closed).
    pub async fn recv(&mut self) -> Option<Message> {
        self.inner.recv().await
    }

    /// Non-blocking receive attempt.
    pub fn try_recv(&mut self) -> Result<Message, TryRecvError> {
        self.inner.try_recv().map_err(Into::into)
    }

    /// Receive with a timeout (critical for Wasm environments).
    pub async fn recv_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Option<Message> {
        match tokio::time::timeout(timeout, self.inner.recv()).await {
            Ok(msg) => msg,
            Err(_) => None,
        }
    }
}

/// Channel utilization information (for metrics).
pub struct ChannelLoad {
    pub capacity: usize,
    pub max_capacity: usize,
}

pub fn create_channel(capacity: usize) -> (MessageSender, MessageReceiver) {
    let (tx, rx) = tokio::sync::mpsc::channel::<Message>(capacity);
    (MessageSender::new(tx), MessageReceiver::new(rx))
}
