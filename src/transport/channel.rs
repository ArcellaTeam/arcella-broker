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

/// Типобезопасный отправитель сообщений.
#[derive(Clone)]
pub struct MessageSender {
    inner: mpsc::Sender<Message>,
}

impl MessageSender {
    pub(crate) fn new(inner: mpsc::Sender<Message>) -> Self {
        Self { inner }
    }

    /// Асинхронная отправка с естественным backpressure.
    /// Если очередь получателя переполнена — вызывающая задача будет приостановлена.
    pub async fn send(&self, message: Message) -> Result<(), BrokerError> {
        self.inner
            .send(message)
            .await
            .map_err(|_| BrokerError::ChannelClosed)
    }

    /// Неблокирующая попытка отправки.
    /// Возвращает сообщение обратно, если очередь переполнена.
    pub fn try_send(&self, message: Message) -> Result<(), (BrokerError, Message)> {
        self.inner
            .try_send(message)
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(msg) => (BrokerError::ChannelFull, msg),
                mpsc::error::TrySendError::Closed(msg) => (BrokerError::ChannelClosed, msg),
            })
    }

    /// Проверка живости получателя.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Текущая загруженность канала (для метрик/телеметрии).
    pub fn load(&self) -> ChannelLoad {
        ChannelLoad {
            capacity: self.inner.capacity(),
            max_capacity: self.inner.max_capacity(),
        }
    }
}

/// Типобезопасный получатель сообщений.
pub struct MessageReceiver {
    inner: mpsc::Receiver<Message>,
}

impl MessageReceiver {
    pub(crate) fn new(inner: mpsc::Receiver<Message>) -> Self {
        Self { inner }
    }

    /// Асинхронное получение следующего сообщения.
    /// Возвращает `None`, если все отправители были удалены (канал закрыт).
    pub async fn recv(&mut self) -> Option<Message> {
        self.inner.recv().await
    }

    /// Неблокирующая попытка получения.
    pub fn try_recv(&mut self) -> Result<Message, mpsc::error::TryRecvError> {
        self.inner.try_recv()
    }

    /// Получение с таймаутом (критично для Wasm-среды).
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

/// Информация о загруженности канала (для метрик).
pub struct ChannelLoad {
    pub capacity: usize,
    pub max_capacity: usize,
}
