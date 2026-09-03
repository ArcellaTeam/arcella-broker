// arcella-broker/src/config/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use serde::{Deserialize, Serialize};

pub const DEFAULT_REPLY_CHANNEL_CAPACITY: usize = 1024;
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_TTL: u8 = 64;

/// Глобальная конфигурация брокера сообщений Arcella.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    /// Емкость внутреннего канала для диспетчеризации ответов в режиме InOut (Request/Response).
    pub reply_channel_capacity: usize,

    /// Таймаут по умолчанию для операций InOut в миллисекундах.
    pub request_timeout_ms: u64,

    /// Значение Time-To-Live (TTL) по умолчанию для каскадной маршрутизации.
    pub default_ttl: u8,

    /// Максимальный разрешенный размер полезной нагрузки (payload) в байтах.
    pub max_payload_size: u32,

    /// Максимальная разрешенная длина адреса в байтах.
    pub max_address_length: u16,

    /// Максимальная разрешенная длина типа сообщения в байтах.
    pub max_msg_type_length: u8,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            reply_channel_capacity: DEFAULT_REPLY_CHANNEL_CAPACITY,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            default_ttl: DEFAULT_TTL,
            max_payload_size: 1_048_576,
            max_address_length: 1024,
            max_msg_type_length: 255,
        }
    }
}

impl BrokerConfig {
    /// Создает новую конфигурацию со значениями по умолчанию.
    pub fn new() -> Self {
        Self::default()
    }

    /// Устанавливает емкость канала для ответов (InOut).
    #[must_use]
    pub fn with_reply_channel_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "reply_channel_capacity must be greater than 0");
        self.reply_channel_capacity = capacity;
        self
    }

    /// Устанавливает таймаут запросов в миллисекундах.
    #[must_use]
    pub fn with_request_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.request_timeout_ms = timeout_ms;
        self
    }

    /// Устанавливает TTL по умолчанию.
    #[must_use]
    pub fn with_default_ttl(mut self, ttl: u8) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Возвращает таймаут в виде `std::time::Duration` для удобства использования в Tokio.
    pub fn request_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.request_timeout_ms)
    }

}

/// Конфигурация для отдельного подписчика (конкретного канала).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriberConfig {
    /// Максимальное количество сообщений, которое может буферизировать канал подписчика.
    /// При переполнении отправитель будет блокироваться (backpressure) или получать ошибку.
    pub channel_capacity: usize,
}

impl Default for SubscriberConfig {
    /// Создает новую конфигурацию подписчика со значениями по умолчанию.
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY
        }
    }
}

impl SubscriberConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Изменяет емкость канала (builder-паттерн).
    #[must_use]
    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        assert!(channel_capacity > 0, "channel_capacity must be greater than 0");
        self.channel_capacity = channel_capacity;
        self
    }    
}
