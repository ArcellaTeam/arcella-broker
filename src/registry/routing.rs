// arcella-broker/src/registry/routing.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use parking_lot::{RwLock, RwLockReadGuard};

use crate::protocol::Message;
use crate::transport::{
    TransportError, 
};

use super::SubscriptionSlot;

/// Политика доставки сообщений
pub enum RoutingPolicy {
    Exclusive,      // Только один подписчик
    //LoadBalanced,   // Round-Robin между подписчиками
    //Broadcast,      // Fan-out всем подписчикам
}

/// Цель маршрутизации (zero-cost для одиночных подписок)
pub enum RouteTarget {
    Single(Arc<SubscriptionSlot>),
    //LoadBalanced(Arc<LoadBalancedGroup>),
    //Broadcast(Arc<BroadcastGroup>),
}

impl RouteTarget {
    /// Отправка сообщения. Делегирует либо напрямую в канал, либо в группу.
    pub async fn send(&self, message: Message) -> Result<(), TransportError> {
        match self {
            Self::Single(slot) => {
                let  guard = slot.sender.load();
                if let Some(sender) = guard.as_ref() {
                    sender.send(message).await.map_err(|_| TransportError::ConnectionClosed)
                } else {
                    Err(TransportError::ConnectionClosed)
                }
            }
            //Self::LoadBalanced(group) => group.send(message).await,
            //Self::Broadcast(group) => group.send(message).await,
        }
    }

    pub fn remove_slot(&self, slot: &Arc<SubscriptionSlot>) -> bool {
        tracing::debug!("RouteTarget remove_slot");
        match self {
            Self::Single(existing_slot) => {
                if Arc::ptr_eq(existing_slot, slot) {
                    existing_slot.mark_removed(); // Мгновенная инвалидация кэша!
                    true // Нужно удалить из дерева
                } else {
                    false
                }
            }
            /*Self::LoadBalanced(group) | Self::Broadcast(group) => {
                group.remove_member(slot);
                group.is_empty() // Нужно удалить из дерева, если участников не осталось
            }*/
        }
    }    

    /// Уникальная версия для защиты кэша (TOCTOU).
    pub fn version(&self) -> u64 {
        match self {
            Self::Single(slot) => slot.version.load(std::sync::atomic::Ordering::Acquire),
            //Self::LoadBalanced(group) => group.version(),
            //Self::Broadcast(group) => group.version(),
        }
    }

    /// Проверка "живости" для инвалидации кэша.
    pub fn is_closed(&self) -> bool {
        match self {
            Self::Single(slot) => slot.is_closed(),
            //Self::LoadBalanced(group) => group.is_empty(),
            //Self::Broadcast(group) => group.is_empty(),
        }
    }
}

/// Базовая структура для управления группой подписчиков.
/// Инкапсулирует общую логику: добавление/удаление участников, версионирование.
pub struct SubscriptionGroupBase {
    /// Список активных слотов (физических каналов).
    pub(crate) members: RwLock<Vec<Arc<SubscriptionSlot>>>,
    
    /// Версия группы. Инкрементируется при добавлении/удалении участников.
    /// Необходима для инвалидации кэша в InMemoryEndpoint (защита от TOCTOU).
    version: AtomicU64,  // Для инвалидации кэша TOCTOU
}

impl SubscriptionGroupBase {
    pub fn new(first_slot: Arc<SubscriptionSlot>) -> Self {
        Self {
            members: RwLock::new(vec![first_slot]),
            version: AtomicU64::new(1),
        }
    }

    /// Добавляет нового подписчика в группу.
    /// Это "холодный" путь, поэтому синхронная блокировка parking_lot допустима и предпочтительна.
    pub fn add_member(&self, slot: Arc<SubscriptionSlot>) {
        let mut members = self.members.write();
        members.push(slot);
        self.version.fetch_add(1, Ordering::Release);
    }

    /// Удаляет подписчика из группы.
    pub fn remove_member(&self, slot_ptr: *const SubscriptionSlot) {
        let mut members = self.members.write();
        members.retain(|s| Arc::as_ptr(s) != slot_ptr);
        self.version.fetch_add(1, Ordering::Release);
    }

    /// Возвращает текущую версию группы для проверки валидности кэша.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Проверяет, пуста ли группа (все ли подписчики отключились).
    pub fn is_empty(&self) -> bool {
        self.members.read().is_empty()
    }

    /// Безопасный способ выполнения операции над списком участников.
    pub fn with_members<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Vec<Arc<SubscriptionSlot>>) -> R,
    {
        let members = self.members.read();
        f(&members)
    }
}
