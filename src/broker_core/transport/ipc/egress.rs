// arcella-broker/src/broker_core/transport/ipc/egress.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::Bytes;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::protocol::Message;

use super::super::channel::{
    create_generic_channel,
    Receiver,
    Sender,
    SendError,
    TrySendError,
};

// ============================================================================
// EgressCommand
// ============================================================================

#[derive(Debug, Clone)]
pub enum EgressCommand {
    Data {
        /// Destination address (opaque to L2).
        address: Bytes,
        /// L3 messages to bundle into this frame.
        messages: Vec<Message>,
    },
}

// ============================================================================
// EgressTx / EgressRx
// ============================================================================

#[derive(Clone)]
pub struct EgressTx {
    inner: Sender<EgressCommand>,
    cancel_token: CancellationToken,
}

/// Consumer side, owned by the writer actor.
pub struct EgressRx {
    inner: Receiver<EgressCommand>,
    cancel_token: CancellationToken,
}

pub fn create_egress_channel(
    outgoing_hot_credit: NonZeroUsize,
    cancel_token: CancellationToken,
) -> (EgressTx, EgressRx) {
    let (tx, rx) = create_generic_channel(outgoing_hot_credit.get(), cancel_token.clone());

    (
        EgressTx {
            inner: tx,
            cancel_token: cancel_token.clone(),
        },
        EgressRx {
            inner: rx,
            cancel_token,
        },
    )
}

impl EgressTx {

    pub async fn send_data(
        &self,
        address: Bytes,
        messages: Vec<Message>,
    ) -> Result<(), SendError<EgressCommand>> {
        self.inner.send(EgressCommand::Data { address, messages }).await
    }

    #[inline]
    pub fn try_send_data(
        &self,
        address: Bytes,
        messages: Vec<Message>,
    ) -> Result<(), TrySendError<EgressCommand>> {
        self.inner
            .try_send(EgressCommand::Data { address, messages })
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    #[inline]
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    } 
}

impl EgressRx {
    pub async fn recv(&mut self) -> Option<EgressCommand> {
        self.inner.recv().await
    }

    #[inline]
    pub fn try_recv(&mut self) -> Option<EgressCommand> {
        self.inner.try_recv().ok()
    }

    #[inline]
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }            
}

#[derive(Debug)]
pub struct CreditCoalescer {
    /// Accumulated hot credits to be returned to the peer.
    deferred_hot_credits: AtomicU32,

    /// Accumulated cold credits to be returned to the peer.
    deferred_cold_credits: AtomicU32,

    /// Wakes the writer when a return becomes pending.
    wake: Notify,    
}

impl CreditCoalescer {
    pub fn new() -> Self {
        Self {
            deferred_hot_credits: AtomicU32::new(0),
            deferred_cold_credits: AtomicU32::new(0),
            wake: Notify::new(),
        }
    }

    #[inline]
    pub fn defer_hot_credit(&self, amount: u32) {
        if amount == 0 {
            return;
        }
        self.deferred_hot_credits.fetch_add(amount, Ordering::AcqRel);
        self.wake.notify_one();
    }

    #[inline]
    pub fn defer_cold_credit(&self, amount: u32) {
        if amount == 0 {
            return;
        }
        self.deferred_cold_credits.fetch_add(amount, Ordering::AcqRel);
        self.wake.notify_one();
    }

    #[inline]
    pub fn drain_hot_credits(&self) -> u32 {
         self.deferred_hot_credits.swap(0, Ordering::AcqRel)
    } 

    #[inline]
    pub fn drain_cold_credits(&self) -> u32 {
        self.deferred_cold_credits.swap(0, Ordering::AcqRel)
    }

    #[inline]
    pub fn pending_hot_credits(&self) -> u32 {
        self.deferred_hot_credits.load(Ordering::Acquire)
    }

    #[inline]
    pub fn pending_cold_credits(&self) -> u32 {
        self.deferred_cold_credits.load(Ordering::Acquire)
    } 

    #[inline]
    pub fn has_pending(&self) -> bool {
        self.pending_hot_credits() != 0 || self.pending_cold_credits() != 0
    }

    pub async fn notified(&self) {
        self.wake.notified().await
    }                             
}

impl Default for CreditCoalescer {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }    

    fn channel(capacity: usize) -> (EgressTx, EgressRx, CancellationToken) {
        let token = CancellationToken::new();
        let (tx, rx) = create_egress_channel(nz(capacity), token.clone());  
        (tx, rx, token)
    }

    #[tokio::test]
    async fn egress_channel_send_and_recv() {
        let (tx, mut rx, _token) = channel(2);

        let addr = Bytes::from("test:address");
        let messages = vec![];
        
        tx.try_send_data(addr.clone(), messages).unwrap();

        match rx.recv().await {
            Some(EgressCommand::Data { address, messages: _ }) => {
                assert_eq!(address, addr);
            }
            _ => panic!("Expected Data command"),
        }
    }

    #[tokio::test]
    async fn egress_channel_bounded() {
        let (tx, _rx, _token) = channel(1);

        let addr = Bytes::from("test");
        let messages = vec![];

        tx.try_send_data(addr.clone(), messages.clone()).unwrap();

        let err = tx.try_send_data(addr, messages).unwrap_err();
        assert!(matches!(err, TrySendError::Full(_)));
    }

    #[tokio::test]
    async fn egress_channel_cancellation() {
        let (_tx, mut rx, token) = channel(2);
        token.cancel();
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn disconnected_when_writer_is_gone() {
        let (tx, rx, _token) = channel(2);
        drop(rx);

        assert!(tx.is_closed());
        let err = tx.try_send_data(Bytes::from("a"), vec![]).unwrap_err();
        assert!(matches!(err, TrySendError::Disconnected(_)));
    }

    #[tokio::test]
    async fn egress_lane_is_fifo() {
        let (tx, mut rx, _token) = channel(3);
        for a in ["a", "b", "c"] {
            tx.try_send_data(Bytes::from(a), vec![]).unwrap();
        }
        for a in ["a", "b", "c"] {
            match rx.recv().await {
                Some(EgressCommand::Data { address, .. }) => assert_eq!(address, Bytes::from(a)),
                None => panic!("unexpected end of lane"),
            }
        }
    }

    #[test]
    fn credit_coalescer_accumulates_and_drains() {
        let coalescer = CreditCoalescer::new();

        coalescer.defer_hot_credit(5);
        coalescer.defer_hot_credit(3);
        assert_eq!(coalescer.pending_hot_credits(), 8);

        let drained = coalescer.drain_hot_credits();
        assert_eq!(drained, 8);
        assert_eq!(coalescer.pending_hot_credits(), 0);
    }

    #[test]
    fn credit_coalescer_separate_hot_and_cold() {
        let coalescer = CreditCoalescer::new();

        coalescer.defer_hot_credit(10);
        coalescer.defer_cold_credit(5);

        assert_eq!(coalescer.drain_hot_credits(), 10);
        assert_eq!(coalescer.drain_cold_credits(), 5);
    }


    #[tokio::test]
    async fn coalescer_wakes_the_writer() {
        let coalescer = CreditCoalescer::new();

        // Wake-up delivered to an already-registered waiter.
        let c = std::sync::Arc::new(coalescer);
        let c2 = std::sync::Arc::clone(&c);
        let handle = tokio::spawn(async move {
            c2.notified().await;
            c2.drain_hot_credits()
        });
        tokio::task::yield_now().await;
        c.defer_hot_credit(7);
        assert_eq!(handle.await.unwrap(), 7);
    }

    #[tokio::test]
    async fn coalescer_wake_is_not_lost_when_defer_precedes_await() {
        let coalescer = CreditCoalescer::new();
        coalescer.defer_hot_credit(1);

        tokio::time::timeout(std::time::Duration::from_millis(200), coalescer.notified())
            .await
            .expect("stored permit must resolve immediately");
        assert_eq!(coalescer.drain_hot_credits(), 1);
    }

    #[tokio::test]
    async fn zero_defer_does_not_wake() {
        let coalescer = CreditCoalescer::new();
        coalescer.defer_hot_credit(0);
        coalescer.defer_cold_credit(0);
        assert!(!coalescer.has_pending());

        let woke = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            coalescer.notified(),
        )
        .await;
        assert!(woke.is_err());
    }
}
