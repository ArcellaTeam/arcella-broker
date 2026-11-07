// arcella-broker/src/test_utils.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing_subscriber::{fmt, EnvFilter};

static TRACING_INIT: OnceLock<()> = OnceLock::new();

use crate::{
    broker::Broker,
    client::BrokerClient,
    config::ClientConfig,
    error::BrokerError,
};

pub use crate::protocol::test_utils::*;

use crate::broker_core::{
    l2::{IngressBridge, IngressResult},
    security::{SessionContext},
};

/// Create a test client with a local reestr
pub fn test_client(address: String) -> Result<BrokerClient, BrokerError> {
    // 1. Initialize config, broker and client
    let broker_config = Broker::default_config();
    let broker = Arc::new(Broker::new(broker_config));
    let client_config = ClientConfig::default();
    broker.client(client_config, address)
}

pub fn init_tracing(directives: &str) {
    TRACING_INIT.get_or_init(|| {    
        // Attempt to read the logging level from RUST_LOG,
        // if not set, default to "info"
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(directives));

        fmt()
            .with_env_filter(env_filter)
            .with_thread_ids(true)       // Useful for multithreaded tests (shows thread ID)
            .with_target(true)           // Optional: hides the long module path for brevity
            .with_level(true)            // Shows the level (DEBUG, TRACE, INFO)
            .init();
    });    
}

// =========================================================================
// Test-only IngressBridge Implementation
// =========================================================================

/// Outcome returned by the stub bridge for a processed L2 Data frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StubOutcome {
    Accepted,
    AddressBlocked,
    Rejected,
}

/// One step in the stub behavior queue.
///
/// NOTE: `frames` counts L2 Data frames, not L3 messages.
#[derive(Debug, Clone, Copy)]
pub struct StubModeStep {
    pub frames: usize,
    pub delay_ms: u64,
    pub outcome: StubOutcome,
}

impl StubModeStep {
    pub fn accept(frames: usize) -> Self {
        Self { frames, delay_ms: 0, outcome: StubOutcome::Accepted }
    }

    pub fn delay(delay_ms: u64, frames: usize) -> Self {
        Self { frames, delay_ms, outcome: StubOutcome::Accepted }
    }

    pub fn block(frames: usize) -> Self {
        Self { frames, delay_ms: 0, outcome: StubOutcome::AddressBlocked }
    }

    #[allow(dead_code)]
    pub fn reject(frames: usize) -> Self {
        Self { frames, delay_ms: 0, outcome: StubOutcome::Rejected }
    }
}

struct StubState {
    steps: VecDeque<StubModeStep>,
    default_delay_ms: u64,
    default_outcome: StubOutcome,
}

/// Test-only stub implementation of IngressBridge for integration tests.
///
/// Supports a queue of modes and dynamic switching during the test.
pub struct StubIngressBridge {
    state: Mutex<StubState>,
    frame_counter: std::sync::atomic::AtomicUsize,
}

impl StubIngressBridge {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(StubState {
                steps: VecDeque::new(),
                default_delay_ms: 0,
                default_outcome: StubOutcome::Accepted,
            }),
            frame_counter: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Sets a queue of behavior steps.
    pub fn with_steps(self, steps: Vec<StubModeStep>) -> Self {
        {
            let mut state = self.state.lock().unwrap();
            state.steps = steps.into_iter().filter(|s| s.frames > 0).collect();
        }
        self
    }

    /// Sets the default mode used when the step queue is empty.
    pub fn with_default_mode(self, delay_ms: u64, outcome: StubOutcome) -> Self {
        {
            let mut state = self.state.lock().unwrap();
            state.default_delay_ms = delay_ms;
            state.default_outcome = outcome;
        }
        self
    }

    /// Compatibility: delay every frame and then accept it.
    pub fn with_delay(self, delay_ms: u64) -> Self {
        self.with_default_mode(delay_ms, StubOutcome::Accepted)
    }

    /// Compatibility: accept first `n` frames, then block the rest.
    pub fn with_blocked_after(self, n: usize) -> Self {
        {
            let mut state = self.state.lock().unwrap();
            state.steps.clear();
            if n > 0 {
                state.steps.push_back(StubModeStep::accept(n));
            }
            state.default_delay_ms = 0;
            state.default_outcome = StubOutcome::AddressBlocked;
        }
        self
    }

    /// Adds a step to the end of the queue during a test.
    #[allow(dead_code)]
    pub fn push_step(&self, step: StubModeStep) {
        if step.frames == 0 { return; }
        let mut state = self.state.lock().unwrap();
        state.steps.push_back(step);
    }

    /// Clears the queue and switches the stub to immediate accept mode.
    /// Useful for Stage 5.4: start with delay, then switch to accept to simulate recovery.
    pub fn switch_to_accept(&self) {
        let mut state = self.state.lock().unwrap();
        state.steps.clear();
        state.default_delay_ms = 0;
        state.default_outcome = StubOutcome::Accepted;
    }    

    /// Clears the queue and switches the stub to delayed accept mode.
    pub fn switch_to_delay(&self, delay_ms: u64) {
        let mut state = self.state.lock().unwrap();
        state.steps.clear();
        state.default_delay_ms = delay_ms;
        state.default_outcome = StubOutcome::Accepted;
    }

    /// Clears the queue and switches the stub to address-blocked mode.
    #[allow(dead_code)]
    pub fn switch_to_block(&self) {
        let mut state = self.state.lock().unwrap();
        state.steps.clear();
        state.default_delay_ms = 0;
        state.default_outcome = StubOutcome::AddressBlocked;
    }

    /// Number of frames for which `process` has been called.
    #[allow(dead_code)]
    pub fn observed_frames(&self) -> usize {
        self.frame_counter.load(Ordering::SeqCst)
    }

} 

impl IngressBridge for StubIngressBridge {
    fn process<'a, C>(
        &'a self,
        _address: &'a Bytes,
        _messages_raw: &'a Bytes,
        _session_ctx: &'a C,
    ) -> impl std::future::Future<Output = IngressResult> + Send + 'a
    where
        C: SessionContext + ?Sized
    {
        async move {
            let step = {
                let mut state = self.state.lock().unwrap();
                loop {
                    if let Some(front) = state.steps.front_mut() {
                        if front.frames == 0 {
                            state.steps.pop_front();
                            continue;
                        }
                        let current = *front;
                        front.frames = front.frames.saturating_sub(1);
                        if front.frames == 0 {
                            state.steps.pop_front();
                        }
                        break current;
                    } else {
                        break StubModeStep {
                            frames: usize::MAX,
                            delay_ms: state.default_delay_ms,
                            outcome: state.default_outcome,
                        };
                    }
                }
            };

            self.frame_counter.fetch_add(1, Ordering::SeqCst);

            if step.delay_ms > 0 {
                sleep(Duration::from_millis(step.delay_ms)).await;
            }

            match step.outcome {
                StubOutcome::Accepted => IngressResult::Accepted,
                StubOutcome::AddressBlocked => IngressResult::AddressBlocked,
                StubOutcome::Rejected => IngressResult::Rejected("Stub rejected".to_string()),
            }
        }
    }
}       
