// arcella-broker/src/tests/load_test.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;
use std::time::{Duration, Instant};
use bytes::Bytes;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing_subscriber::{fmt, EnvFilter};

use arcella_broker::{
    broker::Broker,
    config::{ClientConfig, SubscriberConfig},
    protocol::{Message, TransferMode},
    broker_core::registry::RoutingPolicy,
};

// ============================================================================
// Test Configuration
// ============================================================================
const NUM_RECEIVERS: usize = 100;
const NUM_SENDERS: usize = 100;
const MESSAGES_PER_SENDER: usize = 1_000_000;
const TOTAL_MESSAGES: usize = NUM_SENDERS * MESSAGES_PER_SENDER;

fn init_tracing() {
    // Attempt to read the logging level from RUST_LOG,
    // if not set, default to "info"
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("error"));

    fmt()
        .with_env_filter(env_filter)
        .with_thread_ids(true)       // Useful for multithreaded tests (shows thread ID)
        .with_target(true)           // Optional: hides the long module path for brevity
        .with_level(true)            // Shows the level (DEBUG, TRACE, INFO)
        .init();
}

/// High-throughput load test for the in-memory broker routing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_high_throughput_in_memory_routing() {

    init_tracing();

    // 1. Initialize config, broker and client
    let broker_config = Broker::default_config();
    let broker = Arc::new(Broker::new(broker_config));
    let client_config = ClientConfig::default();
    let recv_client = broker.client(client_config.clone(), "load:test".to_string()).unwrap();

    let mut subscriber_config = SubscriberConfig::default()
        .with_channel_capacity(4096)
        .expect("Channel capacity 4096 should be valid");
    subscriber_config.routing_policy = RoutingPolicy::Exclusive;
    //subscriber_config.routing_policy = RoutingPolicy::LoadBalanced;

    let mut receiver_addresses = Vec::with_capacity(NUM_RECEIVERS);
    let mut message_templates = Vec::with_capacity(NUM_RECEIVERS);    
        
    for i in 0..NUM_RECEIVERS {
        let addr_str = format!("arcella:{}:perf:recv", i);
        let addr_bytes = Bytes::from(addr_str.clone());
        
        receiver_addresses.push(addr_str);

        let msg = Message::new(
            TransferMode::InOnly,
            [0u8; 32], // session_token
            [0u8; 16], // message_id (will be mutated per message)
            [0u8; 4],  // sub_message_id
            0,         // priority
            64,        // ttl
            Bytes::from_static(b"perf:test"),
            addr_bytes,
            Bytes::new(),
            Bytes::from_static(b"performance test payload data"),
        ).expect("Message creation should not fail");

        message_templates.push(msg);
    }

    let mut receiver_task_handles = JoinSet::new();
    let mut subscriber_handles = Vec::new();

    for i in 0..NUM_RECEIVERS {

        let addr = receiver_addresses[i].clone(); 

        let (mut subscriber, handle) = recv_client
            .subscribe(
                addr.clone(),
                subscriber_config.clone(),
            )
            .expect("Failed to subscribe");
        subscriber_handles.push(handle);

        // Spawn a dedicated task for each receiver to consume messages
        receiver_task_handles.spawn(async move {
            tracing::trace!("Test receiver: start task on {}", addr.clone());

            let mut count = 0;
            // The loop will terminate when the channel is closed (all senders dropped)
            while let Some(_msg) = subscriber.recv().await {
                count += 1;
            }
            tracing::trace!("Test receiver: stop task on {}", addr.clone());
            count
        });
    }

    // 5. Main Load Test: Spawn Sender tasks and measure dispatch time
    let start_time = Instant::now();
    let mut sender_task_handles = JoinSet::new();
    
    for sender_id in 0..NUM_SENDERS {
        let target_idx = (sender_id + 50) % NUM_RECEIVERS;
        let addr_str = format!("arcella:{}:perf:recv", target_idx);

        let broker = Arc::clone(&broker);
        let client_config = client_config.clone();
        let base_msg = message_templates[target_idx].clone();        

        sender_task_handles.spawn(async move {
            let sender_addr = format!("arcella:load:test:{}", sender_id);
            tracing::trace!("Test sender: start task on {}", sender_addr);
            let client = broker.client(client_config.clone(), sender_addr.clone()).unwrap();

            // The Publisher is created ONCE per task, which activates and tests its internal cache
            let publisher = client.publisher(addr_str.clone());
                
            for seq in 0..MESSAGES_PER_SENDER {

                let mut msg = base_msg.clone();
                msg.header.message_id[0] = (sender_id % 256) as u8;
                msg.header.message_id[1] = ((seq >> 16) & 0xFF) as u8;
                msg.header.message_id[2] = ((seq >> 8) & 0xFF) as u8;
                msg.header.message_id[3] = (seq & 0xFF) as u8;
                
                publisher.send(msg).await.expect("Send should succeed");
            }
            tracing::trace!("Test sender: stop task on {}", sender_addr);
        });
    }

    // 6. Wait for all senders to finish dispatching
    tracing::trace!("Wait for sender's tasks");
    while let Some(res) = sender_task_handles.join_next().await {
        res.expect("Sender task panicked");
    }
    let dispatch_duration = start_time.elapsed();

    sleep(Duration::from_millis(5)).await;

    // 7. Unbind receivers to close their channels and signal them to terminate
    tracing::trace!("Unbind receiver");
    drop(subscriber_handles);
    drop(recv_client);

    // 8. Wait for all receivers to finish processing and sum up received messages
    tracing::trace!("Wait for receiver's tasks");
    let mut total_received = 0;
    while let Some(res) = receiver_task_handles.join_next().await {
        total_received += res.expect("Receiver task panicked");
    }
    let total_duration = start_time.elapsed();

    // 9. Assertions and Performance Metrics
    assert_eq!(
        total_received, TOTAL_MESSAGES,
        "All messages must be delivered without loss"
    );
    
    let throughput = (TOTAL_MESSAGES as f64) / total_duration.as_secs_f64();
    
    println!("\n==================================================");
    println!("       Arcella Broker Load Test Results           ");
    println!("==================================================");
    println!("Total Messages:      {}", TOTAL_MESSAGES);
    println!("Senders (Tasks):     {}", NUM_SENDERS);
    println!("Receivers (Tasks):   {}", NUM_RECEIVERS);
    println!("--------------------------------------------------");
    println!("Time to dispatch:    {:?}", dispatch_duration);
    println!("Total delivery time: {:?}", total_duration);
    println!("Throughput:          {:.0} msg/sec", throughput);
    println!("==================================================\n");
}
