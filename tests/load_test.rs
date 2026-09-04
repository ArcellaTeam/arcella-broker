// arcella-broker/src/tests/load_test.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;
use std::time::Instant;
use bytes::Bytes;

use arcella_broker::{
    broker::Broker,
    config::SubscriberConfig,
    protocol::{Message, TransferMode},
};

// ============================================================================
// Test Configuration
// ============================================================================
const NUM_RECEIVERS: usize = 100;
const NUM_SENDERS: usize = 100;
const MESSAGES_PER_SENDER: usize = 1000000;
const TOTAL_MESSAGES: usize = NUM_SENDERS * MESSAGES_PER_SENDER;

/// High-throughput load test for the in-memory broker routing.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_high_throughput_in_memory_routing() {
    // 1. Initialize config, broker and client
    let config = Broker::default_config();
    let broker = Arc::new(Broker::new(config));
    let client = broker.client();

    // 2. Register Receivers and spawn receiver tasks
    let mut receiver_addresses = Vec::with_capacity(NUM_RECEIVERS);
    let mut message_templates = Vec::with_capacity(NUM_RECEIVERS);    
    let mut receiver_handles = Vec::with_capacity(NUM_RECEIVERS);

    let subscriber_config = SubscriberConfig::new().with_channel_capacity(4096);
    
    for i in 0..NUM_RECEIVERS {
        let addr_str = format!("arcella:perf:recv:{}", i);
        let addr_bytes = Bytes::from(addr_str.clone());
        
        receiver_addresses.push(addr_str);

        let msg = Message::new(
            TransferMode::InOnly,
            [0u8; 32], // session_token (в реальном сценарии выдается коннектором)
            [0u8; 16], // message_id (будет перезаписан в цикле для уникальности)
            [0u8; 4],  // sub_message_id
            0,         // priority
            64,        // ttl
            Bytes::from_static(b"perf:test"),
            addr_bytes,
            Bytes::from_static(b"performance test payload data"),
        ).expect("Message creation should not fail");

        message_templates.push(msg);
    }

    for i in 0..NUM_RECEIVERS {

        let addr = receiver_addresses[i].clone(); 

        let mut subscriber = client.subscribe(addr, subscriber_config.clone())
            .expect("Failed to subscribe");

        // Spawn a dedicated task for each receiver to consume messages
        let handle = tokio::spawn(async move {
            let mut count = 0;
            // The loop will terminate when the channel is closed (all senders dropped)
            while let Some(_msg) = subscriber.recv().await {
                count += 1;
            }
            count
        });
        receiver_handles.push(handle);
    }

    // 3. Spawn Sender tasks and measure dispatch time
    let start_time = Instant::now();
    let mut sender_handles = Vec::with_capacity(NUM_SENDERS);
    
    for sender_id in 0..NUM_SENDERS {
        let client = broker.client();
        let target_idx = (sender_id + 50) % NUM_RECEIVERS;
        let addr_str = format!("arcella:perf:recv:{}", target_idx);

        let publisher = client.publisher(addr_str.clone());
        let mut base_msg = message_templates[target_idx].clone();

        let handle = tokio::spawn(async move {
                
            for seq in 0..MESSAGES_PER_SENDER {

                let mut msg = base_msg.clone();
                msg.header.message_id[0] = (sender_id % 256) as u8;
                msg.header.message_id[1] = ((seq >> 16) & 0xFF) as u8;
                msg.header.message_id[2] = ((seq >> 8) & 0xFF) as u8;
                msg.header.message_id[3] = (seq & 0xFF) as u8;
                
                publisher.send(msg).await.expect("Send should succeed");
            }
        });
        sender_handles.push(handle);
    }

    // 4. Wait for all senders to finish dispatching
    for handle in sender_handles {
        handle.await.expect("Sender task panicked");
    }
    let dispatch_duration = start_time.elapsed();

    // 5. Unbind receivers to close their channels and signal them to terminate
    for i in 0..NUM_RECEIVERS {
        let addr = format!("arcella:perf:recv:{}", i);
        client.unbind(&addr).unwrap();
    }

    // 6. Wait for all receivers to finish processing and sum up received messages
    let mut total_received = 0;
    for handle in receiver_handles {
        total_received += handle.await.expect("Receiver task panicked");
    }
    let total_duration = start_time.elapsed();

    // 7. Assertions and Performance Metrics
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
