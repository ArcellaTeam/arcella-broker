// arcella-broker/src/bin/arcella-broker.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tracing_subscriber::{fmt, EnvFilter};

use arcella_broker::broker::Broker;
use arcella_broker::config::{BrokerConfig, ServerConfig};

/// Arcella Microbroker CLI
#[derive(Parser)]
#[command(name = "arcella-broker")]
#[command(about = "Arcella Microbroker — isolated message routing process")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {

    /// Start the broker server
    Serve {
        /// Path to Unix Domain Socket
        #[arg(short, long, default_value = "/tmp/arcella-broker.sock")]
        socket: PathBuf,

        /// Log level (trace, debug, info, warn, error)
        #[arg(short, long, default_value = "info")]
        log_level: String,    

        /// Use JSON format for logs
        #[arg(long, default_value_t = false)]
        json: bool,

        /// Hot credit (number of in-flight frames per session)
        #[arg(long, default_value_t = 50)]
        hot_credit: u32,

        /// Cold credit (backpressure buffer slots)
        #[arg(long, default_value_t = 10)]
        cold_credit: u32,
    }    
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse(); 
    
    match cli.command {
        Commands::Serve {
            socket,
            log_level,
            json,
            hot_credit,
            cold_credit,
        } => {
            let filter = EnvFilter::try_new(&log_level)
                .unwrap_or_else(|_| EnvFilter::new("info"));            

            if json {
                fmt::Subscriber::builder()
                    .json()
                    .with_env_filter(filter)
                    .with_target(true)
                    .with_thread_ids(true)
                    .init();
            } else {
                fmt::Subscriber::builder()
                    .with_env_filter(filter)
                    .with_target(true)
                    .with_thread_ids(true)
                    .init();
            }

            tracing::info!(
                ?socket,
                hot_credit,
                cold_credit,
                "Starting Arcella broker server"
            );

            if socket.exists() {
                std::fs::remove_file(&socket)?;
            }            

            // === Server configuration ===
            let mut limits = arcella_broker::protocol::ServerLimits::default();
            limits.hot_credit = hot_credit;
            limits.cold_credit = cold_credit;
            let server_config = ServerConfig {
                socket_path: socket.clone(),
                limits,
                ..Default::default()
            };

            // === Create broker as server ===
            let broker_config = BrokerConfig::default();
            let broker = Arc::new(Broker::as_server(broker_config, server_config));
            let broker_clone = Arc::clone(&broker);

            // === Graceful shutdown по Ctrl+C ===
            tokio::spawn(async move {
                if let Err(e) = signal::ctrl_c().await {
                    tracing::error!(error = %e, "Failed to listen for Ctrl+C");
                    return;
                }
                tracing::info!("Received shutdown signal (Ctrl+C)");
                broker_clone.shutdown();
            });

            tracing::info!("Broker server is running. Press Ctrl+C to stop.");
            broker.run().await?;
            tracing::info!("Broker server stopped gracefully");

            Ok(())
        }
    }

}