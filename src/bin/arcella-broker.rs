// arcella-broker/src/bin/arcella-broker.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use arcella_broker::protocol;


/// Arcella Broker — microbroker Arcella
#[derive(Parser)]
#[command(version, about = "Arcella Broker — microbroker Arcella", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {

    /// Get the broker status
    Status,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let _: Cli = Cli::parse(); 
    //handle_command(cli.command).await
    Ok(())
}