// arcella-broker/src/broker_core/transport/ipc/mod.rs
//
// Copyright (c) 2026 Arcella Team
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE>
// or the MIT license <LICENSE-MIT>, at your option.
// This file may not be copied, modified, or distributed
// except according to those terms.

mod connector;
mod control;
mod endpoint;
mod transport;
mod uds;

pub use connector::IPCConnector;
pub use control::{
    create_control_channel,
    ControlCommand,
    ControlRx,
    ControlTx,
    PongEcho,
};
pub use endpoint::{
    EndpointStateAccess,
    EndpointStateExt,
    EndpointWriter,
    EndpointWriterExt,
    IPCEndpoint,
    IPCEndpointReader,
    IPCEndpointWriter,
    FrameSender,
};
pub use transport::IPCFramedStream;
pub use uds::UdsConnector;

#[cfg(test)]
mod integration_test;
