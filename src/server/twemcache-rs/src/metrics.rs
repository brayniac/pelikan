// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

pub use rustcommon_fastmetrics::{Metric, MetricsBuilder};
use strum::IntoEnumIterator;
use strum_macros::{AsRefStr, EnumIter};

use std::sync::Arc;

pub type Metrics = rustcommon_fastmetrics::Metrics<Stat>;

/// Defines various statistics
#[derive(Copy, Clone, Debug, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum Stat {
    Add,
    AddNotstored,
    AddStored,
    AdminEventError,
    AdminEventLoop,
    AdminEventRead,
    AdminEventTotal,
    AdminEventWrite,
    AdminRequestParse,
    AdminRequestParseEx,
    AdminResponseCompose,
    AdminResponseComposeEx,
    Delete,
    DeleteDeleted,
    DeleteNotfound,
    Get,
    GetKey,
    GetKeyHit,
    GetKeyMiss,
    GetEx,
    Set,
    SetNotstored,
    SetStored,
    SetEx,
    Pid,
    ProcessReq,
    ProcessEx,
    ProcessServerEx,
    Replace,
    ReplaceNotstored,
    ReplaceStored,
    RequestParse,
    RequestParseEx,
    ResponseCompose,
    ResponseComposeEx,
    RuUtime,
    RuStime,
    RuMaxrss,
    RuIxrss,
    RuIdrss,
    RuIsrss,
    RuMinflt,
    RuMajflt,
    RuNswap,
    RuInblock,
    RuOublock,
    RuMsgsnd,
    RuMsgrcv,
    RuNsignals,
    RuNvcsw,
    RuNivcsw,
    ServerEventError,
    ServerEventLoop,
    ServerEventRead,
    ServerEventTotal,
    ServerEventWrite,
    SessionRecv,
    SessionRecvByte,
    SessionRecvEx,
    SessionSend,
    SessionSendByte,
    SessionSendEx,
    TcpAccept,
    TcpAcceptEx,
    TcpClose,
    TcpConnect,
    TcpConnectEx,
    TcpRecv,
    TcpRecvByte,
    TcpRecvEx,
    TcpReject,
    TcpRejectEx,
    TcpSend,
    TcpSendByte,
    TcpSendEx,
    WorkerEventError,
    WorkerEventLoop,
    WorkerEventRead,
    WorkerEventTotal,
    WorkerEventWake,
    WorkerEventWrite,
}

impl Into<usize> for Stat {
    fn into(self) -> usize {
        self as usize
    }
}

impl std::fmt::Display for Stat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl Metric for Stat {}

pub fn init() -> Arc<Metrics> {
    let mut builder = MetricsBuilder::new();
    for metric in Stat::iter() {
        match metric {
            Stat::Pid | Stat::RuMaxrss | Stat::RuIxrss | Stat::RuIdrss | Stat::RuIsrss => {
                builder = builder.gauge(metric)
            }
            _ => {
                builder = builder.counter(metric);
            }
        }
    }

    let metrics = builder.build();

    metrics.record_gauge(Stat::Pid, std::process::id().into());

    Arc::new(metrics)
}
