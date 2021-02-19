// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

pub use rustcommon_fastmetrics::*;
use strum::IntoEnumIterator;
use strum_macros::{AsRefStr, EnumIter};

pub type Metrics = rustcommon_fastmetrics::Metrics<Stat>;

// As a temporary work-around for requiring all metrics to have a common type,
// we combine server and storage metrics here.

/// Defines various statistics
#[derive(Copy, Clone, Debug, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum Stat {
    // server/twemcache-rs
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

    // storage/segcache
    HashLookup,
    HashInsert,
    HashRemove,
    HashTagCollision,
    HashArrayAlloc,
    ItemCurrent,
    ItemCurrentBytes,
    ItemAlloc,
    ItemAllocEx,
    SegmentRequest,
    SegmentRequestEx,
    SegmentReturn,
    SegmentEvict,
    SegmentEvictRetry,
    SegmentEcictEx,
    SegmentExpire,
    SegmentMerge,
    SegmentCurrent,
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

impl Metric for Stat {
    fn source(&self) -> Source {
        match self {
            Stat::Pid | Stat::RuMaxrss | Stat::RuIxrss | Stat::RuIdrss | Stat::RuIsrss => {
                Source::Gauge
            }
            _ => Source::Counter,
        }
    }

    fn index(&self) -> usize {
        (*self).into()
    }
}

pub fn init() {
    let metrics: Vec<Stat> = Stat::iter().collect();
    MetricsBuilder::<Stat>::new()
        .metrics(&metrics)
        .build()
        .unwrap();

    set_gauge!(&Stat::Pid, std::process::id().into());
}
