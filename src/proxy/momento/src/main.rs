// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

#[macro_use]
extern crate logger;

use ::config::{AdminConfig, MomentoProxyConfig, TimeType};
use backtrace::Backtrace;
use clap::{Arg, Command};
use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use logger::configure_logging;
use logger::Drain;
use metriken::*;
use momento::cache::{configurations, CollectionTtl};
use momento::*;
use pelikan_net::TCP_RECV_BYTE;
use protocol_admin::*;
use session::*;
use std::borrow::{Borrow, BorrowMut};
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::time::timeout;

use crate::error::{ProxyError, ProxyResult};

pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;

const S: u64 = 1_000_000_000; // one second in nanoseconds
const US: u64 = 1_000; // one microsecond in nanoseconds

mod admin;
mod error;
mod frontend;
mod klog;
mod listener;
mod protocol;

// NOTES:
//
// This is a simple proxy which translates requests between memcache protocol
// and Momento gRPC. This allows for a standard memcache client to communicate
// with the Momento cache service without any code changes.
//
// The following environment variables are necessary to configure the proxy
// until the config file is finalized:
//
// MOMENTO_AUTHENTICATION - the Momento authentication token

// the default buffer size is matched to the upper-bound on TLS fragment size as
// per RFC 5246 https://datatracker.ietf.org/doc/html/rfc5246#section-6.2.1
pub const INITIAL_BUFFER_SIZE: usize = 16 * KB;

// sets an upper bound on how large a request can be
pub const MAX_REQUEST_SIZE: usize = 100 * MB;

// The Momento cache client requires providing a default TTL. For the current
// implementation of the proxy, we don't actually let the client use the default,
// we always specify a TTL for each `set`.
const DEFAULT_TTL: Duration = Duration::from_secs(3600);

/// Default collection TTL policy used on collection operations.
///
/// Basically, we use the DEFAULT_TTL above and never update the TTL of the
/// item within the momento cache.
const COLLECTION_TTL: CollectionTtl = CollectionTtl::new(None, false);

// we interpret TTLs the same way memcached would
pub const TIME_TYPE: TimeType = TimeType::Memcache;

pub static PERCENTILES: &[(&str, f64)] = &[
    ("p25", 25.0),
    ("p50", 50.0),
    ("p75", 75.0),
    ("p90", 90.0),
    ("p99", 99.0),
    ("p999", 99.9),
    ("p9999", 99.99),
];

#[metric(name = "admin_request_parse")]
pub static ADMIN_REQUEST_PARSE: Counter = Counter::new();

#[metric(name = "admin_response_compose")]
pub static ADMIN_RESPONSE_COMPOSE: Counter = Counter::new();

#[metric(name = "backend_request")]
pub static BACKEND_REQUEST: Counter = Counter::new();

#[metric(name = "backend_ex")]
pub static BACKEND_EX: Counter = Counter::new();

#[metric(name = "backend_ex_rate_limited")]
pub static BACKEND_EX_RATE_LIMITED: Counter = Counter::new();

#[metric(name = "backend_ex_timeout")]
pub static BACKEND_EX_TIMEOUT: Counter = Counter::new();

#[metric(name = "ru_utime")]
pub static RU_UTIME: Counter = Counter::new();

#[metric(name = "ru_stime")]
pub static RU_STIME: Counter = Counter::new();

#[metric(name = "ru_maxrss")]
pub static RU_MAXRSS: Gauge = Gauge::new();

#[metric(name = "ru_ixrss")]
pub static RU_IXRSS: Gauge = Gauge::new();

#[metric(name = "ru_idrss")]
pub static RU_IDRSS: Gauge = Gauge::new();

#[metric(name = "ru_isrss")]
pub static RU_ISRSS: Gauge = Gauge::new();

#[metric(name = "ru_minflt")]
pub static RU_MINFLT: Counter = Counter::new();

#[metric(name = "ru_majflt")]
pub static RU_MAJFLT: Counter = Counter::new();

#[metric(name = "ru_nswap")]
pub static RU_NSWAP: Counter = Counter::new();

#[metric(name = "ru_inblock")]
pub static RU_INBLOCK: Counter = Counter::new();

#[metric(name = "ru_oublock")]
pub static RU_OUBLOCK: Counter = Counter::new();

#[metric(name = "ru_msgsnd")]
pub static RU_MSGSND: Counter = Counter::new();

#[metric(name = "ru_msgrcv")]
pub static RU_MSGRCV: Counter = Counter::new();

#[metric(name = "ru_nsignals")]
pub static RU_NSIGNALS: Counter = Counter::new();

#[metric(name = "ru_nvcsw")]
pub static RU_NVCSW: Counter = Counter::new();

#[metric(name = "ru_nivcsw")]
pub static RU_NIVCSW: Counter = Counter::new();

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // custom panic hook to terminate whole process after unwinding
    std::panic::set_hook(Box::new(|s| {
        error!("{}", s);
        println!("{:?}", Backtrace::new());
        std::process::exit(101);
    }));

    // parse command line options
    let matches = Command::new(env!("CARGO_BIN_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        // .version_short("v")
        .long_about(
            "A proxy that supports a limited subset of the Memcache protocol on
            the client side and communicates with Momento over gRPC to fulfill
            the requests.

            This allows use of the Momento cache offering without code changes
            for existing software which uses Memcached.

            The supported commands are limited to: get/set",
        )
        .arg(
            Arg::new("stats")
                .short('s')
                .long("stats")
                .help("List all metrics in stats")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("CONFIG")
                .help("Server configuration file")
                .action(clap::ArgAction::Set)
                .index(1),
        )
        .get_matches();

    // load config from file
    let config = if let Some(file) = matches.get_one::<String>("CONFIG") {
        match MomentoProxyConfig::load(file) {
            Ok(c) => c,
            Err(e) => {
                println!("{e}");
                std::process::exit(1);
            }
        }
    } else {
        Default::default()
    };

    // initialize logging
    let mut log_drain = configure_logging(&config);

    // validate config parameters
    for cache in config.caches() {
        let name = cache.cache_name();
        let ttl = cache
            .default_ttl()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX);
        let limit = u64::MAX / 1000;
        if ttl > limit {
            error!("default ttl of {ttl} for cache `{name}` is greater than {limit}");
            let _ = log_drain.flush();
            std::process::exit(1);
        }

        if let Err(e) = cache.socket_addr() {
            error!("listen address for cache `{name}` is not valid: {}", e);
            let _ = log_drain.flush();
            std::process::exit(1);
        }
    }

    // initialize metrics
    common::metrics::init();

    // output stats descriptions and exit if the `stats` option was provided
    if matches.get_flag("stats") {
        println!("{:<31} {:<15} DESCRIPTION", "NAME", "TYPE");

        let mut metrics = Vec::new();

        for metric in &metriken::metrics() {
            let any = match metric.as_any() {
                Some(any) => any,
                None => {
                    continue;
                }
            };

            if any.downcast_ref::<Counter>().is_some() {
                metrics.push(format!("{:<31} counter", metric.name()));
            } else if any.downcast_ref::<Gauge>().is_some() {
                metrics.push(format!("{:<31} gauge", metric.name()));
            } else if any.downcast_ref::<AtomicHistogram>().is_some()
                || any.downcast_ref::<RwLockHistogram>().is_some()
            {
                for (label, _) in PERCENTILES {
                    let name = format!("{}_{}", metric.name(), label);
                    metrics.push(format!("{name:<31} percentile"));
                }
            } else {
                continue;
            }
        }

        metrics.sort();
        for metric in metrics {
            println!("{metric}");
        }
        std::process::exit(0);
    }

    let mut runtime = Builder::new_multi_thread();

    runtime.thread_name_fn(|| {
        static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
        let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
        format!("pelikan_wrk_{id}")
    });

    if let Some(threads) = config.threads() {
        runtime.worker_threads(threads);
    }

    let runtime = runtime
        .enable_all()
        .build()
        .expect("failed to launch tokio runtime");

    runtime.block_on(spawn(config, log_drain))
}

async fn spawn(
    config: MomentoProxyConfig,
    mut log_drain: Box<dyn Drain>,
) -> Result<(), Box<dyn std::error::Error>> {
    let admin_addr = config
        .admin()
        .socket_addr()
        .expect("bad admin listen address");
    let admin_listener = TcpListener::bind(&admin_addr).await?;
    info!("starting proxy admin listener on: {}", admin_addr);

    // initialize the Momento cache client
    if std::env::var("MOMENTO_AUTHENTICATION").is_err() {
        eprintln!("environment variable `MOMENTO_AUTHENTICATION` is not set");
        std::process::exit(1);
    }
    let auth_token =
        std::env::var("MOMENTO_AUTHENTICATION").expect("MOMENTO_AUTHENTICATION must be set");
    let credential_provider = CredentialProvider::from_string(auth_token).unwrap_or_else(|e| {
        eprintln!("failed to initialize credential provider. error: {e}");
        std::process::exit(1);
    });

    if config.caches().is_empty() {
        error!("no caches specified in the config");
        let _ = log_drain.flush();
        std::process::exit(1);
    }

    for i in 0..config.caches().len() {
        let config = config.clone();

        let cache = config.caches().get(i).unwrap().clone();
        let addr = match cache.socket_addr() {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "bad listen address for cache `{}`: {}",
                    cache.cache_name(),
                    e
                );
                let _ = log_drain.flush();
                std::process::exit(1);
            }
        };

        let client_builder = CacheClient::builder()
            .default_ttl(DEFAULT_TTL)
            .configuration(configurations::Laptop::latest())
            .credential_provider(credential_provider.clone())
            .with_num_connections(cache.connection_count());

        let tcp_listener = match std::net::TcpListener::bind(addr) {
            Ok(v) => {
                if let Err(e) = v.set_nonblocking(true) {
                    error!(
                        "could not set tcp listener for cache `{}` on address `{}` as non-blocking: {}",
                        cache.cache_name(),
                        addr,
                        e
                    );
                    let _ = log_drain.flush();
                    std::process::exit(1);
                }
                v
            }
            Err(e) => {
                error!(
                    "could not bind tcp listener for cache `{}` on address `{}`: {}",
                    cache.cache_name(),
                    addr,
                    e
                );
                let _ = log_drain.flush();
                std::process::exit(1);
            }
        };

        tokio::spawn(async move {
            info!(
                "starting proxy frontend listener for cache `{}` on: {}",
                cache.cache_name(),
                addr
            );
            let tcp_listener =
                TcpListener::from_std(tcp_listener).expect("could not convert to tokio listener");
            listener::listener(
                tcp_listener,
                client_builder,
                cache.cache_name(),
                cache.protocol(),
            )
            .await;
        });
    }

    admin::admin(log_drain, admin_listener).await;
    Ok(())
}

async fn do_read(
    socket: &mut tokio::net::TcpStream,
    buf: &mut Buffer,
) -> Result<NonZeroUsize, Error> {
    match socket.read(buf.borrow_mut()).await {
        Ok(0) => {
            SESSION_RECV.increment();
            // zero length reads mean we got a HUP. close it
            Err(Error::from(ErrorKind::ConnectionReset))
        }
        Ok(n) => {
            SESSION_RECV.increment();
            SESSION_RECV_BYTE.add(n as _);
            TCP_RECV_BYTE.add(n as _);
            // non-zero means we have some data, mark the buffer as
            // having additional content
            unsafe {
                buf.advance_mut(n);
            }

            // if the buffer is low on space, we will grow the
            // buffer
            if buf.remaining_mut() * 2 < INITIAL_BUFFER_SIZE {
                buf.reserve(INITIAL_BUFFER_SIZE);
            }

            // SAFETY: we have already checked that the number of bytes read was
            // greater than zero, so this unchecked conversion is safe
            Ok(unsafe { NonZeroUsize::new_unchecked(n) })
        }
        Err(e) => {
            SESSION_RECV.increment();
            SESSION_RECV_EX.increment();
            // we has some other error reading from the socket,
            // return an error so the connection can be closed
            Err(e)
        }
    }
}

common::metrics::test_no_duplicates!();
