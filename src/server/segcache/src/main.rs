// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Segcache is an implementation of a cache backend that implements a subset of
//! the Memcache ASCII protocol and is backed with segment based storage. By
//! grouping items with a similar TTL, it is able to provide efficient eager
//! expiration.
//!
//! More details about the benefits of this design can be found in this
//! [blog post](https://twitter.github.io/pelikan/2021/segcache.html).
//!
//! Running this binary is the primary way of using Segcache.

#[macro_use]
extern crate logger;

use std::sync::atomic::AtomicBool;
use protocol_memcache::Response;
use server::ProcessBuilder;
use protocol_memcache::Request;
use protocol_memcache::RequestParser;
use entrystore::Seg;
// use protocol_memcache::Storage;
use crate::config::Engine;
use logger::configure_logging;
use backtrace::Backtrace;
use clap::{Arg, Command};
// use config::SegcacheConfig;
use metriken::*;
// use pelikan_segcache_rs::Segcache;
use server::PERCENTILES;

type Parser = RequestParser;
type Storage = Seg;

mod config;
mod tokio;

use crate::config::*;

static RUNNING: AtomicBool = AtomicBool::new(true);

/// The entry point into the running Segcache instance. This function parses the
/// command line options, loads the configuration, and launches the core
/// threads.
fn main() {
    // custom panic hook to terminate whole process after unwinding
    std::panic::set_hook(Box::new(|s| {
        eprintln!("{s}");
        eprintln!("{:?}", Backtrace::new());
        std::process::exit(101);
    }));

    // parse command line options
    let matches = Command::new(env!("CARGO_BIN_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .long_about(
            "One of the unified cache backends implemented in Rust. It \
            uses segment-based storage to cache key/val pairs. It speaks the \
            memcached ASCII protocol and supports some ASCII memcached \
            commands.",
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
        .arg(
            Arg::new("print-config")
                .short('c')
                .long("config")
                .help("List all options in config")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

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

    // load config from file
    let config = if let Some(file) = matches.get_one::<String>("CONFIG") {
        debug!("loading config: {}", file);
        match Config::load(file) {
            Ok(c) => c,
            Err(error) => {
                eprintln!("error loading config file: {file}\n{error}");
                std::process::exit(1);
            }
        }
    } else {
        Default::default()
    };

    if matches.get_flag("print-config") {
        todo!("not implemented");
        // config.print();
        // std::process::exit(0);
    }

    // initialize logging
    let log = configure_logging(&config);

    // initialize metrics
    common::metrics::init();

    // launch the server
    match config.general.engine {
        Engine::Mio => {
            // initialize storage
            let storage = Storage::new(&config).expect("failed to initialize storage");

            // initialize parser
            let parser = Parser::new()
                .max_value_size(config.seg.segment_size() as usize)
                .time_type(config.time.time_type());

            // initialize process
            let process_builder = ProcessBuilder::<Parser, Request, Response, Storage>::new(
                &config, log, parser, storage,
            )
            .expect("failed to initialize process");

            // spawn threads
            let process = process_builder.spawn();
            process.wait();
        }
        Engine::Tokio => tokio::spawn(config, log),
    }
}
