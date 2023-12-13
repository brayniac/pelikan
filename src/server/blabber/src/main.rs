#[macro_use]
extern crate logger;

use logger::MultiLogBuilder;
use logger::Stdout;
use logger::File;
use logger::Output;
use logger::LogBuilder;
use ::config::Debug;
use ahash::RandomState;
use backtrace::Backtrace;
use broadcaster::{Receiver, RecvError, Sender};
use clap::{Arg, Command};
use rand::distributions::Uniform;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
// use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};

mod config;
mod listener;
mod message;
mod publisher;
mod worker;

use config::Config;
use message::Message;

static MIN_MESSAGE_LEN: u32 = 32;

pub fn hasher() -> RandomState {
    RandomState::with_seeds(
        0xd5b96f9126d61cee,
        0x50af85c9d1b6de70,
        0xbd7bdf2fee6d15b2,
        0x3dbe88bb183ac6f4,
    )
}

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
        .arg(
            Arg::new("CONFIG")
                .help("Server configuration file")
                .action(clap::ArgAction::Set)
                .index(1),
        )
        .get_matches();

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

    // initialize logging
    let debug_output: Box<dyn Output> = if let Some(file) = config.debug.log_file() {
        let backup = config.debug.log_backup().unwrap_or(format!("{file}.old"));
        Box::new(
            File::new(&file, &backup, config.debug.log_max_size())
                .expect("failed to open debug log file"),
        )
    } else {
        Box::new(Stdout::new())
    };

    let debug_log = LogBuilder::new()
        .output(debug_output)
        .log_queue_depth(config.debug.log_queue_depth())
        .single_message_size(config.debug.log_single_message_size())
        .build()
        .expect("failed to initialize debug log");

    let mut log_drain = MultiLogBuilder::new()
        .level_filter(config.debug.log_level().to_level_filter())
        .default(debug_log)
        .build()
        .start();

    if config.publisher.message_len < MIN_MESSAGE_LEN {
        eprintln!(
            "message len: {} must be >= {MIN_MESSAGE_LEN}",
            config.publisher.message_len
        );
        std::process::exit(1);
    }

    // runtime for the listener
    let listener_runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .expect("failed to initialize tokio runtime");

    // runtime for the workers and fanout
    let worker_runtime = Arc::new(
        Builder::new_multi_thread()
            .enable_all()
            .worker_threads(config.threads)
            .build()
            .expect("failed to initialize tokio runtime"),
    );

    // start broadcaster using the worker runtime
    let tx = broadcaster::channel::<Message>(&worker_runtime, config.queue_depth, config.fanout);

    // start the listener
    listener_runtime.spawn(listener::listen(Arc::new(config.clone()), tx.clone(), worker_runtime.clone()));

    listener_runtime.spawn_blocking(move || { loop { let _ = log_drain.flush(); } });

    publisher::publish(config, tx);
}
