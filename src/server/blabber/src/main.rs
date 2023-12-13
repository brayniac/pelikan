#[macro_use]
extern crate logger;

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
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};

mod config;
mod listener;
mod message;
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

    if config.message_len < MIN_MESSAGE_LEN {
        eprintln!(
            "message len: {} must be >= {MIN_MESSAGE_LEN}",
            config.message_len
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
    listener_runtime.spawn(listener::listen(config, tx.clone(), worker_runtime.clone()));

    // continuously publish messages

    let hash_builder = hasher();

    let interval = Duration::from_secs(1).as_nanos() as u64 / config.publish_rate;

    let now = SystemTime::now();
    let offset_ns = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let mut next_period =
        SystemTime::UNIX_EPOCH + Duration::from_nanos((1 + (offset_ns / interval)) * interval);

    loop {
        while SystemTime::now() < next_period {
            std::thread::sleep(Duration::from_micros(100));
        }
        let _ = tx.send(Message::new(&hash_builder, config.message_len));
        next_period += Duration::from_nanos(interval);
    }
}
