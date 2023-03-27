use config::ServerConfig;
use config::WorkerConfig;
use protocol_memcache::Parse;
use session::Buf;
use session::BufMut;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
#[macro_use]
extern crate logger;

use backtrace::Backtrace;
use clap::Arg;
use clap::Command;
use config::seg::Eviction;
use config::time::TimeType;
use config::SegConfig;
use config::SegcacheConfig;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use logger::configure_logging;
use metriken::Counter;
use metriken::Gauge;
use metriken::Heatmap;
use parking_lot::Mutex;
use protocol_memcache::{Request, RequestParser};
use seg::Policy;
use seg::Seg;
use session::Buffer;
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::time::sleep;

pub static PERCENTILES: &[(&str, f64)] = &[
    ("p25", 25.0),
    ("p50", 50.0),
    ("p75", 75.0),
    ("p90", 90.0),
    ("p99", 99.0),
    ("p999", 99.9),
    ("p9999", 99.99),
];

static RUNNING: AtomicBool = AtomicBool::new(true);

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
            } else if any.downcast_ref::<Heatmap>().is_some() {
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
        match SegcacheConfig::load(file) {
            Ok(c) => c,
            Err(error) => {
                eprintln!("error loading config file: {file}\n{error}");
                std::process::exit(1);
            }
        }
    } else {
        Default::default()
    };

    let config = Arc::new(config);

    if matches.get_flag("print-config") {
        config.print();
        std::process::exit(0);
    }

    // initialize logging
    let mut log = configure_logging(&*config);

    // initialize async runtime for control plane tasks
    let control_plane = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .expect("failed to initialize tokio runtime for control plane");

    // spawn logging thread
    control_plane.spawn(async move {
        while RUNNING.load(Ordering::Relaxed) {
            clocksource::refresh_clock();
            sleep(Duration::from_millis(1)).await;
            let _ = log.flush();
        }
        let _ = log.flush();
    });

    // initialize storage

    // build up the eviction policy from the config
    let eviction = match config.seg().eviction() {
        Eviction::None => Policy::None,
        Eviction::Random => Policy::Random,
        Eviction::RandomFifo => Policy::RandomFifo,
        Eviction::Fifo => Policy::Fifo,
        Eviction::Cte => Policy::Cte,
        Eviction::Util => Policy::Util,
        Eviction::Merge => Policy::Merge {
            max: config.seg().merge_max(),
            merge: config.seg().merge_target(),
            compact: config.seg().compact_target(),
        },
    };

    // build the datastructure from the config
    let storage = Arc::new(Mutex::new(
        Seg::builder()
            .hash_power(config.seg().hash_power())
            .overflow_factor(config.seg().overflow_factor())
            .heap_size(config.seg().heap_size())
            .segment_size(config.seg().segment_size())
            .eviction(eviction)
            .datapool_path(config.seg().datapool_path())
            .build()
            .expect("failed to initilize storage"),
    ));

    // let storage = Storage::new(&config).expect("failed to initialize storage");

    // initialize async runtime for data plane tasks
    let data_plane = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.worker().threads())
        .build()
        .expect("failed to initialize tokio runtime for control plane");

    // spawn expiration
    data_plane.spawn(expiration(storage.clone()));

    // spawn listener
    data_plane.spawn(listener(config.clone(), storage));

    while RUNNING.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(500));
    }
}

async fn expiration(storage: Arc<Mutex<Seg>>) {
    while RUNNING.load(Ordering::Relaxed) {
        {
            let mut storage = storage.lock();
            storage.expire();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn listener<T: ServerConfig>(config: Arc<T>, storage: Arc<Mutex<Seg>>) {
    let listener = TcpListener::bind(config.server().socket_addr().expect("bad socket addr"))
        .await
        .expect("failed to bind service port");

    loop {
        if let Ok((socket, _)) = listener.accept().await {
            let storage = storage.clone();

            tokio::spawn(worker(socket, storage));
        } else {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

async fn worker(socket: TcpStream, storage: Arc<Mutex<Seg>>) {
    let mut socket = socket;

    // initialize parser
    let parser = RequestParser::new()
        .max_value_size(256 * 1024)
        .time_type(TimeType::Memcache);

    let mut read_buffer = Buffer::new(4096);
    let mut write_buffer = Buffer::new(4096);

    'session: loop {
        // read data from the socket until we have a complete request
        let request = loop {
            match socket.read(read_buffer.borrow_mut()).await {
                Ok(0) => {
                    break 'session;
                }
                Ok(n) => {
                    unsafe {
                        read_buffer.advance_mut(n);
                    }
                    match parser.parse(read_buffer.borrow()) {
                        Ok(request) => {
                            let consumed = request.consumed();
                            let request = request.into_inner();

                            read_buffer.advance(consumed);

                            break request;
                        }
                        Err(e) => match e.kind() {
                            ErrorKind::WouldBlock => {}
                            _ => {
                                break 'session;
                            }
                        },
                    }
                }
                Err(_) => {
                    break 'session;
                }
            }
        };

        {
            let mut storage = storage.lock();

            match request {
                Request::Get(get) => {
                    for key in get.keys().iter() {
                        if let Some(item) = storage.get(key) {
                            let o = item.optional().unwrap_or(&[0, 0, 0, 0]);
                            let flags = u32::from_be_bytes([o[0], o[1], o[2], o[3]]);

                            write_buffer.put_slice(b"VALUE ");
                            write_buffer.put_slice(key);

                            match item.value() {
                                seg::Value::Bytes(b) => {
                                    let header_fields =
                                        format!(" {} {}\r\n", flags, b.len()).into_bytes();
                                    write_buffer.put_slice(&header_fields);
                                    write_buffer.put_slice(b);
                                }
                                seg::Value::U64(v) => {
                                    let value = format!("{v}").into_bytes();

                                    let header_fields =
                                        format!(" {} {}\r\n", flags, value.len()).into_bytes();
                                    write_buffer.put_slice(&header_fields);
                                    write_buffer.put_slice(&value);
                                }
                            };

                            write_buffer.put_slice(b"\r\n");
                        }
                    }
                    write_buffer.put_slice(b"END\r\n");
                }
                Request::Set(set) => {
                    let ttl = set.ttl().get().unwrap_or(0);
                    if ttl < 0 {
                        // immediate expire maps to a delete
                        storage.delete(set.key());
                        write_buffer.put_slice(b"STORED\r\n");
                    } else if storage
                        .insert(
                            set.key(),
                            set.value(),
                            Some(&set.flags().to_be_bytes()),
                            Duration::from_secs(ttl as u64),
                        )
                        .is_ok()
                    {
                        write_buffer.put_slice(b"STORED\r\n");
                    } else {
                        write_buffer.put_slice(b"SERVER_ERROR\r\n");
                    }
                }
                Request::Quit(_) => {
                    break 'session;
                }
                _ => {
                    write_buffer.put_slice(b"ERROR\r\n");
                }
            }
        };

        // flush the write buffer to the socket
        while write_buffer.remaining() > 0 {
            match socket.write(write_buffer.borrow()).await {
                Ok(0) => {
                    break 'session;
                }
                Ok(n) => {
                    write_buffer.advance(n);
                }
                Err(_) => {
                    break 'session;
                }
            }
        }
    }
}
