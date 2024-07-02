
#[macro_use]
extern crate logger;

use std::io::ErrorKind;
use ::config::BufConfig;
use std::borrow::{Borrow, BorrowMut};
use session::{Buf, BufMut, Buffer};
use protocol_ping::{Compose, Parse, Request, Response};
use server::ProcessBuilder;
use entrystore::Noop;
use protocol_ping::RequestParser;
use logger::Drain;
use crate::config::Engine;
use backtrace::Backtrace;
use clap::{Arg, Command};
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use logger::configure_logging;
use std::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Builder;
use tokio::time::sleep;
use tonic::{transport::Server as TonicServer, Request as TonicRequest, Response as TonicResponse, Status as TonicStatus};

use pingpong::ping_server::{Ping, PingServer};
use pingpong::{PingRequest, PongResponse};

static RUNNING: AtomicBool = AtomicBool::new(true);

pub mod pingpong {
    tonic::include_proto!("pingpong");
}

mod config;
use crate::config::{Config, Protocol};

#[derive(Debug, Default)]
pub struct Server {}

#[tonic::async_trait]
impl Ping for Server {
    async fn ping(&self, _request: TonicRequest<PingRequest>) -> Result<TonicResponse<PongResponse>, TonicStatus> {
        Ok(TonicResponse::new(PongResponse {}))
    }
}

type Parser = RequestParser;
type Storage = Noop;

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
    let log = configure_logging(&config);

    // initialize metrics
    common::metrics::init();

    match config.general.engine {
        Engine::Mio => {
            let _ = mio(config, log).map_err(|e| {
                eprintln!("error launching server: {e}");
                std::process::exit(1)
            });
        }
        Engine::Tokio => {
            tokio(config, log)
        }
    }
}

fn mio(config: Config, log: Box<dyn Drain>) -> Result<(), std::io::Error> {
    // initialize storage
    let storage = Storage::new();

    // initialize parser
    let parser = Parser::new();

    // initialize process
    let process_builder = ProcessBuilder::<Parser, Request, Response, Storage>::new(
        &config, log, parser, storage,
    )?
    .version(env!("CARGO_PKG_VERSION"));

    // spawn threads
    let process = process_builder.spawn();

    process.wait();

    Ok(())
}

fn tokio(config: Config, mut log: Box<dyn Drain>) {
    // initialize async runtime for control plane
    let control_runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .expect("failed to initialize tokio runtime");

    // spawn logging thread
    control_runtime.spawn(async move {
        while RUNNING.load(Ordering::Relaxed) {
            sleep(Duration::from_millis(1)).await;
            let _ = log.flush();
        }
        let _ = log.flush();
    });

    let addr = config
        .server
        .socket_addr()
        .map_err(|e| {
            error!("{}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "Bad listen address")
        })
        .map_err(|_| {
            std::process::exit(1);
        })
        .unwrap();

    // initialize async runtime for the data plane
    let data_runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.worker.threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general.protocol {
        Protocol::Ascii => {
            let _guard = data_runtime.enter();

            let listener = TcpListener::bind(addr).unwrap();
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();

            data_runtime.block_on(async move {
                loop {
                    if let Ok((mut socket, _)) = listener.accept().await {
                        if socket.set_nodelay(true).is_err() {
                            continue;
                        }

                        let buf_size = config.buf().size();

                        tokio::spawn(async move {
                            // initialize parser and the read and write bufs
                            let parser = protocol_ping::RequestParser::new();
                            let mut read_buffer = Buffer::new(buf_size);
                            let mut write_buffer = Buffer::new(buf_size);

                            loop {
                                // read from the socket
                                match socket.read(read_buffer.borrow_mut()).await {
                                    Ok(0) => {
                                        // socket was closed, return to close
                                        return;
                                    }
                                    Ok(n) => {
                                        // bytes received, advance read buffer
                                        // to make them available for parsing
                                        unsafe {
                                            read_buffer.advance_mut(n);
                                        }
                                    }
                                    Err(_) => {
                                        // some other error occurred, return to
                                        // close
                                        return;
                                    }
                                };

                                // parse the read buffer
                                let request = match parser.parse(read_buffer.borrow()) {
                                    Ok(request) => {
                                        // got a complete request, consume the
                                        // bytes for the request by advancing
                                        // the read buffer
                                        let consumed = request.consumed();
                                        read_buffer.advance(consumed);

                                        request
                                    }
                                    Err(e) => match e.kind() {
                                        ErrorKind::WouldBlock => {
                                            // incomplete request, loop to read
                                            // again
                                            continue;
                                        }
                                        _ => {
                                            // some parse error, return to close
                                            return;
                                        }
                                    },
                                };

                                // compose a response into the write buffer
                                match request.into_inner() {
                                    Request::Ping => {
                                        Response::Pong.compose(&mut write_buffer);
                                    }
                                }

                                // flush the write buffer, return to close on
                                // error
                                if socket.write_all(write_buffer.borrow()).await.is_err() {
                                    return;
                                }

                                // clear the write buffer
                                write_buffer.clear();
                            }
                        });
                    }
                }
            });

            while RUNNING.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(250));
            }

            data_runtime.shutdown_timeout(std::time::Duration::from_millis(100));
        }
        Protocol::Grpc => {
            let greeter = Server::default();

            data_runtime.spawn(async move {
                if let Err(e) = TonicServer::builder()
                    .add_service(PingServer::new(greeter))
                    .serve(addr)
                    .await
                {
                    error!("{e}");
                };

                RUNNING.store(false, Ordering::Relaxed);
            });

            while RUNNING.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }
}
