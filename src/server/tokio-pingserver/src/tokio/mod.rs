use ::config::BufConfig;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use logger::Drain;
use metriken::Lazy;
use protocol_ping::{Compose, Parse, Request, Response};
use session::{Buf, BufMut, Buffer};
use std::borrow::{Borrow, BorrowMut};
use std::io::ErrorKind;
use std::net::TcpListener;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Builder;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tonic::transport::Server as TonicServer;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status as TonicStatus};

use pingpong::ping_server::{Ping, PingServer};
use pingpong::{PingRequest, PongResponse};

mod admin;
mod metrics;

static METRICS_SNAPSHOT: Lazy<Arc<RwLock<metrics::MetricsSnapshot>>> =
    Lazy::new(|| Arc::new(RwLock::new(Default::default())));

static RUNNING: AtomicBool = AtomicBool::new(true);

pub mod pingpong {
    tonic::include_proto!("pingpong");
}

use crate::config::{Config, Protocol};

#[derive(Debug, Default)]
pub struct Server {}

#[tonic::async_trait]
impl Ping for Server {
    async fn ping(
        &self,
        _request: TonicRequest<PingRequest>,
    ) -> Result<TonicResponse<PongResponse>, TonicStatus> {
        Ok(TonicResponse::new(PongResponse {}))
    }
}

pub fn run(config: Config, mut log: Box<dyn Drain>) {
    let config = Arc::new(config);

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

    // spawn thread to maintain histogram snapshots
    {
        let interval = config.metrics.interval();
        control_runtime.spawn(async move {
            while RUNNING.load(Ordering::Relaxed) {
                // acquire a lock and update the snapshots
                {
                    let mut snapshots = METRICS_SNAPSHOT.write().await;
                    snapshots.update();
                }

                // delay until next update
                sleep(interval).await;
            }
        });
    }

    // spawn the admin thread
    control_runtime.spawn(admin::http(config.clone()));

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
            data_runtime.spawn(async move {
                if let Err(e) = TonicServer::builder()
                    .add_service(PingServer::new(Server::default()))
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
