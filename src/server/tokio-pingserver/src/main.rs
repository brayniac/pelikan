#[macro_use]
extern crate logger;

use backtrace::Backtrace;
use clap::Arg;
use clap::Command;
use config::PingserverConfig;
use config::{ServerConfig, WorkerConfig};
use core::sync::atomic::AtomicBool;
use core::sync::atomic::Ordering;
use core::time::Duration;
use logger::configure_logging;
use tokio::runtime::Builder;
use tokio::time::sleep;
use tonic::{transport::Server as TonicServer, Request, Response, Status};

use pingpong::ping_server::{Ping, PingServer};
use pingpong::{PingRequest, PongResponse};

static RUNNING: AtomicBool = AtomicBool::new(true);

pub mod pingpong {
    tonic::include_proto!("pingpong");
}

#[derive(Debug, Default)]
pub struct Server {}

#[tonic::async_trait]
impl Ping for Server {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        Ok(Response::new(PongResponse {}))
    }
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
        match PingserverConfig::load(file) {
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
    let mut log = configure_logging(&config);

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
        .server()
        .socket_addr()
        .map_err(|e| {
            error!("{}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "Bad listen address")
        })
        .map_err(|_| {
            std::process::exit(1);
        })
        .unwrap();

    // let addr = "0.0.0.0:12321".parse()?;
    let greeter = Server::default();

    // initialize async runtime for the data plane
    let data_runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.worker().threads())
        .build()
        .expect("failed to initialize tokio runtime");

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

    std::thread::sleep(Duration::from_millis(250));
}
