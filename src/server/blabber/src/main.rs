use std::sync::Arc;
use tokio::runtime::Runtime;
use std::time::Duration;
use broadcaster::RecvError;
use clap::Parser;
use std::time::SystemTime;
use broadcaster::Receiver;
use broadcaster::Sender;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::runtime::Builder;

#[derive(Clone)]
pub struct Message {
    timestamp: u64,
}

impl Default for Message {
    fn default() -> Self {
        Self::new()
    }
}

impl Message {
    pub fn new() -> Self {
        let now = SystemTime::now();
        let unix_ns = now.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

        Self {
            timestamp: unix_ns,
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long, default_value_t = 1)]
    threads: usize,

    #[arg(long, default_value_t = 128)]
    queue_depth: usize,

    #[arg(long, default_value_t = 1)]
    fanout: u8,

    #[arg(long, default_value_t = 1)]
    publish_rate: usize,
}

fn main() {
    // load the configuration from cli args
    let config = Config::parse();

    // runtime for the listener
    let listener_runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .expect("failed to initialize tokio runtime");

    // runtime for the workers and fanout
    let worker_runtime = Arc::new(Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.threads)
        .build()
        .expect("failed to initialize tokio runtime"));

    // start broadcaster using the worker runtime
    let tx = broadcaster::channel::<Message>(&worker_runtime, config.queue_depth, config.fanout);

    // start the listener
    listener_runtime.spawn(listen(tx.clone(), worker_runtime.clone()));

    // continuously publish messages

    let interval = Duration::from_secs(1) / config.publish_rate as u32;

    loop {
        std::thread::sleep(interval);

        let _ = tx.send(Message::new());
    }
}

// a task that listens for new connections and spawns worker tasks to serve the
// new clients
async fn listen(tx: Sender<Message>, worker_runtime: Arc<Runtime>) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind("0.0.0.0:12321").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        // spawn the worker task onto the worker runtime
        worker_runtime.spawn(serve(socket, tx.subscribe()));
    }
}

// a task that serves messages to a client
async fn serve(mut socket: tokio::net::TcpStream, mut rx: Receiver<Message>) -> Result<(), std::io::Error> {
    loop {
        match rx.recv().await {
            Ok(message) => {
                socket.write_all(&message.timestamp.to_be_bytes()).await?;
            }
            Err(RecvError::Lagged(_count)) => {
                // do nothing if we lagged
            }
            Err(_) => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "queue stopped"));
            }
        }
    }
}