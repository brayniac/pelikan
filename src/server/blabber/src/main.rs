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

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long)]
    threads: usize,

    #[arg(long)]
    queue_depth: usize,

    #[arg(long)]
    fanout: u8,

    #[arg(long)]
    publish_rate: usize,
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

fn main() {
    let config = Config::parse();

    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.threads)
        .build()
        .expect("failed to initialize tokio runtime");

    let tx = broadcaster::channel::<Message>(&runtime, config.queue_depth, config.fanout);

    runtime.spawn(listen(tx.clone()));

    loop {
        std::thread::sleep(core::time::Duration::from_secs(1));

        let _ = tx.send(Message::new());
    }
}

async fn listen(tx: Sender<Message>) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind("0.0.0.0:12321").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(serve(socket, tx.subscribe()));
    }
}

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