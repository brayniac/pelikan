use broadcaster::RecvError;
use std::time::SystemTime;
use broadcaster::Receiver;
use broadcaster::Sender;
use std::time::Instant;
use tokio::io::AsyncReadExt;
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

fn main() {
    println!("Hello, world!");

    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .expect("failed to initialize tokio runtime");

    let tx = broadcaster::channel::<Message>(&runtime, 128, 7);


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