use ahash::RandomState;
use broadcaster::Receiver;
use broadcaster::RecvError;
use broadcaster::Sender;
use clap::Parser;
use rand::distributions::Uniform;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

static MIN_MESSAGE_LEN: u32 = 32;

pub fn hasher() -> RandomState {
    RandomState::with_seeds(
        0xd5b96f9126d61cee,
        0x50af85c9d1b6de70,
        0xbd7bdf2fee6d15b2,
        0x3dbe88bb183ac6f4,
    )
}

#[derive(Clone)]
pub struct Message {
    data: Vec<u8>,
}

impl Message {
    pub fn new(hash_builder: &RandomState, len: u32) -> Self {
        let now = SystemTime::now();

        let unix_ns = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let mut data = vec![0; len as usize];

        data[0..4].copy_from_slice(&len.to_be_bytes());
        data[8..16].copy_from_slice(&[0x54, 0x45, 0x53, 0x54, 0x49, 0x4E, 0x47, 0x21]);
        data[24..32].copy_from_slice(&unix_ns.to_be_bytes());

        let checksum = hash_builder.hash_one(&data[8..len as usize]).to_be_bytes();
        data[16..24].copy_from_slice(&checksum);

        Self { data }
    }
}

#[derive(Parser, Debug, Clone, Copy)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long, default_value_t = 1)]
    threads: usize,

    #[arg(long, default_value_t = 128)]
    queue_depth: usize,

    #[arg(long, default_value_t = 1)]
    fanout: u8,

    #[arg(long, default_value_t = 1)]
    publish_rate: u64,

    #[arg(long, default_value_t = MIN_MESSAGE_LEN)]
    message_len: u32,

    #[arg(long, default_value_t = 0)]
    max_delay_us: u64,
}

fn main() {
    // load the configuration from cli args
    let config = Config::parse();

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
    listener_runtime.spawn(listen(config, tx.clone(), worker_runtime.clone()));

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

// a task that listens for new connections and spawns worker tasks to serve the
// new clients
async fn listen(
    config: Config,
    tx: Sender<Message>,
    worker_runtime: Arc<Runtime>,
) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind("0.0.0.0:12321").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        if socket.set_nodelay(true).is_err() {
            eprintln!("couldn't set TCP_NODELAY. Dropping connection");
        }

        // spawn the worker task onto the worker runtime
        worker_runtime.spawn(serve(config, socket, tx.subscribe()));
    }
}

// a task that serves messages to a client
async fn serve(
    config: Config,
    mut socket: tokio::net::TcpStream,
    mut rx: Receiver<Message>,
) -> Result<(), std::io::Error> {
    // create a uniform distribution for selecting a possible delay time
    let delay = if config.max_delay_us == 0 {
        None
    } else {
        Some(Uniform::from(0..config.max_delay_us))
    };

    // small fast PRNG for generating delays
    let mut rng = SmallRng::from_entropy();

    loop {
        match rx.recv().await {
            Ok(message) => {
                // apply random delay if configured
                if let Some(delay) = delay {
                    let delay = rng.sample(delay);

                    // only delay if the delay is non-zero
                    if delay > 0 {
                        tokio::time::sleep(Duration::from_micros(delay)).await;
                    }
                }

                // write to the socket
                socket.write_all(&message.data).await?;
            }
            Err(RecvError::Lagged(_count)) => {
                // do nothing if we lagged
            }
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "queue stopped",
                ));
            }
        }
    }
}
