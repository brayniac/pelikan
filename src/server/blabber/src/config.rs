use std::net::SocketAddr;
use crate::*;

fn default_server_addr() -> SocketAddr {
    "0.0.0.0:12321".parse().unwrap()
}

fn default_threads() -> usize {
    1
}

fn default_queue_depth() -> usize {
    128
}

fn default_fanout() -> u8 {
    1
}

fn default_publish_rate() -> u64 {
    1
}

fn default_message_len() -> u32 {
    MIN_MESSAGE_LEN
}

fn default_max_delay_us() -> u64 {
    0
}

#[derive(Clone)]
#[derive(Debug)]
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Delay {
    Uniform,
    Pareto,
    Poisson,
}

impl Default for Delay {
    fn default() -> Self {
        Self::Uniform
    }
}

// #[derive(Parser, Debug, Clone, Copy)]
// #[command(author, version, about, long_about = None)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub publisher: PublisherConfig,

    #[serde(default)]
    pub debug: Debug,

    #[serde(default = "default_threads")]
    pub threads: usize,

    #[serde(default = "default_queue_depth")]
    pub queue_depth: usize,

    #[serde(default = "default_fanout")]
    pub fanout: u8,

    #[serde(default = "default_max_delay_us")]
    pub max_delay_us: u64,

    #[serde(default)]
    pub delay: Delay,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct ServerConfig {
    #[serde(default = "default_server_addr")]
    pub addr: SocketAddr,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: default_server_addr(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct PublisherConfig {
    #[serde(default = "default_publish_rate")]
    pub rate: u64,

    #[serde(default = "default_message_len")]
    pub message_len: u32,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            rate: default_publish_rate(),
            message_len: default_message_len(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: Default::default(),
            publisher: Default::default(),
            debug: Default::default(),
            threads: default_threads(),
            queue_depth: default_queue_depth(),
            fanout: default_fanout(),
            max_delay_us: default_max_delay_us(),
            delay: Default::default(),
        }
    }
}

impl Config {
    pub fn load(file: &str) -> Result<Self, std::io::Error> {
        let mut file = std::fs::File::open(file)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        match toml::from_str(&content) {
            Ok(t) => Ok(t),
            Err(e) => {
                eprintln!("{e}");
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Error parsing config",
                ))
            }
        }
    }
}
