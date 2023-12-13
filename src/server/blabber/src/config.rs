use crate::*;

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

// #[derive(Parser, Debug, Clone, Copy)]
// #[command(author, version, about, long_about = None)]
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Config {
    #[serde(default = "default_threads")]
    pub threads: usize,

    #[serde(default = "default_queue_depth")]
    pub queue_depth: usize,

    #[serde(default = "default_fanout")]
    pub fanout: u8,

    #[serde(default = "default_publish_rate")]
    pub publish_rate: u64,

    #[serde(default = "default_message_len")]
    pub message_len: u32,

    #[serde(default = "default_max_delay_us")]
    pub max_delay_us: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            threads: default_threads(),
            queue_depth: default_queue_depth(),
            fanout: default_fanout(),
            publish_rate: default_publish_rate(),
            message_len: default_message_len(),
            max_delay_us: default_max_delay_us(),
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
