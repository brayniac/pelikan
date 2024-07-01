use std::io::Read;
use config::{Admin, Debug, DebugConfig, Klog, KlogConfig, Server, Worker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Config {
    pub general: General,

    // application modules
    #[serde(default)]
    pub admin: Admin,
    #[serde(default)]
    pub server: Server,
    #[serde(default)]
    pub worker: Worker,

    #[serde(default)]
    pub debug: Debug,
    #[serde(default)]
    pub klog: Klog,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct General {
    pub protocol: Protocol,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    #[default]
    Ascii,
    Grpc,
}

impl Config {
    pub fn load(file: &str) -> Result<Self, std::io::Error> {
        let mut file = std::fs::File::open(file)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        match toml::from_str(&content) {
            Ok(t) => Ok(t),
            Err(e) => {
                error!("{}", e);
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Error parsing config",
                ))
            }
        }
    }
}

impl DebugConfig for Config {
    fn debug(&self) -> &Debug {
        &self.debug
    }
}

impl KlogConfig for Config {
    fn klog(&self) -> &Klog {
        &self.klog
    }
}
