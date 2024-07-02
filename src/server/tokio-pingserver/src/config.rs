use config::*;
use serde::{Deserialize, Serialize};
use std::io::Read;

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
    pub time: Time,
    #[serde(default)]
    pub tls: Tls,

    // ccommon
    #[serde(default)]
    pub buf: Buf,
    #[serde(default)]
    pub debug: Debug,
    #[serde(default)]
    pub klog: Klog,
    #[serde(default)]
    pub sockio: Sockio,
    #[serde(default)]
    pub tcp: Tcp,

}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct General {
    pub engine: Engine,
    pub protocol: Protocol,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Engine {
    #[default]
    Mio,
    Tokio,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
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

        let config: Config = match toml::from_str(&content) {
            Ok(t) => t,
            Err(e) => {
                error!("{}", e);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Error parsing config",
                ));
            }
        };

        if config.general.protocol == Protocol::Grpc && config.general.engine == Engine::Mio {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "GRPC support requires using the Tokio engine",
            ));
        }

        Ok(config)
    }
}
impl AdminConfig for Config {
    fn admin(&self) -> &Admin {
        &self.admin
    }
}

impl BufConfig for Config {
    fn buf(&self) -> &Buf {
        &self.buf
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

impl ServerConfig for Config {
    fn server(&self) -> &Server {
        &self.server
    }
}

impl SockioConfig for Config {
    fn sockio(&self) -> &Sockio {
        &self.sockio
    }
}

impl TcpConfig for Config {
    fn tcp(&self) -> &Tcp {
        &self.tcp
    }
}

impl TimeConfig for Config {
    fn time(&self) -> &Time {
        &self.time
    }
}

impl TlsConfig for Config {
    fn tls(&self) -> &Tls {
        &self.tls
    }
}

impl WorkerConfig for Config {
    fn worker(&self) -> &Worker {
        &self.worker
    }

    fn worker_mut(&mut self) -> &mut Worker {
        &mut self.worker
    }
}