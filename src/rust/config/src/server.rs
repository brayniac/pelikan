const SERVER_HOST: Option<String> = None;
const SERVER_PORT: &'static str = "12321";
const SERVER_TIMEOUT: usize = 100;
const SERVER_NEVENT: usize = 1024;

pub struct ServerConfig {
	host: Option<String>,
	port: String,
	timeout: usize,
	nevent: usize,
}

impl Default for ServerConfig {
	fn default() -> Self {
		Self {
			host: SERVER_HOST,
			port: SERVER_PORT.to_string(),
			timeout: SERVER_TIMEOUT,
			nevent: SERVER_NEVENT,
		}
	}
}

impl ServerConfig {
	pub fn host(&self) -> Option<String> {
		self.host.clone()
	}

	pub fn port(&self) -> String {
		self.port.clone()
	}

	pub fn timeout(&self) -> usize {
		self.timeout
	}

	pub fn nevent(&self) -> usize {
		self.nevent
	}
}