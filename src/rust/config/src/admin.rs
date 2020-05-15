const ADMIN_HOST: Option<String> = None;
const ADMIN_PORT: &'static str = "9999";
const ADMIN_TIMEOUT: usize = 100;
const ADMIN_NEVENT: usize = 1024;
const ADMIN_TW_TICK: usize = 10;
const ADMIN_TW_CAP: usize = 1000;
const ADMIN_TW_NTICK: usize = 100;

pub struct AdminConfig {
	host: Option<String>,
	port: String,
	timeout: usize,
	nevent: usize,
	tw_tick: usize,
	tw_cap: usize,
	tw_ntick: usize,
}

impl Default for AdminConfig {
	fn default() -> Self {
		Self {
			host: ADMIN_HOST,
			port: ADMIN_PORT.to_string(),
			timeout: ADMIN_TIMEOUT,
			nevent: ADMIN_NEVENT,
			tw_tick: ADMIN_TW_TICK,
			tw_cap: ADMIN_TW_CAP,
			tw_ntick: ADMIN_TW_NTICK,
		}
	}
}

impl AdminConfig {
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

	pub fn tw_tick(&self) -> usize {
		self.tw_tick
	}

	pub fn tw_cap(&self) -> usize {
		self.tw_cap
	}

	pub fn tw_ntick(&self) -> usize {
		self.tw_ntick
	}
}