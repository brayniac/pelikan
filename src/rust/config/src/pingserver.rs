use crate::*;

const DAEMONIZE: bool = false;
const PID_FILENAME: Option<String> = None;
const DLOG_INTERVAL: usize = 500;

pub struct PingserverConfig {
	// top-level
	daemonize: bool,
	pid_filename: Option<String>,
	dlog_interval: usize,

	// application modules
	admin_config: AdminConfig,
	server_config: ServerConfig,
	worker_config: WorkerConfig,
	time_config: TimeConfig,

	// ccommon
	array_config: ArrayConfig,
	buf_config: BufConfig,
	dbuf_config: DbufConfig,
	debug_config: DebugConfig,
	sockio_config: SockioConfig,
	tcp_config: TcpConfig,
}

impl Default for PingserverConfig {
	fn default() -> Self {
		Self {
			daemonize: DAEMONIZE,
			pid_filename: PID_FILENAME,
			dlog_interval: DLOG_INTERVAL,

			admin_config: Default::default(),
			server_config: Default::default(),
			worker_config: Default::default(),
			time_config: Default::default(),

			array_config: Default::default(),
			buf_config: Default::default(),
			dbuf_config: Default::default(),
			debug_config: Default::default(),
			sockio_config: Default::default(),
			tcp_config: Default::default(),
		}
	}
}

impl PingserverConfig {
	pub fn daemonize(&self) -> bool {
		self.daemonize
	}

	pub fn pid_filename(&self) -> Option<String> {
		self.pid_filename.clone()
	}

	pub fn dlog_interval(&self) -> usize {
		self.dlog_interval
	}

	pub fn admin_config(&self) -> &AdminConfig {
		&self.admin_config
	}

	pub fn server_config(&self) -> &ServerConfig {
		&self.server_config
	}

	pub fn worker_config(&self) -> &WorkerConfig {
		&self.worker_config
	}

	pub fn time_config(&self) -> &TimeConfig {
		&self.time_config
	}

	pub fn array_config(&self) -> &ArrayConfig {
		&self.array_config
	}

	pub fn buf_config(&self) -> &BufConfig {
		&self.buf_config
	}

	pub fn dbuf_config(&self) -> &DbufConfig {
		&self.dbuf_config
	}

	pub fn debuf_config(&self) -> &DebugConfig {
		&self.debug_config
	}

	pub fn sockio_config(&self) -> &SockioConfig {
		&self.sockio_config
	}

	pub fn tcp_config(&self) -> &TcpConfig {
		&self.tcp_config
	}
}