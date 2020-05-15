const TCP_BACKLOG: usize = 128;
const TCP_POOLSIZE: usize = 0;

pub struct TcpConfig {
	backlog: usize,
	poolsize: usize,
}

impl Default for TcpConfig {
	fn default() -> Self {
		Self {
			backlog: TCP_BACKLOG,
			poolsize: TCP_POOLSIZE,
		}
	}
}

impl TcpConfig {
	pub fn backlog(&self) -> usize {
		self.backlog
	}

	pub fn poolsize(&self) -> usize {
		self.poolsize
	}
}