const WORKER_TIMEOUT: usize = 100;
const WORKER_NEVENT: usize = 1024;

pub struct WorkerConfig {
	timeout: usize,
	nevent: usize,
}

impl Default for WorkerConfig {
	fn default() -> Self {
		Self {
			timeout: WORKER_TIMEOUT,
			nevent: WORKER_NEVENT,
		}
	}
}

impl WorkerConfig {
	pub fn timeout(&self) -> usize {
		self.timeout
	}

	pub fn nevent(&self) -> usize {
		self.nevent
	}
}