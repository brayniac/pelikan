const BUF_DEFAULT_SIZE: usize = 16 * 1024;
const BUF_POOLSIZE: usize = 0;

pub struct BufConfig {
	size: usize,
	poolsize: usize,
}

impl Default for BufConfig {
	fn default() -> Self {
		Self {
			size: BUF_DEFAULT_SIZE,
			poolsize: BUF_POOLSIZE,
		}
	}
}

impl BufConfig {
	pub fn size(&self) -> usize {
		self.size
	}

	pub fn poolsize(&self) -> usize {
		self.poolsize
	}
}