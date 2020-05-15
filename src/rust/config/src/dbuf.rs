const DBUF_DEFAULT_MAX: usize = 8;

pub struct DbufConfig {
	max_power: usize
}

impl Default for DbufConfig {
	fn default() -> Self {
		Self {
			max_power: DBUF_DEFAULT_MAX,
		}
	}
}

impl DbufConfig {
	fn max_power(&self) -> usize {
		self.max_power
	}
}