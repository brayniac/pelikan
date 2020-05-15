
#[derive(Copy, Clone, Debug)]
pub enum TimeType {
	Unix = 0,
	Delta = 1,
	Memcache = 2,
	Sentinel = 3,
}

pub struct TimeConfig {
	time_type: TimeType,
}

impl Default for TimeConfig {
	fn default() -> Self {
		Self {
			time_type: TimeType::Unix,
		}
	}
}

impl TimeConfig {
	pub fn time_type(&self) -> TimeType {
		self.time_type
	}
}