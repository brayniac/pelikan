const STATS_LOG_FILE: Option<String> = None;
const STATS_LOG_NBUF: usize = 0;

pub struct StatsLogConfig {
	log_file: Option<String>,
	log_nbuf: usize,
}

impl Default for StatsLogConfig {
	fn default() -> Self {
		Self {
			log_file: STATS_LOG_FILE,
			log_nbuf: STATS_LOG_NBUF,
		}
	}
}

impl StatsLogConfig {
	pub fn log_file(&self) -> Option<String> {
		self.log_file.clone()
	}

	pub fn log_nbuf(&self) -> usize {
		self.log_nbuf
	}
}
