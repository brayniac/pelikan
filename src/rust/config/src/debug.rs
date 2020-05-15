// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const DEBUG_LOG_LEVEL: usize = 4;
const DEBUG_LOG_FILE: Option<String> = None;
const DEBUG_LOG_NBUF: usize = 0;

pub struct DebugConfig {
	log_level: usize,
	log_file: Option<String>,
	log_nbuf: usize,
}

impl Default for DebugConfig {
	fn default() -> Self {
		Self {
			log_level: DEBUG_LOG_LEVEL,
			log_file: DEBUG_LOG_FILE,
			log_nbuf: DEBUG_LOG_NBUF,
		}
	}
}

impl DebugConfig {
	pub fn log_level(&self) -> usize {
		self.log_level
	}

	pub fn log_file(&self) -> Option<String> {
		self.log_file.clone()
	}

	pub fn log_nbuf(&self) -> usize {
		self.log_nbuf
	}
}

