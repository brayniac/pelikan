// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const NELEM_DELTA: usize = 16;

pub struct ArrayConfig {
	// max nelem delta during expansion
	nelem_delta: usize,
}

impl Default for ArrayConfig {
	fn default() -> Self {
		Self {
			nelem_delta: NELEM_DELTA,
		}
	}
}

impl ArrayConfig {
	pub fn nelem_delta(&self) -> usize {
		self.nelem_delta
	}
}

