// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use serde::{Deserialize, Serialize};

// constants to define default values
const HASH_POWER: u8 = 16;
const SEG_SIZE: i32 = 1024 * 1024;
const SEGMENTS: i32 = 64;
const EVICTION: Eviction = Eviction::Random;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum Eviction {
    None,
    Random,
    Fifo,
    Cte,
    Util,
}

// helper functions for default values
fn hash_power() -> u8 {
    HASH_POWER
}

fn seg_size() -> i32 {
    SEG_SIZE
}

fn segments() -> i32 {
    SEGMENTS
}

fn eviction() -> Eviction {
    EVICTION
}

// definitions
#[derive(Serialize, Deserialize, Debug)]
pub struct SegCacheConfig {
    #[serde(default = "hash_power")]
    hash_power: u8,
    #[serde(default = "seg_size")]
    seg_size: i32,
    #[serde(default = "segments")]
    segments: i32,
    #[serde(default = "eviction")]
    eviction: Eviction,
}

impl Default for SegCacheConfig {
    fn default() -> Self {
        Self {
            hash_power: hash_power(),
            seg_size: seg_size(),
            segments: segments(),
            eviction: eviction(),
        }
    }
}

// implementation
impl SegCacheConfig {
    pub fn hash_power(&self) -> u8 {
        self.hash_power
    }

    pub fn seg_size(&self) -> i32 {
        self.seg_size
    }

    pub fn segments(&self) -> i32 {
        self.segments
    }

    pub fn eviction(&self) -> Eviction {
        self.eviction
    }
}
