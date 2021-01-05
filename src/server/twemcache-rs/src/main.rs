// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use pelikan_twemcache_rs::TwemcacheBuilder;

use rustcommon_logger::{Level, Logger};

fn main() {
    // initialize logging
    Logger::new()
        .label("twemcache")
        .level(Level::Info)
        .init()
        .expect("Failed to initialize logger");

    // launch twemcache
    TwemcacheBuilder::new(std::env::args().nth(1))
        .spawn()
        .wait()
}
