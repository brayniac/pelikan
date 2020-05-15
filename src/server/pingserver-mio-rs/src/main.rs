// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::net::SocketAddr;
use std::sync::mpsc::*;

use log::*;
use mio::*;
use mio::net::*;
use mio::unix::*;
use slab::Slab;

mod buffer;
mod listener;
mod session;
mod worker;

use crate::listener::Listener;
use crate::worker::Worker;

fn main() {
	// setup rust logging shim
    ccommon_rs::log::init().expect("Failed to initialize logging shim");

    // create channel to move sessions from listener to worker
	let (sender, receiver) = channel();

	// initialize worker
	let mut worker = Worker::new(receiver);
	let worker_thread = std::thread::spawn(move || {
		worker.run()
	});

	// initialize listener
	let mut listener = Listener::new("0.0.0.0:32123".parse().unwrap(), sender);
	let listener_thread = std::thread::spawn(move || {
		listener.run()
	});

	// join threads
	let _ = listener_thread.join();
	let _ = worker_thread.join();
}

