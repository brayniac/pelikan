use crate::*;
use crate::session::*;

/// A `Listener` is used to bind to a given socket address and accept new
/// sessions. These sessions are moved onto a MPSC queue, where they can be
/// handled by a `Worker`.
pub struct Listener {
	addr: SocketAddr,
	listener: TcpListener,
	poll: Poll,
	sender: Sender<Session>,
}

impl Listener {
	/// Creates a new `Listener` that will bind to a given `addr` and push new
	/// `Session`s over the `sender`
	pub fn new(addr: SocketAddr, sender: Sender<Session>) -> Self {
		let listener = TcpListener::bind(&addr).unwrap();
		let poll = Poll::new().unwrap();

		// register listener to event loop
		poll.register(&listener, Token(0), Ready::readable(),
			PollOpt::edge()).unwrap();

		Self {
			addr,
			listener,
			poll,
			sender,
		}
	}

	/// Runs the `Listener` in a loop, accepting new sessions and moving them to
	/// the queue
	pub fn run(&mut self) {
		println!("running listener on: {}", self.addr);

		let mut events = Events::with_capacity(1024);

		// repeatedly run accepting new connections and moving them to the worker
		loop {
			self.poll.poll(&mut events, None).unwrap();
			for event in events.iter() {
				if event.token() == Token(0) {
					if let Ok((stream, addr)) = self.listener.accept() {
						let client = Session::new(addr, stream, State::Reading);
						if self.sender.send(client).is_err() {
							println!("error sending client to worker");
						}
		            } else {
		            	println!("error accepting client");
		            }
				} else {
					println!("unknown token");
				}
			}
		}
	}
}