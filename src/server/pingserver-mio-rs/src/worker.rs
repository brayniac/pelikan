use crate::*;
use crate::session::*;

/// A `Worker` handles events on `Session`s
pub struct Worker {
	sessions: Slab<Session>,
	poll: Poll,
	receiver: Receiver<Session>,
}

impl Worker {
	/// Create a new `Worker` which will get new `Session`s from the MPMC queue
	pub fn new(receiver: Receiver<Session>) -> Self {
		let poll = Poll::new().unwrap();
		let sessions = Slab::<Session>::new();

		Self {
			poll,
			receiver,
			sessions,
		}
	}

	/// Close a session given its token
	fn close(&mut self, token: Token) {
		let session = self.sessions.remove(token.0);
		session.deregister(&self.poll).unwrap();
	}

	/// Handle HUP and zero-length reads
	fn handle_hup(&mut self, token: Token) {
		debug!("session closed by client");
		self.close(token);
	}

	/// Handle errors
	fn handle_error(&mut self, token: Token) {
		error!("error handling event");
		self.close(token);
	}

	/// Reregister the session given its token
	fn reregister(&mut self, token: Token) {
		let session = &mut self.sessions[token.0];
		if session.reregister(&self.poll).is_err() {
			error!("failed to reregister");
			self.close(token);
		}
	}

	/// Handle a read event for the session given its token
	fn do_read(&mut self, token: Token) {
		let session = self.sessions.get_mut(token.0).unwrap();
		
		// read from stream to buffer
		match session.read() {
			Ok(Some(0)) => {
				self.handle_hup(token);
            }
            Ok(Some(n)) => {
                trace!("read {} bytes", n);
                // parse buffer contents
				let buf = session.rx_buffer();
				if buf.len() < 6 || &buf[buf.len() - 2..buf.len()] != b"\r\n" {
		            // Shortest request is "PING\r\n" at 6 bytes
		            // All complete responses end in CRLF
		            self.reregister(token);
		        } else if buf.len() == 6 && &buf[..] == b"PING\r\n" {
		        	session.clear_buffer();
		        	match session.write(b"PONG\r\n") {
		        		Ok(6) => {
		        			session.set_state(State::Writing);
		        			self.reregister(token);
		        		}
		        		_ => self.handle_error(token),
		        	}
		        } else {
		        	self.handle_error(token);
		        }
            }
            Ok(None) => {
                println!("spurious read");
                self.reregister(token);
            }
            Err(e) => {
            	debug!("read error: {:?}", e);
                self.handle_error(token);
            }
		}
	}

	/// Handle a write event for a session given its token
	fn do_write(&mut self, token: Token) {
		let session = &mut self.sessions[token.0];
		match session.flush() {
            Ok(Some(bytes)) => {
                // successful write
                trace!("wrote entire buffer {} bytes", bytes);
                session.set_state(State::Reading);
            }
            Ok(None) => {
                // socket wasn't ready
                trace!("spurious call to write");
            }
            Err(e) => {
                debug!("write error: {:?}", e);
            }
        }
        self.reregister(token);
	}

	/// Run the `Worker` in a loop, handling new session events
	pub fn run(&mut self) -> Self {
		let mut events = Events::with_capacity(1024);

		loop {
			// get client events with timeout
			self.poll.poll(&mut events, Some(std::time::Duration::from_millis(1))).unwrap();

			// process all events
			for event in events.iter() {
				if UnixReady::from(event.readiness()).is_hup() {
					self.handle_hup(event.token());
					continue;
				}

				if event.readiness().is_readable() {
					self.do_read(event.token());
				}

				if event.readiness().is_writable() {
					self.do_write(event.token());
				}
			}

			// handle up to one new connection
			if let Ok(mut s) = self.receiver.try_recv() {
				// reserve vacant slab
				let session = self.sessions.vacant_entry();

				// set client token to match slab
		        s.set_token(Token(session.key()));

		        // register tcp stream
		        s.register(&self.poll).unwrap();

		        // insert client into slab
		        session.insert(s);
			}
		}
	}
}