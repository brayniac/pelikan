// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::common::Message;
use crate::event_loop::EventLoop;
use crate::metrics::*;
use crate::protocol::data::*;
use crate::session::*;
use crate::storage::{HashTable, Item};
use bytes::BytesMut;
use config::TimeType;
use std::time::SystemTime;


use crate::*;

use std::convert::TryInto;
use std::sync::Arc;

/// A `Worker` handles events on `Session`s
pub struct Worker {
    config: Arc<Config>,
    message_receiver: Receiver<Message>,
    message_sender: SyncSender<Message>,
    metrics: Arc<Metrics>,
    poll: Poll,
    session_receiver: Receiver<Session>,
    session_sender: SyncSender<Session>,
    sessions: Slab<Session>,
    data: HashTable,
}

impl Worker {
    /// Create a new `Worker` which will get new `Session`s from the MPSC queue
    pub fn new(config: Arc<Config>, metrics: Arc<Metrics>) -> Result<Self, std::io::Error> {
        let poll = Poll::new().map_err(|e| {
            error!("{}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to create epoll instance")
        })?;
        let sessions = Slab::<Session>::new();

        let (session_sender, session_receiver) = sync_channel(128);
        let (message_sender, message_receiver) = sync_channel(128);

        Ok(Self {
            config,
            poll,
            message_receiver,
            message_sender,
            session_receiver,
            session_sender,
            sessions,
            metrics,
            data: HashTable::new(),
        })
    }

    /// Run the `Worker` in a loop, handling new session events
    pub fn run(&mut self) {
        let mut events = Events::with_capacity(self.config.worker().nevent());
        let timeout = Some(std::time::Duration::from_millis(
            self.config.worker().timeout() as u64,
        ));

        loop {
            #[cfg(feature = "metrics")]
            let _ = self.metrics.increment_counter(Stat::WorkerEventLoop, 1);

            // get events with timeout
            if self.poll.poll(&mut events, timeout).is_err() {
                error!("Error polling");
            }

            #[cfg(feature = "metrics")]
            let _ = self.metrics.increment_counter(
                Stat::WorkerEventTotal,
                events.iter().count().try_into().unwrap(),
            );

            // process all events
            for event in events.iter() {
                let token = event.token();

                // event for existing session
                trace!("got event for session: {}", token.0);

                // handle error events first
                if event.is_error() {
                    #[cfg(feature = "metrics")]
                    self.increment_count(&Stat::WorkerEventError);
                    self.handle_error(token);
                }

                // handle write events before read events to reduce write buffer
                // growth if there is also a readable event
                if event.is_writable() {
                    #[cfg(feature = "metrics")]
                    self.increment_count(&Stat::WorkerEventWrite);
                    self.do_write(token);
                }

                // read events are handled last
                if event.is_readable() {
                    #[cfg(feature = "metrics")]
                    self.increment_count(&Stat::WorkerEventRead);
                    let _ = self.do_read(token);
                }

                if let Some(session) = self.sessions.get_mut(token.0) {
                    trace!(
                        "{} bytes pending in rx buffer for session: {}",
                        session.read_pending(),
                        token.0
                    );
                    trace!(
                        "{} bytes pending in tx buffer for session: {}",
                        session.write_pending(),
                        token.0
                    )
                }
            }

            // poll queue to receive new sessions
            while let Ok(mut session) = self.session_receiver.try_recv() {
                let pending = session.read_pending();
                trace!("{} bytes pending in rx buffer for new session", pending);

                // reserve vacant slab
                let session_entry = self.sessions.vacant_entry();
                let token = Token(session_entry.key());

                // set client token to match slab
                session.set_token(token);

                // register tcp stream and insert into slab if successful
                match session.register(&self.poll) {
                    Ok(_) => {
                        session_entry.insert(session);
                        if pending > 0 {
                            // handle any pending data immediately
                            if self.handle_data(token).is_err() {
                                self.handle_error(token);
                            }
                        }
                    }
                    Err(_) => {
                        error!("Error registering new socket");
                    }
                };
            }

            // poll queue to receive new messages
            #[allow(clippy::never_loop)]
            while let Ok(message) = self.message_receiver.try_recv() {
                match message {
                    Message::Shutdown => {
                        return;
                    }
                }
            }
        }
    }

    pub fn message_sender(&self) -> SyncSender<Message> {
        self.message_sender.clone()
    }

    pub fn session_sender(&self) -> SyncSender<Session> {
        self.session_sender.clone()
    }

    // TODO(bmartin): this does not handle expiry.
    pub fn process_get(
        request: GetRequest,
        write_buffer: &mut BytesMut,
        data: &mut HashTable,
        metrics: &Arc<Metrics>,
    ) {
        let mut found = 0;
        for key in request.keys() {
            #[cfg(feature = "metrics")]
            let _ = metrics.increment_counter(Stat::GetKey, 1);
            if let Ok(item) = data.get(key) {
                write_buffer.extend_from_slice(b"VALUE ");
                write_buffer.extend_from_slice(key);
                write_buffer.extend_from_slice(b" ");
                write_buffer.extend_from_slice(format!("{}", item.flags()).as_bytes());
                write_buffer.extend_from_slice(b" ");
                write_buffer.extend_from_slice(format!("{}", item.value_len()).as_bytes());
                write_buffer.extend_from_slice(b"\r\n");
                write_buffer.extend_from_slice(item.value());
                write_buffer.extend_from_slice(b"\r\n");
                found += 1;
                #[cfg(feature = "metrics")]
                let _ = metrics.increment_counter(Stat::GetKeyHit, 1);
            } else {
                #[cfg(feature = "metrics")]
                let _ = metrics.increment_counter(Stat::GetKeyMiss, 1);
            }
        }
        debug!(
            "get request processed, {} out of {} keys found",
            found,
            request.keys().len()
        );
        write_buffer.extend_from_slice(b"END\r\n");
    }

    // TODO(bmartin): this does not handle expiry.
    pub fn process_set(
        config: &Arc<Config>,
        request: SetRequest,
        write_buffer: &mut BytesMut,
        data: &mut HashTable,
        metrics: &Arc<Metrics>,
    ) {
        #[cfg(feature = "metrics")]
        let _ = metrics.increment_counter(Stat::Set, 1);
        let reply = !request.noreply();
        let expiry = match config.time().time_type() {
            TimeType::Unix => {
                Some(request.expiry().into())
            }
            TimeType::Delta => {
                let epoch: u64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
                Some(epoch + u64::from(request.expiry()))
            }
                // Some(SystemTime::now() + Duration::new(request.expiry().into(), 0)),
            TimeType::Memcache => {
                if request.expiry() == 0 {
                    None
                } else if request.expiry() < 60 * 60 * 24 * 30 {
                    let epoch: u64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
                    Some(epoch + u64::from(request.expiry()))
                } else {
                    Some(request.expiry().into())
                }
            }
        };
        if data
            .set(
                request.key(),
                Item::new(request.value(), request.flags(), None),
                expiry,
            )
            .is_ok()
        {
            #[cfg(feature = "metrics")]
            let _ = metrics.increment_counter(Stat::SetStored, 1);
            if reply {
                write_buffer.extend_from_slice(b"STORED\r\n");
            }
        } else {
            let _ = metrics.increment_counter(Stat::SetNotstored, 1);
            if reply {
                write_buffer.extend_from_slice(b"NOT_STORED\r\n");
            }
        }
    }

    pub fn process_cas(
        config: &Arc<Config>,
        request: CasRequest,
        write_buffer: &mut BytesMut,
        data: &mut HashTable,
        metrics: &Arc<Metrics>,
    ) {
        #[cfg(feature = "metrics")]
        let _ = metrics.increment_counter(Stat::Set, 1);
        let reply = !request.noreply();
        let expiry = match config.time().time_type() {
            TimeType::Unix => {
                Some(request.expiry().into())
            }
            TimeType::Delta => {
                let epoch: u64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
                Some(epoch + u64::from(request.expiry()))
            }
                // Some(SystemTime::now() + Duration::new(request.expiry().into(), 0)),
            TimeType::Memcache => {
                if request.expiry() == 0 {
                    None
                } else if request.expiry() < 60 * 60 * 24 * 30 {
                    let epoch: u64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
                    Some(epoch + u64::from(request.expiry()))
                } else {
                    Some(request.expiry().into())
                }
            }
        };
        match data.cas(
            request.key(),
            Item::new(request.value(), request.flags(), Some(request.cas())),
            expiry,
        ) {
            Ok(_) => {
                // #[cfg(feature = "metrics")]
                // let _ = metrics.increment_counter(Stat::CasStored, 1);
                if reply {
                    write_buffer.extend_from_slice(b"STORED\r\n");
                }
            }
            Err(_) => {
                // TODO(bmartin): fix stats for CAS
                // let _ = metrics.increment_counter(Stat::SetNotstored, 1);
                if reply {
                    write_buffer.extend_from_slice(b"NOT_STORED\r\n");
                }
            }
        }
    }

    // TODO(bmartin): this does not handle expiry.
    pub fn process_add(
        config: &Arc<Config>,
        request: AddRequest,
        write_buffer: &mut BytesMut,
        data: &mut HashTable,
        metrics: &Arc<Metrics>,
    ) {
        #[cfg(feature = "metrics")]
        let _ = metrics.increment_counter(Stat::Add, 1);
        let reply = !request.noreply();
        let expiry = match config.time().time_type() {
            TimeType::Unix => {
                Some(request.expiry().into())
            }
            TimeType::Delta => {
                let epoch: u64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
                Some(epoch + u64::from(request.expiry()))
            }
                // Some(SystemTime::now() + Duration::new(request.expiry().into(), 0)),
            TimeType::Memcache => {
                if request.expiry() == 0 {
                    None
                } else if request.expiry() < 60 * 60 * 24 * 30 {
                    let epoch: u64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
                    Some(epoch + u64::from(request.expiry()))
                } else {
                    Some(request.expiry().into())
                }
            }
        };
        if data
            .add(
                request.key(),
                Item::new(request.value(), request.flags(), None),
                expiry,
            )
            .is_ok()
        {
            #[cfg(feature = "metrics")]
            let _ = metrics.increment_counter(Stat::AddStored, 1);
            if reply {
                write_buffer.extend_from_slice(b"STORED\r\n");
            }
        } else {
            #[cfg(feature = "metrics")]
            let _ = metrics.increment_counter(Stat::AddNotstored, 1);
            if reply {
                write_buffer.extend_from_slice(b"NOT_STORED\r\n");
            }
        }
    }

    // TODO(bmartin): this does not handle expiry.
    pub fn process_replace(
        config: &Arc<Config>,
        request: ReplaceRequest,
        write_buffer: &mut BytesMut,
        data: &mut HashTable,
        metrics: &Arc<Metrics>,
    ) {
        #[cfg(feature = "metrics")]
        let _ = metrics.increment_counter(Stat::Replace, 1);
        let reply = !request.noreply();
        let expiry = match config.time().time_type() {
            TimeType::Unix => {
                Some(request.expiry().into())
            }
            TimeType::Delta => {
                let epoch: u64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
                Some(epoch + u64::from(request.expiry()))
            }
                // Some(SystemTime::now() + Duration::new(request.expiry().into(), 0)),
            TimeType::Memcache => {
                if request.expiry() == 0 {
                    None
                } else if request.expiry() < 60 * 60 * 24 * 30 {
                    let epoch: u64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
                    Some(epoch + u64::from(request.expiry()))
                } else {
                    Some(request.expiry().into())
                }
            }
        };
        if data
            .replace(
                request.key(),
                Item::new(request.value(), request.flags(), None),
                expiry,
            )
            .is_ok()
        {
            #[cfg(feature = "metrics")]
            let _ = metrics.increment_counter(Stat::ReplaceStored, 1);
            if reply {
                write_buffer.extend_from_slice(b"STORED\r\n");
            }
        } else {
            #[cfg(feature = "metrics")]
            let _ = metrics.increment_counter(Stat::ReplaceNotstored, 1);
            if reply {
                write_buffer.extend_from_slice(b"NOT_STORED\r\n");
            }
        }
    }

    pub fn process_delete(
        request: DeleteRequest,
        write_buffer: &mut BytesMut,
        data: &mut HashTable,
        metrics: &Arc<Metrics>,
    ) {
        #[cfg(feature = "metrics")]
        let _ = metrics.increment_counter(Stat::Delete, 1);
        let reply = !request.noreply();
        if data.remove(request.key()).is_ok() {
            #[cfg(feature = "metrics")]
            let _ = metrics.increment_counter(Stat::DeleteDeleted, 1);
            if reply {
                write_buffer.extend_from_slice(b"DELETED\r\n");
            }
        } else {
            #[cfg(feature = "metrics")]
            let _ = metrics.increment_counter(Stat::DeleteNotfound, 1);
            if reply {
                write_buffer.extend_from_slice(b"NOT_FOUND\r\n");
            }
        }
    }
}

impl EventLoop for Worker {
    fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    fn get_mut_session(&mut self, token: Token) -> Option<&mut Session> {
        self.sessions.get_mut(token.0)
    }

    fn handle_data(&mut self, token: Token) -> Result<(), ()> {
        trace!("handling request for session: {}", token.0);
        if let Some(session) = self.sessions.get_mut(token.0) {
            loop {
                // TODO(bmartin): buffer should allow us to check remaining
                // write capacity.
                if session.write_pending() > (1024 - 6) {
                    // if the write buffer is over-full, skip processing
                    break;
                }
                // TODO(bmartin): using a trait here might make this nicer, eg:
                // request.process(self, wbuf: &mut BytesMut, ... )
                // need to check for performance impact.
                match parse(&mut session.read_buffer) {
                    Ok(request) => match request {
                        Request::Get(request) => Worker::process_get(
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
                            &self.metrics,
                        ),
                        Request::Set(request) => Worker::process_set(
                            &self.config,
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
                            &self.metrics,
                        ),
                        Request::Cas(request) => Worker::process_cas(
                            &self.config,
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
                            &self.metrics,
                        ),
                        Request::Add(request) => Worker::process_add(
                            &self.config,
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
                            &self.metrics,
                        ),
                        Request::Replace(request) => Worker::process_replace(
                            &self.config,
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
                            &self.metrics,
                        ),
                        Request::Delete(request) => Worker::process_delete(
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
                            &self.metrics,
                        ),
                    },
                    Err(ParseError::Incomplete) => {
                        break;
                    }
                    Err(_) => {
                        self.handle_error(token);
                        return Err(());
                    }
                }
            }
            #[allow(clippy::collapsible_if)]
            if session.write_pending() > 0 {
                if session.flush().is_ok() && session.write_pending() > 0 {
                    self.reregister(token);
                }
            }
            Ok(())
        } else {
            // no session for the token
            trace!(
                "attempted to handle data for non-existent session: {}",
                token.0
            );
            Ok(())
        }
    }

    /// Reregister the session given its token
    fn reregister(&mut self, token: Token) {
        trace!("reregistering session: {}", token.0);
        if let Some(session) = self.sessions.get_mut(token.0) {
            if session.reregister(&self.poll).is_err() {
                error!("Failed to reregister");
                self.close(token);
            }
        } else {
            trace!("attempted to reregister non-existent session: {}", token.0);
        }
    }

    fn take_session(&mut self, token: Token) -> Option<Session> {
        if self.sessions.contains(token.0) {
            let session = self.sessions.remove(token.0);
            Some(session)
        } else {
            None
        }
    }

    fn poll(&self) -> &Poll {
        &self.poll
    }
}
