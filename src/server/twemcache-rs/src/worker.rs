// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use metrics::Stat;

use core::hash::BuildHasher;

use crate::common::Message;
use crate::event_loop::EventLoop;
use crate::protocol::data::*;
use crate::session::*;
use ahash::RandomState;
use bytes::BytesMut;
use config::TimeType;
use std::convert::TryInto;
use std::time::SystemTime;

use config::seg_cache::Eviction;
use segcache::{Policy, SegCache, SegCacheError};

use rustcommon_time::*;

use crate::*;

use std::sync::Arc;

/// A `Worker` handles events on `Session`s
pub struct Worker<S>
where
    S: BuildHasher,
{
    config: Arc<Config>,
    message_receiver: Receiver<Message>,
    message_sender: SyncSender<Message>,
    poll: Poll,
    session_receiver: Receiver<Session>,
    session_sender: SyncSender<Session>,
    sessions: Slab<Session>,
    data: SegCache<S>,
}

pub struct CacheHasher {
    inner: ahash::RandomState,
}

impl Default for CacheHasher {
    fn default() -> Self {
        let inner = RandomState::with_seeds(
            0xbb8c484891ec6c86,
            0x0522a25ae9c769f9,
            0xeed2797b9571bc75,
            0x4feb29c1fbbd59d0,
        );
        Self { inner }
    }
}

impl BuildHasher for CacheHasher {
    type Hasher = ahash::AHasher;

    fn build_hasher(&self) -> Self::Hasher {
        self.inner.build_hasher()
    }
}

impl Worker<CacheHasher> {
    /// Create a new `Worker` which will get new `Session`s from the MPSC queue
    pub fn new(config: Arc<Config>) -> Result<Self, std::io::Error> {
        let poll = Poll::new().map_err(|e| {
            error!("{}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to create epoll instance")
        })?;
        let sessions = Slab::<Session>::new();

        let (session_sender, session_receiver) = sync_channel(128);
        let (message_sender, message_receiver) = sync_channel(128);

        let eviction = match config.seg_cache().eviction() {
            Eviction::None => Policy::None,
            Eviction::Random => Policy::Random,
            Eviction::Fifo => Policy::Fifo,
            Eviction::Cte => Policy::Cte,
            Eviction::Util => Policy::Util,
        };

        let data = SegCache::builder()
            .power(config.seg_cache().hash_power())
            .segments(config.seg_cache().segments())
            .seg_size(config.seg_cache().seg_size())
            .eviction(eviction)
            .hasher(CacheHasher::default())
            .build();

        Ok(Self {
            config,
            poll,
            message_receiver,
            message_sender,
            session_receiver,
            session_sender,
            sessions,
            data,
        })
    }

    /// Run the `Worker` in a loop, handling new session events
    pub fn run(&mut self) {
        let mut events = Events::with_capacity(self.config.worker().nevent());
        let timeout = Some(std::time::Duration::from_millis(
            self.config.worker().timeout() as u64,
        ));

        loop {
            #[cfg(feature = "stats")]
            increment_counter!(&Stat::WorkerEventLoop);

            self.data.expire();

            // get events with timeout
            if self.poll.poll(&mut events, timeout).is_err() {
                error!("Error polling");
            }

            #[cfg(feature = "stats")]
            increment_counter_by!(
                &Stat::WorkerEventTotal,
                events.iter().count().try_into().unwrap(),
            );

            // process all events
            for event in events.iter() {
                let token = event.token();

                // event for existing session
                trace!("got event for session: {}", token.0);

                // handle error events first
                if event.is_error() {
                    #[cfg(feature = "stats")]
                    increment_counter!(&Stat::WorkerEventError);
                    self.handle_error(token);
                }

                // handle write events before read events to reduce write buffer
                // growth if there is also a readable event
                if event.is_writable() {
                    #[cfg(feature = "stats")]
                    increment_counter!(&Stat::WorkerEventWrite);
                    self.do_write(token);
                }

                // read events are handled last
                if event.is_readable() {
                    #[cfg(feature = "stats")]
                    increment_counter!(&Stat::WorkerEventRead);
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
    pub fn process_get<S: BuildHasher>(
        request: GetRequest,
        write_buffer: &mut BytesMut,
        data: &mut SegCache<S>,
    ) {
        let mut found = 0;
        #[cfg(feature = "stats")]
        increment_counter!(&Stat::Get);
        for key in request.keys() {
            #[cfg(feature = "stats")]
            increment_counter!(&Stat::GetKey);
            #[cfg(not(feature = "noop"))]
            {
                if let Some(item) = data.get(key) {
                    write_buffer.extend_from_slice(b"VALUE ");
                    write_buffer.extend_from_slice(key);
                    let f = item.optional().unwrap();
                    let flags: u32 = u32::from_be_bytes([f[0], f[1], f[2], f[3]]);
                    write_buffer.extend_from_slice(
                        format!(" {} {}\r\n", flags, item.value().len()).as_bytes(),
                    );
                    write_buffer.extend_from_slice(item.value());
                    write_buffer.extend_from_slice(b"\r\n");
                    found += 1;
                    #[cfg(feature = "stats")]
                    increment_counter!(&Stat::GetKeyHit);
                } else {
                    #[cfg(feature = "stats")]
                    increment_counter!(&Stat::GetKeyMiss);
                }
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
    pub fn process_gets<S: BuildHasher>(
        request: GetsRequest,
        write_buffer: &mut BytesMut,
        data: &mut SegCache<S>,
    ) {
        let mut found = 0;
        #[cfg(feature = "stats")]
        increment_counter!(&Stat::Get);
        for key in request.keys() {
            #[cfg(feature = "stats")]
            increment_counter!(&Stat::GetKey);
            #[cfg(not(feature = "noop"))]
            {
                if let Some(item) = data.get(key) {
                    write_buffer.extend_from_slice(b"VALUE ");
                    write_buffer.extend_from_slice(key);
                    let f = item.optional().unwrap();
                    let flags: u32 = u32::from_be_bytes([f[0], f[1], f[2], f[3]]);
                    write_buffer.extend_from_slice(
                        format!(" {} {} {}\r\n", flags, item.value().len(), item.cas()).as_bytes(),
                    );
                    write_buffer.extend_from_slice(item.value());
                    write_buffer.extend_from_slice(b"\r\n");
                    found += 1;
                    #[cfg(feature = "stats")]
                    increment_counter!(&Stat::GetKeyHit);
                } else {
                    #[cfg(feature = "stats")]
                    increment_counter!(&Stat::GetKeyMiss);
                }
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
    pub fn process_set<S: BuildHasher>(
        config: &Arc<Config>,
        request: SetRequest,
        write_buffer: &mut BytesMut,
        data: &mut SegCache<S>,
    ) {
        #[cfg(feature = "stats")]
        increment_counter!(&Stat::Set);
        let reply = !request.noreply();

        // convert the expiry to a delta TTL
        let ttl: u32 = match config.time().time_type() {
            TimeType::Unix => {
                let epoch = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as u32;
                request.expiry().wrapping_sub(epoch)
            }
            TimeType::Delta => request.expiry(),
            TimeType::Memcache => {
                if request.expiry() == 0 {
                    0
                } else if request.expiry() < 60 * 60 * 24 * 30 {
                    request.expiry()
                } else {
                    let epoch = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as u32;
                    request.expiry().wrapping_sub(epoch)
                }
            }
        };
        #[cfg(not(feature = "noop"))]
        {
            #[allow(clippy::collapsible_if)]
            if data
                .insert(
                    request.key(),
                    request.value(),
                    Some(&request.flags().to_be_bytes()),
                    CoarseDuration::from_secs(ttl),
                )
                .is_ok()
            {
                #[cfg(feature = "stats")]
                increment_counter!(&Stat::SetStored);
                if reply {
                    write_buffer.extend_from_slice(b"STORED\r\n");
                }
            } else {
                #[cfg(feature = "stats")]
                increment_counter!(&Stat::SetNotstored);
                if reply {
                    write_buffer.extend_from_slice(b"NOT_STORED\r\n");
                }
            }
        }
        #[cfg(feature = "noop")]
        {
            if reply {
                write_buffer.extend_from_slice(b"NOT_STORED\r\n");
            }
        }
    }

    pub fn process_cas<S: BuildHasher>(
        config: &Arc<Config>,
        request: CasRequest,
        write_buffer: &mut BytesMut,
        data: &mut SegCache<S>,
    ) {
        #[cfg(feature = "metrics")]
        increment_counter!(&Stat::Cas);
        let reply = !request.noreply();
        // convert the expiry to a delta TTL
        let ttl: u32 = match config.time().time_type() {
            TimeType::Unix => {
                let epoch = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as u32;
                request.expiry().wrapping_sub(epoch)
            }
            TimeType::Delta => request.expiry(),
            TimeType::Memcache => {
                if request.expiry() == 0 {
                    0
                } else if request.expiry() < 60 * 60 * 24 * 30 {
                    request.expiry()
                } else {
                    let epoch = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as u32;
                    request.expiry().wrapping_sub(epoch)
                }
            }
        };
        match data.cas(
            request.key(),
            request.value(),
            Some(&request.flags().to_be_bytes()),
            CoarseDuration::from_secs(ttl),
            request.cas() as u32,
        ) {
            Ok(_) => {
                #[cfg(feature = "metrics")]
                let _ = metrics.increment_counter(Stat::CasStored, 1);
                if reply {
                    write_buffer.extend_from_slice(b"STORED\r\n");
                }
            }
            Err(SegCacheError::NotFound) => {
                #[cfg(feature = "metrics")]
                let _ = metrics.increment_counter(Stat::CasNotFound, 1);
                if reply {
                    write_buffer.extend_from_slice(b"NOT_FOUND\r\n");
                }
            }
            Err(SegCacheError::Exists) => {
                #[cfg(feature = "metrics")]
                let _ = metrics.increment_counter(Stat::CasExists, 1);
                if reply {
                    write_buffer.extend_from_slice(b"EXISTS\r\n");
                }
            }
            Err(_) => {
                #[cfg(feature = "metrics")]
                let _ = metrics.increment_counter(Stat::CasEx, 1);
                if reply {
                    write_buffer.extend_from_slice(b"NOT_STORED\r\n");
                }
            }
        }
    }

    // TODO(bmartin): this does not handle expiry.
    pub fn process_add<S: BuildHasher>(
        config: &Arc<Config>,
        request: AddRequest,
        write_buffer: &mut BytesMut,
        data: &mut SegCache<S>,
    ) {
        #[cfg(feature = "stats")]
        increment_counter!(&Stat::Add);
        let reply = !request.noreply();
        // convert the expiry to a delta TTL
        let ttl: u32 = match config.time().time_type() {
            TimeType::Unix => {
                let epoch = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as u32;
                request.expiry().wrapping_sub(epoch)
            }
            TimeType::Delta => request.expiry(),
            TimeType::Memcache => {
                if request.expiry() == 0 {
                    0
                } else if request.expiry() < 60 * 60 * 24 * 30 {
                    request.expiry()
                } else {
                    let epoch = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as u32;
                    request.expiry().wrapping_sub(epoch)
                }
            }
        };
        #[allow(clippy::collapsible_if)]
        if data.get(request.key()).is_none()
            && data
                .insert(
                    request.key(),
                    request.value(),
                    Some(&request.flags().to_be_bytes()),
                    CoarseDuration::from_secs(ttl),
                )
                .is_ok()
        {
            #[cfg(feature = "stats")]
            increment_counter!(&Stat::AddStored);
            if reply {
                write_buffer.extend_from_slice(b"STORED\r\n");
            }
        } else {
            #[cfg(feature = "stats")]
            increment_counter!(&Stat::AddNotstored);
            if reply {
                write_buffer.extend_from_slice(b"NOT_STORED\r\n");
            }
        }
    }

    // TODO(bmartin): this does not handle expiry.
    pub fn process_replace<S: BuildHasher>(
        config: &Arc<Config>,
        request: ReplaceRequest,
        write_buffer: &mut BytesMut,
        data: &mut SegCache<S>,
    ) {
        #[cfg(feature = "stats")]
        increment_counter!(&Stat::Replace);
        let reply = !request.noreply();
        // convert the expiry to a delta TTL
        let ttl: u32 = match config.time().time_type() {
            TimeType::Unix => {
                let epoch = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as u32;
                request.expiry().wrapping_sub(epoch)
            }
            TimeType::Delta => request.expiry(),
            TimeType::Memcache => {
                if request.expiry() == 0 {
                    0
                } else if request.expiry() < 60 * 60 * 24 * 30 {
                    request.expiry()
                } else {
                    let epoch = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as u32;
                    request.expiry().wrapping_sub(epoch)
                }
            }
        };
        #[allow(clippy::collapsible_if)]
        if data.get(request.key()).is_some()
            && data
                .insert(
                    request.key(),
                    request.value(),
                    Some(&request.flags().to_be_bytes()),
                    CoarseDuration::from_secs(ttl),
                )
                .is_ok()
        {
            #[cfg(feature = "stats")]
            increment_counter!(&Stat::ReplaceStored);
            if reply {
                write_buffer.extend_from_slice(b"STORED\r\n");
            }
        } else {
            #[cfg(feature = "stats")]
            increment_counter!(&Stat::ReplaceNotstored);
            if reply {
                write_buffer.extend_from_slice(b"NOT_STORED\r\n");
            }
        }
    }

    pub fn process_delete<S: BuildHasher>(
        request: DeleteRequest,
        write_buffer: &mut BytesMut,
        data: &mut SegCache<S>,
    ) {
        #[cfg(feature = "stats")]
        increment_counter!(&Stat::Delete);
        let reply = !request.noreply();
        #[allow(clippy::collapsible_if)]
        if data.delete(request.key()) {
            #[cfg(feature = "stats")]
            increment_counter!(&Stat::DeleteDeleted);
            if reply {
                write_buffer.extend_from_slice(b"DELETED\r\n");
            }
        } else {
            #[cfg(feature = "stats")]
            increment_counter!(&Stat::DeleteNotfound);
            if reply {
                write_buffer.extend_from_slice(b"NOT_FOUND\r\n");
            }
        }
    }
}

impl<S> EventLoop for Worker<S>
where
    S: BuildHasher,
{
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
                        Request::Get(request) => {
                            Worker::process_get(request, &mut session.write_buffer, &mut self.data)
                        }
                        Request::Gets(request) => {
                            Worker::process_gets(request, &mut session.write_buffer, &mut self.data)
                        }
                        Request::Set(request) => Worker::process_set(
                            &self.config,
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
                        ),
                        Request::Cas(request) => Worker::process_cas(
                            &self.config,
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
                        ),
                        Request::Add(request) => Worker::process_add(
                            &self.config,
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
                        ),
                        Request::Replace(request) => Worker::process_replace(
                            &self.config,
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
                        ),
                        Request::Delete(request) => Worker::process_delete(
                            request,
                            &mut session.write_buffer,
                            &mut self.data,
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
