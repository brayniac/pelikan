
// use std::sync::Arc;
use core::ops::DerefMut;

// pub use async_channel::SendError;
// pub use async_channel::RecvError;
use core::ops::Deref;
use std::sync::Arc;
use crossbeam::queue::ArrayQueue;

use tachyonix::*;


// use async_channel::Sender;

// use async_channel::Receiver;

pub struct Switchboard<T,U> {
    t_senders: Arc<ArrayQueue<DirectedQueueInner<T, U>>>,
    u_senders: Arc<ArrayQueue<DirectedQueueInner<U, T>>>,
}

impl<T: Send, U: Send> Switchboard<T, U> {
    pub fn new(capacity: usize, t_senders: usize, u_senders: usize) -> Self {
        bounded(capacity, t_senders, u_senders)
    }

    pub fn get_t_sender(&self) -> Option<DirectedQueue<T, U>> {
        let mut queue = self.t_senders.pop()?;

        queue.key = queue.key.wrapping_add(1);

        Some(DirectedQueue {
            queue: Some(queue),
            free_q: self.t_senders.clone(),
        })
    }

    pub fn get_u_sender(&self) -> Option<DirectedQueue<U, T>> {
        let mut queue = self.u_senders.pop()?;

        queue.key = queue.key.wrapping_add(1);

        Some(DirectedQueue {
            queue: Some(queue),
            free_q: self.u_senders.clone(),
        })
    }
}

pub struct DirectedQueue<T, U> {
    free_q: Arc<ArrayQueue<DirectedQueueInner<T, U>>>,
    queue: Option<DirectedQueueInner<T, U>>,
}

impl<T, U> Deref for DirectedQueue<T, U> {
    type Target = DirectedQueueInner<T,U>;
    
    fn deref(&self) -> &Self::Target {
        self.queue.as_ref().unwrap()
    }
}

impl<T, U> DerefMut for DirectedQueue<T, U> {
    fn deref_mut(&mut self) -> &mut DirectedQueueInner<T, U> {
        self.queue.as_mut().unwrap()
    }
}

pub struct DirectedQueueInner<T, U> {
    id: usize,
    key: usize,
    senders: Vec<Sender<TrackedMsg<T>>>,
    receiver: Receiver<TrackedMsg<U>>,
}

impl<T, U> DirectedQueueInner<T, U> {
    pub async fn recv(&mut self) -> Result<TrackedMsg<U>, RecvError> {
        self.receiver.recv().await
    }

    pub async fn send_to(&self, dst_id: usize, key: usize, msg: T) -> Result<(), SendError<TrackedMsg<T>>> {
        let id = self.id;

        let item = TrackedMsg {
            msg,
            sender: id,
            key,
        };

        if dst_id > self.senders.len() {
            panic!("invalid dst_id");
        }

        self.senders[dst_id].send(item).await
    }

    pub fn key(&self) -> usize {
        self.key
    }
}

pub struct TrackedMsg<T> {
    msg: T,
    sender: usize,
    key: usize,
}

impl<T> TrackedMsg<T> {
    pub fn sender(&self) -> usize {
        self.sender
    }

    pub fn key(&self) -> usize {
        self.key
    }

    pub fn into_inner(self) -> T {
        self.msg
    }
}

impl<T, U> Drop for DirectedQueue<T, U> {
    fn drop(&mut self) {
        let _ = self.free_q.push(self.queue.take().unwrap());
    }
}

fn bounded<T: Send, U: Send>(capacity: usize, t_senders: usize, u_senders: usize) -> Switchboard<T, U> {
    let senders = t_senders;
    let receivers = u_senders;

    let mut t_senders: Vec<Vec<Sender<TrackedMsg<T>>>> = Vec::with_capacity(senders);
    t_senders.resize_with(senders, || { Vec::with_capacity(receivers) });

    let mut t_receivers: Vec<Receiver<TrackedMsg<T>>> = Vec::with_capacity(receivers);

    for _ in 0..receivers {
        let (t_send, t_recv) = tachyonix::channel(capacity);

        t_receivers.push(t_recv);

        for t_sender in t_senders.iter_mut() {
            t_sender.push(t_send.clone());
        }
    }

    let mut u_senders: Vec<Vec<Sender<TrackedMsg<U>>>> = Vec::with_capacity(receivers);
    u_senders.resize_with(receivers, || { Vec::with_capacity(senders) });

    let mut u_receivers: Vec<Receiver<TrackedMsg<U>>> = Vec::with_capacity(senders);

    for _ in 0..senders {
        let (u_send, u_recv) = tachyonix::channel(capacity);

        u_receivers.push(u_recv);

        for u_sender in u_senders.iter_mut() {
            u_sender.push(u_send.clone());
        }
    }

    let t_sender_free_q: Arc<ArrayQueue<DirectedQueueInner<T, U>>> = Arc::new(ArrayQueue::new(t_senders.len()));

    for (id, (senders, receiver)) in t_senders.drain(..).zip(u_receivers.drain(..)).enumerate() {
        let _ = t_sender_free_q.push(
            DirectedQueueInner {
                senders: senders.clone(),
                receiver,
                id,
                key: 0,
            }
        );
    }

    let u_sender_free_q: Arc<ArrayQueue<DirectedQueueInner<U, T>>> = Arc::new(ArrayQueue::new(u_senders.len()));

    for (id, (senders, receiver)) in u_senders.drain(..).zip(t_receivers.drain(..)).enumerate() {
        let _ = u_sender_free_q.push(
            DirectedQueueInner {
                senders: senders.clone(),
                receiver,
                id,
                key: 0,
            }
        );
    }

    Switchboard {
        t_senders: t_sender_free_q,
        u_senders: u_sender_free_q,
    }
}
