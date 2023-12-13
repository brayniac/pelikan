use crate::*;

// continuously publish messages
pub fn publish(config: Config, tx: Sender<Message>) {
    let hash_builder = hasher();

    let interval = Duration::from_secs(1).as_nanos() as u64 / config.publisher.rate;

    let now = SystemTime::now();
    let offset_ns = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let mut next_period =
        SystemTime::UNIX_EPOCH + Duration::from_nanos((1 + (offset_ns / interval)) * interval);

    loop {
        while SystemTime::now() < next_period {
            std::thread::sleep(Duration::from_micros(100));
        }
        let _ = tx.send(Message::new(&hash_builder, config.publisher.message_len));
        next_period += Duration::from_nanos(interval);
    }
}
