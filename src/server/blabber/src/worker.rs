use crate::*;

// a task that serves messages to a client
pub async fn serve(
    config: Config,
    mut socket: tokio::net::TcpStream,
    mut rx: Receiver<Message>,
) -> Result<(), std::io::Error> {
    // create a uniform distribution for selecting a possible delay time
    let delay = if config.max_delay_us == 0 {
        None
    } else {
        Some(Uniform::from(0..config.max_delay_us))
    };

    // small fast PRNG for generating delays
    let mut rng = SmallRng::from_entropy();

    loop {
        match rx.recv().await {
            Ok(message) => {
                // apply random delay if configured
                if let Some(delay) = delay {
                    let delay = rng.sample(delay);

                    // only delay if the delay is non-zero
                    if delay > 0 {
                        tokio::time::sleep(Duration::from_micros(delay)).await;
                    }
                }

                // write to the socket
                socket.write_all(&message.data).await?;
            }
            Err(RecvError::Lagged(_count)) => {
                // do nothing if we lagged
            }
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "queue stopped",
                ));
            }
        }
    }
}
