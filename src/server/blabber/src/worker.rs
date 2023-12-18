use crate::config::Delay;
use tokio::io::AsyncWriteExt;
use crate::*;
use rand_distr::{Zipf};

pub enum DelayDistr {
    Uniform(Uniform<f64>),
    Zipf(Zipf<f64>)
}

impl DelayDistr {
    pub fn sample(&self, rng: &mut SmallRng) -> Duration {
        match self {
            Self::Uniform(distr) => {
                Duration::from_millis(rng.sample(distr) as u64)
            }
            Self::Zipf(distr) => {
                Duration::from_millis((rng.sample(distr) as u64).saturating_sub(1))
            }
        }
    }
}

// a task that serves messages to a client
pub async fn serve(
    config: Arc<Config>,
    mut socket: tokio::net::TcpStream,
    mut rx: Receiver<Message>,
) -> Result<(), std::io::Error> {
    // create a uniform distribution for selecting a possible delay time
    let delay: Option<DelayDistr> = if config.max_delay == 0 {
        None
    } else {
        match config.delay {
            Delay::Uniform => {
                Some(DelayDistr::Uniform(Uniform::from(0.0..(config.max_delay as f64))))
            }
            Delay::Zipf => {
                Some(DelayDistr::Zipf(Zipf::new(config.max_delay + 1, 1.0).unwrap()))
            }
        }
    };

    // small fast PRNG for generating delays
    let mut rng = SmallRng::from_entropy();

    loop {
        match rx.recv().await {
            Ok(message) => {
                // apply random delay if configured
                if let Some(ref delay) = delay {
                    let delay = delay.sample(&mut rng);

                    // only delay if the delay is non-zero
                    if delay.as_millis() > 0 {
                        tokio::time::sleep(delay).await;
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
