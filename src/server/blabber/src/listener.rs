use crate::*;

// a task that listens for new connections and spawns worker tasks to serve the
// new clients
pub async fn listen(
    config: Arc<Config>,
    tx: Sender<Message>,
    worker_runtime: Arc<Runtime>,
) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(config.server.addr).await?;

    info!(
        "running server on: {}",
        listener
            .local_addr()
            .map(|v| format!("{v}"))
            .unwrap_or_else(|_| "unknown address".to_string())
    );

    loop {
        let (socket, _) = listener.accept().await?;

        if socket.set_nodelay(true).is_err() {
            eprintln!("couldn't set TCP_NODELAY. Dropping connection");
        }

        // spawn the worker task onto the worker runtime
        worker_runtime.spawn(crate::worker::serve(config.clone(), socket, tx.subscribe()));
    }
}
