use http::Version;
use chrono::Utc;
use chrono::DateTime;
use http::HeaderMap;
use bytes::BytesMut;
use tokio::net::TcpListener;
use std::sync::Arc;
use crate::Config;

pub async fn run(config: Arc<Config>) {
	let listener = TcpListener::bind(config.listen()).await.unwrap();

	loop {
        if let Ok((stream, _)) = listener.accept().await {
            let _ = stream.set_nodelay(true).is_err();

            tokio::task::spawn(async move {
                match ::h2::server::handshake(stream).await {
                    Ok(mut conn) => {
                        loop {
                            match conn.accept().await {
                                Some(Ok((request, mut sender))) => {
                                    tokio::spawn(async move {
                                        let (_parts, mut body) = request.into_parts();

                                        let mut content = BytesMut::new();

                                        eprintln!("receiving body");

                                        while let Some(data) = body.data().await {
                                            eprintln!("got body chunk");

                                            if data.is_err() {
                                                eprintln!("couldn't read request body data");
                                                return;
                                            }
                                            
                                            let data = data.unwrap();

                                            content.extend_from_slice(&data);
                                            let _ = body.flow_control().release_capacity(data.len());

                                            eprintln!("receiving trailers");

                                            if !body.is_end_of_stream() {
                                                if body.tailers().await.is_err() {
                                                    eprintln!("couldn't read trailers");
                                                    return;
                                                } else {
                                                    eprintln!("got trailers");
                                                }
                                            } else {
                                                eprintln!("no trailers to get");
                                            }

                                            let now: DateTime<Utc> = Utc::now();

                                            let response = http::response::Builder::new()
                                                .status(200)
                                                .version(Version::HTTP_2)
                                                .header("content-type", "application/grpc")
                                                .header("date", now.to_rfc2822())
                                                .body(())
                                                .unwrap();

                                            let content = BytesMut::zeroed(5);

                                            let mut trailers = HeaderMap::new();
                                            trailers.append("grpc-status", 0.into());

                                            eprintln!("sending response headers");

                                            if let Ok(mut stream) = sender.send_response(response, false) {
                                                eprintln!("sending response content");

                                                if stream.send_data(content.into(), false).is_ok() {
                                                    eprintln!("sending response trailers");

                                                    if stream.send_trailers(trailers).is_err() {
                                                        eprintln!("couldn't send response trailers");
                                                    }
                                                } else {
                                                    eprintln!("couldn't send response content");
                                                }
                                            } else {
                                                eprintln!("couldn't send response headers");
                                            }
                                        }
                                    });
                                }
                                Some(Err(e)) => {
                                    eprintln!("error: {e}");
                                    break;
                                }
                                None => {
                                    continue;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("error during handshake: {e}");
                    }
                }
            });
        }
    }
}
