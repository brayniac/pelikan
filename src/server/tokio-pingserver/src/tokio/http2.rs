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
            if stream.set_nodelay(true).is_err() {
                continue;
            }

            tokio::task::spawn(async move {
                if let Ok(mut conn) = ::h2::server::handshake(stream).await {
                    loop {
                        match conn.accept().await {
                            Some(Ok((request, mut sender))) => {
                                tokio::spawn(async move {
                                    let (_parts, mut body) = request.into_parts();

                                    let mut content = BytesMut::new();
                                    while let Some(data) = body.data().await {
                                        if data.is_err() {
                                            eprintln!("couldn't read request body data");
                                            return;
                                        }
                                        
                                        let data = data.unwrap();

                                        content.extend_from_slice(&data);
                                        let _ = body.flow_control().release_capacity(data.len());

                                        if let Ok(_trailers) = body.trailers().await {
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

                                            if let Ok(mut stream) = sender.send_response(response, false) {
                                                if stream.send_data(content.into(), false).is_ok() {
                                                    if stream.send_trailers(trailers).is_err() {
                                                        eprintln!("couldn't send response trailers");
                                                    }
                                                } else {
                                                    eprintln!("couldn't send response content");
                                                }
                                            } else {
                                                eprintln!("couldn't send response headers");
                                            }
                                        } else {
                                            eprintln!("couldn't read trailers");
                                        }
                                    }
                                });
                            }
                            Some(Err(e)) => {
                                eprintln!("error: {e}");
                                break;
                            }
                            None => {
                                tokio::task::yield_now().await;
                                continue;
                            }
                        }
                    }
                }
            });
        }
    }
}
