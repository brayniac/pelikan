use tonic::{transport::Server as TonicServer, Request, Response, Status};

use pingpong::ping_server::{Ping, PingServer};
use pingpong::{PingRequest, PongResponse};

pub mod pingpong {
    tonic::include_proto!("pingpong");
}

#[derive(Debug, Default)]
pub struct Server {}

#[tonic::async_trait]
impl Ping for Server {
    async fn ping(
        &self,
        _request: Request<PingRequest>,
    ) -> Result<Response<PongResponse>, Status> {
        Ok(Response::new(PongResponse { }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:12321".parse()?;
    let greeter = Server::default();

    TonicServer::builder()
        .add_service(PingServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
