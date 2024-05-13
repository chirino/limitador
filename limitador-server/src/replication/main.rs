use std::net::ToSocketAddrs;
use std::time::Duration;

use clap::{Arg, ArgAction, Command};
use tokio::time;
use uuid::Uuid;

use crate::limitador::service::replication::v1::replication_server::ReplicationServer;
use crate::server::Server;
use crate::shared::PeerId;

mod limitador;
mod clock_skew;
mod server;
mod session;
mod shared;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmdline = Command::new("replication")
        .version("0.1")
        .about("Limitador replication server")
        .arg(
            Arg::new("address")
                .long("address")
                .default_value("[::1]:9001")
                .help("The ip:port to listen on for replication"),
        )
        .arg(
            Arg::new("peer-url")
                .long("peer-url")
                .action(ArgAction::Append)
                .help("A replication peer url"),
        )
        .arg(Arg::new("id").long("id").help("A replication peer"));

    let matches = cmdline.get_matches();

    let addr = matches
        .get_one::<String>("address")
        .expect("`address` is required")
        .as_str()
        .to_socket_addrs()
        .expect("invalid `address`")
        .next()
        .expect("invalid `address`")
        .clone();

    let peer_urls = matches.get_many::<String>("peer-url").clone();

    let peer_id = match matches.get_one::<String>("id") {
        Some(id) => PeerId(id.as_bytes().to_vec()),
        None => PeerId(Uuid::new_v4().as_bytes().to_vec()),
    };


    let server = Server::new(peer_id.clone());

    // Create outbound connections to the configured peers
    if let Some(peer_urls) = peer_urls {
        peer_urls.into_iter().for_each(|peer_url| {
            let server = server.clone();
            let peer_url = peer_url.clone();
            tokio::spawn(async move {
                // Keep trying until we get once successful connection handshake.
                loop {
                    match server.connect_to_peer(peer_url.clone()).await {
                        Ok(_) => break,
                        Err(err) => {
                            println!("failed to connect with peer '{}': {:?}", peer_url, err);
                            time::sleep(Duration::from_secs(1)).await
                        }
                    }
                }
            });
        })
    }

    // Periodically reconnect to failed peers
    {
        let server = server.clone();
        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(1)).await;
                server.reconnect_to_failed_peers().await;
            }
        });
    }

    println!("peer '{}' listening on: id={}", peer_id, addr);
    tonic::transport::Server::builder()
        .add_service(ReplicationServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
