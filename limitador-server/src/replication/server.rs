use std::collections::HashMap;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin};

use clap::{Arg, ArgAction, Command};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, RwLock};
use tokio::time;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use limitador::service::replication::v1::Packet;

use crate::limitador::service::replication::v1::packet::Message;
use crate::limitador::service::replication::v1::replication_client::ReplicationClient;
use crate::limitador::service::replication::v1::replication_server::ReplicationServer;
use crate::limitador::service::replication::v1::{packet, Hello, MembershipUpdate, Peer, Pong};

pub mod limitador {
    pub mod service {
        pub mod replication {
            // clippy will barf on protobuff generated code for enum variants in
            // v3::socket_option::SocketState, so allow this lint
            #[allow(clippy::enum_variant_names, clippy::derive_partial_eq_without_eq)]
            pub mod v1 {
                tonic::include_proto!("limitador.service.replication.v1");
            }
        }
    }
}

fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}

async fn read_hello(in_stream: &mut Streaming<Packet>) -> Result<Hello, Status> {
    match in_stream.next().await {
        Some(Ok(packet)) => match packet.message {
            Some(packet::Message::Hello(value)) => Ok(value),
            _ => Err(Status::invalid_argument("expected Hello")),
        },
        _ => Err(Status::invalid_argument("expected Hello")),
    }
}

async fn read_pong(in_stream: &mut Streaming<Packet>) -> Result<Pong, Status> {
    match in_stream.next().await {
        Some(Ok(packet)) => match packet.message {
            Some(packet::Message::Pong(value)) => Ok(value),
            _ => Err(Status::invalid_argument("expected Pong")),
        },
        _ => Err(Status::invalid_argument("expected Pong")),
    }
}

fn is_disconnect(err: &Status) -> bool {
    if let Some(io_err) = match_for_io_error(err) {
        if io_err.kind() == ErrorKind::BrokenPipe {
            return true;
        }
    }
    return false;
}

#[derive(Clone)]
struct Session {
    state: Arc<RwLock<ReplicationSharedState>>,
    out_stream: MessageSender,
}

impl Session {
    async fn process(&mut self, in_stream: &mut Streaming<Packet>) -> Result<(), Status> {
        // Send a MembershipUpdate to inform the peer about all the members
        {
            let state = self.state.read().await;
            let peers = state.peers();
            self.out_stream
                .clone()
                .send(Ok(Message::MembershipUpdate(MembershipUpdate {
                    peers: peers.clone(),
                })))
                .await?;
        };

        while let Some(result) = in_stream.next().await {
            match result {
                Ok(packet) => {
                    println!("got packet {:?}", packet);
                    match packet.message {
                        Some(packet::Message::Ping(_)) => {
                            self.out_stream
                                .clone()
                                .send(Ok(Message::Pong(Pong {
                                    current_time: SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis()
                                        as u64,
                                })))
                                .await?;
                        }
                        Some(packet::Message::MembershipUpdate(update)) => {
                            // add any new peers to peer_trackers
                            let mut state = self.state.write().await;
                            for peer in update.peers {
                                if !state.peer_trackers.contains_key(&peer.peer_id) {
                                    state.peer_trackers.insert(
                                        peer.peer_id.clone(),
                                        PeerTracker {
                                            peer,
                                            clock_skew: ClockSkew::None(),
                                            session: None,
                                        },
                                    );
                                }
                            }
                        }
                        _ => {
                            return Err(Status::invalid_argument(format!(
                                "unsupported packet {:?}",
                                packet
                            )));
                        }
                    }
                }
                Err(err) => {
                    println!("got err {:?}", err);
                    if is_disconnect(&err) {
                        eprintln!("\tclient disconnected: broken pipe");
                        break;
                    } else {
                        println!("sending err {:?}", err);
                        match self.out_stream.clone().send(Err(err)).await {
                            Ok(_) => (),
                            Err(_err) => break, // response was dropped
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// process_new_stream is called when a new stream is created, it will handle the initial handshake
// and updating the session state in the state.peer_trackers map.
async fn process_new_stream(
    state: Arc<RwLock<ReplicationSharedState>>,
    in_stream: &mut Streaming<Packet>,
    out_stream: &mut MessageSender,
) -> Result<(), Status> {
    // Let the peer know who we are...
    let start = SystemTime::now(); // .duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    {
        let state = state.read().await;
        out_stream
            .clone()
            .send(Ok(Message::Hello(Hello {
                peer_id: state.peer_id.clone(),
                urls: state.urls.clone(),
            })))
            .await?;
    }

    // Wait for the peer to tell us who he is...
    let peer_hello = read_hello(in_stream).await?;

    // respond with a Pong so the peer can calculate the round trip latency
    out_stream
        .clone()
        .send(Ok(Message::Pong(Pong {
            current_time: start.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
        })))
        .await?;

    // Get the pong back from the peer...
    let peer_pong = read_pong(in_stream).await?;
    let end = SystemTime::now();

    let mut session = Session {
        state: state.clone(),
        out_stream: out_stream.clone(),
    };

    // We now know who the peer is and our latency to him.
    {
        let mut state = state.write().await;
        match state.peer_trackers.get_mut(peer_hello.peer_id.as_slice()) {
            Some(tracker) => {
                if tracker.session.is_some() {
                    return Err(Status::already_exists("peer already connected"));
                } else {
                    tracker.session = Some(session.clone());
                }
            }
            None => {
                let latency = end.duration_since(start).unwrap();
                let peer_time = UNIX_EPOCH.add(Duration::from_millis(peer_pong.current_time));
                let peer_time_adj = peer_time.add(latency.div_f32(2.0)); // adjust for round trip latency

                state.peer_trackers.insert(
                    peer_hello.peer_id.clone(),
                    PeerTracker {
                        clock_skew: ClockSkew::new(end, peer_time_adj),
                        peer: Peer {
                            peer_id: peer_hello.peer_id.clone(),
                            urls: peer_hello.urls,
                            latency: latency.as_millis() as u32,
                        },
                        session: Some(session.clone()),
                    },
                );
            }
        }
    }

    let result = session.process(in_stream).await;

    // cleanup, set the session to None
    {
        let mut state = state.write().await;
        match state.peer_trackers.get_mut(peer_hello.peer_id.as_slice()) {
            Some(peer) => {
                peer.session = None;
            }
            None => {}
        }
    }
    return result;
}

#[derive(Copy, Clone, Debug)]
pub enum ClockSkew {
    None(),
    Slow(Duration),
    Fast(Duration),
}

impl ClockSkew {
    fn new(local: SystemTime, remote: SystemTime) -> ClockSkew {
        if local == remote {
            return ClockSkew::None();
        } else if local.gt(&remote) {
            ClockSkew::Slow(local.duration_since(remote).unwrap())
        } else {
            ClockSkew::Fast(remote.duration_since(local).unwrap())
        }
    }
}

pub struct PeerTracker {
    // The peer we are tracking
    peer: Peer,
    // Keep track of the clock skew between us and the peer
    #[allow(dead_code)]
    clock_skew: ClockSkew,
    // The communication session we have with the peer, may be None if not connected
    session: Option<Session>,
}

// ReplicationSharedState holds all the mutable shared state of the server.
pub struct ReplicationSharedState {
    peer_id: Vec<u8>,
    urls: Vec<String>,
    peer_trackers: HashMap<Vec<u8>, PeerTracker>,
}

impl ReplicationSharedState {
    pub fn peers(&self) -> Vec<Peer> {
        let mut peers = Vec::new();
        self.peer_trackers.iter().for_each(|(_, peer_tracker)| {
            peers.push(peer_tracker.peer.clone());
        });
        peers.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        peers
    }
}

// MessageSender is used to abstract the difference between the server and client sender streams...
#[derive(Clone)]
pub enum MessageSender {
    Server(Sender<Result<Packet, Status>>),
    Client(Sender<Packet>),
}
impl MessageSender {
    async fn send(self, message: Result<Message, Status>) -> Result<(), Status> {
        match self {
            MessageSender::Server(sender) => {
                let value = message.map(|x| Packet { message: Some(x) });
                let result = sender.send(value).await;
                result.map_err(|_| Status::unknown("send error"))
            }
            MessageSender::Client(sender) => match message {
                Ok(message) => {
                    let result = sender
                        .send(Packet {
                            message: Some(message),
                        })
                        .await;
                    result.map_err(|_| Status::unknown("send error"))
                }
                Err(err) => Err(err),
            },
        }
    }
}

pub struct Server {
    state: Arc<RwLock<ReplicationSharedState>>,
}

#[tonic::async_trait]
impl limitador::service::replication::v1::replication_server::Replication for Server {
    type StreamStream = Pin<Box<dyn Stream<Item = Result<Packet, Status>> + Send>>;

    // Accepts a connection from a peer and starts a replication session
    async fn stream(
        &self,
        req: Request<Streaming<Packet>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        println!("ReplicationServer::stream");

        let mut in_stream = req.into_inner();
        let (tx, rx) = mpsc::channel(1);

        let state = self.state.clone();
        tokio::spawn(async move {
            let mut sender = MessageSender::Server(tx);
            match process_new_stream(state, &mut in_stream, &mut sender).await {
                Ok(_) => {
                    println!("stream ended");
                }
                Err(err) => {
                    println!("stream ended with error {:?}", err);
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::StreamStream
        ))
    }
}

// Connect to a peer and start a replication session
fn connect_to_peer(state: Arc<RwLock<ReplicationSharedState>>, peer_url: String) {
    // let state = state.clone();
    tokio::spawn(async move {
        // We should keep trying till we have created a peer tracker for the peer_url
        loop {
            time::sleep(Duration::from_secs(1)).await;
            println!("connecting to peer '{}'", peer_url.clone());
            let mut client = match ReplicationClient::connect(peer_url.clone()).await {
                Ok(client) => client,
                Err(err) => {
                    println!("failed to connect with peer '{}': {:?}", peer_url, err);
                    continue;
                }
            };

            let (tx, rx) = mpsc::channel(1);

            let mut in_stream = match client.stream(ReceiverStream::new(rx)).await {
                Ok(response) => response.into_inner(),
                Err(err) => {
                    println!(
                        "failed start replication sessions with peer '{}': {:?}",
                        peer_url, err
                    );
                    continue;
                }
            };

            let mut sender = MessageSender::Client(tx);
            match process_new_stream(state.clone(), &mut in_stream, &mut sender).await {
                Ok(_) => {
                    println!("stream ended");
                    return;
                }
                Err(err) => {
                    println!("failed to connect with peer '{}': {:?}", peer_url, err);
                    continue;
                }
            }
        }
    });
}

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
        Some(id) => id.as_bytes().to_vec(),
        None => Uuid::new_v4().as_bytes().to_vec(),
    };

    let state = Arc::new(RwLock::new(ReplicationSharedState {
        peer_id,
        urls: vec![],
        peer_trackers: HashMap::new(),
    }));

    // Create outbound connections to the configured peers
    if let Some(peer_urls) = peer_urls {
        peer_urls.into_iter().for_each(|peer_url| {
            connect_to_peer(state.clone(), peer_url.clone());
        });
    }

    println!("listening on: id={}", addr);
    tonic::transport::Server::builder()
        .add_service(ReplicationServer::new(Server { state }))
        .serve(addr)
        .await?;

    Ok(())
}
