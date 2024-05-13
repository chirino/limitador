use std::{error::Error, io::ErrorKind, pin::Pin};
use std::collections::HashMap;
use std::ops::Add;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;

use tokio::sync::{mpsc, RwLock};
use tokio::sync::mpsc::Sender;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Code, Request, Response, Status, Streaming};

use crate::clock_skew::ClockSkew;
use crate::limitador::service::replication::v1::{Hello, packet, Packet, Peer, Pong};
use crate::limitador::service::replication::v1::packet::Message;
use crate::limitador::service::replication::v1::replication_client::ReplicationClient;
use crate::limitador::service::replication::v1::replication_server::Replication;
use crate::session::Session;
use crate::shared::{PeerId, PeerTracker, ReplicationSharedState};

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

pub fn is_disconnect(err: &Status) -> bool {
    if let Some(io_err) = match_for_io_error(err) {
        if io_err.kind() == ErrorKind::BrokenPipe {
            return true;
        }
    }
    return false;
}

// MessageSender is used to abstract the difference between the server and client sender streams...
#[derive(Clone)]
pub enum MessageSender {
    Server(Sender<Result<Packet, Status>>),
    Client(Sender<Packet>),
}

impl MessageSender {
    pub async fn send(self, message: Result<Message, Status>) -> Result<(), Status> {
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

#[derive(Clone)]
pub struct Server {
    state: Arc<RwLock<ReplicationSharedState>>,
}

impl Server {
    // Create a new server with the given peer_id
    pub(crate) fn new(peer_id: PeerId) -> Server {
        Server {
            state: Arc::new(RwLock::new(ReplicationSharedState {
                peer_id,
                urls: vec![],
                peer_trackers: HashMap::new(),
            })),
        }
    }

    // Connect to a peer and start a replication session.  This returns once the session handshake
    // completes.
    pub async fn connect_to_peer(&self,
                                 peer_url: String,
    ) -> Result<(), Status> {
        println!("connecting to peer '{}'", peer_url.clone());
        let mut client = match ReplicationClient::connect(peer_url.clone()).await {
            Ok(client) => client,
            Err(err) => {
                return Err(Status::new(Code::Unknown, err.to_string()));
            }
        };

        let (tx, rx) = mpsc::channel(1);

        let mut in_stream = client.stream(ReceiverStream::new(rx)).await?.into_inner();
        let mut sender = MessageSender::Client(tx);
        let mut session = self.handshake(&mut in_stream, &mut sender).await?;

        // Session is now established, process the session async...
        tokio::spawn(async move {
            match session.process(&mut in_stream).await {
                Ok(_) => {
                    println!("client initiated stream ended");
                }
                Err(err) => {
                    println!("client initiated stream processing failed {:?}", err);
                }
            }
            session.close().await;
        });

        Ok(())
    }

    // Reconnect failed peers periodically
    pub async fn reconnect_to_failed_peers(&self) -> () {
        let failed_peers: Vec<_> = {
            let state = self.state.read().await;
            state
                .peer_trackers
                .iter()
                .filter_map(|(_, peer_tracker)| {
                    if peer_tracker.session.is_none() {
                        Some(peer_tracker.peer.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        for peer in failed_peers {
            for url in peer.urls {
                match self.connect_to_peer(url.clone()).await {
                    Ok(_) => break,
                    Err(err) => {
                        println!("failed to connect with peer '{}': {:?}", url, err);
                    }
                }
            }
        }
    }

    // handshake is called when a new stream is created, it will handle the initial handshake
    // and updating the session state in the state.peer_trackers map.
    async fn handshake(&self,
                       in_stream: &mut Streaming<Packet>,
                       out_stream: &mut MessageSender,
    ) -> Result<Session, Status> {
        // Let the peer know who we are...
        let start = SystemTime::now(); // .duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
        {
            let state = self.state.read().await;
            out_stream
                .clone()
                .send(Ok(Message::Hello(Hello {
                    peer_id: state.peer_id.vec(),
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

        let peer_id = PeerId(peer_hello.peer_id.clone());
        let session = Session {
            peer_id: peer_id.clone(),
            state: self.state.clone(),
            out_stream: out_stream.clone(),
        };

        // We now know who the peer is and our latency to him.
        {
            let mut state = self.state.write().await;
            match state.peer_trackers.get_mut(&peer_id) {
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
                    let tracker = PeerTracker {
                        clock_skew: ClockSkew::new(end, peer_time_adj),
                        peer: Peer {
                            peer_id: peer_hello.peer_id.clone(),
                            urls: peer_hello.urls,
                            latency: latency.as_millis() as u32,
                        },
                        session: Some(session.clone()),
                    };
                    println!("peer {} clock skew: {}", peer_id, &tracker.clock_skew);
                    state.peer_trackers.insert(peer_id.clone(), tracker);
                }
            }
        }
        return Ok(session);
    }
}

#[tonic::async_trait]
impl Replication for Server {
    type StreamStream = Pin<Box<dyn Stream<Item=Result<Packet, Status>> + Send>>;

    // Accepts a connection from a peer and starts a replication session
    async fn stream(
        &self,
        req: Request<Streaming<Packet>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        println!("ReplicationServer::stream");

        let mut in_stream = req.into_inner();
        let (tx, rx) = mpsc::channel(1);

        let server = self.clone();
        tokio::spawn(async move {
            let mut sender = MessageSender::Server(tx);
            match server.handshake(&mut in_stream, &mut sender).await {
                Ok(mut session) => {
                    match session.process(&mut in_stream).await {
                        Ok(_) => {
                            println!("server accepted stream ended");
                        }
                        Err(err) => {
                            println!("server accepted stream processing failed {:?}", err);
                        }
                    }
                    session.close().await;
                }
                Err(err) => {
                    println!("stream handshake failed {:?}", err);
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::StreamStream
        ))
    }
}