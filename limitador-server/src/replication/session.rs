use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};

use crate::clock_skew::ClockSkew;
use crate::limitador::service::replication::v1::{MembershipUpdate, Packet, packet, Pong};
use crate::limitador::service::replication::v1::packet::Message;
use crate::server;
use crate::server::MessageSender;
use crate::shared::{PeerId, PeerTracker, ReplicationSharedState};

#[derive(Clone)]
pub struct Session {
    pub state: Arc<RwLock<ReplicationSharedState>>,
    pub out_stream: MessageSender,
    pub peer_id: PeerId,
}

impl Session {
    pub async fn close(&mut self) {
        let mut state = self.state.write().await;
        match state.peer_trackers.get_mut(&self.peer_id) {
            Some(peer) => {
                peer.session = None;
            }
            None => {}
        }
    }

    pub async fn process(&mut self, in_stream: &mut Streaming<Packet>) -> Result<(), Status> {
        // Send a MembershipUpdate to inform the peer about all the members
        // We should resend it again if we learn of new members.
        self.out_stream
            .clone()
            .send(Ok(Message::MembershipUpdate(MembershipUpdate {
                peers: {
                    let state = self.state.read().await;
                    state.peers().clone()
                },
            })))
            .await?;

        while let Some(result) = in_stream.next().await {
            match result {
                Ok(packet) => {
                    match packet.message {
                        Some(packet::Message::Ping(_)) => {
                            println!("got Ping");
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
                            println!("got MembershipUpdate {:?}", update);
                            // add any new peers to peer_trackers
                            let mut state = self.state.write().await;
                            for peer in update.peers {
                                let peer_id = PeerId(peer.peer_id.clone());
                                if !state.peer_trackers.contains_key(&peer_id) {
                                    state.peer_trackers.insert(
                                        peer_id.clone(),
                                        PeerTracker {
                                            peer,
                                            clock_skew: ClockSkew::None(),
                                            session: None,
                                        },
                                    );
                                }
                            }
                        }
                        Some(packet::Message::SubscribeRequest(request)) => {
                            println!("got SubscribeRequest {:?}", request);
                            // TODO: adjust which counter values are sent to the peer.
                        }
                        Some(packet::Message::CounterUpdate(update)) => {
                            println!("got CounterUpdate {:?}", update);
                            // TODO: update the counters
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
                    if server::is_disconnect(&err) {
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
