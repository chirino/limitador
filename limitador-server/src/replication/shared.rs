use std::collections::HashMap;

use base64::{display::Base64Display, engine::general_purpose::STANDARD};

use crate::clock_skew::ClockSkew;
use crate::limitador::service::replication::v1::Peer;
use crate::session::Session;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PeerId(pub Vec<u8>);

impl PeerId {
    pub fn vec(&self) -> Vec<u8> {
        self.0.clone()
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return Base64Display::new(&self.0.as_slice(), &STANDARD).fmt(f);
    }
}


pub struct PeerTracker {
    // The peer we are tracking
    pub peer: Peer,
    // Keep track of the clock skew between us and the peer
    pub clock_skew: ClockSkew,
    // The communication session we have with the peer, may be None if not connected
    pub session: Option<Session>,
}

// ReplicationSharedState holds all the mutable shared state of the server.
pub struct ReplicationSharedState {
    pub peer_id: PeerId,
    pub urls: Vec<String>,
    pub peer_trackers: HashMap<PeerId, PeerTracker>,
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
