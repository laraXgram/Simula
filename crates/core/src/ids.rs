//! Peer identifiers, their Bot API encoding, and per-viewer access hashes.

use sha2::{Digest, Sha256};
use simula_tl::{enums, types};

/// A user or bot account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct UserId(pub i64);

/// A basic (legacy) group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct ChatId(pub i64);

/// A supergroup, channel or forum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct ChannelId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Peer {
    User(UserId),
    Chat(ChatId),
    Channel(ChannelId),
}

/// Bot API channel ids are `-(10^12 + channel_id)`, printed as `-100…`.
const CHANNEL_OFFSET: i64 = 1_000_000_000_000;

impl Peer {
    /// Bot API chat id: users as-is, basic groups `-id`, channels `-100…`.
    /// Also the encoding used in storage.
    pub fn bot_api_id(self) -> i64 {
        match self {
            Self::User(UserId(id)) => id,
            Self::Chat(ChatId(id)) => -id,
            Self::Channel(ChannelId(id)) => -(CHANNEL_OFFSET + id),
        }
    }

    pub fn from_bot_api_id(id: i64) -> Option<Self> {
        match id {
            0 => None,
            id if id > 0 => Some(Self::User(UserId(id))),
            id if id < -CHANNEL_OFFSET => Some(Self::Channel(ChannelId(-id - CHANNEL_OFFSET))),
            id => Some(Self::Chat(ChatId(-id))),
        }
    }

    pub fn to_tl(self) -> enums::Peer {
        match self {
            Self::User(UserId(user_id)) => types::PeerUser { user_id }.into(),
            Self::Chat(ChatId(chat_id)) => types::PeerChat { chat_id }.into(),
            Self::Channel(ChannelId(channel_id)) => types::PeerChannel { channel_id }.into(),
        }
    }

    pub fn from_tl(peer: &enums::Peer) -> Self {
        match peer {
            enums::Peer::User(p) => Self::User(UserId(p.user_id)),
            enums::Peer::Chat(p) => Self::Chat(ChatId(p.chat_id)),
            enums::Peer::Channel(p) => Self::Channel(ChannelId(p.channel_id)),
        }
    }
}

impl From<UserId> for Peer {
    fn from(id: UserId) -> Self {
        Self::User(id)
    }
}

/// Access hashes differ per (viewer, target) pair, as in Telegram: a hash one account
/// learned is useless to another. Derived from the sandbox secret, so nothing is stored.
pub fn access_hash(secret: &[u8; 32], viewer: UserId, target: Peer) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(secret);
    hasher.update(viewer.0.to_le_bytes());
    hasher.update(target.bot_api_id().to_le_bytes());
    let digest = hasher.finalize();
    i64::from_le_bytes(digest[..8].try_into().expect("sha256 is 32 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bot_api_ids_round_trip() {
        for peer in [
            Peer::User(UserId(1_000_000_001)),
            Peer::Chat(ChatId(4_000_000_001)),
            Peer::Channel(ChannelId(1_234_567_890)),
        ] {
            assert_eq!(Peer::from_bot_api_id(peer.bot_api_id()), Some(peer));
        }
        assert_eq!(Peer::Channel(ChannelId(1_234_567_890)).bot_api_id(), -1_001_234_567_890);
    }

    #[test]
    fn access_hashes_are_per_viewer() {
        let secret = [7; 32];
        let target = Peer::User(UserId(5));
        assert_ne!(access_hash(&secret, UserId(1), target), access_hash(&secret, UserId(2), target));
        assert_eq!(access_hash(&secret, UserId(1), target), access_hash(&secret, UserId(1), target));
    }
}
