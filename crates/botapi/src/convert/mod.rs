//! TL objects (as the bot sees them) → Bot API objects. One file per type family.

pub mod callback;
pub mod chat;
pub mod entities;
pub mod link_preview;
pub mod markup;
pub mod member;
pub mod message;
pub mod permissions;
pub mod update;
pub mod user;

use simula_core::Peer;
use simula_tl::{enums, types};

/// What the bot knows of a supergroup or channel from the objects that came along.
#[derive(Debug, Clone, Copy)]
pub struct ChannelInfo<'a> {
    pub broadcast: bool,
    pub title: &'a str,
    pub username: Option<&'a str>,
}

/// What came with the TL objects being converted: users, and the messages they reply to
/// (fetched beforehand, as telegram-bot-api has tdlib fetch replied messages).
#[derive(Clone, Copy, Default)]
pub struct Entities<'a> {
    pub users: &'a [enums::User],
    pub chats: &'a [enums::Chat],
    pub messages: &'a [enums::Message],
}

impl<'a> Entities<'a> {
    pub fn user(&self, id: i64) -> Option<&'a types::User> {
        self.users.iter().find_map(|u| match u {
            enums::User::User(u) if u.id == id => Some(&**u),
            _ => None,
        })
    }

    /// A basic group's title, if it came along.
    pub fn chat_title(&self, id: i64) -> Option<&'a str> {
        self.chats.iter().find_map(|c| match c {
            enums::Chat::Chat(c) if c.id == id => Some(c.title.as_str()),
            enums::Chat::Forbidden(c) if c.id == id => Some(c.title.as_str()),
            _ => None,
        })
    }

    /// A supergroup or channel, if it came along.
    pub fn channel(&self, id: i64) -> Option<ChannelInfo<'a>> {
        self.chats.iter().find_map(|c| match c {
            enums::Chat::Channel(c) if c.id == id => Some(ChannelInfo {
                broadcast: c.broadcast,
                title: &c.title,
                username: c.username.as_deref(),
            }),
            enums::Chat::ChannelForbidden(c) if c.id == id => Some(ChannelInfo {
                broadcast: c.broadcast,
                title: &c.title,
                username: None,
            }),
            _ => None,
        })
    }

    /// A fetched message of chat `peer`, by id.
    pub fn message(&self, peer: Peer, id: i32) -> Option<&'a types::Message> {
        self.messages.iter().find_map(|m| match m {
            enums::Message::Message(m) if m.id == id && Peer::from_tl(&m.peer_id) == peer => {
                Some(&**m)
            }
            _ => None,
        })
    }
}
