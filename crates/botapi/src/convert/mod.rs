//! TL objects (as the bot sees them) → Bot API objects. One file per type family.

pub mod callback;
pub mod chat;
pub mod entities;
pub mod link_preview;
pub mod markup;
pub mod member;
pub mod message;
pub mod update;
pub mod user;

use simula_tl::{enums, types};

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

    /// A message from the bot's box, by id.
    pub fn message(&self, id: i32) -> Option<&'a types::Message> {
        self.messages.iter().find_map(|m| match m {
            enums::Message::Message(m) if m.id == id => Some(&**m),
            _ => None,
        })
    }
}
