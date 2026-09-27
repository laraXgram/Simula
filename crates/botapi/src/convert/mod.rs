//! TL objects (as the bot sees them) → Bot API objects. One file per type family.

pub mod chat;
pub mod entities;
pub mod message;
pub mod update;
pub mod user;

use simula_tl::{enums, types};

/// The users and chats that came with an `Updates`, for resolving peers.
pub struct Entities<'a> {
    pub users: &'a [enums::User],
}

impl<'a> Entities<'a> {
    pub fn user(&self, id: i64) -> Option<&'a types::User> {
        self.users.iter().find_map(|u| match u {
            enums::User::User(u) if u.id == id => Some(&**u),
            _ => None,
        })
    }
}
