//! A basic group as the bot can see it (`messages.getFullChat`), for the chat and member
//! methods.

use simula_botapi_types::types as bot;
use simula_core::ChatId;
use simula_tl::{enums, functions};

use crate::convert::{Entities, member, user::user};
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};

pub struct Group {
    pub title: String,
    pub about: String,
    pub participants: Vec<enums::ChatParticipant>,
    pub users: Vec<enums::User>,
    pub chats: Vec<enums::Chat>,
}

pub async fn group(ctx: &BotCtx, chat: ChatId) -> BotResult<Group> {
    let full = ctx
        .call(
            functions::messages::GetFullChat { chat_id: chat.0 },
            ErrorContext::Plain,
        )
        .await?;
    let enums::messages::ChatFull::ChatFull(full) = full;
    let enums::ChatFull::ChatFull(info) = full.full_chat else {
        return Err(BotError::internal("not a basic group"));
    };
    let participants = match info.participants {
        enums::ChatParticipants::ChatParticipants(p) => p.participants,
        enums::ChatParticipants::Forbidden(_) => Vec::new(),
    };
    let title = full
        .chats
        .iter()
        .find_map(|c| match c {
            enums::Chat::Chat(c) => Some(c.title.clone()),
            _ => None,
        })
        .unwrap_or_default();
    Ok(Group {
        title,
        about: info.about,
        participants,
        users: full.users,
        chats: full.chats,
    })
}

impl Group {
    pub fn entities(&self) -> Entities<'_> {
        Entities {
            users: &self.users,
            chats: &self.chats,
            messages: &[],
        }
    }

    /// A member's status; anyone else has "left".
    pub fn member(&self, user_id: i64) -> bot::ChatMember {
        let participant = self.participants.iter().find(|p| user_id_of(p) == user_id);
        member::status(participant, self.user(user_id))
    }

    pub fn user(&self, user_id: i64) -> bot::User {
        self.entities()
            .user(user_id)
            .map(|u| user(u, false))
            .unwrap_or(bot::User {
                id: user_id,
                ..Default::default()
            })
    }
}

pub fn user_id_of(participant: &enums::ChatParticipant) -> i64 {
    match participant {
        enums::ChatParticipant::ChatParticipant(p) => p.user_id,
        enums::ChatParticipant::Creator(p) => p.user_id,
        enums::ChatParticipant::Admin(p) => p.user_id,
    }
}
