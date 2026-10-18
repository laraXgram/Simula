//! Groups and channels as the bot can see them (`messages.getFullChat`,
//! `channels.getFullChannel`, `channels.getParticipant(s)`), for the chat and member
//! methods.

use simula_botapi_types::types as bot;
use simula_core::{ChatId, Peer};
use simula_tl::{enums, functions, types};

use crate::convert::{Entities, member, user::user};
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};

pub struct Group {
    pub title: String,
    pub about: String,
    pub participants: Vec<enums::ChatParticipant>,
    /// The newest pinned message.
    pub pinned: Option<i32>,
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
        pinned: info.pinned_msg_id,
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

/// A supergroup or channel as the bot can see it (`channels.getFullChannel`).
pub struct Channel {
    pub info: types::Channel,
    pub full: types::ChannelFull,
}

pub async fn channel(ctx: &BotCtx, peer: Peer) -> BotResult<Channel> {
    let full = ctx
        .call(
            functions::channels::GetFullChannel {
                channel: ctx.input_channel(peer),
            },
            ErrorContext::Plain,
        )
        .await?;
    let enums::messages::ChatFull::ChatFull(full) = full;
    let enums::ChatFull::ChannelFull(info) = full.full_chat else {
        return Err(BotError::internal("not a channel"));
    };
    let channel = full
        .chats
        .iter()
        .find_map(|c| match c {
            enums::Chat::Channel(c) => Some((**c).clone()),
            _ => None,
        })
        .ok_or_else(|| BotError::internal("getFullChannel returned no channel"))?;
    Ok(Channel {
        info: channel,
        full: *info,
    })
}

/// One member's status in a supergroup or channel (`channels.getParticipant`); users
/// without one have "left".
pub async fn channel_member(
    ctx: &BotCtx,
    peer: Peer,
    broadcast: bool,
    user_id: i64,
) -> BotResult<bot::ChatMember> {
    let found = ctx
        .call(
            functions::channels::GetParticipant {
                channel: ctx.input_channel(peer),
                participant: ctx.input_peer(Peer::User(simula_core::UserId(user_id))),
            },
            ErrorContext::Plain,
        )
        .await;
    let left = || {
        bot::ChatMember::from(bot::ChatMemberLeft {
            user: bot::User {
                id: user_id,
                ..Default::default()
            },
            ..Default::default()
        })
    };
    let found = match found {
        Ok(enums::channels::ChannelParticipant::ChannelParticipant(found)) => found,
        Err(e) if e.description.ends_with("USER_NOT_PARTICIPANT") => return Ok(left()),
        Err(e) => return Err(e),
    };
    let entities = Entities {
        users: &found.users,
        chats: &found.chats,
        messages: &[],
    };
    let person = entities
        .user(user_id)
        .map(|u| user(u, false))
        .unwrap_or(bot::User {
            id: user_id,
            ..Default::default()
        });
    Ok(member::channel_status(
        Some(&found.participant),
        person,
        broadcast,
    ))
}

/// The admins of a supergroup or channel.
pub async fn channel_admins(
    ctx: &BotCtx,
    peer: Peer,
    broadcast: bool,
) -> BotResult<Vec<bot::ChatMember>> {
    let found = ctx
        .call(
            functions::channels::GetParticipants {
                channel: ctx.input_channel(peer),
                filter: enums::ChannelParticipantsFilter::ChannelParticipantsAdmins,
                offset: 0,
                limit: 200,
                hash: 0,
            },
            ErrorContext::Plain,
        )
        .await?;
    let enums::channels::ChannelParticipants::ChannelParticipants(found) = found else {
        return Ok(Vec::new());
    };
    let entities = Entities {
        users: &found.users,
        chats: &found.chats,
        messages: &[],
    };
    Ok(found
        .participants
        .iter()
        .map(|p| {
            let id = channel_user_id(p);
            let person = entities
                .user(id)
                .map(|u| user(u, false))
                .unwrap_or(bot::User {
                    id,
                    ..Default::default()
                });
            member::channel_status(Some(p), person, broadcast)
        })
        .collect())
}

pub fn channel_user_id(participant: &enums::ChannelParticipant) -> i64 {
    use enums::ChannelParticipant as P;
    match participant {
        P::ChannelParticipant(p) => p.user_id,
        P::ChannelParticipantSelf(p) => p.user_id,
        P::Creator(p) => p.user_id,
        P::Admin(p) => p.user_id,
        P::Banned(p) => match &p.peer {
            enums::Peer::User(u) => u.user_id,
            _ => 0,
        },
        P::Left(p) => match &p.peer {
            enums::Peer::User(u) => u.user_id,
            _ => 0,
        },
    }
}
