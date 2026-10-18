//! TL messages → Bot API messages, with what they refer to. telegram-bot-api has tdlib
//! fetch messages before it emits an update (the replied message, a callback query's
//! message); the gateway does the same, calling `messages.getMessages` (or
//! `channels.getMessages`) as the bot.

use simula_botapi_types::types as bot;
use simula_core::{Caller, Peer, UserId};
use simula_tl::{enums, functions, types};

use crate::convert::{self, Entities};
use crate::state::Gateway;

/// Users and fetched messages that conversions look things up in.
#[derive(Default)]
pub struct Context {
    pub users: Vec<enums::User>,
    pub chats: Vec<enums::Chat>,
    pub messages: Vec<enums::Message>,
}

impl Context {
    pub fn entities(&self) -> Entities<'_> {
        Entities {
            users: &self.users,
            chats: &self.chats,
            messages: &self.messages,
        }
    }

    fn add(&mut self, found: types::messages::Messages) {
        self.messages.extend(found.messages);
        for chat in found.chats {
            if !self.chats.iter().any(|c| c.id() == chat.id()) {
                self.chats.push(chat);
            }
        }
        for user in found.users {
            if !self.users.iter().any(|u| u.id() == user.id()) {
                self.users.push(user);
            }
        }
    }
}

/// Fetch messages `ids` (by chat) as `bot` sees them, and the messages those reply to.
/// `users` and `chats` came with the update being presented.
pub async fn context(
    gateway: &Gateway,
    bot: UserId,
    ids: Vec<(Peer, i32)>,
    users: &[enums::User],
    chats: &[enums::Chat],
) -> Context {
    let mut context = Context {
        users: users.to_vec(),
        chats: chats.to_vec(),
        messages: Vec::new(),
    };
    let mut wanted = ids;
    // Two rounds: the messages, then what they reply to (never deeper, like
    // telegram-bot-api's `need_reply`).
    for _ in 0..2 {
        wanted.sort_unstable();
        wanted.dedup();
        wanted.retain(|&(peer, id)| context.entities().message(peer, id).is_none());
        if wanted.is_empty() {
            break;
        }
        let mut next = Vec::new();
        let mut chats: Vec<Peer> = wanted.iter().map(|&(peer, _)| peer).collect();
        chats.dedup();
        for chat in chats {
            let ids: Vec<i32> = wanted
                .iter()
                .filter(|&&(peer, _)| peer == chat)
                .map(|&(_, id)| id)
                .collect();
            let Some(found) = fetch(gateway, bot, chat, &ids).await else {
                continue;
            };
            next.extend(found.messages.iter().filter_map(|m| match m {
                enums::Message::Message(m) => convert::message::reply_ref(m),
                _ => None,
            }));
            context.add(found);
        }
        wanted = next;
    }
    context
}

/// Messages of one chat: from the bot's own box, or the channel's.
async fn fetch(
    gateway: &Gateway,
    bot: UserId,
    chat: Peer,
    ids: &[i32],
) -> Option<types::messages::Messages> {
    let caller = Caller {
        account: bot,
        session: Some(gateway.session),
    };
    let id = ids
        .iter()
        .map(|&id| types::InputMessageID { id }.into())
        .collect();
    let result = match gateway.sandbox.input_peer(bot, chat) {
        enums::InputPeer::Channel(channel) => {
            let call = functions::channels::GetMessages {
                channel: types::InputChannel {
                    channel_id: channel.channel_id,
                    access_hash: channel.access_hash,
                }
                .into(),
                id,
            };
            gateway.sandbox.call(caller, call).await
        }
        _ => {
            gateway
                .sandbox
                .call(caller, functions::messages::GetMessages { id })
                .await
        }
    };
    match result {
        Ok(found) => Some(into_messages(found)),
        // The update is still delivered, just without the fetched parts.
        Err(error) => {
            tracing::warn!(%error, "fetching messages for an update failed");
            None
        }
    }
}

/// The messages, chats and users of any `messages.Messages` shape.
pub fn into_messages(found: enums::messages::Messages) -> types::messages::Messages {
    match found {
        enums::messages::Messages::Messages(m) => *m,
        enums::messages::Messages::Slice(m) => types::messages::Messages {
            messages: m.messages,
            topics: m.topics,
            chats: m.chats,
            users: m.users,
        },
        enums::messages::Messages::ChannelMessages(m) => types::messages::Messages {
            messages: m.messages,
            topics: m.topics,
            chats: m.chats,
            users: m.users,
        },
        enums::messages::Messages::NotModified(_) => types::messages::Messages::default(),
    }
}

/// One message as the bot sees it, with its replied message.
pub async fn message(
    gateway: &Gateway,
    bot: UserId,
    m: &types::Message,
    users: &[enums::User],
    chats: &[enums::Chat],
) -> bot::Message {
    let ids = convert::message::reply_ref(m).into_iter().collect();
    let context = context(gateway, bot, ids, users, chats).await;
    convert::message::message(m, &context.entities())
}
