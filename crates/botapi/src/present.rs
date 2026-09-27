//! TL messages → Bot API messages, with what they refer to. telegram-bot-api has tdlib
//! fetch messages before it emits an update (the replied message, a callback query's
//! message); the gateway does the same, calling `messages.getMessages` as the bot.

use simula_botapi_types::types as bot;
use simula_core::{Caller, UserId};
use simula_tl::{enums, functions, types};

use crate::convert::{self, Entities};
use crate::state::Gateway;

/// Users and fetched messages that conversions look things up in.
#[derive(Default)]
pub struct Context {
    pub users: Vec<enums::User>,
    pub messages: Vec<enums::Message>,
}

impl Context {
    pub fn entities(&self) -> Entities<'_> {
        Entities {
            users: &self.users,
            messages: &self.messages,
        }
    }

    fn add(&mut self, found: types::messages::Messages) {
        self.messages.extend(found.messages);
        for user in found.users {
            if !self.users.iter().any(|u| u.id() == user.id()) {
                self.users.push(user);
            }
        }
    }
}

/// Fetch messages `ids` from `bot`'s box, and the messages those reply to. `users`
/// came with the update being presented.
pub async fn context(
    gateway: &Gateway,
    bot: UserId,
    ids: Vec<i32>,
    users: &[enums::User],
) -> Context {
    let mut context = Context {
        users: users.to_vec(),
        messages: Vec::new(),
    };
    let mut wanted = ids;
    // Two rounds: the messages, then what they reply to (never deeper, like
    // telegram-bot-api's `need_reply`).
    for _ in 0..2 {
        wanted.sort_unstable();
        wanted.dedup();
        wanted.retain(|&id| context.entities().message(id).is_none());
        if wanted.is_empty() {
            break;
        }
        let Some(found) = fetch(gateway, bot, &wanted).await else {
            break;
        };
        wanted = found
            .messages
            .iter()
            .filter_map(|m| match m {
                enums::Message::Message(m) => convert::message::reply_id(m),
                _ => None,
            })
            .collect();
        context.add(found);
    }
    context
}

async fn fetch(gateway: &Gateway, bot: UserId, ids: &[i32]) -> Option<types::messages::Messages> {
    let caller = Caller {
        account: bot,
        session: Some(gateway.session),
    };
    let call = functions::messages::GetMessages {
        id: ids
            .iter()
            .map(|&id| types::InputMessageID { id }.into())
            .collect(),
    };
    match gateway.sandbox.call(caller, call).await {
        Ok(enums::messages::Messages::Messages(found)) => Some(*found),
        Ok(_) => None,
        // The update is still delivered, just without the fetched parts.
        Err(error) => {
            tracing::warn!(%error, "fetching messages for an update failed");
            None
        }
    }
}

/// One message as the bot sees it, with its replied message.
pub async fn message(
    gateway: &Gateway,
    bot: UserId,
    m: &types::Message,
    users: &[enums::User],
) -> bot::Message {
    let ids = convert::message::reply_id(m).into_iter().collect();
    let context = context(gateway, bot, ids, users).await;
    convert::message::message(m, &context.entities())
}
