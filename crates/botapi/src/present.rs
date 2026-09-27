//! TL messages → Bot API messages, with what they refer to. telegram-bot-api has tdlib
//! fetch a replied message before it emits an update; the gateway does the same, calling
//! `messages.getMessages` as the bot.

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
}

/// Fetch what `messages` (in `bot`'s box) refer to. `users` came with them.
pub async fn context(
    gateway: &Gateway,
    bot: UserId,
    messages: &[&types::Message],
    users: &[enums::User],
) -> Context {
    let mut context = Context {
        users: users.to_vec(),
        messages: Vec::new(),
    };
    let mut ids: Vec<i32> = messages
        .iter()
        .filter_map(|m| match &m.reply_to {
            Some(enums::MessageReplyHeader::MessageReplyHeader(h))
                if h.reply_to_peer_id.is_none() =>
            {
                h.reply_to_msg_id
            }
            _ => None,
        })
        .collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return context;
    }

    let caller = Caller {
        account: bot,
        session: Some(gateway.session),
    };
    let call = functions::messages::GetMessages {
        id: ids
            .into_iter()
            .map(|id| types::InputMessageID { id }.into())
            .collect(),
    };
    match gateway.sandbox.call(caller, call).await {
        Ok(enums::messages::Messages::Messages(found)) => {
            context.messages = found.messages;
            for user in found.users {
                if !context.users.iter().any(|u| u.id() == user.id()) {
                    context.users.push(user);
                }
            }
        }
        Ok(_) => {}
        // The update is still delivered, just without `reply_to_message`.
        Err(error) => tracing::warn!(%error, "fetching replied messages failed"),
    }
    context
}

/// One message as the bot sees it, fetching its replied message first.
pub async fn message(
    gateway: &Gateway,
    bot: UserId,
    m: &types::Message,
    users: &[enums::User],
) -> bot::Message {
    let context = context(gateway, bot, &[m], users).await;
    convert::message::message(m, &context.entities())
}
