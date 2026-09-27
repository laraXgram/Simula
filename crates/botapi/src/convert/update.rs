//! TL updates (rendered for the bot) → Bot API `Update` payloads, without `update_id`
//! (the queue assigns it).

use simula_botapi_types::types as bot;
use simula_tl::{enums, types};

use super::{Entities, callback, message};

/// Bot API updates for a TL batch. A bot's own outgoing messages produce nothing (the
/// Bot API never echoes them back); deletions in private chats aren't reported either.
pub fn updates(updates: &enums::Updates, context: &Entities<'_>) -> Vec<bot::Update> {
    let enums::Updates::Updates(batch) = updates else {
        return Vec::new();
    };
    batch
        .updates
        .iter()
        .filter_map(|update| {
            Some(match update {
                enums::Update::NewMessage(u) => bot::Update {
                    message: Some(Box::new(message::message(incoming(&u.message)?, context))),
                    ..Default::default()
                },
                enums::Update::EditMessage(u) => bot::Update {
                    edited_message: Some(Box::new(message::message(
                        incoming(&u.message)?,
                        context,
                    ))),
                    ..Default::default()
                },
                enums::Update::BotCallbackQuery(q) => bot::Update {
                    callback_query: Some(Box::new(callback::query(q, context))),
                    ..Default::default()
                },
                _ => return None,
            })
        })
        .collect()
}

/// Messages [`updates`] needs from the bot's box: what incoming messages reply to, and
/// the messages of callback queries.
pub fn wanted(updates: &enums::Updates) -> Vec<i32> {
    let enums::Updates::Updates(batch) = updates else {
        return Vec::new();
    };
    batch
        .updates
        .iter()
        .filter_map(|update| match update {
            enums::Update::NewMessage(u) => incoming(&u.message).and_then(message::reply_id),
            enums::Update::EditMessage(u) => incoming(&u.message).and_then(message::reply_id),
            enums::Update::BotCallbackQuery(q) => Some(q.msg_id),
            _ => None,
        })
        .collect()
}

fn incoming(message: &enums::Message) -> Option<&types::Message> {
    match message {
        enums::Message::Message(m) if !m.out => Some(m),
        _ => None,
    }
}
