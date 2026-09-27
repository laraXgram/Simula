//! TL updates (rendered for the bot) → Bot API `Update` payloads, without `update_id`
//! (the queue assigns it).

use simula_botapi_types::types as bot;
use simula_tl::{enums, types};

use super::{Entities, message::message};

/// Bot API update kinds this TL update produces. A bot's own outgoing messages produce
/// nothing: the Bot API never echoes them back. Deletions in private chats aren't
/// reported to bots either.
pub fn updates(updates: &enums::Updates, context: &Entities<'_>) -> Vec<bot::Update> {
    incoming(updates)
        .into_iter()
        .map(|(m, edited)| {
            let m = Some(Box::new(message(m, context)));
            if edited {
                bot::Update {
                    edited_message: m,
                    ..Default::default()
                }
            } else {
                bot::Update {
                    message: m,
                    ..Default::default()
                }
            }
        })
        .collect()
}

/// The messages [`updates`] will present, so their context can be fetched first.
pub fn messages(updates: &enums::Updates) -> Vec<&types::Message> {
    incoming(updates).into_iter().map(|(m, _)| m).collect()
}

/// New and edited incoming messages, with whether each was an edit.
fn incoming(updates: &enums::Updates) -> Vec<(&types::Message, bool)> {
    let enums::Updates::Updates(batch) = updates else {
        return Vec::new();
    };
    batch
        .updates
        .iter()
        .filter_map(|update| {
            let (message, edited) = match update {
                enums::Update::NewMessage(u) => (&u.message, false),
                enums::Update::EditMessage(u) => (&u.message, true),
                _ => return None,
            };
            match message {
                enums::Message::Message(m) if !m.out => Some((&**m, edited)),
                _ => None,
            }
        })
        .collect()
}
