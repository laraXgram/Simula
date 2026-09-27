//! TL updates (rendered for the bot) → Bot API `Update` payloads, without `update_id`
//! (the queue assigns it).

use simula_botapi_types::types as bot;
use simula_tl::{enums, types};

use super::{Entities, message::message};

/// Bot API update kinds this TL update produces. A bot's own outgoing messages produce
/// nothing: the Bot API never echoes them back.
pub fn updates(updates: &enums::Updates, context: &Entities<'_>) -> Vec<bot::Update> {
    messages(updates)
        .into_iter()
        .map(|m| bot::Update {
            message: Some(Box::new(message(m, context))),
            ..Default::default()
        })
        .collect()
}

/// The messages [`updates`] will present, so their context can be fetched first.
pub fn messages(updates: &enums::Updates) -> Vec<&types::Message> {
    let enums::Updates::Updates(batch) = updates else {
        return Vec::new();
    };
    batch
        .updates
        .iter()
        .filter_map(|update| match update {
            enums::Update::NewMessage(u) => match &u.message {
                enums::Message::Message(m) if !m.out => Some(&**m),
                _ => None,
            },
            _ => None,
        })
        .collect()
}
