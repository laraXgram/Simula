//! TL updates (rendered for the bot) → Bot API `Update` payloads, without `update_id`
//! (the queue assigns it).

use simula_botapi_types::types as bot;
use simula_tl::enums;

use super::{Entities, message::message};

/// Bot API update kinds this TL update produces. A bot's own outgoing messages produce
/// nothing: the Bot API never echoes them back.
pub fn updates(updates: &enums::Updates) -> Vec<bot::Update> {
    let enums::Updates::Updates(batch) = updates else {
        return Vec::new();
    };
    let entities = Entities {
        users: &batch.users,
    };
    batch
        .updates
        .iter()
        .filter_map(|update| match update {
            enums::Update::NewMessage(u) => match &u.message {
                enums::Message::Message(m) if !m.out => Some(bot::Update {
                    message: Some(Box::new(message(m, &entities))),
                    ..Default::default()
                }),
                _ => None,
            },
            _ => None,
        })
        .collect()
}
