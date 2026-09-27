//! TL `message` → Bot API `Message`.

use simula_botapi_types::types as bot;
use simula_core::Peer;
use simula_tl::types;

use super::{Entities, chat::chat, entities::entities, user::user};

pub fn message(m: &types::Message, entities_: &Entities<'_>) -> bot::Message {
    let from = m
        .from_id
        .as_ref()
        .map(Peer::from_tl)
        .and_then(|peer| match peer {
            Peer::User(id) => entities_.user(id.0).map(|u| Box::new(user(u, false))),
            _ => None,
        });
    bot::Message {
        message_id: m.id.into(),
        from,
        date: m.date.into(),
        chat: chat(Peer::from_tl(&m.peer_id), entities_),
        edit_date: m.edit_date.map(Into::into),
        text: (!m.message.is_empty() && m.media.is_none()).then(|| m.message.clone()),
        entities: entities(m.entities.as_ref(), entities_),
        ..Default::default()
    }
}
