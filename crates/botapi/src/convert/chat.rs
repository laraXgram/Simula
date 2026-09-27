//! TL peer (+ its user or chat object) → Bot API `Chat`.

use simula_botapi_types::types as bot;
use simula_core::Peer;

use super::Entities;

pub fn chat(peer: Peer, entities: &Entities<'_>) -> bot::Chat {
    match peer {
        Peer::User(id) => {
            let user = entities.user(id.0);
            bot::Chat {
                id: id.0,
                r#type: "private".into(),
                first_name: user.and_then(|u| u.first_name.clone()),
                last_name: user.and_then(|u| u.last_name.clone()),
                username: user.and_then(|u| u.username.clone()),
                ..Default::default()
            }
        }
        // Groups and channels arrive with M4.
        other => bot::Chat {
            id: other.bot_api_id(),
            r#type: "group".into(),
            ..Default::default()
        },
    }
}
