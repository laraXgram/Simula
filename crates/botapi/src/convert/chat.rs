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
        Peer::Chat(id) => bot::Chat {
            id: peer.bot_api_id(),
            r#type: "group".into(),
            title: entities.chat_title(id.0).map(str::to_owned),
            ..Default::default()
        },
        // Supergroups and channels come with the next M4 slice.
        Peer::Channel(_) => bot::Chat {
            id: peer.bot_api_id(),
            r#type: "supergroup".into(),
            ..Default::default()
        },
    }
}
