//! TL peer (+ its user, chat or channel object) → Bot API `Chat`.

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
        Peer::Channel(id) => {
            let info = entities.channel(id.0);
            bot::Chat {
                id: peer.bot_api_id(),
                r#type: if info.is_some_and(|c| c.broadcast) {
                    "channel"
                } else {
                    "supergroup"
                }
                .into(),
                title: info.map(|c| c.title.to_owned()),
                username: info.and_then(|c| c.username.map(str::to_owned)),
                is_forum: info.is_some_and(|c| c.forum && !c.broadcast),
                ..Default::default()
            }
        }
    }
}
