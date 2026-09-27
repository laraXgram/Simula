//! TL `updateBotCallbackQuery` → Bot API `CallbackQuery` (telegram-bot-api's
//! `JsonCallbackQuery`). A message the bot can't fetch becomes an `InaccessibleMessage`.

use simula_botapi_types::types as bot;
use simula_core::Peer;
use simula_tl::types;

use super::{Entities, chat::chat, message::message, user::user};

pub fn query(q: &types::UpdateBotCallbackQuery, context: &Entities<'_>) -> bot::CallbackQuery {
    let chat_peer = Peer::from_tl(&q.peer);
    let found = context
        .message(q.msg_id)
        .filter(|m| Peer::from_tl(&m.peer_id) == chat_peer);
    let message = match found {
        Some(m) => bot::MaybeInaccessibleMessage::Message(Box::new(message(m, context))),
        None => {
            bot::MaybeInaccessibleMessage::InaccessibleMessage(Box::new(bot::InaccessibleMessage {
                chat: chat(chat_peer, context),
                message_id: q.msg_id.into(),
                date: Default::default(),
            }))
        }
    };
    bot::CallbackQuery {
        id: q.query_id.to_string(),
        from: context
            .user(q.user_id)
            .map(|u| user(u, false))
            .unwrap_or_else(|| bot::User {
                id: q.user_id,
                ..Default::default()
            }),
        message: Some(Box::new(message)),
        chat_instance: q.chat_instance.to_string(),
        data: q
            .data
            .as_ref()
            .map(|data| String::from_utf8_lossy(data).into_owned()),
        game_short_name: q.game_short_name.clone(),
        ..Default::default()
    }
}
