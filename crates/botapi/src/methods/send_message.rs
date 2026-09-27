//! `sendMessage`

use simula_botapi_types::{methods, types as bot};
use simula_core::Peer;
use simula_tl::{enums, functions};

use crate::convert::{self, Entities};
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::resolve;

pub async fn handle(ctx: BotCtx, params: methods::SendMessage) -> BotResult<bot::Message> {
    // telegram-bot-api's own checks come first, then tdlib's.
    if params.text.is_empty() {
        return Err(BotError::bad_request("message text is empty"));
    }
    if params.parse_mode.is_some() || params.entities.is_some() || params.reply_markup.is_some() {
        return Err(BotError::not_implemented(
            "sendMessage with formatting or keyboards (M2)",
        ));
    }
    let peer = resolve::chat(&ctx, &params.chat_id).await?;
    if params.text.trim().is_empty() {
        return Err(BotError::bad_request("text must be non-empty"));
    }

    let result = ctx
        .call(
            functions::messages::SendMessage {
                peer: ctx.input_peer(peer),
                message: params.text,
                random_id: rand::random(),
                silent: params.disable_notification.unwrap_or(false),
                ..Default::default()
            },
            ErrorContext {
                private_chat: matches!(peer, Peer::User(_)),
            },
        )
        .await?;
    sent_message(&result)
        .ok_or_else(|| BotError::internal("messages.sendMessage returned no message"))
}

/// The bot's copy of the message it just sent, from the call's `Updates`.
pub fn sent_message(updates: &enums::Updates) -> Option<bot::Message> {
    let enums::Updates::Updates(batch) = updates else {
        return None;
    };
    let entities = Entities {
        users: &batch.users,
    };
    batch.updates.iter().find_map(|update| match update {
        enums::Update::NewMessage(u) => match &u.message {
            enums::Message::Message(m) => Some(convert::message::message(m, &entities)),
            _ => None,
        },
        _ => None,
    })
}
