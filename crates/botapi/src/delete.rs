//! What `deleteMessage` and `deleteMessages` share: tdlib's checks before it deletes for
//! everyone (bots always do), then `messages.deleteMessages` with `revoke`.

use simula_core::Peer;
use simula_tl::{functions, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};

/// tdlib lets bots delete for everyone only within 48 hours.
const BOT_REVOKE_TIME_LIMIT: i32 = 2 * 86400;

pub async fn delete(ctx: &BotCtx, chat: Peer, messages: &[types::Message]) -> BotResult<bool> {
    let now = ctx.gateway.sandbox.clock().now();
    if messages
        .iter()
        .any(|m| now - m.date > BOT_REVOKE_TIME_LIMIT)
    {
        return Err(BotError::bad_request(
            "message can't be deleted for everyone",
        ));
    }
    if messages.is_empty() {
        return Ok(true);
    }
    ctx.call(
        functions::messages::DeleteMessages {
            revoke: true,
            id: messages.iter().map(|m| m.id).collect(),
        },
        ErrorContext {
            private_chat: matches!(chat, Peer::User(_)),
        },
    )
    .await?;
    Ok(true)
}
