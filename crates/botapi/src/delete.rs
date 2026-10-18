//! What `deleteMessage` and `deleteMessages` share: tdlib's checks before it deletes for
//! everyone (bots always do), then `messages.deleteMessages` with `revoke`, or
//! `channels.deleteMessages` in supergroups and channels.

use simula_core::Peer;
use simula_tl::{functions, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};

/// tdlib lets bots delete for everyone only within 48 hours.
const BOT_REVOKE_TIME_LIMIT: i32 = 2 * 86400;

pub async fn delete(ctx: &BotCtx, messages: &[types::Message]) -> BotResult<bool> {
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
    let id = messages.iter().map(|m| m.id).collect();
    match Peer::from_tl(&messages[0].peer_id) {
        chat @ Peer::Channel(_) => {
            ctx.call(
                functions::channels::DeleteMessages {
                    channel: ctx.input_channel(chat),
                    id,
                },
                ErrorContext::Plain,
            )
            .await?;
        }
        _ => {
            ctx.call(
                functions::messages::DeleteMessages { revoke: true, id },
                ErrorContext::Plain,
            )
            .await?;
        }
    }
    Ok(true)
}
