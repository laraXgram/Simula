//! A chat's pinned message as the bot sees it: the newest one, from the chat's full
//! object (`pinned_msg_id`); unpinning everything (in a chat or a forum topic).

use simula_core::Peer;
use simula_tl::{enums, functions};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::members;

pub async fn pinned_id(ctx: &BotCtx, chat: Peer) -> BotResult<Option<i32>> {
    Ok(match chat {
        Peer::User(user) => {
            let full = ctx
                .call(
                    functions::users::GetFullUser {
                        id: ctx.input_user(user.0),
                    },
                    ErrorContext::Plain,
                )
                .await?;
            let enums::users::UserFull::UserFull(full) = full;
            let enums::UserFull::UserFull(info) = full.full_user;
            info.pinned_msg_id
        }
        Peer::Chat(chat) => members::group(ctx, chat).await?.pinned,
        Peer::Channel(_) => members::channel(ctx, chat).await?.full.pinned_msg_id,
    })
}

/// Unpin everything in the chat, or in one forum topic (`messages.unpinAllMessages`).
pub async fn unpin_all(ctx: &BotCtx, chat: Peer, topic: Option<i32>) -> BotResult<()> {
    ctx.call(
        functions::messages::UnpinAllMessages {
            peer: ctx.input_peer(chat),
            top_msg_id: topic,
            ..Default::default()
        },
        ErrorContext::Plain,
    )
    .await
    .map_err(rights_error)?;
    Ok(())
}

/// tdlib checks the right itself: "Not enough rights to manage pinned messages".
fn rights_error(error: BotError) -> BotError {
    if error.description.ends_with("CHAT_ADMIN_REQUIRED") {
        BotError::bad_request("not enough rights to manage pinned messages in the chat")
    } else {
        error
    }
}
