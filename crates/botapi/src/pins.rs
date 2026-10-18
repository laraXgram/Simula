//! A chat's pinned message as the bot sees it: the newest one, from the chat's full
//! object (`pinned_msg_id`).

use simula_core::Peer;
use simula_tl::{enums, functions};

use crate::ctx::BotCtx;
use crate::error::{BotResult, ErrorContext};
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
