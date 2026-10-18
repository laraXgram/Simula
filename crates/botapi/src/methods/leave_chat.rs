//! `leaveChat`

use simula_botapi_types::methods;
use simula_core::Peer;
use simula_tl::{enums, functions};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::resolve;

pub async fn handle(ctx: BotCtx, params: methods::LeaveChat) -> BotResult<bool> {
    match resolve::chat(&ctx, &params.chat_id).await? {
        Peer::User(_) => Err(BotError::bad_request("can't leave private chats")),
        Peer::Chat(chat) => {
            ctx.call(
                functions::messages::DeleteChatUser {
                    revoke_history: false,
                    chat_id: chat.0,
                    user_id: enums::InputUser::InputUserSelf,
                },
                ErrorContext::Plain,
            )
            .await?;
            Ok(true)
        }
        peer @ Peer::Channel(_) => {
            ctx.call(
                functions::channels::LeaveChannel {
                    channel: ctx.input_channel(peer),
                },
                ErrorContext::Plain,
            )
            .await?;
            Ok(true)
        }
    }
}
